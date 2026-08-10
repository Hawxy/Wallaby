using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Client;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Suspend/resume end to end: suspension drops every managed slot (the state an RDS/Aurora major-version
/// upgrade precheck requires) and survives restarts; resume recreates the slot and recovers changes
/// committed while suspended via slot-loss gap detection's full re-backfill.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SuspendResumeTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task Client_suspend_drops_all_slots_survives_restart_and_resume_rebackfills()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var extSlot = names.Named("elt_slot");
        var extPub = names.Named("elt_pub");
        names.TrackExternal(extSlot, extPub);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            // Node 1: stream one product, then suspend remotely.
            var firstCapture = new CaptureSink();
            int categoryId, firstId;
            await using (var node = await WallabyTestNode.StartAsync(
                BuildServices(names, firstCapture, external: (extSlot, extPub))))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                categoryId = await Db.AddCategoryAsync();
                firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
                await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);

                var state = await client.SuspendAsync(new WallabySuspendOptions
                {
                    Reason = "PG major-version upgrade",
                    Timeout = TimeSpan.FromSeconds(60),
                });

                // The primary AND the external slot are gone — the state the upgrade precheck requires.
                state.State.ShouldBe(WallabySuspensionState.Suspended);
                state.Slots.ShouldContain(s => s.SlotName == names.Slot && !s.ExistsOnServer);
                state.Slots.ShouldContain(s => s.SlotName == extSlot && !s.ExistsOnServer);
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
                node.Services.GetRequiredService<IWallabyStatus>().Current.Faulted.ShouldBeFalse();
            }

            // Committed while suspended: no slot exists, so only the resume re-backfill can deliver it.
            var missedId = await Db.AddProductAsync(categoryId, $"missed_{names.Suffix}");

            // Node 2 against the same database: the suspension survives the restart — no slot is recreated.
            var secondCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(
                BuildServices(names, secondCapture, external: (extSlot, extPub))))
            {
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
                (await SlotExistsAsync(names.Slot)).ShouldBeFalse();

                await client.ResumeAsync();

                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                await secondCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);
                var latest = secondCapture.LatestByDocumentId(destination: "products");
                latest[missedId.ToString()].Document!["name"].ShouldBe($"missed_{names.Suffix}");
                (await SlotExistsAsync(extSlot)).ShouldBeTrue();
                node.Services.GetRequiredService<IWallabyStatus>().Current.Faulted.ShouldBeFalse();
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Two_phase_deployment_suspends_with_the_flag_and_resumes_without_it()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        try
        {
            // Phase 0: a normal deployment streams one product.
            var firstCapture = new CaptureSink();
            int categoryId, firstId;
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, firstCapture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                categoryId = await Db.AddCategoryAsync();
                firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
                await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);
            }

            // Phase 1: deploy with Suspend() — the node itself drops the slots and idles.
            var suspendedCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(
                BuildServices(names, suspendedCapture, suspendFlag: true)))
            {
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
                (await SlotExistsAsync(names.Slot)).ShouldBeFalse();

                var status = node.Services.GetRequiredService<IWallabyStatus>().Current;
                status.SuspensionReason.ShouldBe("engine upgrade");
                status.Faulted.ShouldBeFalse();
            }

            // (The platform's engine upgrade would run here — no logical slots exist.)
            var missedId = await Db.AddProductAsync(categoryId, $"missed_{names.Suffix}");

            // Phase 2: deploy without the flag — auto-resume, slot recreation, and a full re-backfill.
            var resumedCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, resumedCapture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                await resumedCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Resume_with_purge_converges_deletes_committed_while_suspended()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            // One capture across both nodes: it plays the durable external destination whose stale
            // documents only a purge can remove.
            var capture = new CaptureSink();
            int categoryId, keptId, staleId;
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, capture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                categoryId = await Db.AddCategoryAsync();
                keptId = await Db.AddProductAsync(categoryId, $"kept_{names.Suffix}");
                staleId = await Db.AddProductAsync(categoryId, $"stale_{names.Suffix}");
                await capture.WaitForDocumentsAsync([keptId.ToString(), staleId.ToString()]);

                await client.SuspendAsync(new WallabySuspendOptions { Timeout = TimeSpan.FromSeconds(60) });
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
            }

            // Deleted while no slot exists: the delete is never streamed, and the resume re-backfill
            // only upserts current rows, so without a purge the document would linger forever.
            await Db.DeleteProductAsync(staleId);

            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, capture)))
            {
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);

                await client.ResumeAsync(purge: true);

                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                await Polling.UntilAsync(() => capture.Purges.Count > 0);
                await capture.WaitForDocumentsAsync([keptId.ToString()]);

                capture.Purges.ShouldContain(p => p.TableName == "products");
                var latest = capture.LatestByDocumentId(destination: "products");
                latest.ContainsKey(staleId.ToString()).ShouldBeFalse();
                latest[keptId.ToString()].Document!["name"].ShouldBe($"kept_{names.Suffix}");

                // The repair consumed the flag, so a later unrelated repair will not purge unrequested.
                (await ReadPurgeOnResumeAsync()).ShouldBeFalse();
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    private async Task<bool> ReadPurgeOnResumeAsync()
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT purge_on_resume FROM wallaby.control", conn);
        return await cmd.ExecuteScalarAsync() is true;
    }

    [Test]
    public async Task A_resume_before_the_first_checkpoint_still_re_backfills()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        try
        {
            // Phase 0: a normal deployment streams one product.
            var firstCapture = new CaptureSink();
            int categoryId, firstId;
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, firstCapture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                categoryId = await Db.AddCategoryAsync();
                firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
                await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);
            }

            await DeleteCheckpointAsync(names.Slot);

            // Phase 1: deploy with Suspend(); the node drops the slots and idles.
            await using (var node = await WallabyTestNode.StartAsync(
                BuildServices(names, new CaptureSink(), suspendFlag: true)))
            {
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
                (await SlotExistsAsync(names.Slot)).ShouldBeFalse();
            }

            var missedId = await Db.AddProductAsync(categoryId, $"missed_{names.Suffix}");

            // Phase 2: with no checkpoint, only the recreated slot's surviving slot_registry row can
            // prove the installation predates the new slot; the repair must still fire.
            var resumedCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, resumedCapture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                await resumedCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);
                var latest = resumedCapture.LatestByDocumentId(destination: "products");
                latest[missedId.ToString()].Document!["name"].ShouldBe($"missed_{names.Suffix}");
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task A_resume_onto_a_rewound_wal_history_still_re_backfills()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            // Phase 0: stream one product, then suspend remotely.
            var firstCapture = new CaptureSink();
            int categoryId, firstId;
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, firstCapture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                categoryId = await Db.AddCategoryAsync();
                firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
                await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);

                await client.SuspendAsync(new WallabySuspendOptions { Timeout = TimeSpan.FromSeconds(60) });
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
            }

            // Simulates the cluster being rebuilt during the outage (restore, blue/green): the wallaby
            // tables survive with the old timeline's checkpoint, whose LSN is far ahead of anything the
            // new cluster will allocate. Naive LSN comparison would read this as continuity.
            await SetCheckpointAsync(names.Slot, "FFFF/FFFF0000");
            var missedId = await Db.AddProductAsync(categoryId, $"missed_{names.Suffix}");

            // Resume: the rewind must be detected and repaired; only the re-backfill can deliver the
            // missed product.
            var secondCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, secondCapture)))
            {
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);

                await client.ResumeAsync();

                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                await secondCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);
                node.Services.GetRequiredService<IWallabyStatus>().Current.Faulted.ShouldBeFalse();
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    // Overwrites (or creates) the slot's checkpoint row, standing in for a checkpoint written on a
    // different WAL timeline.
    private async Task SetCheckpointAsync(string slot, string lsn)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO wallaby.checkpoint (slot_name, confirmed_lsn, updated_at)
            VALUES (@s, @l::pg_lsn, now())
            ON CONFLICT (slot_name) DO UPDATE SET confirmed_lsn = EXCLUDED.confirmed_lsn, updated_at = now()
            """, conn);
        cmd.Parameters.AddWithValue("s", slot);
        cmd.Parameters.AddWithValue("l", lsn);
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task A_restart_with_an_intact_slot_and_no_checkpoint_does_not_re_backfill()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);

        var firstCapture = new CaptureSink();
        int categoryId, firstId;
        await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, firstCapture)))
        {
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            categoryId = await Db.AddCategoryAsync();
            firstId = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
            await firstCapture.WaitForDocumentsAsync([firstId.ToString()]);
        }

        await DeleteCheckpointAsync(names.Slot);

        // The slot survived the restart, so WAL continuity is intact: a missing checkpoint row alone
        // must not trigger a re-backfill.
        var secondCapture = new CaptureSink();
        await using (var restarted = await WallabyTestNode.StartAsync(BuildServices(names, secondCapture)))
        {
            await WallabyReadiness.WaitForStreamingAsync(restarted.Services);
            var liveId = await Db.AddProductAsync(categoryId, $"live_{names.Suffix}");
            await secondCapture.WaitForDocumentsAsync([liveId.ToString()]);

            secondCapture.LatestByDocumentId(destination: "products")
                .ContainsKey(firstId.ToString()).ShouldBeFalse();
        }
    }

    [Test]
    public async Task A_suspended_flag_node_keeps_refreshing_its_assertion()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        try
        {
            var capture = new CaptureSink();
            await using var node = await WallabyTestNode.StartAsync(
                BuildServices(names, capture, suspendFlag: true));
            await WallabyReadiness.WaitForSuspendedAsync(node.Services);

            // The idle loop re-runs the control gate every pass, so the assertion heartbeat keeps
            // advancing for as long as the flag-carrying node lives.
            var first = await AssertedAtAsync();
            first.ShouldNotBeNull();
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
            while (await AssertedAtAsync() <= first)
            {
                if (DateTime.UtcNow >= deadline)
                {
                    throw new TimeoutException("The suspended flag node never refreshed configuration_asserted_at.");
                }
                await Task.Delay(100);
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task A_mixed_deployment_stays_suspended_until_the_flag_node_dies()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        try
        {
            // A rolling deploy in flight: a flag pod and a flag-less pod alive at the same time, each
            // wanting the opposite control state.
            var flagCapture = new CaptureSink();
            var flaglessCapture = new CaptureSink();
            await using var flagNode = await WallabyTestNode.StartAsync(
                BuildServices(names, flagCapture, suspendFlag: true));
            await WallabyReadiness.WaitForSuspendedAsync(flagNode.Services);

            await using var flaglessNode = await WallabyTestNode.StartAsync(
                BuildServices(names, flaglessCapture));
            await WallabyReadiness.WaitForSuspendedAsync(flaglessNode.Services);

            // Well past the 2s grace: the live flag node's heartbeat must keep the resume refused.
            await Task.Delay(TimeSpan.FromSeconds(4));
            flaglessNode.Services.GetRequiredService<IWallabyStatus>()
                .Current.Role.ShouldBe(WallabyNodeRole.Suspended);
            (await SlotExistsAsync(names.Slot)).ShouldBeFalse();

            // The flag node dies (the rollout completes); its assertion goes stale and the flag-less
            // node auto-resumes on its own.
            await flagNode.DisposeAsync();
            await WallabyReadiness.WaitForStreamingAsync(flaglessNode.Services);
            (await SlotExistsAsync(names.Slot)).ShouldBeTrue();
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    private async Task<DateTime?> AssertedAtAsync()
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT configuration_asserted_at FROM wallaby.control", conn);
        return await cmd.ExecuteScalarAsync() as DateTime?;
    }

    [Test]
    public async Task Client_origin_suspension_is_not_auto_resumed_by_a_flagless_host()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            var firstCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, firstCapture)))
            {
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
                await client.SuspendAsync(new WallabySuspendOptions { Timeout = TimeSpan.FromSeconds(60) });
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);
            }

            // A flag-less restart auto-resumes only configuration-origin suspensions — not this one.
            var secondCapture = new CaptureSink();
            await using (var node = await WallabyTestNode.StartAsync(BuildServices(names, secondCapture)))
            {
                await WallabyReadiness.WaitForSuspendedAsync(node.Services);

                // Give the election loop time to (wrongly) resume before asserting it did not.
                await Task.Delay(TimeSpan.FromSeconds(2));
                var status = node.Services.GetRequiredService<IWallabyStatus>();
                status.Current.Role.ShouldBe(WallabyNodeRole.Suspended);
                (await SlotExistsAsync(names.Slot)).ShouldBeFalse();

                await client.ResumeAsync();
                await WallabyReadiness.WaitForStreamingAsync(node.Services);
            }
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task A_deployed_suspend_flag_reasserts_over_a_remote_resume()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            var capture = new CaptureSink();
            await using var node = await WallabyTestNode.StartAsync(
                BuildServices(names, capture, suspendFlag: true));
            await WallabyReadiness.WaitForSuspendedAsync(node.Services);

            await client.ResumeAsync();

            // The deployed flag wins: the node re-asserts the suspension instead of streaming.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while ((await client.GetStateAsync()).State == WallabySuspensionState.Running)
            {
                if (DateTime.UtcNow >= deadline)
                {
                    throw new TimeoutException("The Suspend()-flagged node never re-asserted the suspension.");
                }
                await Task.Delay(100);
            }
            await WallabyReadiness.WaitForSuspendedAsync(node.Services);
            (await SlotExistsAsync(names.Slot)).ShouldBeFalse();
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    private ServiceCollection BuildServices(
        WallabyNames names, CaptureSink capture, bool suspendFlag = false,
        (string Slot, string Publication)? external = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination("products")
                   .UsingTransform(TestTransforms.ProductNames));
            if (external is { } ext)
            {
                cdc.AddExternalSlot(ext.Slot, e => e.WithPublication(ext.Publication).ForEntity<Product>());
            }
            if (suspendFlag)
            {
                cdc.Suspend("engine upgrade");
            }
        });
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
            o.Advanced.ControlPollInterval = TimeSpan.FromMilliseconds(500);
            // The default 60s floor would stall the flag-less phases; grace = max(4 * poll, floor) = 2s.
            o.Advanced.SuspensionAutoResumeGraceFloor = TimeSpan.FromSeconds(2);
        });
        services.ReplaceWallabySink("capture", capture);
        return services;
    }

    // Simulates the pre-first-checkpoint window deterministically: streaming checkpoint saves race
    // disposal, and an idle or backfill-only install never writes one at all.
    private async Task DeleteCheckpointAsync(string slot)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("DELETE FROM wallaby.checkpoint WHERE slot_name = @s", conn);
        cmd.Parameters.AddWithValue("s", slot);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> SlotExistsAsync(string slot)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = @s", conn);
        cmd.Parameters.AddWithValue("s", slot);
        return (long)(await cmd.ExecuteScalarAsync())! > 0;
    }

    // The control row is installation-wide on the shared test database: a leaked suspension would idle
    // every later test's node, so each test restores the running state.
    private async Task ResetControlAsync()
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        try
        {
            await using var cmd = new NpgsqlCommand("DELETE FROM wallaby.control", conn);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
        }
    }
}
