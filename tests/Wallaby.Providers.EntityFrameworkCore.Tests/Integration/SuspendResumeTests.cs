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
                try
                {
                    await resumedCapture.WaitForDocumentsAsync([firstId.ToString(), missedId.ToString()]);
                }
                catch (TimeoutException ex)
                {
                    // TEMP diagnostics: dump the wallaby state tables only after the failure has happened.
                    throw new TimeoutException(ex.Message + await DumpStateAsync(names), ex);
                }
            }
        }
        finally
        {
            await ResetControlAsync();
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

    // TEMP diagnostics for the intermittent phase-2 timeout.
    private async Task<string> DumpStateAsync(WallabyNames names)
    {
        await using var conn = new NpgsqlConnection(pg.ConnectionString);
        await conn.OpenAsync();
        var sb = new System.Text.StringBuilder("\n--- state dump ---\n");
        foreach (var (label, sql) in new (string, string)[]
        {
            ("checkpoint", "SELECT slot_name, confirmed_lsn::text, updated_at::text FROM wallaby.checkpoint"),
            ("backfill_state", "SELECT table_qualified, status, coalesce(transform_version,'<null>'), rows_copied::text, updated_at::text FROM wallaby.backfill_state"),
            ("slot_registry", "SELECT slot_name, coalesce(consistent_point::text,'<null>'), kind FROM wallaby.slot_registry"),
            ("control", "SELECT state, origin, coalesce(reason,'<null>'), updated_at::text FROM wallaby.control"),
            ("pg_replication_slots", "SELECT slot_name, active::text, confirmed_flush_lsn::text FROM pg_replication_slots"),
            ("fanout_queue", "SELECT table_qualified, status, attempts::text FROM wallaby.fanout_queue"),
        })
        {
            sb.Append(label).Append(":\n");
            try
            {
                await using var cmd = new NpgsqlCommand(sql, conn);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var fields = new string[reader.FieldCount];
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        fields[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i).ToString() ?? "";
                    }
                    sb.Append("  ").AppendJoin(" | ", fields).Append('\n');
                }
            }
            catch (Exception ex)
            {
                sb.Append("  <query failed: ").Append(ex.Message).Append(">\n");
            }
        }
        sb.Append("this test's slot: ").Append(names.Slot).Append('\n');
        return sb.ToString();
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
