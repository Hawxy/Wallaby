using Npgsql;
using Wallaby.Client.Internal;
using Wallaby.TestInfrastructure;

namespace Wallaby.Client.Tests.Integration;

/// <summary>
/// Exercises the control client against a bare database; no Wallaby host anywhere. The grace-period
/// fallback makes the client itself drop the managed slots, which is the scaled-to-zero /
/// provision-only-already-exited story for an RDS/Aurora upgrade.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ControlClientTests(PostgresFixture pg)
{
    // The wallaby.control row is installation-wide; leave the shared database running for other tests.
    private async Task ResetControlAsync()
    {
        try
        {
            await using var cmd = pg.DataSource.CreateCommand("DELETE FROM wallaby.control");
            await cmd.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
        }
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<long> CountSlotsAsync(params string[] names)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ANY($1)");
        cmd.Parameters.AddWithValue(names);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<long> CountPublicationsAsync(params string[] names)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            "SELECT count(*) FROM pg_publication WHERE pubname = ANY($1)");
        cmd.Parameters.AddWithValue(names);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    // The client performs no DDL itself, so these tests simulate a host having run against the
    // database with the real bootstrapper (single source of truth for the wallaby DDL).
    private Task EnsureStateSchemaAsync() => WallabyStateSchema.EnsureAsync(pg.DataSource);

    [Test]
    public async Task A_database_wallaby_never_touched_reads_running()
    {
        // A dedicated database proves GetStateAsync needs no wallaby schema and performs no DDL.
        await ExecAsync("CREATE DATABASE control_virgin");
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString) { Database = "control_virgin" };
        await using var client = new WallabyControlClient(builder.ConnectionString);

        var state = await client.GetStateAsync();

        state.State.ShouldBe(WallabySuspensionState.Running);
        state.Slots.ShouldBeEmpty();

        // Resume needs no control table: with none there is nothing to resume.
        (await client.ResumeAsync()).State.ShouldBe(WallabySuspensionState.Running);

        // Suspend refuses instead of creating the table: only the host performs DDL.
        await Should.ThrowAsync<InvalidOperationException>(() => client.SuspendAsync());

        // The client never creates the wallaby schema.
        await using var virginSource = NpgsqlDataSource.Create(builder.ConnectionString);
        await using var check = virginSource.CreateCommand(
            "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'wallaby'");
        (await check.ExecuteScalarAsync()).ShouldBe(0L);
    }

    [Test]
    public async Task Slots_report_retained_wal_bytes_when_present_on_the_server()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var live = $"cdc_slot_{suffix}";
        var gone = $"dropped_slot_{suffix}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            await ExecAsync($"SELECT pg_create_logical_replication_slot('{live}', 'pgoutput')");
            await ExecAsync(
                $"""
                 INSERT INTO wallaby.slot_registry (slot_name, publication, kind)
                 VALUES ('{live}', 'pub_{suffix}', 'primary'), ('{gone}', 'ext_pub_{suffix}', 'external')
                 """);

            var state = await client.GetStateAsync();

            // A slot on the server retains WAL from its restart_lsn; a slot missing from it reads null.
            state.Slots.Single(s => s.SlotName == live).RetainedWalBytes
                .ShouldNotBeNull().ShouldBeGreaterThanOrEqualTo(0L);
            state.Slots.Single(s => s.SlotName == gone).RetainedWalBytes.ShouldBeNull();
        }
        finally
        {
            await ExecAsync(
                $"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{live}'");
            await ExecAsync($"DELETE FROM wallaby.slot_registry WHERE slot_name IN ('{live}', '{gone}')");
        }
    }

    [Test]
    public async Task Suspend_without_a_host_drops_managed_slots_and_publications_from_the_client()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var primary = $"cdc_slot_{suffix}";
        var external = $"elt_slot_{suffix}";
        var unmanaged = $"byo_slot_{suffix}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            await ExecAsync($"CREATE TABLE quiesce_target_{suffix} (id int PRIMARY KEY, code text)");
            await ExecAsync($"CREATE PUBLICATION pub_{suffix} FOR TABLE quiesce_target_{suffix} (id, code)");
            await ExecAsync($"CREATE PUBLICATION ext_pub_{suffix} FOR TABLE quiesce_target_{suffix}");
            await ExecAsync($"CREATE PUBLICATION byo_pub_{suffix} FOR TABLE quiesce_target_{suffix}");
            await ExecAsync($"SELECT pg_create_logical_replication_slot('{primary}', 'pgoutput')");
            await ExecAsync($"SELECT pg_create_logical_replication_slot('{external}', 'pgoutput')");
            await ExecAsync(
                $"""
                 INSERT INTO wallaby.slot_registry (slot_name, publication, kind, publication_managed)
                 VALUES ('{primary}', 'pub_{suffix}', 'primary', true),
                        ('{external}', 'ext_pub_{suffix}', 'external', true),
                        ('{unmanaged}', 'byo_pub_{suffix}', 'primary', false)
                 """);

            var state = await client.SuspendAsync(new WallabySuspendOptions
            {
                Reason = "PG18 upgrade",
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(60),
            });

            state.State.ShouldBe(WallabySuspensionState.Suspended);
            state.Origin.ShouldBe(WallabySuspensionOrigin.Client);
            state.Reason.ShouldBe("PG18 upgrade");
            state.SuspendedAt.ShouldNotBeNull();
            state.Slots.Count.ShouldBe(3);
            state.Slots.ShouldAllBe(s => !s.ExistsOnServer);
            (await CountSlotsAsync(primary, external)).ShouldBe(0);

            // Managed publications are dropped with the slots; the unmanaged one (Wallaby cannot
            // recreate it) survives.
            (await CountPublicationsAsync($"pub_{suffix}", $"ext_pub_{suffix}")).ShouldBe(0);
            (await CountPublicationsAsync($"byo_pub_{suffix}")).ShouldBe(1);

            // The quiesced state is the point: a column-type migration blocked by the publication
            // column list now runs.
            await ExecAsync($"ALTER TABLE quiesce_target_{suffix} ALTER COLUMN code TYPE varchar(64)");
        }
        finally
        {
            await ExecAsync($"DROP PUBLICATION IF EXISTS byo_pub_{suffix}");
            await ExecAsync($"DROP TABLE IF EXISTS quiesce_target_{suffix}");
            await ExecAsync(
                $"DELETE FROM wallaby.slot_registry WHERE slot_name IN ('{primary}', '{external}', '{unmanaged}')");
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Double_suspend_is_a_noop_that_keeps_the_original_request()
    {
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            var first = await client.SuspendAsync(new WallabySuspendOptions
            {
                Reason = "original",
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });
            first.State.ShouldBe(WallabySuspensionState.Suspended);

            var second = await client.SuspendAsync(new WallabySuspendOptions
            {
                Reason = "overwritten?",
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });

            second.State.ShouldBe(WallabySuspensionState.Suspended);
            second.Reason.ShouldBe("original");
            second.RequestedAt.ShouldBe(first.RequestedAt);
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Resume_ends_the_suspension_and_is_a_noop_when_running()
    {
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            await client.SuspendAsync(new WallabySuspendOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });

            var resumed = await client.ResumeAsync();
            resumed.State.ShouldBe(WallabySuspensionState.Running);
            resumed.ResumedAt.ShouldNotBeNull();

            // Resume when nothing is suspended: an unchanged running state, no exception.
            (await client.ResumeAsync()).State.ShouldBe(WallabySuspensionState.Running);
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Resume_with_purge_records_the_purge_for_the_repair()
    {
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            await client.SuspendAsync(new WallabySuspendOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });

            var resumed = await client.ResumeAsync(purge: true);

            // The flag rides the resume transition durably; the host's slot-gap repair consumes it.
            resumed.State.ShouldBe(WallabySuspensionState.Running);
            (await ReadPurgeOnResumeAsync()).ShouldBeTrue();
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Resume_with_purge_when_nothing_is_suspended_does_not_mark()
    {
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();

            (await client.ResumeAsync(purge: true)).State.ShouldBe(WallabySuspensionState.Running);

            // No transition, no purge: the flag only ever accompanies an actual resume.
            (await ReadPurgeOnResumeAsync()).ShouldBeFalse();
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    private async Task<bool> ReadPurgeOnResumeAsync()
    {
        await using var cmd = pg.DataSource.CreateCommand("SELECT purge_on_resume FROM wallaby.control");
        return await cmd.ExecuteScalarAsync() is true;
    }

    [Test]
    public async Task A_stranded_suspend_request_converges_when_retried()
    {
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            // Simulates a finalizer crash after the drops but before the mark: the request row persists
            // with no slots left to drop.
            await EnsureStateSchemaAsync();
            await ControlOperations.RequestSuspendAsync(
                pg.DataSource, ControlContract.OriginClient, "crashed mid-suspend", "test", CancellationToken.None);
            (await client.GetStateAsync()).State.ShouldBe(WallabySuspensionState.SuspendRequested);

            var state = await client.SuspendAsync(new WallabySuspendOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });

            state.State.ShouldBe(WallabySuspensionState.Suspended);
            state.Reason.ShouldBe("crashed mid-suspend");
        }
        finally
        {
            await ResetControlAsync();
        }
    }
}
