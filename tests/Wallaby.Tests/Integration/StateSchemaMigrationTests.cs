using Npgsql;
using Wallaby.Internal.State;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// The state-schema migrator against real databases: fresh bootstrap, fast-path re-run, adoption of a
/// deployed (pre-versioning) schema, the newer-schema guard, ordered synthetic steps, and the
/// concurrent-bootstrap race. Each test creates an isolated database — the shared session schema is
/// never dropped, so schema-state tests can't run against it.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class StateSchemaMigrationTests(PostgresFixture pg)
{
    private async Task<NpgsqlDataSource> CreateIsolatedDbAsync(string prefix)
    {
        var name = $"{prefix}_{Guid.NewGuid():N}";
        await using (var cmd = pg.DataSource.CreateCommand($"CREATE DATABASE {name}"))
        {
            await cmd.ExecuteNonQueryAsync();
        }
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString) { Database = name };
        return NpgsqlDataSource.Create(builder.ConnectionString);
    }

    private static async Task EnsureAsync(NpgsqlDataSource db)
    {
        await using var conn = await db.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
    }

    private static async Task<long> ScalarAsync(NpgsqlDataSource db, string sql)
    {
        await using var cmd = db.CreateCommand(sql);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task ExecAsync(NpgsqlDataSource db, string sql)
    {
        await using var cmd = db.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    [Test]
    public async Task Fresh_database_bootstraps_and_stamps_the_current_version()
    {
        await using var db = await CreateIsolatedDbAsync("schema_fresh");

        await EnsureAsync(db);

        (await ScalarAsync(db, "SELECT count(*) FROM pg_tables WHERE schemaname = 'wallaby'"))
            .ShouldBe(7); // 6 state tables + schema_version
        (await ScalarAsync(db, "SELECT max(version) FROM wallaby.schema_version"))
            .ShouldBe(StateSchemaMigrations.CurrentVersion);
        await using var applied = db.CreateCommand("SELECT applied_by FROM wallaby.schema_version");
        (await applied.ExecuteScalarAsync() as string).ShouldNotBeNullOrWhiteSpace();
    }

    [Test]
    public async Task Rerun_at_the_current_version_is_a_noop()
    {
        await using var db = await CreateIsolatedDbAsync("schema_rerun");

        await EnsureAsync(db);
        await EnsureAsync(db);

        (await ScalarAsync(db, "SELECT count(*) FROM wallaby.schema_version")).ShouldBe(1);
    }

    [Test]
    public async Task A_deployed_schema_without_a_ledger_is_adopted_without_touching_data()
    {
        await using var db = await CreateIsolatedDbAsync("schema_adopt");

        // Exactly what real deployments have: the beta schema (no schema_version table) with live data.
        await ExecAsync(db, "CREATE SCHEMA IF NOT EXISTS wallaby");
        await ExecAsync(db, StateSchemaMigrations.Steps[0].Ddl);
        await ExecAsync(db, "INSERT INTO wallaby.checkpoint (slot_name, confirmed_lsn) VALUES ('s1', '0/2A')");
        await ExecAsync(db, "INSERT INTO wallaby.backfill_state (table_qualified, status, rows_copied) VALUES ('public.orders', 'Completed', 42)");

        await EnsureAsync(db);

        (await ScalarAsync(db, "SELECT max(version) FROM wallaby.schema_version"))
            .ShouldBe(StateSchemaMigrations.CurrentVersion);
        (await ScalarAsync(db, "SELECT count(*) FROM wallaby.checkpoint WHERE slot_name = 's1'")).ShouldBe(1);
        (await ScalarAsync(db, "SELECT rows_copied FROM wallaby.backfill_state WHERE table_qualified = 'public.orders'"))
            .ShouldBe(42);
    }

    [Test]
    public async Task A_newer_schema_refuses_to_run()
    {
        await using var db = await CreateIsolatedDbAsync("schema_newer");
        await EnsureAsync(db);
        var newer = StateSchemaMigrations.CurrentVersion + 1;
        await ExecAsync(db, $"INSERT INTO wallaby.schema_version (version, applied_by) VALUES ({newer}, 'future')");

        var ex = await Should.ThrowAsync<WallabyConfigurationException>(() => EnsureAsync(db));

        ex.Message.ShouldContain($"version {newer}");
        ex.Message.ShouldContain($"up to version {StateSchemaMigrations.CurrentVersion}");
    }

    [Test]
    public async Task Pending_steps_apply_in_order_and_only_once()
    {
        await using var db = await CreateIsolatedDbAsync("schema_steps");
        IReadOnlyList<(int Version, string Ddl)> v1 =
            [(1, "CREATE TABLE IF NOT EXISTS wallaby.mig_probe (id int PRIMARY KEY)")];
        IReadOnlyList<(int Version, string Ddl)> v2 =
        [
            .. v1,
            (2, "ALTER TABLE wallaby.mig_probe ADD COLUMN IF NOT EXISTS extra text NOT NULL DEFAULT ''"),
        ];
        var bootstrapper = new StateSchemaBootstrapper();

        await using (var conn = await db.OpenConnectionAsync())
        {
            await bootstrapper.EnsureAsync(conn, v1, currentVersion: 1, CancellationToken.None);
        }
        (await ScalarAsync(db, "SELECT max(version) FROM wallaby.schema_version")).ShouldBe(1);

        // Upgrading applies only the pending step; the ledger records both.
        await using (var conn = await db.OpenConnectionAsync())
        {
            await bootstrapper.EnsureAsync(conn, v2, currentVersion: 2, CancellationToken.None);
        }
        (await ScalarAsync(db, "SELECT count(*) FROM wallaby.schema_version")).ShouldBe(2);
        (await ScalarAsync(db,
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'wallaby' AND table_name = 'mig_probe' AND column_name = 'extra'"))
            .ShouldBe(1);
    }

    [Test]
    public async Task Concurrent_bootstraps_serialize_on_the_migration_lock()
    {
        await using var db = await CreateIsolatedDbAsync("schema_race");

        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => EnsureAsync(db)));

        (await ScalarAsync(db, "SELECT count(*) FROM wallaby.schema_version")).ShouldBe(1);
        (await ScalarAsync(db, "SELECT count(*) FROM pg_tables WHERE schemaname = 'wallaby'")).ShouldBe(7);
    }
}
