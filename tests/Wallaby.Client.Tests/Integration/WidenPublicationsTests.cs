using Npgsql;
using Wallaby.TestInfrastructure;

namespace Wallaby.Client.Tests.Integration;

/// <summary>
/// Publication widening against a bare database, no Wallaby host anywhere: the grace-period fallback
/// makes the client rewrite the managed publications itself, which is the scaled-to-zero story for
/// running a blocked schema migration without suspending.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class WidenPublicationsTests(PostgresFixture pg)
{
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

    // A publication is narrowed when any member carries a column list (prattrs) or row filter (prqual).
    private async Task<long> CountNarrowedAsync(params string[] pubs)
    {
        await using var cmd = pg.DataSource.CreateCommand(
            """
            SELECT count(*) FROM pg_publication p
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            WHERE p.pubname = ANY($1) AND (pr.prattrs IS NOT NULL OR pr.prqual IS NOT NULL)
            """);
        cmd.Parameters.AddWithValue(pubs);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    // The client performs no DDL on the wallaby schema, so these tests simulate a host having run
    // against the database with the real bootstrapper (single source of truth for the wallaby DDL).
    private Task EnsureStateSchemaAsync() => WallabyStateSchema.EnsureAsync(pg.DataSource);

    [Test]
    public async Task Widen_without_a_host_widens_managed_publications_from_the_client()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var managed = $"cdc_slot_{suffix}";
        var unmanaged = $"byo_slot_{suffix}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            await ExecAsync($"CREATE TABLE widen_target_{suffix} (id int PRIMARY KEY, code text, secret text)");
            await ExecAsync($"CREATE PUBLICATION pub_{suffix} FOR TABLE widen_target_{suffix} (id, code)");
            await ExecAsync($"CREATE PUBLICATION byo_pub_{suffix} FOR TABLE widen_target_{suffix} (id, code)");
            await ExecAsync(
                $"""
                 INSERT INTO wallaby.slot_registry (slot_name, publication, kind, publication_managed)
                 VALUES ('{managed}', 'pub_{suffix}', 'primary', true),
                        ('{unmanaged}', 'byo_pub_{suffix}', 'primary', false)
                 """);

            // The blocked migration this feature exists for.
            var blocked = await Should.ThrowAsync<PostgresException>(
                () => ExecAsync($"ALTER TABLE widen_target_{suffix} ALTER COLUMN code TYPE varchar(64)"));
            blocked.Message.ShouldContain("publication");

            var state = await client.WidenPublicationsAsync(new WallabyWidenOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(60),
            });

            state.State.ShouldBe(WallabySuspensionState.Running);
            state.PublicationsWidened.ShouldBeTrue();
            state.WidenedAt.ShouldNotBeNull();
            state.WidenedBy.ShouldNotBeNull();
            state.Slots.Single(s => s.SlotName == managed).PublicationNarrowed.ShouldBeFalse();

            // Only the managed publication was rewritten; the unmanaged one (Wallaby cannot restore
            // its lists) keeps its narrowing, and both stay members of the table.
            (await CountNarrowedAsync($"pub_{suffix}")).ShouldBe(0);
            (await CountNarrowedAsync($"byo_pub_{suffix}")).ShouldBe(1);
            state.Slots.Single(s => s.SlotName == unmanaged).PublicationNarrowed.ShouldBeTrue();

            // The point of the exercise: drop the unmanaged narrowing manually (a documented operator
            // step) and the migration runs while capture stays fully configured.
            await ExecAsync($"DROP PUBLICATION byo_pub_{suffix}");
            await ExecAsync($"ALTER TABLE widen_target_{suffix} ALTER COLUMN code TYPE varchar(64)");
        }
        finally
        {
            await ExecAsync($"DROP PUBLICATION IF EXISTS pub_{suffix}");
            await ExecAsync($"DROP PUBLICATION IF EXISTS byo_pub_{suffix}");
            await ExecAsync($"DROP TABLE IF EXISTS widen_target_{suffix}");
            await ExecAsync(
                $"DELETE FROM wallaby.slot_registry WHERE slot_name IN ('{managed}', '{unmanaged}')");
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Restore_clears_the_flag_but_only_a_host_renarrows()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var slot = $"cdc_slot_{suffix}";
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            await ExecAsync($"CREATE TABLE restore_target_{suffix} (id int PRIMARY KEY, code text)");
            await ExecAsync($"CREATE PUBLICATION pub_{suffix} FOR TABLE restore_target_{suffix} (id, code)");
            await ExecAsync(
                $"""
                 INSERT INTO wallaby.slot_registry (slot_name, publication, kind, publication_managed)
                 VALUES ('{slot}', 'pub_{suffix}', 'primary', true)
                 """);
            await client.WidenPublicationsAsync(new WallabyWidenOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(60),
            });

            var state = await client.RestorePublicationsAsync();

            // The flag clears immediately, but the narrow lists come from the captured model: with no
            // host running the publication stays wide until the next host startup reconciles it.
            state.PublicationsWidened.ShouldBeFalse();
            state.WidenedAt.ShouldBeNull();
            (await CountNarrowedAsync($"pub_{suffix}")).ShouldBe(0);
            state.Slots.Single(s => s.SlotName == slot).PublicationNarrowed.ShouldBeFalse();

            // Restore with nothing widened is a no-op, not an error.
            (await client.RestorePublicationsAsync()).PublicationsWidened.ShouldBeFalse();
        }
        finally
        {
            await ExecAsync($"DROP PUBLICATION IF EXISTS pub_{suffix}");
            await ExecAsync($"DROP TABLE IF EXISTS restore_target_{suffix}");
            await ExecAsync($"DELETE FROM wallaby.slot_registry WHERE slot_name = '{slot}'");
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Double_widen_is_idempotent()
    {
        await using var client = new WallabyControlClient(pg.ConnectionString);
        try
        {
            await EnsureStateSchemaAsync();
            var first = await client.WidenPublicationsAsync(new WallabyWidenOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });
            first.PublicationsWidened.ShouldBeTrue();

            var second = await client.WidenPublicationsAsync(new WallabyWidenOptions
            {
                HostGracePeriod = TimeSpan.Zero,
                Timeout = TimeSpan.FromSeconds(30),
            });

            second.PublicationsWidened.ShouldBeTrue();
            second.WidenedAt.ShouldBe(first.WidenedAt);
        }
        finally
        {
            await ResetControlAsync();
        }
    }

    [Test]
    public async Task Widen_on_a_database_wallaby_never_touched_throws()
    {
        // A dedicated database proves the client refuses instead of creating the control table.
        await ExecAsync("CREATE DATABASE widen_virgin");
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString) { Database = "widen_virgin" };
        await using var client = new WallabyControlClient(builder.ConnectionString);

        var ex = await Should.ThrowAsync<InvalidOperationException>(() => client.WidenPublicationsAsync());

        ex.Message.ShouldContain("wallaby.control");
    }

    [Test]
    public async Task Widen_while_suspended_throws()
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

            // A suspension already dropped the managed publications; widening has nothing to do.
            var ex = await Should.ThrowAsync<InvalidOperationException>(() => client.WidenPublicationsAsync());
            ex.Message.ShouldContain("suspended");
        }
        finally
        {
            await ResetControlAsync();
        }
    }
}
