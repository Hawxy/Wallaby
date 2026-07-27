using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Client.Internal;
using Wallaby.DependencyInjection;
using Wallaby.Internal;
using Wallaby.Internal.Control;
using Wallaby.Internal.State;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// The configuration-assertion heartbeat and the grace-guarded auto-resume: a live flag-carrying node
/// refreshing <c>configuration_asserted_at</c> holds off flag-less nodes' auto-resume (so a mixed
/// rolling deployment can't flap slots), a stale or absent assertion resumes exactly once, and a
/// database bootstrapped by an older host self-heals the missing column.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ControlAssertionGraceTests(PostgresFixture pg)
{
    private WallabyDataSource? _dataSource;

    private PostgresControlStore Store(TimeSpan? graceFloor = null)
    {
        _dataSource ??= new WallabyDataSource(pg.ConnectionString);
        var options = new WallabyOptions { ConnectionString = pg.ConnectionString };
        options.Advanced.ControlPollInterval = TimeSpan.FromMilliseconds(500);
        options.Advanced.SuspensionAutoResumeGraceFloor = graceFloor ?? TimeSpan.FromMinutes(10);
        return new PostgresControlStore(_dataSource, options, NullLogger.Instance);
    }

    [Before(Test)]
    public async Task ResetControlAsync()
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
        await ExecAsync("DELETE FROM wallaby.control");
    }

    [After(Test)]
    public async Task DisposeDataSourceAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
            _dataSource = null;
        }
    }

    [Test]
    public async Task A_fresh_configuration_assertion_blocks_the_auto_resume()
    {
        var store = Store();
        (await store.RequestConfigurationSuspendAsync("engine upgrade", CancellationToken.None)).ShouldBeTrue();

        // The assertion is stamped atomically with the request: no window where a racing flag-less
        // node could observe the suspension without its heartbeat.
        (await AssertedAtAsync()).ShouldNotBeNull();

        (await store.ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeFalse();
        (await StateAsync()).ShouldBe(ControlContract.StateSuspendRequested);
    }

    [Test]
    public async Task A_heartbeat_refreshes_a_backdated_assertion()
    {
        var store = Store();
        await store.RequestConfigurationSuspendAsync(null, CancellationToken.None);
        await ExecAsync("UPDATE wallaby.control SET configuration_asserted_at = now() - interval '10 minutes'");

        await store.HeartbeatConfigurationAssertionAsync(CancellationToken.None);

        var asserted = (await AssertedAtAsync())!.Value;
        (DateTimeOffset.UtcNow - asserted).ShouldBeLessThan(TimeSpan.FromMinutes(1));
        (await store.ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeFalse();
    }

    [Test]
    public async Task A_stale_assertion_resumes_exactly_once()
    {
        var store = Store(graceFloor: TimeSpan.FromMinutes(1));
        await store.RequestConfigurationSuspendAsync(null, CancellationToken.None);
        await ExecAsync("UPDATE wallaby.control SET configuration_asserted_at = now() - interval '10 minutes'");
        var backdated = (await AssertedAtAsync())!.Value;

        // The staleness predicate lives inside the guarded UPDATE: of two racing flag-less nodes,
        // exactly one makes the transition.
        (await store.ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeTrue();
        (await store.ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeFalse();
        (await StateAsync()).ShouldBe(ControlContract.StateRunning);

        // The heartbeat is guarded to non-Running rows; a late flag node can't stamp a resumed row.
        await store.HeartbeatConfigurationAssertionAsync(CancellationToken.None);
        (await AssertedAtAsync()).ShouldBe(backdated);
    }

    [Test]
    public async Task A_never_stamped_assertion_resumes_immediately()
    {
        // A configuration suspension written by an older host: the column exists (schema is current)
        // but nothing ever stamped it, so there is no live-asserter evidence to wait out.
        await ExecAsync(
            $"""
             INSERT INTO wallaby.control (scope, state, origin, requested_at, updated_at)
             VALUES ('wallaby', '{ControlContract.StateSuspendRequested}', '{ControlContract.OriginConfiguration}', now(), now())
             """);

        (await Store().ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeTrue();
        (await StateAsync()).ShouldBe(ControlContract.StateRunning);
    }

    [Test]
    public async Task A_client_origin_suspension_is_neither_stamped_nor_auto_resumed()
    {
        await ExecAsync(
            $"""
             INSERT INTO wallaby.control (scope, state, origin, requested_at, updated_at)
             VALUES ('wallaby', '{ControlContract.StateSuspendRequested}', '{ControlContract.OriginClient}', now(), now())
             """);
        var store = Store(graceFloor: TimeSpan.Zero);

        await store.HeartbeatConfigurationAssertionAsync(CancellationToken.None);
        (await AssertedAtAsync()).ShouldBeNull();

        (await store.ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeFalse();
        (await StateAsync()).ShouldBe(ControlContract.StateSuspendRequested);
    }

    [Test]
    public async Task A_missing_heartbeat_column_is_healed_by_the_heartbeat()
    {
        var store = Store();
        await store.RequestConfigurationSuspendAsync(null, CancellationToken.None);
        // A database last touched by a pre-heartbeat host: no column, and a ledger that doesn't know v3.
        await ExecAsync("ALTER TABLE wallaby.control DROP COLUMN configuration_asserted_at");
        await ExecAsync("DELETE FROM wallaby.schema_version WHERE version >= 3");

        await store.HeartbeatConfigurationAssertionAsync(CancellationToken.None);

        (await AssertedAtAsync()).ShouldNotBeNull();
    }

    [Test]
    public async Task A_missing_heartbeat_column_falls_back_to_the_ungraced_resume()
    {
        var store = Store();
        await store.RequestConfigurationSuspendAsync(null, CancellationToken.None);
        await ExecAsync("ALTER TABLE wallaby.control DROP COLUMN configuration_asserted_at");
        await ExecAsync("DELETE FROM wallaby.schema_version WHERE version >= 3");
        try
        {
            // No column means no heartbeats can exist: pre-heartbeat semantics, resume immediately.
            (await store.ResumeConfigurationSuspensionAsync(CancellationToken.None)).ShouldBeTrue();
            (await StateAsync()).ShouldBe(ControlContract.StateRunning);
        }
        finally
        {
            // Restore the schema for the tests that follow.
            await using var conn = await pg.DataSource.OpenConnectionAsync();
            await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
        }
    }

    private async Task ExecAsync(string sql)
    {
        await using var cmd = pg.DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<string?> StateAsync()
    {
        await using var cmd = pg.DataSource.CreateCommand("SELECT state FROM wallaby.control");
        return await cmd.ExecuteScalarAsync() as string;
    }

    private async Task<DateTimeOffset?> AssertedAtAsync()
    {
        await using var cmd = pg.DataSource.CreateCommand("SELECT configuration_asserted_at FROM wallaby.control");
        var value = await cmd.ExecuteScalarAsync();
        return value is DateTime dt ? new DateTimeOffset(dt.ToUniversalTime()) : value as DateTimeOffset?;
    }
}
