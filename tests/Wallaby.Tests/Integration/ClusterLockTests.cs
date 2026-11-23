using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Npgsql;
using Wallaby.Internal;
using Wallaby.Internal.Cluster;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ClusterLockTests(PostgresFixture pg)
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Only_one_holder_acquires_the_same_key(bool transactional)
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var node1 = new PostgresAdvisoryLock(pg.DataSource, transactional);
        var node2 = new PostgresAdvisoryLock(pg.DataSource, transactional);

        var first = await node1.TryAcquireAsync(key, CancellationToken.None);
        var second = await node2.TryAcquireAsync(key, CancellationToken.None);

        first.ShouldNotBeNull();
        first!.IsHeld.ShouldBeTrue();
        second.ShouldBeNull(); // contended

        // Release the leader; the standby can now take over.
        await first.DisposeAsync();
        first.IsHeld.ShouldBeFalse();

        var third = await node2.TryAcquireAsync(key, CancellationToken.None);
        third.ShouldNotBeNull();
        await third!.DisposeAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Different_keys_do_not_contend(bool transactional)
    {
        var locker = new PostgresAdvisoryLock(pg.DataSource, transactional);

        var a = await locker.TryAcquireAsync($"a_{Guid.NewGuid():N}", CancellationToken.None);
        var b = await locker.TryAcquireAsync($"b_{Guid.NewGuid():N}", CancellationToken.None);

        a.ShouldNotBeNull();
        b.ShouldNotBeNull();

        await a!.DisposeAsync();
        await b!.DisposeAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Releasing_frees_the_lock_before_the_connection_returns_to_the_pool(bool transactional)
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var locker = new PostgresAdvisoryLock(pg.DataSource, transactional);

        await using var handle = await locker.TryAcquireAsync(key, CancellationToken.None);
        handle.ShouldNotBeNull();
        (await CountHoldersAsync(key)).ShouldBe(1);

        await handle!.DisposeAsync();

        // A session lock survives on a pooled connection until it is reused, so it must be unlocked explicitly
        (await CountHoldersAsync(key)).ShouldBe(0);
    }

    [Test]
    [Arguments(false, 0)]
    [Arguments(true, 0)]
    [Arguments(false, 1)]
    [Arguments(true, 1)]
    public async Task Monitoring_keeps_the_lock_until_its_backend_is_terminated(bool transactional, int npgsqlKeepAlive)
    {
        var applicationName = $"lock_{Guid.NewGuid():N}";
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString)
        {
            ApplicationName = applicationName,
            // transaction_timeout exists from PostgreSQL 17
            Options = await ServerMajorVersionAsync() >= 17
                ? "-c statement_timeout=500 -c transaction_timeout=500"
                : "-c statement_timeout=500",
            KeepAlive = npgsqlKeepAlive,
        };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var logger = new FakeLogger();
        var locker = new PostgresAdvisoryLock(dataSource, transactional, logger);
        var key = $"slot_{Guid.NewGuid():N}";

        var handle = await locker.TryAcquireAsync(key, CancellationToken.None);
        handle.ShouldNotBeNull();
        try
        {
            // Passive monitoring leaves the session idle; with Npgsql KeepAlive on it parks pg_sleep instead
            await Polling.UntilAsync(
                async () =>
                {
                    var (state, query) = await SessionActivityAsync(applicationName);
                    return npgsqlKeepAlive == 0
                        ? state.StartsWith("idle", StringComparison.Ordinal) && !query.Contains("pg_sleep")
                        : state == "active" && query.Contains("pg_sleep");
                },
                TimeSpan.FromSeconds(10));

            // Past the timeouts, which must cut neither a monitoring sleep nor the lock's transaction short
            await Task.Delay(TimeSpan.FromSeconds(1));
            handle!.Lost.IsCancellationRequested.ShouldBeFalse();
            handle.IsHeld.ShouldBeTrue();

            await PgExec.ExecuteAsync(
                pg.DataSource,
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = @a",
                CancellationToken.None,
                ("a", applicationName));

            await Polling.UntilAsync(() => handle.Lost.IsCancellationRequested, TimeSpan.FromSeconds(10));
            handle.IsHeld.ShouldBeFalse();

            // The cause is logged before Lost fires
            var lost = logger.Collector.GetSnapshot().ShouldHaveSingleItem();
            lost.Level.ShouldBe(LogLevel.Warning);
            lost.Message.ShouldContain(key);
            lost.Exception.ShouldNotBeNull();
        }
        finally
        {
            await handle!.DisposeAsync();
        }
    }

#if !NET11_0_OR_GREATER
    [Test]
    public async Task Transactional_lock_excludes_and_releases_over_multiplexing()
    {
        // Npgsql 11 removed multiplexing
        var builder = new NpgsqlConnectionStringBuilder(pg.ConnectionString) { Multiplexing = true };
        await using var node1Source = NpgsqlDataSource.Create(builder.ConnectionString);
        await using var node2Source = NpgsqlDataSource.Create(builder.ConnectionString);
        var node1 = new PostgresAdvisoryLock(node1Source, transactional: true);
        var node2 = new PostgresAdvisoryLock(node2Source, transactional: true);
        var key = $"slot_{Guid.NewGuid():N}";

        await using var first = await node1.TryAcquireAsync(key, CancellationToken.None);
        first.ShouldNotBeNull();
        node1.MonitorsPassively.ShouldBeFalse();
        (await node2.TryAcquireAsync(key, CancellationToken.None)).ShouldBeNull();

        // Held through monitoring
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        first!.IsHeld.ShouldBeTrue();
        (await CountHoldersAsync(key)).ShouldBe(1);

        await first.DisposeAsync();
        (await CountHoldersAsync(key)).ShouldBe(0);

        await using var second = await node2.TryAcquireAsync(key, CancellationToken.None);
        second.ShouldNotBeNull();
    }
#endif

    private async Task<int> ServerMajorVersionAsync()
    {
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        return connection.PostgreSqlVersion.Major;
    }

    private async Task<long> CountHoldersAsync(string key)
    {
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        return await PgExec.ScalarLongAsync(
            connection,
            """
            SELECT count(*) FROM pg_locks
            WHERE locktype = 'advisory' AND classid::bigint = @c AND objid::bigint = @o
              AND objsubid = 1 AND granted
            """,
            CancellationToken.None,
            KeyParameters(key));
    }

    private async Task<(string State, string Query)> SessionActivityAsync(string applicationName)
    {
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            "SELECT state, query FROM pg_stat_activity WHERE application_name = @a",
            connection);
        command.Parameters.AddWithValue("a", applicationName);
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();
        var activity = (reader.GetString(0), reader.GetString(1));
        (await reader.ReadAsync()).ShouldBeFalse(); // one lock connection
        return activity;
    }

    // pg_try_advisory_lock(bigint) registers as classid = high 32 bits, objid = low 32, objsubid = 1.
    private static (string, object?)[] KeyParameters(string key)
    {
        var lockKey = PostgresAdvisoryLock.StableKey(key);
        return [("c", (long)((ulong)lockKey >> 32)), ("o", (long)((ulong)lockKey & 0xFFFFFFFF))];
    }
}
