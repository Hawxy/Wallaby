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
    public async Task Lost_does_not_fire_while_the_lock_is_held(bool transactional)
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var locker = new PostgresAdvisoryLock(pg.DataSource, transactional);

        var handle = await locker.TryAcquireAsync(key, CancellationToken.None);
        handle.ShouldNotBeNull();
        try
        {
            // The connection monitor is active; a healthy connection must not be reported as lost.
            await Task.Delay(TimeSpan.FromMilliseconds(250));

            handle!.Lost.IsCancellationRequested.ShouldBeFalse();
            handle.IsHeld.ShouldBeTrue();
        }
        finally
        {
            await handle!.DisposeAsync();
        }
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
    [Arguments(false, 1)]
    [Arguments(true, 2)]
    public async Task Held_locks_share_a_connection_only_when_multiplexing(bool transactional, int expectedBackends)
    {
        // A dedicated data source, since the shared one may already have several pooled multiplexed connections
        await using var dataSource = NpgsqlDataSource.Create(pg.ConnectionString);
        var locker = new PostgresAdvisoryLock(dataSource, transactional);
        var keys = new[] { $"a_{Guid.NewGuid():N}", $"b_{Guid.NewGuid():N}" };

        var a = await locker.TryAcquireAsync(keys[0], CancellationToken.None);
        var b = await locker.TryAcquireAsync(keys[1], CancellationToken.None);
        try
        {
            a.ShouldNotBeNull();
            b.ShouldNotBeNull();

            (await CountBackendsHoldingAsync(keys)).ShouldBe(expectedBackends);
        }
        finally
        {
            if (a is not null) await a.DisposeAsync();
            if (b is not null) await b.DisposeAsync();
        }
    }

    [Test]
    public async Task Losing_the_shared_connection_fires_lost_for_every_multiplexed_lock()
    {
        // A dedicated data source, so both locks land on the same connection
        await using var dataSource = NpgsqlDataSource.Create(pg.ConnectionString);
        var locker = new PostgresAdvisoryLock(dataSource);
        var keys = new[] { $"a_{Guid.NewGuid():N}", $"b_{Guid.NewGuid():N}" };

        var a = await locker.TryAcquireAsync(keys[0], CancellationToken.None);
        var b = await locker.TryAcquireAsync(keys[1], CancellationToken.None);
        try
        {
            a.ShouldNotBeNull();
            b.ShouldNotBeNull();

            // Terminating the one backend holding the first lock drops both
            await PgExec.ExecuteAsync(
                pg.DataSource,
                """
                SELECT pg_terminate_backend(pid) FROM pg_locks
                WHERE locktype = 'advisory' AND classid::bigint = @c AND objid::bigint = @o
                  AND objsubid = 1 AND granted
                """,
                CancellationToken.None,
                KeyParameters(keys[0]));

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (!a!.Lost.IsCancellationRequested || !b!.Lost.IsCancellationRequested)
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("Lost did not fire for every lock on the terminated connection.");
                }
                await Task.Delay(50);
            }

            a.IsHeld.ShouldBeFalse();
            b.IsHeld.ShouldBeFalse();

            // Both locks were released server side, so another node can take either
            var otherNode = new PostgresAdvisoryLock(pg.DataSource);
            foreach (var key in keys)
            {
                var handle = await otherNode.TryAcquireAsync(key, CancellationToken.None);
                handle.ShouldNotBeNull();
                await handle!.DisposeAsync();
            }
        }
        finally
        {
            if (a is not null) await a.DisposeAsync();
            if (b is not null) await b.DisposeAsync();
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Terminating_the_lock_connections_backend_fires_lost(bool transactional)
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var locker = new PostgresAdvisoryLock(pg.DataSource, transactional);

        var handle = await locker.TryAcquireAsync(key, CancellationToken.None);
        handle.ShouldNotBeNull();
        try
        {
            // pg_try_advisory_lock(bigint) registers as classid = high 32 bits, objid = low 32, objsubid = 1.
            var lockKey = PostgresAdvisoryLock.StableKey(key);
            await PgExec.ExecuteAsync(
                pg.DataSource,
                """
                SELECT pg_terminate_backend(pid) FROM pg_locks
                WHERE locktype = 'advisory' AND classid::bigint = @c AND objid::bigint = @o
                  AND objsubid = 1 AND granted
                """,
                CancellationToken.None,
                ("c", (long)((ulong)lockKey >> 32)), ("o", (long)((ulong)lockKey & 0xFFFFFFFF)));

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (!handle!.Lost.IsCancellationRequested)
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("Lost did not fire after the lock backend was terminated.");
                }
                await Task.Delay(50);
            }

            handle.IsHeld.ShouldBeFalse();
        }
        finally
        {
            await handle!.DisposeAsync();
        }
    }

    private async Task<long> CountBackendsHoldingAsync(string[] keys)
    {
        var pids = new HashSet<long>();
        await using var connection = await pg.DataSource.OpenConnectionAsync();
        foreach (var key in keys)
        {
            pids.Add(await PgExec.ScalarLongAsync(
                connection,
                """
                SELECT pid FROM pg_locks
                WHERE locktype = 'advisory' AND classid::bigint = @c AND objid::bigint = @o
                  AND objsubid = 1 AND granted
                """,
                CancellationToken.None,
                KeyParameters(key)));
        }
        return pids.Count;
    }

    // pg_try_advisory_lock(bigint) registers as classid = high 32 bits, objid = low 32, objsubid = 1.
    private static (string, object?)[] KeyParameters(string key)
    {
        var lockKey = PostgresAdvisoryLock.StableKey(key);
        return [("c", (long)((ulong)lockKey >> 32)), ("o", (long)((ulong)lockKey & 0xFFFFFFFF))];
    }
}
