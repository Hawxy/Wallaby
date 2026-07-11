using Wallaby.Internal;
using Wallaby.Internal.Cluster;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class ClusterLockTests(PostgresFixture pg)
{
    [Test]
    public async Task Only_one_holder_acquires_the_same_key()
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var node1 = new PostgresAdvisoryLock(pg.DataSource);
        var node2 = new PostgresAdvisoryLock(pg.DataSource);

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
    public async Task Lost_does_not_fire_while_the_lock_is_held()
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var locker = new PostgresAdvisoryLock(pg.DataSource);

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
    public async Task Different_keys_do_not_contend()
    {
        var locker = new PostgresAdvisoryLock(pg.DataSource);

        var a = await locker.TryAcquireAsync($"a_{Guid.NewGuid():N}", CancellationToken.None);
        var b = await locker.TryAcquireAsync($"b_{Guid.NewGuid():N}", CancellationToken.None);

        a.ShouldNotBeNull();
        b.ShouldNotBeNull();

        await a!.DisposeAsync();
        await b!.DisposeAsync();
    }

    [Test]
    public async Task Terminating_the_lock_connections_backend_fires_lost()
    {
        var key = $"slot_{Guid.NewGuid():N}";
        var locker = new PostgresAdvisoryLock(pg.DataSource);

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
}
