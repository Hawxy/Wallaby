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
        var locker = new PostgresAdvisoryLock(pg.DataSource, TimeSpan.FromMilliseconds(50));

        var handle = await locker.TryAcquireAsync(key, CancellationToken.None);
        handle.ShouldNotBeNull();
        try
        {
            // Several heartbeat probes elapse; a healthy connection must not be reported as lost.
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
}
