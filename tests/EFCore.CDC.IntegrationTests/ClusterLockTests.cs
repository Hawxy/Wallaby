using EFCore.CDC.Internal.Cluster;
using EFCore.CDC.TestInfrastructure;

namespace EFCore.CDC.IntegrationTests;

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

        await Assert.That(first).IsNotNull();
        await Assert.That(first!.IsHeld).IsTrue();
        await Assert.That(second).IsNull(); // contended

        // Release the leader; the standby can now take over.
        await first.DisposeAsync();
        await Assert.That(first.IsHeld).IsFalse();

        var third = await node2.TryAcquireAsync(key, CancellationToken.None);
        await Assert.That(third).IsNotNull();
        await third!.DisposeAsync();
    }

    [Test]
    public async Task Different_keys_do_not_contend()
    {
        var locker = new PostgresAdvisoryLock(pg.DataSource);

        var a = await locker.TryAcquireAsync($"a_{Guid.NewGuid():N}", CancellationToken.None);
        var b = await locker.TryAcquireAsync($"b_{Guid.NewGuid():N}", CancellationToken.None);

        await Assert.That(a).IsNotNull();
        await Assert.That(b).IsNotNull();

        await a!.DisposeAsync();
        await b!.DisposeAsync();
    }
}
