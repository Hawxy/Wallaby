using System.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.State;
using Wallaby.Model;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Integration;

/// <summary>
/// The fan-out queue wakes its worker on demand via LISTEN/NOTIFY (instead of polling every second):
/// <see cref="PostgresFanoutQueueStore.EnqueueAsync"/> notifies, and a subscription's wait returns the instant
/// the notification arrives, falling back to a timed poll otherwise.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class FanoutQueueNotifyTests(PostgresFixture pg)
{
    private static readonly CapturedTable Table = new()
    {
        EntityClrType = typeof(object),
        Schema = "public",
        TableName = "products",
        Columns = [],
        PrimaryKey = [],
    };

    [Test]
    public async Task Enqueue_wakes_the_subscription_via_notify()
    {
        await EnsureSchemaAsync();
        var store = new PostgresFanoutQueueStore(pg.DataSource);

        await using var subscription = store.Subscribe();
        // Prime the subscription so it is LISTENing before we enqueue — otherwise the NOTIFY is missed.
        await subscription.WaitAsync(TimeSpan.FromMilliseconds(100), CancellationToken.None);

        await store.EnqueueAsync(new ScopedFanoutSpec(Table, ["category_id"], [new object?[] { 1 }]), CancellationToken.None);

        var stopwatch = Stopwatch.StartNew();
        await subscription.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        stopwatch.Stop();

        // Woken by the NOTIFY, not the 30s fallback poll.
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Wait_returns_after_the_fallback_when_no_job_is_enqueued()
    {
        await EnsureSchemaAsync();
        var store = new PostgresFanoutQueueStore(pg.DataSource);

        await using var subscription = store.Subscribe();

        var stopwatch = Stopwatch.StartNew();
        await subscription.WaitAsync(TimeSpan.FromMilliseconds(500), CancellationToken.None);
        stopwatch.Stop();

        // No notification arrived, so it returns at ~the fallback — neither instantly nor hanging.
        stopwatch.Elapsed.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(400));
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));
    }

    private async Task EnsureSchemaAsync()
    {
        await using var conn = await pg.DataSource.OpenConnectionAsync();
        await new StateSchemaBootstrapper().EnsureAsync(conn, CancellationToken.None);
    }
}
