using Wallaby.Abstractions;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Keepalive behaviour while the consumer is stuck in sink delivery and not reading the replication
/// stream. The fixture's <c>wal_sender_timeout=2s</c> means the server disconnects a client that goes
/// silent — a delivery slower than that only survives because keepalives flow while it runs.
/// </summary>
[NotInParallel]
[ClassDataSource<ShortWalSenderTimeoutPostgresFixture>(Shared = SharedType.PerTestSession)]
public class KeepaliveTests(ShortWalSenderTimeoutPostgresFixture pg)
{
    /// <summary>A sink whose delivery is slow enough that several keepalives fire while the consumer is
    /// not reading the replication stream.</summary>
    private sealed class SlowSink(string name, TimeSpan delay) : ISink
    {
        public string Name => name;
        public int Delivered { get; private set; }

        public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
        {
            await Task.Delay(delay, ct);
            Delivered += batch.Records.Count;
            return DeliveryResult.Success;
        }
    }

    /// <summary>A sink that never completes a delivery on its own but honors cancellation.</summary>
    private sealed class HungSink(string name) : ISink
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Name => name;
        public Task Entered => _entered.Task;

        public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
        {
            _entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return DeliveryResult.Success; // unreachable
        }
    }

    [Test]
    public async Task Keepalives_during_slow_processing_do_not_break_streaming()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var sink = new SlowSink("slow", TimeSpan.FromMilliseconds(400));
        harness.AddSink(sink);
        // Fire keepalives well within the slow delivery so the in-flight status-update path is exercised
        // against the real connection while the stream isn't being read.
        harness.KeepaliveInterval = TimeSpan.FromMilliseconds(50);
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "alpha");

        // If a keepalive corrupted the replication protocol, the pipeline would fault and this would time out.
        await harness.RunUntilAsync(() => sink.Delivered >= 1);

        sink.Delivered.ShouldBeGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task A_delivery_slower_than_the_walsender_timeout_survives_and_streaming_continues()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var sink = new SlowSink("slow", TimeSpan.FromSeconds(5));
        harness.AddSink(sink);
        harness.KeepaliveInterval = TimeSpan.FromMilliseconds(250);
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "slow-one");

        await harness.StartAsync();
        try
        {
            // The 5s delivery far outlives wal_sender_timeout=2s; only the keepalives sent while the
            // stream isn't being read keep the server from disconnecting the walsender.
            await harness.WaitUntilAsync(() => sink.Delivered >= 1, TimeSpan.FromSeconds(30));

            // And the connection is still healthy afterwards: a second change streams through.
            await harness.Db.AddProductAsync(categoryId, "slow-two");
            await harness.WaitUntilAsync(() => sink.Delivered >= 2, TimeSpan.FromSeconds(30));
        }
        finally
        {
            await harness.StopAsync();
        }
    }

    [Test]
    public async Task Stopping_while_a_sink_hangs_completes_cleanly()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var sink = new HungSink("hung");
        harness.AddSink(sink);
        harness.KeepaliveInterval = TimeSpan.FromMilliseconds(250);
        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "stuck");

        await harness.StartAsync();
        await sink.Entered.WaitAsync(TimeSpan.FromSeconds(30));

        // Cancellation must unwind the hung delivery, the keepalive loop, and the retry pipeline —
        // a stop that hangs here means shutdown is held hostage by a stuck sink.
        var stop = harness.StopAsync();
        var finished = await Task.WhenAny(stop, Task.Delay(TimeSpan.FromSeconds(10)));

        finished.ShouldBe(stop);
        await stop; // surfaces a fault if the cancellation was misclassified
    }
}
