using Wallaby.Abstractions;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;

namespace Wallaby.EntityFrameworkCore.IntegrationTests;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class KeepaliveTests(TestModelPostgresFixture pg)
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

    [Test]
    public async Task Keepalives_during_slow_processing_do_not_break_streaming()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast();
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
}
