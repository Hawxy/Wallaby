using EFCore.CDC.TestModel;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;

namespace Wallaby.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class SinkInitializerTests(PostgresFixture pg)
{
    /// <summary>A sink that records how many times its one-time initializer ran.</summary>
    private sealed class InitCountingSink : ISink, ISinkInitializer
    {
        public int InitCount { get; private set; }
        public string Name => "init-stub";

        public Task InitializeAsync(CancellationToken ct)
        {
            InitCount++;
            return Task.CompletedTask;
        }

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
            => Task.FromResult(DeliveryResult.Success);
    }

    [Test]
    public async Task Sink_initializer_runs_once_on_start()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var sink = new InitCountingSink();
        harness.AddSink(sink)
            .Project<Product>("init-stub", destination: null, p => new CdcDocument { ["name"] = p.Name });

        await harness.SelfConfigureAsync();
        await harness.StartAsync(); // awaits ISinkInitializer.InitializeAsync before streaming
        try
        {
            await Assert.That(sink.InitCount).IsEqualTo(1);
        }
        finally
        {
            await harness.StopAsync();
        }
    }
}
