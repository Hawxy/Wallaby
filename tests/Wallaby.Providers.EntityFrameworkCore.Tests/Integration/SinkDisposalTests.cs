using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Proves the sink disposal capability end to end: a sink implementing <see cref="IAsyncDisposable"/>
/// is disposed exactly once when the node shuts down, after streaming has stopped.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SinkDisposalTests(TestModelPostgresFixture pg)
{
    /// <summary>A capture sink that records when it is disposed.</summary>
    private sealed class DisposableCaptureSink : ISink, IAsyncDisposable
    {
        private readonly CaptureSink _inner = new();

        public int DisposeCount { get; private set; }

        public string Name => _inner.Name;

        public Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct) =>
            _inner.DeliverAsync(batch, ct);

        public Task WaitForDocumentsAsync(IReadOnlyList<string> ids) => _inner.WaitForDocumentsAsync(ids);

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Sinks_are_disposed_once_at_node_shutdown()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var sink = new DisposableCaptureSink();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(s => s
                   .Map<Product>()
                   .ToDestination("products")
                   .UsingTransform(TestTransforms.ProductNames));
        });
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
        });
        services.ReplaceWallabySink("capture", sink);

        var db = new TestDatabase(pg.ConnectionString);
        await using (var node = await WallabyTestNode.StartAsync(services))
        {
            await WallabyReadiness.WaitForStreamingAsync(node.Services);
            var categoryId = await db.AddCategoryAsync();
            var productId = await db.AddProductAsync(categoryId, $"disposal_{names.Suffix}");
            await sink.WaitForDocumentsAsync([productId.ToString()]);

            // Streaming delivers while the sink is live; disposal only happens at shutdown.
            sink.DisposeCount.ShouldBe(0);
        }

        sink.DisposeCount.ShouldBe(1);
    }

    [Test]
    public async Task The_harness_disposes_sinks_it_materialized()
    {
        var sink = new DisposableCaptureSink();
        var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        try
        {
            harness.AddSink(sink)
                .Project<Product>(sink.Name, destination: null, p => new WallabyDocument { ["name"] = p.Name });

            await harness.SelfConfigureAsync();
            await harness.StartAsync();
            await harness.StopAsync();
            sink.DisposeCount.ShouldBe(0);
        }
        finally
        {
            // Harness disposal drops its own slot/publication.
            await harness.DisposeAsync();
        }

        sink.DisposeCount.ShouldBe(1);
    }
}
