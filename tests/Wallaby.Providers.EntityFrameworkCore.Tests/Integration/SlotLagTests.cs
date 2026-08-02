using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// A leader session samples the WAL bytes the server retains for its slot and publishes them as the
/// <c>wallaby.slot.retained_wal</c> gauge.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class SlotLagTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task The_leader_publishes_the_retained_wal_gauge()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var capture = new CaptureSink();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString(pg.ConnectionString)
            .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
            .WithMappings(sink => sink.Map<Product>()
                .ToDestination("products")
                .UsingTransform(TestTransforms.ProductNames)));
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
            o.Advanced.SlotLagSampleInterval = TimeSpan.FromMilliseconds(200);
        });
        services.ReplaceWallabySink("capture", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        var instrumentation = node.Services.GetRequiredService<WallabyInstrumentation>();
        using var gauge = new MetricCollector<long>(instrumentation.Meter, "wallaby.slot.retained_wal");

        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        // Observable gauges are pull-based: poll until the sampler's first cached sample is visible.
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        while (gauge.GetMeasurementSnapshot().Count == 0 && DateTimeOffset.UtcNow < deadline)
        {
            gauge.RecordObservableInstruments();
            await Task.Delay(100);
        }

        var snapshot = gauge.GetMeasurementSnapshot();
        snapshot.ShouldNotBeEmpty();
        snapshot[^1].Value.ShouldBeGreaterThanOrEqualTo(0L);
        snapshot[^1].Tags["wallaby.slot"].ShouldBe(names.Slot);
    }
}
