using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Testing;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestModel;
using Wallaby.TraceDemo;

// Runs a curated CDC scenario against a throwaway Postgres and exports every Wallaby span/metric to a
// local Aspire Dashboard, so the traces (transaction trees, backfill span links, sink retries) can be
// explored interactively. Not a test: dotnet run --project tests/Wallaby.TraceDemo
var timeout = TimeSpan.FromSeconds(60);

var dashboard = new AspireDashboard();
Console.WriteLine("Starting Aspire Dashboard container (reused across runs)...");
await dashboard.StartAsync();

Console.WriteLine("Starting Postgres container...");
var pg = new TestModelPostgresFixture();
await pg.InitializeAsync();

try
{
    var resource = ResourceBuilder.CreateDefault().AddService("wallaby-trace-demo");
    using var tracing = Sdk.CreateTracerProviderBuilder()
        .SetResourceBuilder(resource)
        .AddSource(WallabyInstrumentation.ActivitySourceName)
        .AddSource("Npgsql") // enrichment queries nest under Wallaby's transform spans
        .SetSampler(new AlwaysOnSampler())
        .AddOtlpExporter(o => o.Endpoint = new Uri(dashboard.OtlpEndpoint))
        .Build();
    using var metrics = Sdk.CreateMeterProviderBuilder()
        .SetResourceBuilder(resource)
        .AddMeter(WallabyInstrumentation.MeterName)
        .AddOtlpExporter(o => o.Endpoint = new Uri(dashboard.OtlpEndpoint))
        .Build();

    await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
    var capture = new CaptureSink();
    harness.AddSink(new RetryOnceSink(capture));
    harness
        .Project<Product>("capture", destination: null,
            p => new WallabyDocument { ["name"] = p.Name, ["price"] = p.Price, ["sku"] = p.Sku },
            backfill: true, backfillVersion: harness.Names.Suffix)
        .DependsOn<Product, Category?>(p => p.Category);

    int Delivered() => capture.For("products").Count();

    // Seeded before self-config, so these 20 rows arrive via the backfill below, not the live stream.
    Console.WriteLine("Seeding a category with 20 products...");
    var categoryId = await harness.Db.AddCategoryAsync("Electronics");
    await harness.Db.AddProductsAsync(categoryId, [.. Enumerable.Range(1, 20).Select(i => $"product-{i:00}")]);

    await harness.SelfConfigureAsync();
    await harness.StartAsync();

    Console.WriteLine("Live insert/update/delete (the first delivery fails retryably, so its sink.deliver span shows a retry)...");
    var liveId = await harness.Db.AddProductAsync(categoryId, "product-live-1", 9.99m);
    var doomedId = await harness.Db.AddProductAsync(categoryId, "product-live-2", 19.99m);
    await harness.WaitUntilAsync(() => Delivered() >= 2, timeout);

    await harness.Db.UpdateProductNameAsync(liveId, "product-live-1-renamed");
    await harness.WaitUntilAsync(() => Delivered() >= 3, timeout);

    await harness.Db.DeleteProductAsync(doomedId);
    await harness.WaitUntilAsync(() => Delivered() >= 4, timeout);

    Console.WriteLine("Dependent fan-out: renaming the category re-emits its 21 products...");
    await harness.Db.SetCategoryNameAsync(categoryId, "Electronics & Gadgets");
    await harness.DrainFanoutAsync();
    await harness.WaitUntilAsync(() => Delivered() >= 25, timeout);

    Console.WriteLine("Whole-table backfill of products (backfill root span + linked backfill.chunk spans)...");
    await harness.RunBackfillAsync();
    await harness.WaitUntilAsync(() => Delivered() >= 46, timeout);

    await harness.StopAsync();
    Console.WriteLine($"Scenario complete: {Delivered()} records delivered.");

    tracing.ForceFlush();
    metrics.ForceFlush();
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Trace demo failed: {ex}");
    return 1;
}
finally
{
    // The dashboard is intentionally not disposed: stopping it would discard the exported traces.
    await pg.DisposeAsync();
}

Console.WriteLine();
Console.WriteLine($"Traces:  {dashboard.UiUrl}/traces");
Console.WriteLine($"Metrics: {dashboard.UiUrl}/metrics");
Console.WriteLine("The dashboard container keeps running; remove it with: docker rm -f wallaby-trace-dashboard");
return 0;

/// <summary>Fails the first delivery retryably (to make a sink retry visible in the trace), then delegates.</summary>
file sealed class RetryOnceSink(ISink inner) : ISink
{
    private int _calls;

    public string Name => inner.Name;

    public async Task<DeliveryResult> DeliverAsync(SinkBatch batch, CancellationToken ct)
        => Interlocked.Increment(ref _calls) == 1
            ? DeliveryResult.Retry("simulated transient failure (trace demo)")
            : await inner.DeliverAsync(batch, ct);
}
