using System.Collections.Concurrent;
using System.Diagnostics;
using EFCore.CDC.TestInfrastructure;
using EFCore.CDC.TestModel;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace EFCore.CDC.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class TelemetryTests(PostgresFixture pg)
{
    [Test]
    public async Task Pipeline_emits_metrics_and_spans_for_live_changes()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString).Broadcast();
        var capture = harness.AddCaptureSink();

        using var changes = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.changes.received");
        using var lag = new MetricCollector<double>(harness.Instrumentation.Meter, "wallaby.ingestion.lag");

        var spanNames = new ConcurrentBag<string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == WallabyInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => spanNames.Add(activity.OperationName),
        };
        ActivitySource.AddActivityListener(listener);

        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "alpha");
        await harness.Db.AddProductAsync(categoryId, "beta");

        await harness.RunUntilAsync(() => capture.For("products").Count(r => r.Document is not null) >= 2);

        // Throughput + lag metrics were recorded for the live changes.
        await Assert.That(changes.GetMeasurementSnapshot().Sum(m => m.Value)).IsGreaterThanOrEqualTo(2L);
        await Assert.That(lag.GetMeasurementSnapshot()).IsNotEmpty();

        // A transaction root span and a sink-delivery span were emitted.
        await Assert.That(spanNames).Contains("transaction");
        await Assert.That(spanNames).Contains("sink.deliver");
    }

    [Test]
    public async Task Backfill_records_rows_copied_and_moves_the_active_gauge()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new CdcDocument { ["name"] = p.Name }, backfill: true);

        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductsAsync(categoryId, "b1", "b2", "b3");

        using var rows = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.backfill.rows");
        using var active = new MetricCollector<int>(harness.Instrumentation.Meter, "wallaby.backfill.active");

        await harness.StartAsync();
        await harness.RunBackfillAsync();
        await harness.WaitUntilAsync(() => capture.For("products").Count() >= 3);
        await harness.StopAsync();

        await Assert.That(rows.GetMeasurementSnapshot().Sum(m => m.Value)).IsGreaterThanOrEqualTo(3L);
        await Assert.That(active.GetMeasurementSnapshot().Any(m => m.Value == 1)).IsTrue();  // entered a backfill
    }

    [Test]
    public async Task Dependent_fanout_records_synthetic_changes()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        harness.AddCaptureSink();
        harness.Project<Product>("capture", destination: null, p => new CdcDocument { ["name"] = p.Name });
        harness.DependsOn<Product, Category?>(p => p.Category);

        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync("Cat");
        await harness.Db.AddProductAsync(categoryId, "P1");

        using var synthetic = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.dependent.synthetic");

        await harness.StartAsync();
        try
        {
            // Renaming the category alone fans out to the products that reference it.
            await harness.Db.SetCategoryNameAsync(categoryId, "Renamed");
            await harness.WaitUntilAsync(() => synthetic.GetMeasurementSnapshot().Sum(m => m.Value) >= 1);
        }
        finally
        {
            await harness.StopAsync();
        }

        await Assert.That(synthetic.GetMeasurementSnapshot().Sum(m => m.Value)).IsGreaterThanOrEqualTo(1L);
    }
}
