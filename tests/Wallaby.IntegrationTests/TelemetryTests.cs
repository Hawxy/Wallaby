using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Diagnostics;
using Wallaby.TestInfrastructure;

namespace Wallaby.IntegrationTests;

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

    // Backfill metrics (wallaby.backfill.rows / wallaby.backfill.active) are asserted by
    // BackfillSchedulerIntegrationTests; dependent fan-out metrics (wallaby.dependent.synthetic)
    // by FanoutScalabilityTests — both ride flows those suites already run.
}
