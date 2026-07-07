using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class TelemetryTests(TestModelPostgresFixture pg)
{
    [Test]
    public async Task Pipeline_emits_metrics_and_spans_for_live_changes()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString).Broadcast().Capture<Product>();
        var capture = harness.AddCaptureSink();

        using var changes = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.changes.received");
        using var lag = new MetricCollector<double>(harness.Instrumentation.Meter, "wallaby.ingestion.lag");

        using var activities = new ActivityCapture(harness.Instrumentation);

        await harness.SelfConfigureAsync();

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "alpha");
        await harness.Db.AddProductAsync(categoryId, "beta");

        await harness.RunUntilAsync(() => capture.For("products").Count(r => r.Document is not null) >= 2);

        // Throughput + lag metrics were recorded for the live changes.
        changes.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBeGreaterThanOrEqualTo(2L);
        lag.GetMeasurementSnapshot().ShouldNotBeEmpty();

        // A transaction root span and a sink-delivery span were emitted.
        activities.OperationNames.ShouldContain("transaction");
        activities.OperationNames.ShouldContain("sink.deliver");
    }

    // Backfill metrics (wallaby.backfill.rows / wallaby.backfill.active) are asserted by
    // BackfillSchedulerIntegrationTests; dependent fan-out metrics (wallaby.dependent.synthetic)
    // by FanoutScalabilityTests — both ride flows those suites already run.
}
