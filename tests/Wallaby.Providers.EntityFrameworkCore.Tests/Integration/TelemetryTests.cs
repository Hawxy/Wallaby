using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
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
        activities.OperationNames.ShouldContain("transaction.process");
        activities.OperationNames.ShouldContain("sink.deliver");
    }

    // Backfill metrics (wallaby.backfill.rows / wallaby.backfill.active) are asserted by
    // BackfillSchedulerIntegrationTests; dependent fan-out metrics (wallaby.dependent.synthetic)
    // by FanoutScalabilityTests — both ride flows those suites already run.

    [Test]
    public async Task Backfill_emits_a_root_span_linked_from_each_chunk()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var version = harness.Names.Suffix; // unique => isolates this table's shared backfill_state row
        harness.AddCaptureSink();
        harness.Project<Product>("capture", "products",
            p => new WallabyDocument { ["name"] = p.Name }, backfill: true, backfillVersion: version);

        var categoryId = await harness.Db.AddCategoryAsync();
        await harness.Db.AddProductAsync(categoryId, "alpha");

        using var activities = new ActivityCapture(harness.Instrumentation);

        await harness.SelfConfigureAsync();
        await harness.StartAsync();
        try
        {
            await harness.RunBackfillAsync(version);
        }
        finally
        {
            await harness.StopAsync();
        }

        // The run gets its own root span, tagged with the table and total rows.
        var root = activities.Last("backfill");
        root.ShouldNotBeNull();
        root.GetTagItem("wallaby.table").ShouldNotBeNull();
        // The shared fixture database may hold rows from other tests; at least our row was copied.
        ((long)root.GetTagItem("wallaby.backfill.rows")!).ShouldBeGreaterThanOrEqualTo(1L);

        // The chunk span lives in the delivering transaction's trace, linked back to the run.
        var chunk = activities.Last("backfill.chunk");
        chunk.ShouldNotBeNull();
        chunk.Links.ShouldContain(l => l.Context.TraceId == root.TraceId && l.Context.SpanId == root.SpanId);
    }
}
