using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace Wallaby.Tests.Unit;

/// <summary>The backfill progress gauges mirror the running whole-table backfill and go silent when none runs.</summary>
public class BackfillProgressGaugeTests
{
    [Test]
    public void Gauges_report_the_running_backfill_and_nothing_otherwise()
    {
        using var instrumentation = new WallabyInstrumentation();
        using var copied = new MetricCollector<long>(instrumentation.Meter, "wallaby.backfill.rows_copied");
        using var estimated = new MetricCollector<long>(instrumentation.Meter, "wallaby.backfill.rows_estimated");

        copied.RecordObservableInstruments();
        copied.GetMeasurementSnapshot().ShouldBeEmpty();

        instrumentation.RecordActiveBackfill(new WallabyBackfillProgress("public.orders", 250, 1_000, DateTimeOffset.UtcNow));
        copied.RecordObservableInstruments();
        estimated.RecordObservableInstruments();

        var rows = copied.LastMeasurement.ShouldNotBeNull();
        rows.Value.ShouldBe(250);
        rows.Tags["wallaby.table"].ShouldBe("public.orders");
        estimated.LastMeasurement.ShouldNotBeNull().Value.ShouldBe(1_000);

        instrumentation.RecordActiveBackfill(new WallabyBackfillProgress("public.orders", 300, null, DateTimeOffset.UtcNow));
        estimated.Clear();
        estimated.RecordObservableInstruments();
        estimated.GetMeasurementSnapshot().ShouldBeEmpty();

        instrumentation.RecordActiveBackfill(null);
        copied.Clear();
        copied.RecordObservableInstruments();
        copied.GetMeasurementSnapshot().ShouldBeEmpty();
    }
}
