using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Pipeline;
using Wallaby.Sinks;
using Wallaby.TestInfrastructure;

namespace Wallaby.Tests.Unit.OpenTelemetry;

public class SinkDispatcherTelemetryTests
{
    private static readonly ChangeMetadata Meta = new("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, false);

    private static IReadOnlyList<RoutedDocument> OneRecord() =>
        [new RoutedDocument("sink", new SinkRecord(Destination: "products", "1", Document: new WallabyDocument { ["x"] = 1 }, IsDeletion: false, Meta))];

    [Test]
    public async Task Successful_delivery_records_records_delivered_and_duration()
    {
        var instr = new WallabyInstrumentation();
        using var delivered = new MetricCollector<long>(instr.Meter, "wallaby.sink.records.delivered");
        using var duration = new MetricCollector<double>(instr.Meter, "wallaby.sink.delivery.duration");

        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)),
        };

        await new SinkDispatcher(sinks, instrumentation: instr).DispatchAsync(OneRecord(), CancellationToken.None);

        delivered.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBe(1L);

        var durations = duration.GetMeasurementSnapshot();
        durations.Count.ShouldBe(1); // one delivery attempt
        durations.Any(m => Equals(m.Tags.GetValueOrDefault("wallaby.delivery.outcome"), "success")).ShouldBeTrue();
    }

    [Test]
    public async Task Retryable_then_success_records_every_attempt_and_each_retryable_failure()
    {
        var instr = new WallabyInstrumentation();
        using var duration = new MetricCollector<double>(instr.Meter, "wallaby.sink.delivery.duration");
        using var failures = new MetricCollector<long>(instr.Meter, "wallaby.sink.delivery.failures");

        var calls = 0;
        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) =>
            {
                calls++;
                return Task.FromResult(calls < 3 ? DeliveryResult.Retry("temporary") : DeliveryResult.Success);
            }),
        };

        await new SinkDispatcher(sinks, instrumentation: instr).DispatchAsync(OneRecord(), CancellationToken.None);

        // The delivery-duration histogram is recorded once per attempt, so its count is the attempt count.
        duration.GetMeasurementSnapshot().Count.ShouldBe(3); // 2 retryable + 1 success
        failures.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBe(2L); // 2 retryable failures
    }

    [Test]
    public async Task Successful_delivery_updates_the_sink_delivery_lag_gauge_and_status()
    {
        using var instr = new WallabyInstrumentation();
        using var lag = new MetricCollector<double>(instr.Meter, "wallaby.sink.delivery.lag");
        var status = new WallabyStatus();

        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Success)),
        };

        var before = DateTimeOffset.UtcNow;
        await new SinkDispatcher(sinks, instrumentation: instr, status: status)
            .DispatchAsync(OneRecord(), CancellationToken.None);

        lag.RecordObservableInstruments();
        var measurement = lag.LastMeasurement;
        measurement.ShouldNotBeNull();
        measurement.Value.ShouldBeGreaterThanOrEqualTo(0);
        measurement.Tags.GetValueOrDefault("wallaby.sink").ShouldBe("sink");

        status.Current.LastSinkDeliveryAt.ShouldContainKey("sink");
        status.Current.LastSinkDeliveryAt["sink"].ShouldBeGreaterThanOrEqualTo(before);
    }

    [Test]
    public async Task Failed_delivery_does_not_report_sink_delivery_lag()
    {
        using var instr = new WallabyInstrumentation();
        using var lag = new MetricCollector<double>(instr.Meter, "wallaby.sink.delivery.lag");
        var status = new WallabyStatus();

        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Permanent("boom"))),
        };

        await Should.ThrowAsync<Exception>(
            async () => await new SinkDispatcher(sinks, instrumentation: instr, status: status)
                .DispatchAsync(OneRecord(), CancellationToken.None));

        lag.RecordObservableInstruments();
        lag.GetMeasurementSnapshot().ShouldBeEmpty();
        status.Current.LastSinkDeliveryAt.ShouldBeEmpty();
    }

    [Test]
    public async Task Permanent_failure_records_a_failure_and_marks_the_span_as_error()
    {
        var instr = new WallabyInstrumentation();
        using var failures = new MetricCollector<long>(instr.Meter, "wallaby.sink.delivery.failures");
        using var activities = new ActivityCapture(instr);

        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Permanent("boom"))),
        };

        await Should.ThrowAsync<Exception>(
            async () => await new SinkDispatcher(sinks, instrumentation: instr).DispatchAsync(OneRecord(), CancellationToken.None));

        failures.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBeGreaterThanOrEqualTo(1L);
        var captured = activities.Last("sink.deliver");
        captured.ShouldNotBeNull();
        captured!.Status.ShouldBe(ActivityStatusCode.Error);
        captured.GetTagItem("wallaby.sink").ShouldBe("sink");
    }
}
