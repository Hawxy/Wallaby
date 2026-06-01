using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Pipeline;
using Wallaby.Sinks;

namespace EFCore.CDC.UnitTests;

public class SinkDispatcherTelemetryTests
{
    private static readonly ChangeMetadata Meta = new("public", "products", DateTimeOffset.UtcNow, 1, 0, false);

    private static IReadOnlyList<RoutedDocument> OneRecord() =>
        [new RoutedDocument("sink", new SinkRecord(Destination: "products", "1", Document: new CdcDocument { ["x"] = 1 }, IsDeletion: false, Meta))];

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

        await Assert.That(delivered.GetMeasurementSnapshot().Sum(m => m.Value)).IsEqualTo(1L);

        var durations = duration.GetMeasurementSnapshot();
        await Assert.That(durations.Count).IsEqualTo(1); // one delivery attempt
        await Assert.That(durations.Any(m => Equals(m.Tags.GetValueOrDefault("wallaby.delivery.outcome"), "success"))).IsTrue();
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
        await Assert.That(duration.GetMeasurementSnapshot().Count).IsEqualTo(3); // 2 retryable + 1 success
        await Assert.That(failures.GetMeasurementSnapshot().Sum(m => m.Value)).IsEqualTo(2L); // 2 retryable failures
    }

    [Test]
    public async Task Permanent_failure_records_a_failure_and_marks_the_span_as_error()
    {
        var instr = new WallabyInstrumentation();
        using var failures = new MetricCollector<long>(instr.Meter, "wallaby.sink.delivery.failures");

        Activity? captured = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == WallabyInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "sink.deliver")
                {
                    captured = activity;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Permanent("boom"))),
        };

        await Assert.That(async () =>
                await new SinkDispatcher(sinks, instrumentation: instr).DispatchAsync(OneRecord(), CancellationToken.None))
            .Throws<Exception>();

        await Assert.That(failures.GetMeasurementSnapshot().Sum(m => m.Value)).IsGreaterThanOrEqualTo(1L);
        await Assert.That(captured).IsNotNull();
        await Assert.That(captured!.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(captured.GetTagItem("wallaby.sink")).IsEqualTo("sink");
    }

    [Test]
    public async Task Dead_lettered_batch_records_a_dead_letter_failure()
    {
        var instr = new WallabyInstrumentation();
        using var failures = new MetricCollector<long>(instr.Meter, "wallaby.sink.delivery.failures");

        var sinks = new Dictionary<string, ISink>
        {
            ["sink"] = new DelegateSink("sink", (_, _) => Task.FromResult(DeliveryResult.Permanent("boom"))),
        };

        // skipFailedBatches: completes without throwing, dead-lettering the batch.
        await new SinkDispatcher(sinks, skipFailedBatches: true, instrumentation: instr)
            .DispatchAsync(OneRecord(), CancellationToken.None);

        var snapshot = failures.GetMeasurementSnapshot();
        await Assert.That(snapshot.Any(m => Equals(m.Tags.GetValueOrDefault("wallaby.delivery.outcome"), "dead_letter"))).IsTrue();
    }
}
