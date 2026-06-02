using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Retry;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;

namespace Wallaby.Internal.Pipeline;

/// <summary>Raised internally to drive Polly retries on a retryable sink failure.</summary>
internal sealed class SinkRetryableException(string sinkName, string error, Exception? inner)
    : Exception($"Sink '{sinkName}' reported a retryable failure: {error}", inner);

/// <summary>Raised when a sink permanently fails (or retries are exhausted); stops the pipeline by default.</summary>
internal sealed class SinkDeliveryException(string sinkName, string error, Exception? inner)
    : Exception($"Sink '{sinkName}' failed to deliver: {error}", inner);

/// <summary>
/// Groups routed documents by sink (preserving commit order) and delivers each group as a
/// <see cref="SinkBatch"/>, retrying retryable failures with exponential backoff. Sinks are delivered
/// sequentially in v1; per-sink ordering is preserved.
/// </summary>
internal sealed class SinkDispatcher
{
    private readonly IReadOnlyDictionary<string, ISink> _sinks;
    private readonly bool _skipFailedBatches;
    private readonly ILogger _logger;
    private readonly WallabyInstrumentation _instr;
    private readonly ResiliencePipeline _retry;

    public SinkDispatcher(
        IReadOnlyDictionary<string, ISink> sinks, bool skipFailedBatches = false, ILogger? logger = null,
        WallabyInstrumentation? instrumentation = null)
    {
        _sinks = sinks;
        _skipFailedBatches = skipFailedBatches;
        _logger = logger ?? NullLogger.Instance;
        _instr = instrumentation ?? WallabyInstrumentation.NoOp;
        _retry = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<SinkRetryableException>(),
                MaxRetryAttempts = 10,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(200),
                MaxDelay = TimeSpan.FromMinutes(3),
            })
            .Build();
    }

    public async Task DispatchAsync(IReadOnlyList<RoutedDocument> routed, CancellationToken ct)
    {
        foreach (var (sinkName, records) in GroupBySinkPreservingOrder(routed))
        {
            if (!_sinks.TryGetValue(sinkName, out var sink))
            {
                throw new SinkDeliveryException(sinkName, "no sink is registered with this name", inner: null);
            }

            var batch = new SinkBatch(sinkName, records);
            using var activity = _instr.StartSinkDelivery();
            if (activity is not null)
            {
                activity.SetTag(WallabyInstrumentation.SinkTag, sinkName);
                activity.SetTag(WallabyInstrumentation.DestinationTag, records.Count > 0 ? records[0].Destination : null);
                activity.SetTag("wallaby.batch.size", records.Count);
            }

            try
            {
                await _retry.ExecuteAsync(async static (state, token) =>
                {
                    var attemptStart = WallabyInstrumentation.StartTimer();
                    var result = await state.Sink.DeliverAsync(state.SinkBatch, token);
                    switch (result.Status)
                    {
                        case DeliveryStatus.Success:
                            state.Instr.RecordSinkDelivery(state.SinkName, WallabyInstrumentation.DeliverySuccess, attemptStart);
                            state.Instr.RecordSinkRecordsDelivered(state.SinkName, state.SinkBatch.Records.Count);
                            return;
                        case DeliveryStatus.RetryableFailure:
                            state.Instr.RecordSinkDelivery(state.SinkName, WallabyInstrumentation.DeliveryRetryable, attemptStart);
                            state.Instr.RecordSinkFailure(state.SinkName, WallabyInstrumentation.DeliveryRetryable);
                            state.Activity?.AddEvent(new ActivityEvent("retry"));
                            throw new SinkRetryableException(state.SinkName, result.Error ?? "(unspecified)", result.Exception);
                        default:
                            state.Instr.RecordSinkDelivery(state.SinkName, WallabyInstrumentation.DeliveryPermanent, attemptStart);
                            state.Instr.RecordSinkFailure(state.SinkName, WallabyInstrumentation.DeliveryPermanent);
                            throw new SinkDeliveryException(state.SinkName, result.Error ?? "(unspecified)", result.Exception);
                    }
                }, (Sink: sink, SinkName: sinkName, SinkBatch: batch, Instr: _instr, Activity: activity), ct);
            }
            catch (Exception ex) when (ex is SinkRetryableException or SinkDeliveryException)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddException(ex);

                if (!_skipFailedBatches)
                {
                    throw;
                }

                _instr.RecordSinkFailure(sinkName, WallabyInstrumentation.DeliveryDeadLetter);
                _logger.DeadLettering(ex, records.Count, sinkName);
            }
        }
    }

    private static IEnumerable<(string SinkName, List<SinkRecord> Records)> GroupBySinkPreservingOrder(
        IReadOnlyList<RoutedDocument> routed)
    {
        var groups = new Dictionary<string, List<SinkRecord>>();
        var order = new List<string>();

        foreach (var item in routed)
        {
            if (!groups.TryGetValue(item.SinkName, out var list))
            {
                list = [];
                groups[item.SinkName] = list;
                order.Add(item.SinkName);
            }
            list.Add(item.Record);
        }

        foreach (var name in order)
        {
            yield return (name, groups[name]);
        }
    }
}

/// <summary>Source-generated log messages for <see cref="SinkDispatcher"/>.</summary>
internal static partial class SinkDispatcherLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Dead-lettering {Count} record(s) for sink '{Sink}' (DeadLetterPolicy=Skip).")]
    internal static partial void DeadLettering(this ILogger logger, Exception ex, int count, string sink);
}
