using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Retry;
using Wallaby.Abstractions;

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
    private readonly ResiliencePipeline _retry;

    public SinkDispatcher(IReadOnlyDictionary<string, ISink> sinks, bool skipFailedBatches = false, ILogger? logger = null)
    {
        _sinks = sinks;
        _skipFailedBatches = skipFailedBatches;
        _logger = logger ?? NullLogger.Instance;
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
            try
            {
                await _retry.ExecuteAsync(async token =>
                {
                    var result = await sink.DeliverAsync(batch, token);
                    switch (result.Status)
                    {
                        case DeliveryStatus.Success:
                            return;
                        case DeliveryStatus.RetryableFailure:
                            throw new SinkRetryableException(sinkName, result.Error ?? "(unspecified)", result.Exception);
                        default:
                            throw new SinkDeliveryException(sinkName, result.Error ?? "(unspecified)", result.Exception);
                    }
                }, ct);
            }
            catch (Exception ex) when (ex is SinkRetryableException or SinkDeliveryException)
            {
                if (!_skipFailedBatches)
                {
                    throw;
                }

                _logger.LogWarning(ex, "Dead-lettering {Count} record(s) for sink '{Sink}' (DeadLetterPolicy=Skip).",
                    records.Count, sinkName);
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
