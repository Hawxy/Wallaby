using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;

namespace Wallaby.Internal.Pipeline;

/// <summary>Raised internally to drive Polly retries on a retryable sink failure.</summary>
internal sealed class SinkRetryableException(string sinkName, string error, Exception? inner)
    : Exception($"Sink '{sinkName}' reported a retryable failure: {error}", inner)
{
    public string SinkName { get; } = sinkName;
}

/// <summary>Raised when a sink permanently fails (or retries are exhausted); halts the pipeline.</summary>
internal sealed class SinkDeliveryException(string sinkName, string error, Exception? inner)
    : Exception($"Sink '{sinkName}' failed to deliver: {error}", inner)
{
    public string SinkName { get; } = sinkName;
}

/// <summary>
/// Groups routed documents by sink (preserving commit order) and delivers each group as a
/// <see cref="SinkBatch"/>, retrying retryable failures with exponential backoff. Sinks are independent,
/// so their batches are delivered concurrently; per-sink ordering is preserved (one batch per sink, records
/// in commit order). A permanent failure (or exhausted retries) on any sink halts the pipeline, after
/// every in-flight delivery has settled, so no batch is abandoned mid-write.
/// </summary>
internal sealed class SinkDispatcher
{
    private readonly IReadOnlyDictionary<string, ISink> _sinks;
    private readonly WallabyInstrumentation _instr;
    private readonly WallabyStatus? _status;
    private readonly ResiliencePipeline _retry;

    public SinkDispatcher(
        IReadOnlyDictionary<string, ISink> sinks, ILogger logger, WallabyInstrumentation? instrumentation = null,
        SinkRetryOptions? retry = null, WallabyStatus? status = null)
    {
        _sinks = sinks;
        _instr = instrumentation ?? WallabyInstrumentation.NoOp;
        _status = status;
        retry ??= new SinkRetryOptions();
        // MaxAttempts = 0 skips the retry strategy entirely: the first retryable failure propagates and
        // halts the leader session, whose own backoff then governs the retry cadence.
        _retry = retry.MaxAttempts == 0
            ? ResiliencePipeline.Empty
            : new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    ShouldHandle = new PredicateBuilder().Handle<SinkRetryableException>(),
                    MaxRetryAttempts = retry.MaxAttempts,
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = true,
                    Delay = retry.BaseDelay,
                    MaxDelay = retry.MaxDelay,
                    OnRetry = args =>
                    {
                        var ex = args.Outcome.Exception!;
                        logger.SinkDeliveryRetrying(
                            ex, (ex as SinkRetryableException)?.SinkName ?? "(unknown)",
                            args.AttemptNumber + 1, args.RetryDelay);
                        return default;
                    },
                })
                .Build();
    }

    public async Task DispatchAsync(IReadOnlyList<RoutedDocument> routed, CancellationToken ct)
    {
        var groups = GroupBySinkPreservingOrder(routed);
        if (groups.Count == 1)
        {
            await DeliverGroupAsync(groups[0].SinkName, groups[0].Records, ct);
            return;
        }

        // Sinks are independent: fan the groups out concurrently so a batch's ack latency is the slowest
        // sink, not the sum. WhenAll settles every delivery before propagating the first failure, so no
        // sink is abandoned mid-write when another faults.
        var tasks = new Task[groups.Count];
        for (var i = 0; i < groups.Count; i++)
        {
            tasks[i] = DeliverGroupAsync(groups[i].SinkName, groups[i].Records, ct);
        }
        await Task.WhenAll(tasks);
    }

    private async Task DeliverGroupAsync(string sinkName, List<SinkRecord> records, CancellationToken ct)
    {
        // Defensive: mappings are attached to a registered sink by construction, so a routed name is
        // always registered, unless a custom router produced a stray name.
        if (!_sinks.TryGetValue(sinkName, out var sink))
        {
            throw new SinkDeliveryException(sinkName, "no sink is registered with this name", inner: null);
        }

        var batch = new SinkBatch(sinkName, records);
        using var activity = _instr.StartSinkDelivery();
        if (activity is not null)
        {
            activity.SetTag(WallabyInstrumentation.SinkTag, sinkName);
            activity.SetTag(WallabyInstrumentation.DestinationTag, DescribeDestinations(records));
            activity.SetTag("wallaby.batch.size", records.Count);
        }

        try
        {
            await _retry.ExecuteAsync(async static (state, token) =>
            {
                var attempt = ++state.Attempts.Value;
                var attemptStart = WallabyInstrumentation.StartTimer();
                var result = await state.Sink.DeliverAsync(state.SinkBatch, token);
                var name = state.SinkBatch.SinkName;
                var outcome = result.Status switch
                {
                    DeliveryStatus.Success => WallabyInstrumentation.DeliverySuccess,
                    DeliveryStatus.RetryableFailure => WallabyInstrumentation.DeliveryRetryable,
                    _ => WallabyInstrumentation.DeliveryPermanent,
                };
                state.Instr.RecordSinkAttempt(name, outcome, attemptStart, state.SinkBatch.Records.Count);
                switch (result.Status)
                {
                    case DeliveryStatus.Success:
                        state.Status?.RecordSinkDelivered(name, DateTimeOffset.UtcNow);
                        return;
                    case DeliveryStatus.RetryableFailure:
                        state.Activity?.AddEvent(new ActivityEvent("retry", tags: new ActivityTagsCollection
                        {
                            ["attempt"] = attempt,
                            ["error"] = result.Error,
                        }));
                        throw new SinkRetryableException(name, result.Error ?? "(unspecified)", result.Exception);
                    default:
                        throw new SinkDeliveryException(
                            name,
                            $"{result.Error ?? "(unspecified)"} (records from {DescribeTables(state.SinkBatch)})",
                            result.Exception);
                }
            }, (Sink: sink, SinkBatch: batch, Instr: _instr, Status: _status, Activity: activity, Attempts: new StrongBox<int>()), ct);
        }
        catch (Exception ex) when (ex is SinkRetryableException or SinkDeliveryException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    // Distinct destinations across the batch, in first-seen order; a per-sink batch can mix
    // destinations when a scoped mapping resolves them per scope key.
    private static string? DescribeDestinations(List<SinkRecord> records)
    {
        List<string>? destinations = null;
        foreach (var record in records)
        {
            if (record.Destination is { } destination)
            {
                destinations ??= [];
                if (!destinations.Contains(destination))
                {
                    destinations.Add(destination);
                }
            }
        }

        return destinations is null ? null : string.Join(", ", destinations);
    }

    // Distinct source tables of a failed batch, for the halt diagnostics.
    private static string DescribeTables(SinkBatch batch)
    {
        var tables = new List<string>();
        foreach (var record in batch.Records)
        {
            var table = record.Metadata.QualifiedTableName;
            if (!tables.Contains(table))
            {
                tables.Add(table);
            }
        }

        return tables.Count == 0 ? "(none)" : string.Join(", ", tables);
    }

    private static List<(string SinkName, List<SinkRecord> Records)> GroupBySinkPreservingOrder(
        IReadOnlyList<RoutedDocument> routed)
    {
        var groups = new Dictionary<string, List<SinkRecord>>();
        var order = new List<(string, List<SinkRecord>)>();

        foreach (var item in routed)
        {
            if (!groups.TryGetValue(item.SinkName, out var list))
            {
                list = [];
                groups[item.SinkName] = list;
                order.Add((item.SinkName, list));
            }
            list.Add(item.Record);
        }

        return order;
    }
}

/// <summary>Source-generated log messages for <see cref="SinkDispatcher"/>.</summary>
internal static partial class SinkDispatcherLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Sink '{Sink}' delivery attempt {Attempt} failed with a retryable error; retrying in {Delay}.")]
    internal static partial void SinkDeliveryRetrying(this ILogger logger, Exception ex, string sink, int attempt, TimeSpan delay);
}
