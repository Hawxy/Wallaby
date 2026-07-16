using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Wallaby.Abstractions;

namespace Wallaby.Diagnostics;

/// <summary>
/// Owns Wallaby's OpenTelemetry <see cref="System.Diagnostics.Metrics.Meter"/> and
/// <see cref="System.Diagnostics.ActivitySource"/> plus every instrument they expose.
/// </summary>
public sealed class WallabyInstrumentation : IDisposable
{
    /// <summary>The name of the <see cref="System.Diagnostics.Metrics.Meter"/> Wallaby publishes metrics through.</summary>
    public const string MeterName = "Wallaby";

    /// <summary>The name of the <see cref="System.Diagnostics.ActivitySource"/> Wallaby publishes traces through.</summary>
    public const string ActivitySourceName = "Wallaby";

    // ---- attribute keys ----
    internal const string SlotTag = "wallaby.slot";
    internal const string SinkTag = "wallaby.sink";
    internal const string EntityTag = "wallaby.entity";
    internal const string TableTag = "wallaby.table";
    internal const string ActionTag = "wallaby.action";
    internal const string SourceTag = "wallaby.source";
    internal const string DeliveryOutcomeTag = "wallaby.delivery.outcome";
    internal const string DestinationTag = "wallaby.destination";
    internal const string BackfillKindTag = "wallaby.backfill.kind";

    // ---- span names ----
    internal const string TransactionActivity = "transaction.process";
    internal const string DependentResolveActivity = "dependent.resolve";
    internal const string RouteActivity = "route";
    internal const string TransformActivity = "transform";
    internal const string SinkDeliverActivity = "sink.deliver";
    internal const string BackfillActivity = "backfill";
    internal const string BackfillChunkActivity = "backfill.chunk";
    internal const string AckActivity = "ack";
    internal const string LeaderBootstrapActivity = "leader.bootstrap";
    internal const string SelfConfigActivity = "selfconfig";
    internal const string SlotRepairActivity = "slot.repair";
    internal const string SinkInitializeActivity = "sink.initialize";

    // ---- low-cardinality attribute values ----
    internal const string SourceLive = "live";
    internal const string SourceBackfill = "backfill";
    internal const string SourceFanout = "fanout";
    internal const string DeliverySuccess = "success";
    internal const string DeliveryRetryable = "retryable";
    internal const string DeliveryPermanent = "permanent";
    internal const string BackfillKindTable = "table";
    internal const string BackfillKindFanout = "fanout";

    /// <summary>A shared, never-observed instance for components constructed outside DI (tests, direct use).</summary>
    internal static readonly WallabyInstrumentation NoOp = new();

    private readonly Meter _meter;
    private readonly ActivitySource _activitySource;

    private readonly Counter<long> _changesReceived;
    private readonly Histogram<double> _ingestionLag;
    private readonly Counter<long> _dependentSynthetic;
    private readonly Histogram<double> _transformDuration;
    private readonly Histogram<double> _sinkDeliveryDuration;
    private readonly Counter<long> _sinkRecordsDelivered;
    private readonly Counter<long> _sinkDeliveryFailures;
    private readonly Counter<long> _backfillRows;
    private readonly UpDownCounter<int> _backfillActive;
    private readonly Histogram<double> _backfillChunkDuration;
    private readonly ObservableGauge<long> _fanoutQueueDepthGauge;

    // Cached by the fan-out worker once per drain pass, so the exporter thread never queries the database.
    // -1 = never sampled (no leader session yet); the gauge emits nothing until a real sample exists.
    private long _fanoutQueueDepth = -1;

    // Per sink, the Stopwatch timestamp of the last successful delivery; the lag gauge derives seconds-since.
    private readonly ConcurrentDictionary<string, long> _sinkLastDeliveredAt = new(StringComparer.Ordinal);

    /// <summary>Create instrumentation whose meter is owned by the host's <see cref="IMeterFactory"/>.</summary>
    internal WallabyInstrumentation(IMeterFactory meterFactory)
        : this(meterFactory.Create(MeterName))
    {
    }

    /// <summary>Create stand-alone instrumentation (no <see cref="IMeterFactory"/>); used for tests and the no-op instance.</summary>
    internal WallabyInstrumentation()
        : this(new Meter(MeterName))
    {
    }

    private WallabyInstrumentation(Meter meter)
    {
        _meter = meter;
        _activitySource = new ActivitySource(ActivitySourceName);

        _changesReceived = _meter.CreateCounter<long>(
            "wallaby.changes.received", unit: "{change}", description: "Materialized change events received from replication and backfill.");
        _ingestionLag = _meter.CreateHistogram<double>(
            "wallaby.ingestion.lag", unit: "s", description: "Delay between a source transaction's commit and Wallaby receiving it.");
        _dependentSynthetic = _meter.CreateCounter<long>(
            "wallaby.dependent.synthetic", unit: "{change}", description: "Synthetic parent changes produced by dependent-table fan-out.");
        _transformDuration = _meter.CreateHistogram<double>(
            "wallaby.transform.duration", unit: "s", description: "Time spent invoking a mapping's transform for a batch.");
        _sinkDeliveryDuration = _meter.CreateHistogram<double>(
            "wallaby.sink.delivery.duration", unit: "s", description: "Duration of a single sink delivery attempt.");
        _sinkRecordsDelivered = _meter.CreateCounter<long>(
            "wallaby.sink.records.delivered", unit: "{record}", description: "Records accepted by a sink.");
        _sinkDeliveryFailures = _meter.CreateCounter<long>(
            "wallaby.sink.delivery.failures", unit: "{failure}", description: "Failed sink deliveries by outcome.");
        _backfillRows = _meter.CreateCounter<long>(
            "wallaby.backfill.rows", unit: "{row}", description: "Rows copied during backfill.");
        _backfillActive = _meter.CreateUpDownCounter<int>(
            "wallaby.backfill.active", unit: "{table}", description: "Tables currently being backfilled.");
        _backfillChunkDuration = _meter.CreateHistogram<double>(
            "wallaby.backfill.chunk.duration", unit: "s", description: "Time to read and emit one backfill chunk.");
        _fanoutQueueDepthGauge = _meter.CreateObservableGauge(
            "wallaby.fanout.queue.depth", ObserveFanoutQueueDepth, unit: "{job}",
            description: "Scoped fan-out jobs currently due (Requested or InProgress), sampled once per drain pass.");
        _meter.CreateObservableGauge(
            "wallaby.sink.delivery.lag", ObserveSinkDeliveryLag, unit: "s",
            description: "Seconds since each sink last accepted a batch.");
    }

    /// <summary>The underlying meter (exposed for tests that attach a <c>MetricCollector</c>).</summary>
    internal Meter Meter => _meter;

    /// <summary>The underlying activity source (exposed so tests can scope an <c>ActivityListener</c> to this instance).</summary>
    internal ActivitySource ActivitySource => _activitySource;

    // ---- timing ----

    /// <summary>Capture a start timestamp for a duration measurement (pair with a <c>Record*Duration</c> call).</summary>
    internal static long StartTimer() => Stopwatch.GetTimestamp();

    private static double ElapsedSeconds(long startTimestamp) => Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;

    // ---- spans ----

    internal Activity? StartTransaction() => _activitySource.StartActivity(TransactionActivity, ActivityKind.Consumer);
    internal Activity? StartDependentResolve() => _activitySource.StartActivity(DependentResolveActivity);
    internal Activity? StartRoute() => _activitySource.StartActivity(RouteActivity);
    internal Activity? StartTransform() => _activitySource.StartActivity(TransformActivity);
    internal Activity? StartSinkDelivery() => _activitySource.StartActivity(SinkDeliverActivity, ActivityKind.Producer);

    /// <summary>Root span for one backfill run (whole-table or scoped fan-out); chunks link back to it.</summary>
    internal Activity? StartBackfill() => _activitySource.StartActivity(BackfillActivity);

    // The chunk is delivered inside a slot commit, so it parents under that transaction's span; the link
    // ties it back to the backfill run that produced it (which lives in a different trace).
    internal Activity? StartBackfillChunk(ActivityContext backfillRun) => _activitySource.StartActivity(
        BackfillChunkActivity, ActivityKind.Internal, parentContext: default,
        links: backfillRun == default ? null : [new ActivityLink(backfillRun)]);

    internal Activity? StartAck() => _activitySource.StartActivity(AckActivity);

    // ---- leader bootstrap (per leadership term, before streaming) ----

    internal Activity? StartLeaderBootstrap() => _activitySource.StartActivity(LeaderBootstrapActivity);
    internal Activity? StartSelfConfig() => _activitySource.StartActivity(SelfConfigActivity);
    internal Activity? StartSlotRepair() => _activitySource.StartActivity(SlotRepairActivity);
    internal Activity? StartSinkInitialize() => _activitySource.StartActivity(SinkInitializeActivity);

    // ---- ingestion / pipeline ----

    internal void RecordChange(string slot, ChangeAction action, bool backfill)
    {
        if (!_changesReceived.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { SlotTag, slot },
            { ActionTag, ActionString(action) },
            { SourceTag, backfill ? SourceBackfill : SourceLive },
        };
        _changesReceived.Add(1, tags);
    }

    internal void RecordIngestionLag(string slot, double lagSeconds)
    {
        if (lagSeconds >= 0)
        {
            _ingestionLag.Record(lagSeconds, new KeyValuePair<string, object?>(SlotTag, slot));
        }
    }

    internal void RecordDependentSynthetic(string table, int count)
    {
        if (count > 0)
        {
            _dependentSynthetic.Add(count, new KeyValuePair<string, object?>(TableTag, table));
        }
    }

    // ---- transform ----

    internal void RecordTransformDuration(string entity, string sink, long startTimestamp)
    {
        if (_transformDuration.Enabled)
        {
            // Tagged by sink too: an entity mapped to several sinks runs one transform per mapping, and
            // their durations must stay distinguishable.
            _transformDuration.Record(
                ElapsedSeconds(startTimestamp), new TagList { { EntityTag, entity }, { SinkTag, sink } });
        }
    }

    // ---- sink delivery ----

    /// <summary>
    /// Record one finished delivery attempt: the duration histogram always; on success also the
    /// records-delivered counter and the lag-gauge timestamp, otherwise the failure counter.
    /// </summary>
    internal void RecordSinkAttempt(string sink, string outcome, long startTimestamp, int recordCount)
    {
        if (_sinkDeliveryDuration.Enabled)
        {
            _sinkDeliveryDuration.Record(
                ElapsedSeconds(startTimestamp), new TagList { { SinkTag, sink }, { DeliveryOutcomeTag, outcome } });
        }

        if (outcome == DeliverySuccess)
        {
            if (recordCount > 0)
            {
                _sinkRecordsDelivered.Add(recordCount, new KeyValuePair<string, object?>(SinkTag, sink));
            }
            _sinkLastDeliveredAt[sink] = Stopwatch.GetTimestamp();
        }
        else
        {
            _sinkDeliveryFailures.Add(1, new TagList { { SinkTag, sink }, { DeliveryOutcomeTag, outcome } });
        }
    }

    private IEnumerable<Measurement<double>> ObserveSinkDeliveryLag()
    {
        foreach (var (sink, timestamp) in _sinkLastDeliveredAt)
        {
            yield return new Measurement<double>(
                Stopwatch.GetElapsedTime(timestamp).TotalSeconds, new KeyValuePair<string, object?>(SinkTag, sink));
        }
    }

    // ---- fan-out queue ----

    /// <summary>True when something is collecting the depth gauge, so the worker knows whether counting is worth a query.</summary>
    internal bool FanoutQueueDepthEnabled => _fanoutQueueDepthGauge.Enabled;

    internal void RecordFanoutQueueDepth(long depth) => Interlocked.Exchange(ref _fanoutQueueDepth, depth);

    private IEnumerable<Measurement<long>> ObserveFanoutQueueDepth()
    {
        var depth = Interlocked.Read(ref _fanoutQueueDepth);
        if (depth >= 0)
        {
            yield return new Measurement<long>(depth);
        }
    }

    // ---- backfill ----

    internal void BackfillStarted() => _backfillActive.Add(1);
    internal void BackfillCompleted() => _backfillActive.Add(-1);

    internal void RecordBackfillRows(string table, long rows)
    {
        if (rows > 0)
        {
            _backfillRows.Add(rows, new KeyValuePair<string, object?>(TableTag, table));
        }
    }

    internal void RecordBackfillChunkDuration(string table, long startTimestamp)
    {
        if (_backfillChunkDuration.Enabled)
        {
            _backfillChunkDuration.Record(ElapsedSeconds(startTimestamp), new KeyValuePair<string, object?>(TableTag, table));
        }
    }

    private static string ActionString(ChangeAction action) => action switch
    {
        ChangeAction.Insert => "insert",
        ChangeAction.Update => "update",
        ChangeAction.Delete => "delete",
        ChangeAction.Read => "read",
        _ => "unknown",
    };

    /// <summary>Disposes the meter and activity source (invoked on host shutdown for the DI singleton).</summary>
    public void Dispose()
    {
        _meter.Dispose();
        _activitySource.Dispose();
    }
}
