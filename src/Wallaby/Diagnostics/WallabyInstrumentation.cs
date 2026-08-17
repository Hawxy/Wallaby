using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Wallaby.Abstractions;
using Wallaby.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.Internal.Replication;

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
    internal const string FanoutKeysTag = "wallaby.fanout.keys";
    internal const string BatchTxnCountTag = "wallaby.batch.txn_count";
    internal const string BatchFlushReasonTag = "wallaby.batch.flush_reason";
    internal const string TxnCommitLsnTag = "wallaby.txn.lsn.commit";
    internal const string TxnEndLsnTag = "wallaby.txn.lsn.end";
    internal const string TxnSizeTag = "wallaby.txn.size";
    internal const string TxnStreamedTag = "wallaby.txn.streamed";
    internal const string SpillRowsTag = "wallaby.spill.rows";
    internal const string IngestionLagTag = "wallaby.ingestion.lag_s";
    internal const string WatermarkTag = "wallaby.watermark";
    internal const string HeartbeatTag = "wallaby.heartbeat";
    internal const string TruncateTag = "wallaby.truncate";
    internal const string ReselectOutcomeTag = "wallaby.reselect.outcome";

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
    internal const string SinkPurgeActivity = "sink.purge";

    // ---- low-cardinality attribute values ----
    internal const string SourceLive = "live";
    internal const string SourceBackfill = "backfill";
    internal const string SourceFanout = "fanout";
    internal const string DeliverySuccess = "success";
    internal const string DeliveryRetryable = "retryable";
    internal const string DeliveryPermanent = "permanent";
    internal const string BackfillKindTable = "table";
    internal const string BackfillKindFanout = "fanout";
    internal const string ReselectHealed = "healed";
    internal const string ReselectRowGone = "row_gone";

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
    private readonly Counter<long> _changesReselected;
    private readonly Counter<long> _spillRows;
    private readonly Counter<long> _spillFlushes;
    private readonly Counter<long> _backfillRows;
    private readonly UpDownCounter<int> _backfillActive;
    private readonly Histogram<double> _backfillChunkDuration;
    private readonly ObservableGauge<long> _fanoutQueueDepthGauge;
    private readonly ObservableGauge<long> _slotRetainedWalGauge;

    // Cached by the fan-out worker once per drain pass, so the exporter thread never queries the database.
    // -1 = never sampled (no leader session yet); the gauge emits nothing until a real sample exists.
    private long _fanoutQueueDepth = -1;

    // Cached by the leader's slot-lag sampler once per tick, so the exporter thread never queries the
    // database. Null = never sampled; the gauge emits nothing until a real sample exists.
    private SlotWalSample? _slotRetainedWal;

    private sealed record SlotWalSample(string Slot, long Bytes);

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
        _changesReselected = _meter.CreateCounter<long>(
            "wallaby.changes.reselected", unit: "{change}",
            description: "Changes healed by re-reading the row after an unavailable (unchanged TOAST) value, by outcome.");
        _spillRows = _meter.CreateCounter<long>(
            "wallaby.spill.rows", unit: "{change}",
            description: "Changes written to the transaction spill while streamed (large) transactions are buffered before commit.");
        _spillFlushes = _meter.CreateCounter<long>(
            "wallaby.spill.flushes", unit: "{flush}",
            description: "Spill buffer flushes (binary COPY batches into wallaby.stream_buffer) by the database spill backend.");
        _backfillRows = _meter.CreateCounter<long>(
            "wallaby.backfill.rows", unit: "{row}", description: "Rows copied during backfill.");
        _backfillActive = _meter.CreateUpDownCounter<int>(
            "wallaby.backfill.active", unit: "{table}", description: "Tables currently being backfilled.");
        _backfillChunkDuration = _meter.CreateHistogram<double>(
            "wallaby.backfill.chunk.duration", unit: "s", description: "Time to read and emit one backfill chunk.");
        _fanoutQueueDepthGauge = _meter.CreateObservableGauge(
            "wallaby.fanout.queue.depth", ObserveFanoutQueueDepth, unit: "{job}",
            description: "Scoped fan-out jobs currently due (Requested or InProgress), sampled once per drain pass.");
        _slotRetainedWalGauge = _meter.CreateObservableGauge(
            "wallaby.slot.retained_wal", ObserveSlotRetainedWal, unit: "By",
            description: "WAL bytes the server retains for the slot (restart_lsn to the current write position), sampled by the leader.");
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

    /// <summary>Root span for one solo-dispatched transaction, carrying its batch, lag, and marker attributes.</summary>
    internal Activity? StartTransaction(
        string slot, CommittedTransaction transaction, BatchFlushReason flushReason, double lagSeconds)
    {
        var activity = StartTransactionCore(
            slot, txnCount: 1, flushReason, transaction.CommitLsn, transaction.EndLsn, lagSeconds);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(TxnStreamedTag, transaction.IsStreamed);
        // A streamed transaction's size is unknown until its spill is read; the pipeline tags it afterwards.
        if (!transaction.IsStreamed)
        {
            activity.SetTag(TxnSizeTag, transaction.Changes.Count);
        }
        else
        {
            activity.SetTag(SpillRowsTag, transaction.SpilledChanges);
        }
        // Marks the tiny transactions that exist only to bracket a backfill chunk, so they can be
        // filtered in a trace viewer.
        if (transaction.Watermarks.Count > 0)
        {
            activity.SetTag(WatermarkTag,
                transaction.Watermarks[0].Prefix == WallabySchema.WatermarkLowPrefix ? "low" : "high");
        }
        // Marks idle-slot heartbeat transactions, so they can be filtered in a trace viewer.
        if (transaction.ContainsHeartbeat)
        {
            activity.SetTag(HeartbeatTag, true);
        }
        if (transaction.TruncatedTables.Count > 0)
        {
            activity.SetTag(TruncateTag, string.Join(",", transaction.TruncatedTables));
        }
        return activity;
    }

    /// <summary>Root span for one coalesced batch of small transactions.</summary>
    internal Activity? StartTransaction(
        string slot, IReadOnlyList<CommittedTransaction> batch, int totalChanges,
        BatchFlushReason flushReason, double lagSeconds)
    {
        var activity = StartTransactionCore(
            slot, batch.Count, flushReason, batch[0].CommitLsn, batch[^1].EndLsn, lagSeconds);
        activity?.SetTag(TxnSizeTag, totalChanges);
        return activity;
    }

    private Activity? StartTransactionCore(
        string slot, int txnCount, BatchFlushReason flushReason, ulong commitLsn, ulong endLsn, double lagSeconds)
    {
        var activity = _activitySource.StartActivity(TransactionActivity, ActivityKind.Consumer);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(SlotTag, slot);
        activity.SetTag(BatchTxnCountTag, txnCount);
        activity.SetTag(BatchFlushReasonTag, FlushReasonString(flushReason));
        activity.SetTag(TxnCommitLsnTag, (long)commitLsn);
        activity.SetTag(TxnEndLsnTag, (long)endLsn);
        if (lagSeconds >= 0)
        {
            activity.SetTag(IngestionLagTag, lagSeconds);
        }
        return activity;
    }

    internal Activity? StartDependentResolve() => _activitySource.StartActivity(DependentResolveActivity);
    internal Activity? StartRoute() => _activitySource.StartActivity(RouteActivity);
    internal Activity? StartTransform() => _activitySource.StartActivity(TransformActivity);
    internal Activity? StartSinkDelivery() => _activitySource.StartActivity(SinkDeliverActivity, ActivityKind.Producer);

    /// <summary>
    /// Root span for one backfill run (whole-table or scoped fan-out); chunks link back to it. A scoped
    /// fan-out run passes the enqueuing trigger's context so the run links back to the trace that caused it.
    /// </summary>
    internal Activity? StartBackfill(string qualifiedTable, string backfillKind, int fanoutKeys = 0, ActivityContext trigger = default)
    {
        var activity = _activitySource.StartActivity(
            BackfillActivity, ActivityKind.Internal, parentContext: default,
            links: trigger == default ? null : [new ActivityLink(trigger)]);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(TableTag, qualifiedTable);
        activity.SetTag(BackfillKindTag, backfillKind);
        if (fanoutKeys > 0)
        {
            activity.SetTag(FanoutKeysTag, fanoutKeys);
        }
        return activity;
    }

    // The chunk is delivered inside a slot commit, so it parents under that transaction's span; the link
    // ties it back to the backfill run that produced it (which lives in a different trace).
    internal Activity? StartBackfillChunk(ActivityContext backfillRun) => _activitySource.StartActivity(
        BackfillChunkActivity, ActivityKind.Internal, parentContext: default,
        links: backfillRun == default ? null : [new ActivityLink(backfillRun)]);

    /// <summary>Ack span covering the server acknowledgement and checkpoint write; a batch ack also carries its transaction count.</summary>
    internal Activity? StartAck(string slot, ulong endLsn, int? txnCount = null)
    {
        var activity = _activitySource.StartActivity(AckActivity);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(SlotTag, slot);
        activity.SetTag(TxnEndLsnTag, (long)endLsn);
        if (txnCount is not null)
        {
            activity.SetTag(BatchTxnCountTag, txnCount);
        }
        return activity;
    }

    // ---- leader bootstrap (per leadership term, before streaming) ----

    internal Activity? StartLeaderBootstrap() => _activitySource.StartActivity(LeaderBootstrapActivity);
    internal Activity? StartSelfConfig() => _activitySource.StartActivity(SelfConfigActivity);
    internal Activity? StartSlotRepair() => _activitySource.StartActivity(SlotRepairActivity);
    internal Activity? StartSinkInitialize() => _activitySource.StartActivity(SinkInitializeActivity);

    /// <summary>One destination purge before a fresh backfill; a root span like <see cref="StartBackfill"/>.</summary>
    internal Activity? StartSinkPurge() => _activitySource.StartActivity(SinkPurgeActivity);

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

    /// <summary>Record a reselect (counter + an event on the ambient transaction span), by outcome.</summary>
    internal void RecordReselect(string table, string outcome)
    {
        Activity.Current?.AddEvent(new ActivityEvent("change.reselected", tags: new ActivityTagsCollection
        {
            [TableTag] = table,
            [ReselectOutcomeTag] = outcome,
        }));

        if (!_changesReselected.Enabled)
        {
            return;
        }

        var tags = new TagList
        {
            { TableTag, table },
            { ReselectOutcomeTag, outcome },
        };
        _changesReselected.Add(1, tags);
    }

    internal void RecordIngestionLag(string slot, double lagSeconds)
    {
        if (lagSeconds >= 0)
        {
            _ingestionLag.Record(lagSeconds, new KeyValuePair<string, object?>(SlotTag, slot));
        }
    }

    /// <summary>One change written to the transaction spill (a streamed transaction is buffering before commit).</summary>
    internal void RecordSpilledChange(string slot)
    {
        if (_spillRows.Enabled)
        {
            _spillRows.Add(1, new KeyValuePair<string, object?>(SlotTag, slot));
        }
    }

    /// <summary>One spill buffer flush (a binary COPY batch into <c>wallaby.stream_buffer</c>).</summary>
    internal void RecordSpillFlush(string slot)
    {
        if (_spillFlushes.Enabled)
        {
            _spillFlushes.Add(1, new KeyValuePair<string, object?>(SlotTag, slot));
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

    // ---- replication slot ----

    /// <summary>True when something is collecting the retained-WAL gauge, so the sampler knows whether a query is worth it.</summary>
    internal bool SlotRetainedWalEnabled => _slotRetainedWalGauge.Enabled;

    internal void RecordSlotRetainedWal(string slot, long bytes) => _slotRetainedWal = new SlotWalSample(slot, bytes);

    private IEnumerable<Measurement<long>> ObserveSlotRetainedWal()
    {
        if (_slotRetainedWal is { } sample)
        {
            yield return new Measurement<long>(
                sample.Bytes, new KeyValuePair<string, object?>(SlotTag, sample.Slot));
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

    private static string FlushReasonString(BatchFlushReason reason) => reason switch
    {
        BatchFlushReason.Disabled => "disabled",
        BatchFlushReason.Boundary => "boundary",
        BatchFlushReason.Idle => "idle",
        BatchFlushReason.TransactionCap => "txn_cap",
        BatchFlushReason.SizeCap => "size_cap",
        BatchFlushReason.Ended => "ended",
        _ => "unknown",
    };

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
