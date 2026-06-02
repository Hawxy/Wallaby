using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.Backfill;
using Wallaby.Internal.Replication;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// The live replication pipeline: reads committed transactions, materializes change events, routes them
/// to sinks, and — only after all sinks accept the batch — acknowledges the commit to the server and
/// records the checkpoint. This ordering preserves at-least-once delivery.
/// <para>
/// Dependent-table changes fan out to synthetic updates of the affected primary rows. The first page per
/// binding is dispatched inline (excluding any primary key already changed live in the same transaction —
/// live wins); when more rows remain, the tail is enqueued as a scoped backfill so the transaction can be
/// acknowledged without waiting on a potentially huge re-index. Every dispatch is sliced into batches of
/// at most <c>maxBatchSize</c> records so no sink/transform sees an unbounded batch.
/// </para>
/// <para>
/// When a <see cref="WatermarkBackfillCoordinator"/> is supplied, the pipeline also recognizes the
/// <c>wallaby.watermark.*</c> generic WAL messages that bracket backfill chunks: it records concurrent
/// live-change keys between the watermarks and, at the high watermark, emits the chunk's surviving
/// snapshot rows through the same routing/sink path (guaranteeing correct ordering relative to live
/// changes).
/// </para>
/// </summary>
internal sealed class CdcPipeline(
    LogicalReplicationStream stream,
    ChangeEventFactory changeEventFactory,
    IChangeRouter router,
    SinkDispatcher dispatcher,
    ICheckpointStore checkpoints,
    string slotName,
    ILogger logger,
    int maxBatchSize,
    TimeSpan keepaliveInterval,
    WatermarkBackfillCoordinator? backfill = null,
    DependentChangeResolver? dependentResolver = null,
    IFanoutQueueStore? fanoutQueue = null,
    WallabyInstrumentation? instrumentation = null)
{
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    /// <summary>The highest LSN acknowledged to the server. Useful for observing progress.</summary>
    public ulong LastAcknowledgedLsn { get; private set; }

    public async Task RunAsync(CancellationToken ct)
    {
        logger.PipelineStarted(slotName);

        await foreach (var transaction in stream.ReadAsync(ct))
        {
            // Keep the connection alive while we process this transaction (the stream isn't being read,
            // so Npgsql can't answer the server's keepalives). Disposed before the next read resumes.
            await using var keepalive = stream.StartKeepalive(keepaliveInterval, ct);

            var lagSeconds = transaction.CommitTimestamp is { } commitTs
                ? Math.Max(0, (DateTimeOffset.UtcNow - commitTs).TotalSeconds)
                : -1;

            using var activity = _instr.StartTransaction();
            if (activity is not null)
            {
                activity.SetTag(WallabyInstrumentation.SlotTag, slotName);
                activity.SetTag("wallaby.txn.lsn.commit", (long)transaction.CommitLsn);
                activity.SetTag("wallaby.txn.lsn.end", (long)transaction.EndLsn);
                activity.SetTag("wallaby.txn.size", transaction.Changes.Count);
                if (lagSeconds >= 0)
                {
                    activity.SetTag("wallaby.ingestion.lag_s", lagSeconds);
                }
            }

            _instr.RecordIngestionLag(slotName, lagSeconds);

            // Materialize the live changes (no inline fan-out; that is resolved separately below).
            var appEvents = new List<ChangeEvent>(transaction.Changes.Count);
            foreach (var raw in transaction.Changes)
            {
                var changeEvent = changeEventFactory.Create(raw);
                if (changeEvent is not null)
                {
                    appEvents.Add(changeEvent);
                }
            }

            foreach (var ev in appEvents)
            {
                _instr.RecordChange(slotName, ev.Action, backfill: false);
            }

            // A low watermark opens recording for its table before subsequent live changes are seen.
            if (backfill is not null)
            {
                foreach (var wm in transaction.Watermarks)
                {
                    if (wm.Prefix == CdcSchema.WatermarkLowPrefix)
                    {
                        backfill.OnLowWatermark(wm.Token);
                    }
                }

                RecordLiveKeys(appEvents);
            }

            await DispatchChunkedAsync(appEvents, ct);

            // Dependent fan-out: dispatch the first page per binding inline, offload any tail.
            if (dependentResolver is not null)
            {
                await ResolveAndDispatchFanoutAsync(transaction.Changes, appEvents, ct);
            }

            // A high watermark closes the chunk: emit its surviving snapshot rows in stream order.
            if (backfill is not null)
            {
                foreach (var wm in transaction.Watermarks)
                {
                    if (wm.Prefix == CdcSchema.WatermarkHighPrefix && backfill.TryTakeHighWindow(wm.Token, out var window))
                    {
                        await EmitBackfillChunkAsync(window, ct);
                    }
                }
            }

            using (var ackActivity = _instr.StartAck())
            {
                ackActivity?.SetTag(WallabyInstrumentation.SlotTag, slotName);
                ackActivity?.SetTag("wallaby.txn.lsn.end", (long)transaction.EndLsn);
                await stream.AcknowledgeAsync(transaction.EndLsn, ct);
                LastAcknowledgedLsn = transaction.EndLsn;
                await checkpoints.SaveAsync(slotName, new Checkpoint(transaction.EndLsn, DateTimeOffset.UtcNow), ct);
            }
        }
    }

    private async Task ResolveAndDispatchFanoutAsync(
        IReadOnlyList<RawChange> changes, List<ChangeEvent> liveEvents, CancellationToken ct)
    {
        var results = await dependentResolver!.ResolveFirstPagesAsync(changes, maxBatchSize, ct);
        if (results.Count == 0)
        {
            return;
        }

        // A primary row changed live in this transaction already carries the dependent change (the
        // transform re-reads current state), so skip re-emitting it from the fan-out — live wins.
        var liveIndex = BuildLiveKeyIndex(liveEvents);

        foreach (var result in results)
        {
            var events = new List<ChangeEvent>(result.FirstPage.Count);
            foreach (var raw in result.FirstPage)
            {
                var changeEvent = changeEventFactory.Create(raw);
                if (changeEvent is null || liveIndex.Contains((changeEvent.Metadata.QualifiedTableName, changeEvent.Key)))
                {
                    continue;
                }
                events.Add(changeEvent);
            }

            foreach (var ev in events)
            {
                _instr.RecordChange(slotName, ev.Action, backfill: false);
            }
            if (backfill is not null)
            {
                RecordLiveKeys(events);
            }

            await DispatchChunkedAsync(events, ct);

            if (result.Continuation is not null && fanoutQueue is not null)
            {
                await fanoutQueue.EnqueueAsync(result.Continuation, ct);
            }
        }
    }

    private void RecordLiveKeys(List<ChangeEvent> events)
    {
        foreach (var ev in events)
        {
            // Avoid forcing DocumentKey materialization unless a backfill window is recording for the
            // same table — the common steady-state hot path.
            if (backfill!.IsRecording(ev.Metadata.QualifiedTableName))
            {
                backfill.RecordLiveKey(ev.Metadata.QualifiedTableName, ev.Key);
            }
        }
    }

    private static HashSet<(string Table, DocumentKey Key)> BuildLiveKeyIndex(List<ChangeEvent> events)
    {
        var index = new HashSet<(string, DocumentKey)>(events.Count);
        foreach (var ev in events)
        {
            index.Add((ev.Metadata.QualifiedTableName, ev.Key));
        }
        return index;
    }

    private async Task EmitBackfillChunkAsync(PendingWindow window, CancellationToken ct)
    {
        using var activity = _instr.StartBackfillChunk();
        try
        {
            var events = new List<ChangeEvent>(window.Buffer.Count);
            foreach (var raw in window.Buffer)
            {
                var changeEvent = changeEventFactory.Create(raw);
                if (changeEvent is not null && !window.SeenKeys.Contains(changeEvent.Key))
                {
                    events.Add(changeEvent);
                }
            }

            if (activity is not null)
            {
                activity.SetTag(WallabyInstrumentation.TableTag, window.QualifiedTable);
                activity.SetTag("wallaby.chunk.size", events.Count);
            }

            foreach (var ev in events)
            {
                _instr.RecordChange(slotName, ev.Action, backfill: true);
            }

            await DispatchChunkedAsync(events, ct);
        }
        finally
        {
            window.Completed.TrySetResult();
        }
    }

    /// <summary>Route and deliver a set of events, slicing it into batches of at most <c>maxBatchSize</c>.</summary>
    private async Task DispatchChunkedAsync(IReadOnlyList<ChangeEvent> events, CancellationToken ct)
    {
        if (events.Count == 0)
        {
            return;
        }

        if (events.Count <= maxBatchSize)
        {
            await DispatchAsync(events, ct);
            return;
        }

        for (var start = 0; start < events.Count; start += maxBatchSize)
        {
            var count = Math.Min(maxBatchSize, events.Count - start);
            var slice = new List<ChangeEvent>(count);
            for (var i = 0; i < count; i++)
            {
                slice.Add(events[start + i]);
            }
            await DispatchAsync(slice, ct);
        }
    }

    private async Task DispatchAsync(IReadOnlyList<ChangeEvent> events, CancellationToken ct)
    {
        using var activity = _instr.StartRoute();
        activity?.SetTag("wallaby.batch.size", events.Count);
        var routed = await router.RouteAsync(events, ct);
        if (routed.Count > 0)
        {
            await dispatcher.DispatchAsync(routed, ct);
        }
    }
}

/// <summary>Source-generated log messages for <see cref="CdcPipeline"/>.</summary>
internal static partial class CdcPipelineLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "CDC pipeline started for slot '{Slot}'.")]
    internal static partial void PipelineStarted(this ILogger logger, string slot);
}
