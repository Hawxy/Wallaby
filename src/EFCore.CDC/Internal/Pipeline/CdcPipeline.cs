using EFCore.CDC.Abstractions;
using EFCore.CDC.Internal.Backfill;
using EFCore.CDC.Internal.Replication;
using EFCore.CDC.Internal.State;
using Microsoft.Extensions.Logging;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// The live replication pipeline: reads committed transactions, materializes change events, routes them
/// to sinks, and — only after all sinks accept the batch — acknowledges the commit to the server and
/// records the checkpoint. This ordering preserves at-least-once delivery.
/// <para>
/// When a <see cref="WatermarkBackfillCoordinator"/> is supplied, the pipeline also recognizes the
/// <c>cdc.watermark.*</c> generic WAL messages that bracket backfill chunks: it records concurrent
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
    WatermarkBackfillCoordinator? backfill = null,
    DependentChangeResolver? dependentResolver = null)
{
    /// <summary>The highest LSN acknowledged to the server. Useful for observing progress.</summary>
    public ulong LastAcknowledgedLsn { get; private set; }

    public async Task RunAsync(CancellationToken ct)
    {
        logger.LogInformation("CDC pipeline started for slot '{Slot}'.", slotName);

        await foreach (var transaction in stream.ReadAsync(ct))
        {
            var appEvents = new List<ChangeEvent>(transaction.Changes.Count);
            foreach (var raw in transaction.Changes)
            {
                var changeEvent = changeEventFactory.Create(raw);
                if (changeEvent is not null)
                {
                    appEvents.Add(changeEvent);
                }

                if (dependentResolver is not null)
                {
                    foreach (var synthetic in await dependentResolver.ResolveAsync(raw, ct))
                    {
                        var fannedOut = changeEventFactory.Create(synthetic);
                        if (fannedOut is not null)
                        {
                            appEvents.Add(fannedOut);
                        }
                    }
                }
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

                foreach (var ev in appEvents)
                {
                    // Avoid forcing DocumentKey materialization unless a backfill window is recording
                    // for the same table — the common steady-state hot path.
                    if (backfill.IsRecording(ev.Metadata.QualifiedTableName))
                    {
                        backfill.RecordLiveKey(ev.Metadata.QualifiedTableName, ev.Key);
                    }
                }
            }

            if (appEvents.Count > 0)
            {
                await DispatchAsync(appEvents, ct);
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

            await stream.AcknowledgeAsync(transaction.EndLsn, ct);
            LastAcknowledgedLsn = transaction.EndLsn;
            await checkpoints.SaveAsync(slotName, new Checkpoint(transaction.EndLsn, DateTimeOffset.UtcNow), ct);
        }
    }

    private async Task EmitBackfillChunkAsync(PendingWindow window, CancellationToken ct)
    {
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

            if (events.Count > 0)
            {
                await DispatchAsync(events, ct);
            }
        }
        finally
        {
            window.Completed.TrySetResult();
        }
    }

    private async Task DispatchAsync(IReadOnlyList<ChangeEvent> events, CancellationToken ct)
    {
        var routed = await router.RouteAsync(events, ct);
        if (routed.Count > 0)
        {
            await dispatcher.DispatchAsync(routed, ct);
        }
    }
}
