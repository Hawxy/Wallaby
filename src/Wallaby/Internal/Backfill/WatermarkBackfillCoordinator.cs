using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Coordinates Sequin-style watermark backfill. The backfill task snapshots a table in keyset chunks,
/// bracketing each chunk with low/high watermark emissions via <c>pg_logical_emit_message</c>. The live
/// pipeline (which receives those messages through pgoutput as <c>LogicalDecodingMessage</c>) records
/// concurrent change keys between the watermarks and emits the deduplicated snapshot rows at the high
/// watermark — guaranteeing no gaps and that live changes always win for overlapping keys.
/// <para>
/// Chunks are pipelined: while the live pipeline delivers one chunk, the loop already reads the next
/// (its progress still persists in emission order), so snapshot reads overlap transform/sink delivery.
/// </para>
/// <para>
/// The same chunk loop also runs <em>scoped</em> backfills (<see cref="BackfillScopeAsync"/>) that restrict
/// the snapshot to the rows affected by a dependent fan-out, which is how a wide fan-out's tail is
/// re-indexed asynchronously without stalling the live stream.
/// </para>
/// </summary>
internal sealed class WatermarkBackfillCoordinator(
    NpgsqlDataSource dataSource, IBackfillStateStore store, ILogger logger, WallabyInstrumentation? instrumentation = null)
{
    // Windows awaiting their high watermark. Added by the backfill/fan-out tasks, removed by the pipeline,
    // so this is the one structure that genuinely crosses threads.
    private readonly ConcurrentDictionary<string, PendingWindow> _byToken = new();

    // Active recording windows per table. A table can have several at once (a whole-table backfill plus one
    // or more scoped fan-out backfills), so a live key is fanned into every active window for the table.
    private readonly Dictionary<string, List<PendingWindow>> _recordingByTable = [];
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    public int ChunkSize { get; init; } = 500;

    /// <summary>The opt-in visibility fence each chunk waits on after its low-watermark emission; null = disabled.</summary>
    public VisibilityFence? Fence { get; init; }

    // A long backfill reports progress at most this often (its start/completion are always logged).
    private static readonly TimeSpan ProgressLogInterval = TimeSpan.FromSeconds(30);

    // ---- backfill task side ----

    /// <summary>Snapshot a whole table chunk-by-chunk, resuming from persisted state. The live pipeline must be running.</summary>
    public async Task BackfillTableAsync(CapturedTable table, string? transformVersion, CancellationToken ct)
    {
        // One token per run, deliberately not persisted: a crash-resume re-delivers under a fresh token,
        // which is harmless (upsert-only) and avoids a state column.
        var runId = Guid.NewGuid().ToString("N");
        var pager = new KeysetPager(table, backfillRunId: runId);
        var pkColumns = table.PrimaryKey.Select(c => c.ColumnName).ToArray();
        var pkTypes = table.PrimaryKey.Select(c => c.ClrType).ToArray();
        var existing = await store.GetAsync(table.QualifiedName, ct);

        long startRows;
        if (KeysetCodec.TryDeserializeCursor(existing?.CursorJson, pkColumns, pkTypes, out var cursor))
        {
            startRows = existing?.RowsCopied ?? 0;
        }
        else
        {
            // The persisted cursor was built against a different key shape (or format) — resuming with it
            // would page incorrectly, so restart the snapshot from the beginning.
            logger.BackfillCursorRejected(table.QualifiedName);
            startRows = 0;
        }

        logger.BackfillStarting(table.QualifiedName);

        var rowsCopied = await RunChunkLoopAsync(
            pager, table.QualifiedName, WallabyInstrumentation.BackfillKindTable, fanoutKeys: 0, cursor, startRows,
            // Guarded save: a manual request arriving mid-run wins over every later progress write,
            // so the row stays Requested and the scheduler re-runs the table fresh.
            (cur, rows, hasMore, token) => store.SaveProgressAsync(
                new BackfillState(
                    table.QualifiedName,
                    hasMore ? BackfillStatus.InProgress : BackfillStatus.Completed,
                    transformVersion,
                    KeysetCodec.SerializeCursor(cur, pkColumns),
                    rows,
                    DateTimeOffset.UtcNow),
                token),
            ct);

        logger.BackfillComplete(table.QualifiedName, rowsCopied);
    }

    /// <summary>
    /// Snapshot only the rows of <paramref name="spec"/>'s primary table matching its lookup values
    /// (a dependent fan-out's affected set). The lookup filter may span several bounded-parameter
    /// batches (see <see cref="KeysetFilter.ForLookup"/>), scanned sequentially; resume is
    /// (<paramref name="startBatch"/>, <paramref name="startCursor"/>). <paramref name="saveProgress"/>
    /// receives (batch, cursor, rows, hasMore) — hasMore stays true until the last chunk of the last
    /// batch, so the job completes only when the whole scope is done.
    /// </summary>
    public async Task<long> BackfillScopeAsync(
        ScopedFanoutSpec spec, int startBatch, object?[]? startCursor, long startRows,
        Func<int, object?[]?, long, bool, CancellationToken, Task> saveProgress, CancellationToken ct)
    {
        var filters = KeysetFilter.ForLookup(spec.LookupColumns, spec.LookupValues);
        if (startBatch >= filters.Count)
        {
            // A resume point past the current batch count (e.g. a changed batching bound) can't be
            // trusted; rescan the whole scope — upsert-only, so overlap is safe.
            startBatch = 0;
            startCursor = null;
        }

        logger.ScopedFanoutStarting(spec.PrimaryTable.QualifiedName, spec.LookupValues.Count);

        // Hoisted above the batch loop so one scoped run shares one token across all its filter batches.
        var runId = Guid.NewGuid().ToString("N");
        var rowsCopied = startRows;
        for (var b = startBatch; b < filters.Count; b++)
        {
            var batch = b;
            var isLastBatch = batch == filters.Count - 1;
            var pager = new KeysetPager(spec.PrimaryTable, filters[batch], runId);

            rowsCopied = await RunChunkLoopAsync(
                pager, spec.PrimaryTable.QualifiedName, WallabyInstrumentation.BackfillKindFanout, spec.LookupValues.Count,
                batch == startBatch ? startCursor : null, rowsCopied,
                // A finished non-final batch persists (batch + 1, null): resume at the next batch's start.
                (cur, rows, hasMore, token) => saveProgress(
                    hasMore ? batch : batch + 1,
                    hasMore ? cur : null,
                    rows,
                    hasMore || !isLastBatch,
                    token),
                ct);
        }

        logger.ScopedFanoutComplete(spec.PrimaryTable.QualifiedName, rowsCopied);
        return rowsCopied;
    }

    private async Task<long> RunChunkLoopAsync(
        KeysetPager pager, string qualifiedTable, string backfillKind, int fanoutKeys,
        object?[]? startCursor, long startRows,
        Func<object?[]?, long, bool, CancellationToken, Task> saveProgress, CancellationToken ct)
    {
        var cursor = startCursor;
        var rowsCopied = startRows;
        var sessionStart = Stopwatch.GetTimestamp();
        var sessionRows = 0L;
        var lastProgressLog = sessionStart;

        // The chunk whose high watermark is emitted but whose delivery the pipeline hasn't finished. The
        // next chunk is read while it settles, overlapping the snapshot read with transform/sink delivery.
        // Safe because the protocol is stream-ordered, not wall-clock-ordered: each window's low watermark
        // precedes its concurrent live changes in the stream regardless of when the snapshot read ran, and
        // the coordinator already fans live keys into every active window. Costs at most two chunks in memory.
        (PendingWindow Window, BackfillChunk Chunk, long ChunkStart)? inFlight = null;
        PendingWindow? current = null;

        using var activity = _instr.StartBackfill();
        if (activity is not null)
        {
            activity.SetTag(WallabyInstrumentation.TableTag, qualifiedTable);
            activity.SetTag(WallabyInstrumentation.BackfillKindTag, backfillKind);
            if (fanoutKeys > 0)
            {
                activity.SetTag("wallaby.fanout.keys", fanoutKeys);
            }
        }

        _instr.BackfillStarted();
        try
        {
            // Hold a single connection across all watermark emissions for this backfill — keeps the
            // session alive and avoids the per-watermark open/auth overhead (two emissions per chunk).
            await using var emitter = await dataSource.OpenConnectionAsync(ct);

            while (!ct.IsCancellationRequested)
            {
                var chunkStart = WallabyInstrumentation.StartTimer();
                current = new PendingWindow
                {
                    QualifiedTable = qualifiedTable,
                    Token = Guid.NewGuid().ToString("N"),
                    SourceContext = activity?.Context ?? default,
                };
                _byToken[current.Token] = current;

                await EmitWatermarkAsync(emitter, WallabySchema.WatermarkLowPrefix, current.Token, ct);
                if (Fence is not null)
                {
                    await Fence.WaitAsync(emitter, qualifiedTable, ct);
                }
                var chunk = await pager.ReadChunkAsync(emitter, cursor, ChunkSize, ct);
                current.Buffer = chunk.Rows;
                await EmitWatermarkAsync(emitter, WallabySchema.WatermarkHighPrefix, current.Token, ct);
                cursor = chunk.NextCursor;

                // Settle the previous chunk before waiting on this one, so progress persists in emission order.
                if (inFlight is { } previous)
                {
                    await SettleAsync(previous);
                    inFlight = null;
                }

                if (!chunk.HasMore)
                {
                    await SettleAsync((current, chunk, chunkStart));
                    current = null;
                    break;
                }

                inFlight = (current, chunk, chunkStart);
                current = null;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
        finally
        {
            // On success the pipeline evicted each window via TryTakeHighWindow (no-ops here). On fault/cancel
            // (a chunk read or watermark emit failed, or a wait was cancelled) evict whatever is still
            // registered so an orphaned window can't leak in _byToken (with its buffered rows) or be re-paired
            // against a replayed high watermark in a later leadership session.
            if (current is not null)
            {
                _byToken.TryRemove(current.Token, out _);
            }
            if (inFlight is { } orphaned)
            {
                _byToken.TryRemove(orphaned.Window.Token, out _);
            }
            _instr.BackfillCompleted();
        }

        activity?.SetTag("wallaby.backfill.rows", rowsCopied);
        return rowsCopied;

        async Task SettleAsync((PendingWindow Window, BackfillChunk Chunk, long ChunkStart) entry)
        {
            await entry.Window.Completed.Task.WaitAsync(ct);

            rowsCopied += entry.Chunk.Rows.Count;
            sessionRows += entry.Chunk.Rows.Count;
            await saveProgress(entry.Chunk.NextCursor, rowsCopied, entry.Chunk.HasMore, ct);

            _instr.RecordBackfillRows(qualifiedTable, entry.Chunk.Rows.Count);
            _instr.RecordBackfillChunkDuration(qualifiedTable, entry.ChunkStart);

            if (Stopwatch.GetElapsedTime(lastProgressLog) >= ProgressLogInterval)
            {
                var rate = (long)(sessionRows / Stopwatch.GetElapsedTime(sessionStart).TotalSeconds);
                logger.BackfillProgress(qualifiedTable, rowsCopied, rate);
                lastProgressLog = Stopwatch.GetTimestamp();
            }
        }
    }

    // Transactional=true so the message commits with its own auto-commit transaction, preserving
    // commit-order interleaving with data-change transactions in pgoutput.
    private static Task EmitWatermarkAsync(NpgsqlConnection connection, string prefix, string token, CancellationToken ct)
        => PgExec.ExecuteAsync(
            connection,
            "SELECT pg_logical_emit_message(true, @prefix, @token)", ct,
            ("prefix", prefix), ("token", token));

    // ---- pipeline side ----

    public void OnLowWatermark(string token)
    {
        if (!_byToken.TryGetValue(token, out var window))
        {
            return;
        }

        if (!_recordingByTable.TryGetValue(window.QualifiedTable, out var list))
        {
            list = [];
            _recordingByTable[window.QualifiedTable] = list;
        }
        list.Add(window);
    }

    public bool IsRecording(string qualifiedTable)
    {
        if (!_recordingByTable.TryGetValue(qualifiedTable, out var list) || list.Count == 0)
        {
            return false;
        }

        // Drop windows abandoned by a faulted/cancelled backfill (evicted from _byToken but never taken by a
        // high watermark) so a dead window neither forces key materialization nor grows its SeenKeys for the
        // rest of the session.
        list.RemoveAll(w => !_byToken.ContainsKey(w.Token));
        return list.Count > 0;
    }

    public void RecordLiveKey(string qualifiedTable, DocumentKey key)
    {
        if (_recordingByTable.TryGetValue(qualifiedTable, out var list))
        {
            foreach (var window in list)
            {
                if (_byToken.ContainsKey(window.Token))
                {
                    window.SeenKeys.Add(key);
                }
            }
        }
    }

    public bool TryTakeHighWindow(string token, out PendingWindow window)
    {
        if (_byToken.TryRemove(token, out var found))
        {
            if (_recordingByTable.TryGetValue(found.QualifiedTable, out var list))
            {
                list.Remove(found);
            }
            window = found;
            return true;
        }

        window = null!;
        return false;
    }
}

/// <summary>Source-generated log messages for <see cref="WatermarkBackfillCoordinator"/>.</summary>
internal static partial class WatermarkBackfillCoordinatorLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Starting backfill of {Table}.")]
    internal static partial void BackfillStarting(this ILogger logger, string table);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Persisted cursor for {Table} does not match the table's current primary key; restarting the backfill from scratch.")]
    internal static partial void BackfillCursorRejected(this ILogger logger, string table);

    [LoggerMessage(Level = LogLevel.Information, Message = "Backfill of {Table} complete ({Rows} rows).")]
    internal static partial void BackfillComplete(this ILogger logger, string table, long rows);

    [LoggerMessage(Level = LogLevel.Information, Message = "Backfill of {Table} in progress: {Rows} row(s) copied so far ({Rate} rows/s).")]
    internal static partial void BackfillProgress(this ILogger logger, string table, long rows, long rate);

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting scoped fan-out backfill of {Table} ({Keys} key set(s)).")]
    internal static partial void ScopedFanoutStarting(this ILogger logger, string table, int keys);

    [LoggerMessage(Level = LogLevel.Information, Message = "Scoped fan-out backfill of {Table} complete ({Rows} rows).")]
    internal static partial void ScopedFanoutComplete(this ILogger logger, string table, long rows);
}
