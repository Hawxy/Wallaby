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

    // A long backfill reports progress at most this often (its start/completion are always logged).
    private static readonly TimeSpan ProgressLogInterval = TimeSpan.FromSeconds(30);

    // ---- backfill task side ----

    /// <summary>Snapshot a whole table chunk-by-chunk, resuming from persisted state. The live pipeline must be running.</summary>
    public async Task BackfillTableAsync(CapturedTable table, string? transformVersion, CancellationToken ct)
    {
        var pager = new KeysetPager(table);
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
            pager, table.QualifiedName, cursor, startRows,
            (cur, rows, hasMore, token) => store.SaveAsync(
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
    /// (a dependent fan-out's affected set), resuming from <paramref name="startCursor"/>.
    /// </summary>
    public async Task<long> BackfillScopeAsync(
        ScopedFanoutSpec spec, object?[]? startCursor, long startRows,
        Func<object?[]?, long, bool, CancellationToken, Task> saveProgress, CancellationToken ct)
    {
        var filter = KeysetFilter.ForLookup(spec.LookupColumns, spec.LookupValues);
        var pager = new KeysetPager(spec.PrimaryTable, filter);

        logger.ScopedFanoutStarting(spec.PrimaryTable.QualifiedName, spec.LookupValues.Count);

        var rowsCopied = await RunChunkLoopAsync(pager, spec.PrimaryTable.QualifiedName, startCursor, startRows, saveProgress, ct);

        logger.ScopedFanoutComplete(spec.PrimaryTable.QualifiedName, rowsCopied);
        return rowsCopied;
    }

    private async Task<long> RunChunkLoopAsync(
        KeysetPager pager, string qualifiedTable, object?[]? startCursor, long startRows,
        Func<object?[]?, long, bool, CancellationToken, Task> saveProgress, CancellationToken ct)
    {
        var cursor = startCursor;
        var rowsCopied = startRows;
        var sessionStart = Stopwatch.GetTimestamp();
        var sessionRows = 0L;
        var lastProgressLog = sessionStart;

        _instr.BackfillStarted();
        try
        {
            // Hold a single connection across all watermark emissions for this backfill — keeps the
            // session alive and avoids the per-watermark open/auth overhead (two emissions per chunk).
            await using var emitter = await dataSource.OpenConnectionAsync(ct);

            while (!ct.IsCancellationRequested)
            {
                var chunkStart = WallabyInstrumentation.StartTimer();
                var window = new PendingWindow
                {
                    QualifiedTable = qualifiedTable,
                    Token = Guid.NewGuid().ToString("N"),
                };
                _byToken[window.Token] = window;

                try
                {
                    await EmitWatermarkAsync(emitter, CdcSchema.WatermarkLowPrefix, window.Token, ct);
                    var chunk = await pager.ReadChunkAsync(emitter, cursor, ChunkSize, ct);
                    window.Buffer = chunk.Rows;
                    await EmitWatermarkAsync(emitter, CdcSchema.WatermarkHighPrefix, window.Token, ct);

                    await window.Completed.Task.WaitAsync(ct);

                    rowsCopied += chunk.Rows.Count;
                    sessionRows += chunk.Rows.Count;
                    cursor = chunk.NextCursor;
                    await saveProgress(cursor, rowsCopied, chunk.HasMore, ct);

                    _instr.RecordBackfillRows(qualifiedTable, chunk.Rows.Count);
                    _instr.RecordBackfillChunkDuration(qualifiedTable, chunkStart);

                    if (Stopwatch.GetElapsedTime(lastProgressLog) >= ProgressLogInterval)
                    {
                        var rate = (long)(sessionRows / Stopwatch.GetElapsedTime(sessionStart).TotalSeconds);
                        logger.BackfillProgress(qualifiedTable, rowsCopied, rate);
                        lastProgressLog = Stopwatch.GetTimestamp();
                    }

                    if (!chunk.HasMore)
                    {
                        break;
                    }
                }
                finally
                {
                    // On success the pipeline already evicted this window via TryTakeHighWindow (a no-op here).
                    // On fault/cancel (a chunk read or watermark emit failed, or the wait was cancelled) evict
                    // the orphaned window so it can't leak in _byToken (with its buffered rows) or be re-paired
                    // against a replayed high watermark in a later leadership session. 
                    _byToken.TryRemove(window.Token, out _);
                }
            }
        }
        finally
        {
            _instr.BackfillCompleted();
        }

        return rowsCopied;
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
