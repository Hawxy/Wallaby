using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Drains the scoped fan-out queue on the leader: claims due jobs, reconstructs their lookup against the
/// model, and runs each as a scoped backfill via the <see cref="WatermarkBackfillCoordinator"/>. A finished
/// job's row is removed only if it is still <c>InProgress</c>, so a trigger that re-arms it mid-run is
/// not lost (it re-runs on the next pass).
/// </summary>
internal sealed class FanoutQueueWorker(
    IFanoutQueueStore store, WatermarkBackfillCoordinator coordinator, WallabyModel model, ILogger logger,
    TimeSpan pollInterval, WallabyStatus? status = null, WallabyInstrumentation? instrumentation = null)
{
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    // Base delay before retrying after a failed drain pass. A pass fails when a job errors (e.g. a poison
    // scoped re-snapshot); the job is left in place to retry — never dropped — so the delay grows
    // exponentially to a cap, keeping a deterministically-failing job from hot-looping the worker.
    private static readonly TimeSpan BaseErrorRetryDelay = TimeSpan.FromSeconds(1);

    // Per-table lookups (table, column types, PK types), built once and shared by every job.
    private readonly Dictionary<string, TableLookup> _tablesByName =
        model.Tables.ToDictionary(t => t.QualifiedName, TableLookup.For, StringComparer.Ordinal);

    // Job keys already warned about as model-divergent, so a deferred (retrying) job logs only once.
    private readonly HashSet<string> _warnedDivergent = [];

    public async Task RunAsync(CancellationToken ct)
    {
        await using var signal = store.Subscribe();
        var backoff = new RetryBackoff(BaseErrorRetryDelay);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var drained = await DrainOnceAsync(ct);
                backoff.Reset();
                status?.ResetFanoutFailures();
                if (_instr.FanoutQueueDepthEnabled)
                {
                    // Sampled once per pass into a cached field, so the metric exporter never touches the DB.
                    _instr.RecordFanoutQueueDepth(await store.CountDueAsync(ct));
                }
                if (drained == 0)
                {
                    // Idle: wake the moment a job is enqueued (NOTIFY), or after the fallback interval elapses.
                    await signal.WaitForJobAsync(pollInterval, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.WorkerPassFailed(ex);
                status?.RecordFanoutFailure($"{ex.GetType().Name}: {ex.Message}");
                try { await Task.Delay(backoff.Next(), ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>Process every currently-due job exactly once; returns how many actually ran (deferred jobs don't count).</summary>
    public async Task<int> DrainOnceAsync(CancellationToken ct)
    {
        var processed = new HashSet<string>();
        var count = 0;
        while (!ct.IsCancellationRequested)
        {
            var job = await store.GetNextDueAsync(ct);
            if (job is null)
            {
                break;
            }

            // A job re-armed (or deferred) during this pass would otherwise spin the loop; once we've seen
            // it, stop — the next pass picks it up again.
            if (!processed.Add($"{job.TableQualified}|{job.LookupHash}"))
            {
                break;
            }

            if (await RunJobAsync(job, ct))
            {
                count++;
            }
        }
        return count;
    }

    // Returns true if the job actually ran; false if it was deferred (so callers don't treat a deferred
    // job as progress and hot-loop on it).
    private async Task<bool> RunJobAsync(FanoutJobRow job, CancellationToken ct)
    {
        if (!_tablesByName.TryGetValue(job.TableQualified, out var lookup) ||
            !TryResolveColumnTypes(lookup.ColumnTypesByName, job.LookupColumns, out var columnTypes))
        {
            // The model doesn't (yet) include this table/columns — likely a transient deploy-time
            // divergence. Defer rather than drop, so the job survives until the model converges; warn once.
            if (_warnedDivergent.Add($"{job.TableQualified}|{job.LookupHash}"))
            {
                logger.UnknownFanoutTable(job.TableQualified);
            }
            await store.DeferAsync(job.TableQualified, job.LookupHash, ct);
            return false;
        }

        var values = KeysetCodec.DeserializeTuples(job.LookupValuesJson, columnTypes);
        var spec = new ScopedFanoutSpec(lookup.Table, job.LookupColumns, values);

        // Requested = run fresh; an orphaned InProgress (leader crashed mid-run) resumes from its cursor.
        var fresh = job.Status == BackfillStatus.Requested;
        var startBatch = 0;
        object?[]? startCursor = null;
        if (!fresh && !KeysetCodec.TryDeserializeScopedCursor(
                job.CursorJson, lookup.PkColumns, lookup.PkTypes, out startBatch, out startCursor))
        {
            // The job's cursor was built against a different key shape (or format) — rerun the scope fresh.
            logger.FanoutCursorRejected(job.TableQualified);
            fresh = true;
            startBatch = 0;
            startCursor = null;
        }
        var startRows = fresh ? 0 : job.RowsCopied;

        await store.MarkInProgressAsync(job.TableQualified, job.LookupHash, fresh ? null : job.CursorJson, ct);

        await coordinator.BackfillScopeAsync(
            spec, startBatch, startCursor, startRows,
            (batch, cursor, rows, _, token) => store.SaveProgressAsync(
                job.TableQualified, job.LookupHash,
                KeysetCodec.SerializeScopedCursor(batch, cursor, lookup.PkColumns), rows, token),
            ct);

        await store.CompleteAsync(job.TableQualified, job.LookupHash, ct);
        return true;
    }

    private static bool TryResolveColumnTypes(
        IReadOnlyDictionary<string, Type> byName, IReadOnlyList<string> columns, out Type[] types)
    {
        types = new Type[columns.Count];
        for (var i = 0; i < columns.Count; i++)
        {
            if (!byName.TryGetValue(columns[i], out var type))
            {
                types = [];
                return false;
            }
            types[i] = type;
        }
        return true;
    }

    private sealed record TableLookup(
        CapturedTable Table, Dictionary<string, Type> ColumnTypesByName, string[] PkColumns, Type[] PkTypes)
    {
        public static TableLookup For(CapturedTable table) => new(
            table,
            table.Columns.ToDictionary(c => c.ColumnName, c => c.ClrType, StringComparer.Ordinal),
            [.. table.PrimaryKey.Select(c => c.ColumnName)],
            [.. table.PrimaryKey.Select(c => c.ClrType)]);
    }
}

/// <summary>Source-generated log messages for <see cref="FanoutQueueWorker"/>.</summary>
internal static partial class FanoutQueueWorkerLog
{
    [LoggerMessage(Level = LogLevel.Error, Message = "Fan-out queue worker pass failed; retrying.")]
    internal static partial void WorkerPassFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Fan-out job for {Table} references a table/column not in the current model; deferring it (it will retry once the model includes it — if a binding was removed, clear it from wallaby.fanout_queue).")]
    internal static partial void UnknownFanoutTable(this ILogger logger, string table);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Fan-out job cursor for {Table} does not match the table's current primary key; rerunning the scoped backfill from scratch.")]
    internal static partial void FanoutCursorRejected(this ILogger logger, string table);
}
