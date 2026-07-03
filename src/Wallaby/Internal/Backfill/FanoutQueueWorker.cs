using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Drains the scoped fan-out queue on the leader: claims due jobs, reconstructs their lookup against the
/// model, and runs each as a scoped backfill via the <see cref="WatermarkBackfillCoordinator"/>. A job is
/// marked <c>Completed</c> only if it is still <c>InProgress</c>, so a trigger that re-arms it mid-run is
/// not lost (it re-runs on the next pass).
/// </summary>
internal sealed class FanoutQueueWorker(
    IFanoutQueueStore store, WatermarkBackfillCoordinator coordinator, WallabyModel model, ILogger logger,
    TimeSpan pollInterval, CdcStatus? status = null)
{
    // Base delay before retrying after a failed drain pass. A pass fails when a job errors (e.g. a poison
    // scoped re-snapshot); the job is left in place to retry — never dropped — so the delay grows
    // exponentially to a cap, keeping a deterministically-failing job from hot-looping the worker.
    private static readonly TimeSpan BaseErrorRetryDelay = TimeSpan.FromSeconds(1);

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
        var table = model.Tables.FirstOrDefault(t => t.QualifiedName == job.TableQualified);
        if (table is null || !TryResolveColumnTypes(table, job.LookupColumns, out var columnTypes))
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
        var spec = new ScopedFanoutSpec(table, job.LookupColumns, values);

        // Requested = run fresh; an orphaned InProgress (leader crashed mid-run) resumes from its cursor.
        var fresh = job.Status == BackfillStatus.Requested;
        var pkTypes = table.PrimaryKey.Select(c => c.ClrType).ToArray();
        var startCursor = fresh ? null : KeysetCodec.Deserialize(job.CursorJson, pkTypes);
        var startRows = fresh ? 0 : job.RowsCopied;

        await store.MarkInProgressAsync(job.TableQualified, job.LookupHash, fresh ? null : job.CursorJson, ct);

        await coordinator.BackfillScopeAsync(
            spec, startCursor, startRows,
            (cursor, rows, _, token) =>
                store.SaveProgressAsync(job.TableQualified, job.LookupHash, KeysetCodec.Serialize(cursor), rows, token),
            ct);

        await store.CompleteAsync(job.TableQualified, job.LookupHash, ct);
        return true;
    }

    private static bool TryResolveColumnTypes(CapturedTable table, IReadOnlyList<string> columns, out Type[] types)
    {
        var byName = table.Columns.ToDictionary(c => c.ColumnName, c => c.ClrType);
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
}

/// <summary>Source-generated log messages for <see cref="FanoutQueueWorker"/>.</summary>
internal static partial class FanoutQueueWorkerLog
{
    [LoggerMessage(Level = LogLevel.Error, Message = "Fan-out queue worker pass failed; retrying.")]
    internal static partial void WorkerPassFailed(this ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Fan-out job for {Table} references a table/column not in the current model; deferring it (it will retry once the model includes it — if a binding was removed, clear it from wallaby.fanout_queue).")]
    internal static partial void UnknownFanoutTable(this ILogger logger, string table);
}
