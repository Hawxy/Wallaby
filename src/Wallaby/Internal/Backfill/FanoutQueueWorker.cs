using Microsoft.Extensions.Logging;
using Wallaby.Abstractions;
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
    IFanoutQueueStore store, WatermarkBackfillCoordinator coordinator, CdcModel model, ILogger logger)
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var drained = await DrainOnceAsync(ct);
                if (drained == 0)
                {
                    await Task.Delay(PollInterval, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Fan-out queue worker pass failed; retrying.");
                try { await Task.Delay(PollInterval, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>Process every currently-due job exactly once; returns how many were run.</summary>
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

            // A job re-armed during this pass would otherwise spin the loop; defer it to the next pass.
            if (!processed.Add($"{job.TableQualified}|{job.LookupHash}"))
            {
                break;
            }

            await RunJobAsync(job, ct);
            count++;
        }
        return count;
    }

    private async Task RunJobAsync(FanoutJobRow job, CancellationToken ct)
    {
        var table = model.Tables.FirstOrDefault(t => t.QualifiedName == job.TableQualified);
        if (table is null || !TryResolveColumnTypes(table, job.LookupColumns, out var columnTypes))
        {
            logger.LogWarning(
                "Fan-out job for {Table} references a table/column not in the current model; completing it.",
                job.TableQualified);
            await store.CompleteAsync(job.TableQualified, job.LookupHash, ct);
            return;
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
