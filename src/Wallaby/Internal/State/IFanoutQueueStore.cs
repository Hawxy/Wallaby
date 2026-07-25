using Wallaby.Internal.Backfill;

namespace Wallaby.Internal.State;

/// <summary>
/// Persists the queue of scoped dependent fan-out jobs (<c>wallaby.fanout_queue</c>). Triggers for the
/// same primary table + lookup set coalesce into a single job so a hot dependent re-snapshots once.
/// </summary>
internal interface IFanoutQueueStore
{
    /// <summary>Enqueue (or re-arm) a scoped fan-out job for the given lookup set, marking it <c>Requested</c>.</summary>
    Task EnqueueAsync(ScopedFanoutSpec spec, CancellationToken ct);

    /// <summary>
    /// The next job that is <c>Requested</c> or (orphaned) <c>InProgress</c> and whose retry delay has
    /// elapsed, or null when nothing is currently runnable.
    /// </summary>
    Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct);

    /// <summary>How many jobs are currently due (<c>Requested</c> or <c>InProgress</c>); feeds the queue-depth gauge.</summary>
    Task<long> CountDueAsync(CancellationToken ct);

    /// <summary>
    /// The highest persisted attempt count among pending jobs (0 when none are failing). Feeds health
    /// grading: finished jobs are deleted, so a recovered job stops holding the value up on its own.
    /// </summary>
    Task<int> MaxAttemptsAsync(CancellationToken ct);

    /// <summary>Mark a job <c>InProgress</c> and set its starting cursor (null restarts from the beginning).</summary>
    Task MarkInProgressAsync(string tableQualified, string lookupHash, string? startCursorJson, CancellationToken ct);

    /// <summary>Persist the resume cursor + running row count for an in-progress job (status untouched).</summary>
    Task SaveProgressAsync(string tableQualified, string lookupHash, string? cursorJson, long rowsCopied, CancellationToken ct);

    /// <summary>Remove a finished job's row, but only if it is still <c>InProgress</c> (so a concurrent re-arm survives and re-runs).</summary>
    Task CompleteAsync(string tableQualified, string lookupHash, CancellationToken ct);

    /// <summary>
    /// Postpone a job by <paramref name="delay"/>, leaving its status and attempt count unchanged. Used when
    /// the job can't run yet — e.g. its table/columns aren't in the current model (a transient deploy-time
    /// divergence) — so it is retried later without dropping it or starving others.
    /// </summary>
    Task DeferAsync(string tableQualified, string lookupHash, TimeSpan delay, CancellationToken ct);

    /// <summary>
    /// Record a failed run: increment the job's attempt count, store <paramref name="error"/>, and push the
    /// next attempt out by a backoff derived from that count, so a failing job retries on its own schedule
    /// instead of blocking the jobs behind it.
    /// </summary>
    Task FailAsync(string tableQualified, string lookupHash, string error, CancellationToken ct);

    /// <summary>All queued jobs (for diagnostics/tests).</summary>
    Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct);

    /// <summary>
    /// Open a subscription the worker waits on between drains, so it wakes the moment a job is enqueued
    /// (via LISTEN/NOTIFY) instead of polling every second. Scoped to the worker's lifetime.
    /// </summary>
    INotifySubscription Subscribe();
}
