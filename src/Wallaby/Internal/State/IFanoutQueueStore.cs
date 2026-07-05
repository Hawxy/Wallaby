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

    /// <summary>The oldest job that is <c>Requested</c> or (orphaned) <c>InProgress</c>, or null when the queue is drained.</summary>
    Task<FanoutJobRow?> GetNextDueAsync(CancellationToken ct);

    /// <summary>How many jobs are currently due (<c>Requested</c> or <c>InProgress</c>); feeds the queue-depth gauge.</summary>
    Task<long> CountDueAsync(CancellationToken ct);

    /// <summary>Mark a job <c>InProgress</c> and set its starting cursor (null restarts from the beginning).</summary>
    Task MarkInProgressAsync(string tableQualified, string lookupHash, string? startCursorJson, CancellationToken ct);

    /// <summary>Persist the resume cursor + running row count for an in-progress job (status untouched).</summary>
    Task SaveProgressAsync(string tableQualified, string lookupHash, string? cursorJson, long rowsCopied, CancellationToken ct);

    /// <summary>Mark a job <c>Completed</c>, but only if it is still <c>InProgress</c> (so a concurrent re-arm survives).</summary>
    Task CompleteAsync(string tableQualified, string lookupHash, CancellationToken ct);

    /// <summary>
    /// Postpone a job by moving it to the back of the queue (bump <c>requested_at</c>), leaving its status
    /// unchanged. Used when the job can't run yet — e.g. its table/columns aren't in the current model
    /// (a transient deploy-time divergence) — so it is retried later without dropping it or starving others.
    /// </summary>
    Task DeferAsync(string tableQualified, string lookupHash, CancellationToken ct);

    /// <summary>All queued jobs (for diagnostics/tests).</summary>
    Task<IReadOnlyList<FanoutJobRow>> ListAsync(CancellationToken ct);

    /// <summary>
    /// Open a subscription the worker waits on between drains, so it wakes the moment a job is enqueued
    /// (via LISTEN/NOTIFY) instead of polling every second. Scoped to the worker's lifetime — dispose it to
    /// release any resources (e.g. a dedicated listening connection).
    /// </summary>
    IFanoutQueueSubscription Subscribe();
}

/// <summary>
/// A wait handle the fan-out worker blocks on between drains. <see cref="WaitForJobAsync"/> returns as soon as
/// a job is enqueued (event-driven wake) or after the fallback timeout elapses (safety poll), whichever is first.
/// </summary>
internal interface IFanoutQueueSubscription : IAsyncDisposable
{
    /// <summary>Wait until a job is signalled or <paramref name="fallbackTimeout"/> elapses.</summary>
    Task WaitForJobAsync(TimeSpan fallbackTimeout, CancellationToken ct);
}
