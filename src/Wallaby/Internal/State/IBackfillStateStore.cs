using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>Persists per-table backfill state in <c>wallaby.backfill_state</c>.</summary>
internal interface IBackfillStateStore
{
    Task<BackfillState?> GetAsync(string tableQualifiedName, CancellationToken ct);

    /// <summary>
    /// Unconditionally write a full state row: the scheduler's fresh-run transition, which stamps the
    /// declared transform version and clears any pending purge mark.
    /// </summary>
    Task SaveAsync(BackfillState state, CancellationToken ct);

    /// <summary>
    /// Write a running backfill's progress (status, cursor, row count) and nothing else: the transform
    /// version keeps the value the fresh run started with, and a purge mark is never touched. A no-op
    /// when the row was concurrently marked <c>Requested</c> — the request survives and the table
    /// re-runs fresh.
    /// </summary>
    Task SaveProgressAsync(
        string tableQualifiedName, BackfillStatus status, string? cursorJson, long rowsCopied, CancellationToken ct);

    /// <summary>
    /// Mark a table <c>Requested</c> (cursor and row count reset) and signal the backfill notify channel,
    /// atomically with the row becoming visible. The single request write path (manual, remote client,
    /// slot-gap repair, fan-out overflow): an existing row keeps its transform version, and purge is
    /// sticky-OR — true marks a sink purge due before the fresh run, false leaves any pending mark in
    /// place (sticky until served).
    /// </summary>
    Task RequestAsync(string tableQualifiedName, bool purge, CancellationToken ct);

    /// <summary>
    /// Cancel a queued request: flip a <c>Requested</c> row to <c>Cancelled</c> and clear its pending
    /// purge mark. Returns false when the table has no queued request (absent, running, or completed).
    /// Best-effort against the scheduler: a request it has already begun serving proceeds.
    /// </summary>
    Task<bool> CancelRequestAsync(string tableQualifiedName, CancellationToken ct);

    /// <summary>Every table name currently marked <c>Requested</c>, mapped or not.</summary>
    Task<IReadOnlyList<string>> ListRequestedAsync(CancellationToken ct);

    Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct);

    /// <summary>
    /// Open a subscription the scheduler waits on between passes, so it wakes the moment a backfill is
    /// requested (via LISTEN/NOTIFY) instead of waiting out the poll interval.
    /// </summary>
    INotifySubscription Subscribe();
}
