using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>Persists per-table backfill state in <c>wallaby.backfill_state</c>.</summary>
internal interface IBackfillStateStore
{
    Task<BackfillState?> GetAsync(string tableQualifiedName, CancellationToken ct);

    /// <summary>Unconditionally write a state row (scheduler transitions, slot-gap repair).</summary>
    Task SaveAsync(BackfillState state, CancellationToken ct);

    /// <summary>
    /// Write a running backfill's progress, unless the row was concurrently marked <c>Requested</c> —
    /// then the save is a no-op so the request survives and the table re-runs fresh.
    /// </summary>
    Task SaveProgressAsync(BackfillState state, CancellationToken ct);

    /// <summary>
    /// Mark a table <c>Requested</c> (cursor and row count reset) and signal the backfill notify channel,
    /// atomically with the row becoming visible.
    /// </summary>
    Task RequestAsync(string tableQualifiedName, string? transformVersion, CancellationToken ct);

    /// <summary>The subset of <paramref name="tableQualifiedNames"/> currently marked <c>Requested</c>.</summary>
    Task<IReadOnlyList<string>> ListRequestedAsync(IReadOnlyList<string> tableQualifiedNames, CancellationToken ct);

    Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct);

    /// <summary>
    /// Open a subscription the scheduler waits on between passes, so it wakes the moment a backfill is
    /// requested (via LISTEN/NOTIFY) instead of waiting out the poll interval.
    /// </summary>
    INotifySubscription Subscribe();
}
