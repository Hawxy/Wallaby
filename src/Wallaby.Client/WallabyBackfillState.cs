namespace Wallaby.Client;

/// <summary>Lifecycle of a table's backfill, as tracked in <c>wallaby.backfill_state</c>.</summary>
public enum WallabyBackfillStatus
{
    /// <summary>No backfill has been recorded for the table.</summary>
    NotStarted,

    /// <summary>A backfill has been requested and is awaiting/running on the leader.</summary>
    Requested,

    /// <summary>A backfill is in progress.</summary>
    InProgress,

    /// <summary>The backfill completed.</summary>
    Completed,
}

/// <summary>A tracked table's backfill state, read remotely by <see cref="WallabyControlClient"/>.</summary>
/// <param name="Table">Schema-qualified source table name (e.g. <c>public.orders</c>).</param>
/// <param name="Status">Current lifecycle status.</param>
/// <param name="RowsCopied">Number of rows snapshotted so far.</param>
/// <param name="UpdatedAt">When the state last changed.</param>
public sealed record WallabyBackfillState(
    string Table, WallabyBackfillStatus Status, long RowsCopied, DateTimeOffset UpdatedAt);
