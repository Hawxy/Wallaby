namespace Wallaby.Client;

/// <summary>Lifecycle of a table's backfill, as tracked in <c>wallaby.backfill_state</c>.</summary>
/// <remarks>
/// Member names are parsed from the strings the host's <c>BackfillStatus</c> persists in
/// <c>wallaby.backfill_state</c>; the two enums must stay name-aligned. A persisted name this client
/// does not know reads as <see cref="Unknown"/>.
/// </remarks>
public enum WallabyBackfillStatus
{
    /// <summary>A backfill has been requested and is awaiting/running on the leader.</summary>
    Requested,

    /// <summary>A backfill is in progress.</summary>
    InProgress,

    /// <summary>The backfill completed.</summary>
    Completed,

    /// <summary>A queued request was cancelled before the leader served it; the table is skipped until requested again.</summary>
    Cancelled,

    /// <summary>A status written by a newer Wallaby host that this client version does not recognize.</summary>
    Unknown,
}

/// <summary>A tracked table's backfill state, read remotely by <see cref="WallabyControlClient"/>.</summary>
/// <param name="Table">Schema-qualified source table name (e.g. <c>public.orders</c>).</param>
/// <param name="Status">Current lifecycle status.</param>
/// <param name="RowsCopied">Number of rows snapshotted so far.</param>
/// <param name="UpdatedAt">When the state last changed.</param>
public sealed record WallabyBackfillState(
    string Table, WallabyBackfillStatus Status, long RowsCopied, DateTimeOffset UpdatedAt);
