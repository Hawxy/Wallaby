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
    string Table, WallabyBackfillStatus Status, long RowsCopied, DateTimeOffset UpdatedAt)
{
    /// <summary>
    /// The planner's row estimate (partitions summed) captured when the run started fresh; null when
    /// unknown or written by a host older than schema version 10.
    /// </summary>
    public long? EstimatedRows { get; init; }

    /// <summary>When the run started fresh (a resumed run keeps it); null when written by a host older than schema version 10.</summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>Fraction complete (0 to 1) when an estimate exists; null otherwise.</summary>
    public double? Progress => EstimatedRows is > 0 ? Math.Min(1d, RowsCopied / (double)EstimatedRows.Value) : null;

    /// <summary>
    /// Time left at the average rate since <see cref="StartedAt"/>; null unless the run is
    /// <see cref="WallabyBackfillStatus.InProgress"/> with an estimate and some progress. The rate is
    /// averaged over wall-clock time, so a run resumed after downtime reads pessimistic.
    /// </summary>
    public TimeSpan? EstimatedRemaining
    {
        get
        {
            if (Status != WallabyBackfillStatus.InProgress || EstimatedRows is not > 0 || RowsCopied <= 0 || StartedAt is not { } started)
            {
                return null;
            }
            var elapsed = UpdatedAt - started;
            if (elapsed <= TimeSpan.Zero)
            {
                return null;
            }
            var remaining = Math.Max(0, EstimatedRows.Value - RowsCopied);
            return TimeSpan.FromSeconds(remaining * elapsed.TotalSeconds / RowsCopied);
        }
    }
}
