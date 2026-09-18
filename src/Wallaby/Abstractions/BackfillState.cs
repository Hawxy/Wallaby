namespace Wallaby.Abstractions;

/// <summary>Lifecycle status of a per-table backfill.</summary>
/// <remarks>
/// Member names are persisted in <c>wallaby.backfill_state</c> and parsed back (also by
/// <c>Wallaby.Client</c>'s <c>WallabyBackfillStatus</c>); never rename.
/// </remarks>
public enum BackfillStatus
{
    /// <summary>A backfill has been requested (manually or automatically) and is awaiting/running on the leader.</summary>
    Requested,

    /// <summary>A backfill is in progress; it resumes from its persisted keyset cursor.</summary>
    InProgress,

    /// <summary>The backfill completed for the recorded <see cref="BackfillState.TransformVersion"/>.</summary>
    Completed,

    /// <summary>
    /// A queued request was cancelled before the leader served it (any pending purge mark was cleared
    /// with it). The scheduler skips the table, including on a version change, until a new request
    /// (or slot-gap repair) marks it <see cref="Requested"/> again; sinks the withdrawn backfill would
    /// have converged stay as they are.
    /// </summary>
    Cancelled,
}

/// <summary>
/// Persisted per-table backfill bookkeeping, stored in <c>wallaby.backfill_state</c>.
/// </summary>
/// <param name="TableQualifiedName">Schema-qualified source table name (e.g. <c>public.orders</c>).</param>
/// <param name="Status">Current lifecycle status.</param>
/// <param name="TransformVersion">
/// The transform/projection version this backfill is for. A change versus the declared version
/// triggers an automatic re-backfill.
/// </param>
/// <param name="RowsCopied">Number of rows snapshotted so far.</param>
/// <param name="UpdatedAt">When the row was last updated.</param>
/// <param name="Purge">
/// A sink purge is due before the next fresh run (see <see cref="ISinkPurger"/>); cleared when
/// that run starts, so a resumed backfill never re-purges.
/// </param>
/// <param name="Attempts">
/// Consecutive failures of this table's backfill. The scheduler retries with exponential backoff per
/// table (other tables keep running); reset when a run starts fresh or completes.
/// </param>
/// <param name="NextAttemptAt">
/// Not before this time will the scheduler run the table again (failure backoff); null or a past time
/// means it is due whenever the scheduler next decides to run it.
/// </param>
/// <param name="LastError">The most recent failure (exception type + message); null when not failing.</param>
public sealed record BackfillState(
    string TableQualifiedName,
    BackfillStatus Status,
    string? TransformVersion,
    long RowsCopied,
    DateTimeOffset UpdatedAt,
    bool Purge = false,
    int Attempts = 0,
    DateTimeOffset? NextAttemptAt = null,
    string? LastError = null)
{
    /// <summary>Serialized keyset cursor (last primary key) for resuming an in-progress backfill.</summary>
    internal string? CursorJson { get; init; }

    /// <summary>
    /// The planner's row estimate for the table (partitions summed) when the current run started fresh;
    /// null when unknown (never analysed, or the row predates this column). Compare with
    /// <see cref="RowsCopied"/> for progress; it is an estimate, so the final count may differ.
    /// </summary>
    public long? EstimatedRows { get; init; }

    /// <summary>When the current run started fresh; a resumed run keeps it. Null when the row predates this column.</summary>
    public DateTimeOffset? StartedAt { get; init; }
}
