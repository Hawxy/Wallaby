namespace EFCore.CDC.Abstractions;

/// <summary>Lifecycle status of a per-table backfill.</summary>
public enum BackfillStatus
{
    /// <summary>No backfill has been recorded for the table.</summary>
    NotStarted,

    /// <summary>A backfill has been requested (manually or automatically) and is awaiting/running on the leader.</summary>
    Requested,

    /// <summary>A backfill is in progress; <see cref="BackfillState.CursorJson"/> holds the resume point.</summary>
    InProgress,

    /// <summary>The backfill completed for the recorded <see cref="BackfillState.TransformVersion"/>.</summary>
    Completed,
}

/// <summary>
/// Persisted per-table backfill bookkeeping, stored in <c>cdc.backfill_state</c>.
/// </summary>
/// <param name="TableQualifiedName">Schema-qualified source table name (e.g. <c>public.orders</c>).</param>
/// <param name="Status">Current lifecycle status.</param>
/// <param name="TransformVersion">
/// The transform/projection version this backfill is for. A change versus the declared version
/// triggers an automatic re-backfill.
/// </param>
/// <param name="CursorJson">Serialized keyset cursor (last primary key) for resuming an in-progress backfill.</param>
/// <param name="RowsCopied">Number of rows snapshotted so far.</param>
/// <param name="UpdatedAt">When the row was last updated.</param>
public sealed record BackfillState(
    string TableQualifiedName,
    BackfillStatus Status,
    string? TransformVersion,
    string? CursorJson,
    long RowsCopied,
    DateTimeOffset UpdatedAt);
