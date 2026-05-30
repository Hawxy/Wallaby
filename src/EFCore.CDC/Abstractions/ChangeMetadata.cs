namespace EFCore.CDC.Abstractions;

/// <summary>
/// Provenance for a change: which table it came from and where it sits in the
/// commit stream. Used for ordering, idempotency, and observability.
/// </summary>
/// <param name="TableSchema">Postgres schema of the source table (e.g. <c>public</c>).</param>
/// <param name="TableName">Source table name.</param>
/// <param name="CommitTimestamp">
/// Commit timestamp of the originating transaction, when available. Null for backfill reads.
/// </param>
/// <param name="CommitLsn">
/// Log Sequence Number of the commit, as a <see cref="ulong"/>. Zero for backfill reads.
/// </param>
/// <param name="CommitIdx">Zero-based index of this change within its transaction.</param>
/// <param name="IsBackfill">True when this change originated from a backfill snapshot.</param>
public sealed record ChangeMetadata(
    string TableSchema,
    string TableName,
    DateTimeOffset? CommitTimestamp,
    ulong CommitLsn,
    int CommitIdx,
    bool IsBackfill)
{
    private string? _qualifiedTableName;

    /// <summary>The schema-qualified table name, e.g. <c>public.orders</c>.</summary>
    public string QualifiedTableName => _qualifiedTableName ??= $"{TableSchema}.{TableName}";
}
