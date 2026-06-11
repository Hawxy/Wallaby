using Wallaby.Abstractions;

namespace Wallaby.Model;

/// <summary>
/// A decoded, copied-out change for one row, prior to materialization into a typed entity. Holds no
/// references to Npgsql message objects (all values are read and copied within the streaming loop).
/// </summary>
public sealed record RawChange
{
    /// <summary>The pgoutput relation id (table OID) this change belongs to.</summary>
    public required uint RelationId { get; init; }

    /// <summary>The source table's schema (pgoutput relation namespace).</summary>
    public required string Schema { get; init; }

    /// <summary>The source table name (pgoutput relation name).</summary>
    public required string TableName { get; init; }

    /// <summary>The kind of change.</summary>
    public required ChangeAction Action { get; init; }

    /// <summary>New row values (for insert/update/read). Empty for deletes.</summary>
    public IReadOnlyList<RawColumn> NewValues { get; init; } = [];

    /// <summary>
    /// Old row values for update/delete, as carried by the table's <c>REPLICA IDENTITY</c>
    /// (DEFAULT = primary key only and only when the key changed, FULL = all columns). Null when not available.
    /// </summary>
    public IReadOnlyList<RawColumn>? OldValues { get; init; }

    /// <summary>Commit LSN of the originating transaction; zero for backfill reads.</summary>
    public ulong CommitLsn { get; internal set; }

    /// <summary>Commit timestamp of the originating transaction; null for backfill reads.</summary>
    public DateTimeOffset? CommitTimestamp { get; internal set; }

    /// <summary>Zero-based index of this change within its transaction.</summary>
    public int CommitIdx { get; internal set; }

    /// <summary>The schema-qualified table name, e.g. <c>public.orders</c>.</summary>
    public string QualifiedName => field ??= $"{Schema}.{TableName}";
}
