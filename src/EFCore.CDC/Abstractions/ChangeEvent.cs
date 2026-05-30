namespace EFCore.CDC.Abstractions;

/// <summary>
/// A materialized change. The current row state is exposed
/// both as a typed entity (<see cref="Entity"/>) and as a property-name keyed bag
/// (<see cref="Record"/>); modified columns' previous values are in <see cref="Changes"/>.
/// </summary>
/// <param name="Action">The kind of change.</param>
/// <param name="Metadata">Source/commit provenance.</param>
/// <param name="Entity">
/// The current row materialized into its mapped CLR entity. Null for deletes when the
/// row no longer exists, or when materialization is not applicable.
/// </param>
/// <param name="Record">Current column values keyed by EF Core property name.</param>
/// <param name="Changes">
/// Previous values of changed columns (keyed by EF Core property name) for updates/deletes,
/// subject to the table's <c>REPLICA IDENTITY</c>. Null for inserts and backfill reads.
/// </param>
/// <param name="PrimaryKey">The primary key values, in key ordinal order.</param>
public record ChangeEvent(
    ChangeAction Action,
    ChangeMetadata Metadata,
    object? Entity,
    IReadOnlyDictionary<string, object?> Record,
    IReadOnlyDictionary<string, object?>? Changes,
    IReadOnlyList<object> PrimaryKey)
{
    /// <summary>The CLR type of the mapped entity for the source table.</summary>
    public Type EntityClrType { get; init; } = typeof(object);

    /// <summary>
    /// The primary key of this change as a <see cref="DocumentKey"/>.
    /// </summary>
    public DocumentKey Key => field ??= new DocumentKey(PrimaryKey);
}

/// <summary>
/// Strongly-typed view of a <see cref="ChangeEvent"/> for a known entity type, handed to
/// <see cref="ICdcTransform{TEntity,TDocument}"/> implementations.
/// </summary>
public sealed record ChangeEvent<TEntity>(
    ChangeAction Action,
    ChangeMetadata Metadata,
    TEntity? Entity,
    IReadOnlyDictionary<string, object?> Record,
    IReadOnlyDictionary<string, object?>? Changes,
    IReadOnlyList<object> PrimaryKey)
    where TEntity : class
{
    /// <summary>The CLR type of the mapped entity (<typeparamref name="TEntity"/>).</summary>
    public Type EntityClrType => typeof(TEntity);

    /// <summary>The primary key of this change as a <see cref="DocumentKey"/>.</summary>
    public DocumentKey Key
    {
        get => field ??= new DocumentKey(PrimaryKey);
        internal init;
    }

    public TKey GetPrimaryKey<TKey>()
    {
        return (TKey)PrimaryKey[0];
    }
}
