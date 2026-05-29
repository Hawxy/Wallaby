namespace EFCore.CDC.Model;

/// <summary>A mapped column of a captured table, bridging the EF Core property and its Postgres column.</summary>
public sealed class CapturedColumn
{
    /// <summary>The EF Core property name.</summary>
    public required string PropertyName { get; init; }

    /// <summary>The physical Postgres column name.</summary>
    public required string ColumnName { get; init; }

    /// <summary>The CLR type of the property.</summary>
    public required Type ClrType { get; init; }

    /// <summary>True when the column participates in the primary key.</summary>
    public required bool IsPrimaryKey { get; init; }
}

/// <summary>
/// A table selected for capture, resolved from the EF Core model. Drives publication membership,
/// replica-identity validation, and entity materialization.
/// </summary>
public sealed class CapturedTable
{
    /// <summary>The mapped entity CLR type.</summary>
    public required Type EntityClrType { get; init; }

    /// <summary>The Postgres schema (defaults to <c>public</c> when unspecified in the model).</summary>
    public required string Schema { get; init; }

    /// <summary>The Postgres table name.</summary>
    public required string TableName { get; init; }

    /// <summary>All mapped columns, in model order.</summary>
    public required IReadOnlyList<CapturedColumn> Columns { get; init; }

    /// <summary>The primary key columns, in key ordinal order.</summary>
    public required IReadOnlyList<CapturedColumn> PrimaryKey { get; init; }

    /// <summary>
    /// True when a transform for this table requires old values / full row availability and therefore
    /// the table should use <c>REPLICA IDENTITY FULL</c>.
    /// </summary>
    public bool RequiresFullReplicaIdentity { get; init; }

    /// <summary>The schema-qualified table name, e.g. <c>public.orders</c>.</summary>
    public string QualifiedName => $"{Schema}.{TableName}";
}
