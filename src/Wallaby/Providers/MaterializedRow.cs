namespace Wallaby.Providers;

/// <summary>The result of materializing a <see cref="Model.RawChange"/> against the provider's model.</summary>
/// <param name="Entity">The materialized CLR entity (may be partial for deletes).</param>
/// <param name="Record">Current (or, for deletes, last-known) values keyed by model property name.</param>
/// <param name="Changes">Previous values of changed properties for updates (when old values are available), else null.</param>
/// <param name="PrimaryKey">Primary key values in key ordinal order.</param>
/// <param name="EntityClrType">The mapped entity CLR type.</param>
public sealed record MaterializedRow(
    object? Entity,
    IReadOnlyDictionary<string, object?> Record,
    IReadOnlyDictionary<string, object?>? Changes,
    IReadOnlyList<object> PrimaryKey,
    Type EntityClrType);
