using Wallaby.Abstractions;

namespace Wallaby.Providers;

/// <summary>The result of materializing a <see cref="Model.RawChange"/> against the provider's model.</summary>
/// <param name="Action">
/// The action the routed <see cref="ChangeEvent"/> carries. Normally the raw change's action; a provider
/// may substitute what the change <em>means</em> in its model — e.g. surface a soft-delete UPDATE as a
/// Delete. Backfill detection keys off the raw action, not this.
/// </param>
/// <param name="Entity">The materialized CLR entity (may be partial for deletes).</param>
/// <param name="Record">Current (or, for deletes, last-known) values keyed by model property name.</param>
/// <param name="Changes">Previous values of changed properties for updates (when old values are available), else null.</param>
/// <param name="PrimaryKey">Primary key values in key ordinal order.</param>
/// <param name="EntityClrType">The mapped entity CLR type.</param>
public sealed record MaterializedRow(
    ChangeAction Action,
    object? Entity,
    IReadOnlyDictionary<string, object?> Record,
    IReadOnlyDictionary<string, object?>? Changes,
    IReadOnlyList<object> PrimaryKey,
    Type EntityClrType);
