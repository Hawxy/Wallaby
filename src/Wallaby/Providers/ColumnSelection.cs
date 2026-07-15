namespace Wallaby.Providers;

/// <summary>How a <see cref="ColumnSelection"/> interprets its property names.</summary>
public enum ColumnSelectionMode
{
    /// <summary>Capture only the named properties (primary-key and dependency-lookup columns are always kept).</summary>
    Include,

    /// <summary>Capture every mapped property except the named ones.</summary>
    Exclude,
}

/// <summary>
/// One mapping's declaration of the properties its transform consumes, made through provider mapping
/// extensions (e.g. EF Core's <c>Consumes</c>/<c>ConsumesAllExcept</c>). An entity's captured column set
/// is the union of the selections across all of its mappings; a mapping without a selection keeps the
/// entity at consume-all.
/// </summary>
public sealed record ColumnSelection(ColumnSelectionMode Mode, IReadOnlyList<string> PropertyNames);
