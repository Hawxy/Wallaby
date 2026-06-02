using Meilisearch;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// Declarative configuration for a single Meilisearch index, applied once when the sink initializes:
/// the index is ensured to exist (created with the sink's <see cref="MeilisearchSinkOptions.PrimaryKey"/>
/// if missing) and, when <see cref="Settings"/> is provided, its settings are applied.
/// </summary>
public sealed class MeilisearchIndexConfig
{
    /// <summary>The index uid to ensure exists and configure.</summary>
    public required string Name { get; set; }

    /// <summary>
    /// Optional index settings — searchable / filterable / sortable attributes, ranking rules, stop words,
    /// synonyms, etc. Re-applied on every initialization (idempotent). Null leaves Meilisearch defaults.
    /// </summary>
    public Settings? Settings { get; set; }
}
