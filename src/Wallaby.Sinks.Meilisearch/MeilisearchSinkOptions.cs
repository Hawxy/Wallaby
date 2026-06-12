using Meilisearch;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>Configuration for a <see cref="MeilisearchSink"/>.</summary>
public sealed class MeilisearchSinkOptions
{
    /// <summary>Meilisearch base URL, e.g. <c>http://localhost:7700</c>.</summary>
    public required string Host { get; set; }

    /// <summary>API key (master or a write key). Null for an unsecured instance.</summary>
    public string? ApiKey { get; set; }

    /// <summary>Default index used when a routed record has no explicit destination.</summary>
    public string? DefaultIndex { get; set; }

    /// <summary>The document primary-key field injected into every document (defaults to <c>id</c>).</summary>
    public string PrimaryKey { get; set; } = "id";

    /// <summary>
    /// When true (default), each indexing task is awaited to completion before the batch is considered
    /// delivered. This keeps delivery honest for at-least-once semantics at the cost of throughput.
    /// </summary>
    public bool WaitForCompletion { get; set; } = true;

    /// <summary>Maximum time to wait for a task when <see cref="WaitForCompletion"/> is true.</summary>
    public double WaitTimeoutMs { get; set; } = 60_000;

    /// <summary>Polling interval while waiting for a task.</summary>
    public int WaitIntervalMs { get; set; } = 50;

    /// <summary>
    /// When true (the default), every upsert document is checked against its index's configured attributes
    /// (searchable/filterable/sortable from <see cref="ConfigureIndex"/>): if a configured attribute is not a
    /// key on the document, delivery fails permanently rather than silently indexing a document Meilisearch
    /// cannot filter or sort on.
    /// </summary>
    public bool ValidateConfiguredAttributes { get; set; } = true;

    /// <summary>
    /// Indexes to ensure exist and configure when the sink initializes (before streaming begins). Indexes
    /// reached only at runtime (e.g. per-tenant <c>ScopedDestination</c> indexes) are not listed here; they
    /// auto-create on first write with <see cref="PrimaryKey"/> and receive no custom settings.
    /// </summary>
    public IList<MeilisearchIndexConfig> Indexes { get; } = [];

    /// <summary>
    /// Declare an index to ensure exists (created with <see cref="PrimaryKey"/> if missing) and optionally
    /// configure at startup. Fluent; call once per index.
    /// </summary>
    public MeilisearchSinkOptions ConfigureIndex(string name, Action<Settings>? configure = null)
    {
        Settings? settings = null;
        if (configure is not null) {
            settings = new Settings();
            configure(settings);
        }
        
        var config = new MeilisearchIndexConfig
        {
            Name = name,
            Settings = settings
        };
        Indexes.Add(config);
        return this;
    }
}
