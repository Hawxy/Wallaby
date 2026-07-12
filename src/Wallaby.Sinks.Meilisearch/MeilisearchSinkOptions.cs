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
    /// Maximum time to wait for an indexing task to complete. Every task is awaited to completion before
    /// the batch is considered delivered, keeping delivery honest for at-least-once semantics.
    /// </summary>
    public double WaitTimeoutMs { get; set; } = 60_000;

    /// <summary>Polling interval while waiting for a task.</summary>
    public int WaitIntervalMs { get; set; } = 50;

    /// <summary>
    /// Maximum records per indexing request. A larger batch is split into sequential requests, keeping
    /// each payload safely under Meilisearch's request body limit (100 MB by default —
    /// <c>payload_too_large</c> fails delivery permanently).
    /// </summary>
    public int MaxRecordsPerBatch { get; set; } = 500;

    /// <summary>
    /// <see cref="IHttpMessageHandlerFactory"/> client name whose handler pipeline the sink sends
    /// through. Null uses <see cref="MeilisearchSink.ClientNameFor"/> for the sink's name.
    /// </summary>
    public string? HttpClientName { get; set; }

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
