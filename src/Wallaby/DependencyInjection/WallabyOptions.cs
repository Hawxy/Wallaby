namespace Wallaby.DependencyInjection;

/// <summary>Configuration for a Wallaby instance, set via the fluent builder / <c>ConfigureOptions</c>.</summary>
public sealed class WallabyOptions
{
    /// <summary>Logical replication slot name.</summary>
    public string SlotName { get; set; } = "wallaby_cdc_slot";

    /// <summary>Publication name.</summary>
    public string PublicationName { get; set; } = "wallaby_cdc_pub";

    /// <summary>
    /// Backfill keyset page size. Chunk rows are held in memory — up to two chunks at once (the one being
    /// delivered plus the prefetched next) — so capped at 100,000.
    /// </summary>
    public int ChunkSize { get; set; } = 500;

    /// <summary>
    /// Maximum number of records handed to a sink (and to a transform) in a single batch. Bounds the
    /// working set for large live transactions, dependent fan-out, and backfill alike: the pipeline
    /// slices each dispatch into windows of at most this many records. It also caps the inline portion
    /// of a dependent fan-out — a wider fan-out's tail is offloaded to a scoped backfill job.
    /// Batches are materialized lists, so capped at 100,000.
    /// </summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>Reconcile an existing publication's table set to match the captured model.</summary>
    public bool ManagePublicationTables { get; set; } = true;

    /// <summary>
    /// Publish only each table's captured columns (PostgreSQL publication column lists), so excluded
    /// and unmapped columns never leave the server. Applies to the primary publication and only when
    /// <see cref="ManagePublicationTables"/> is true; external publications always publish whole
    /// tables, as do tables requiring <c>REPLICA IDENTITY FULL</c>.
    /// </summary>
    public bool PublicationColumnLists { get; set; } = true;

    /// <summary>Fail (instead of warn) when a table needs <c>REPLICA IDENTITY FULL</c> but lacks it.</summary>
    public bool RequireFullReplicaIdentity { get; set; }

    /// <summary>Automatically backfill a newly declared table.</summary>
    public bool AutoBackfillNewTables { get; set; } = true;

    /// <summary>Automatically re-backfill a table when its declared transform version changes.</summary>
    public bool AutoBackfillOnVersionChange { get; set; } = true;

    /// <summary>Retry policy for sink delivery (attempts, base delay, delay ceiling).</summary>
    public SinkRetryOptions SinkRetry { get; set; } = new();

    /// <summary>
    /// Internal tuning knobs (HA election cadence, connection keepalives, buffering ceilings). The
    /// defaults are safe for almost all deployments.
    /// </summary>
    public WallabyAdvancedOptions Advanced { get; } = new();

    /// <summary>
    /// Postgres connection string used for replication, checkpoint storage, advisory locks, and backfill
    /// reads. Supply it via <see cref="WallabyBuilder.UseConnectionString(string)"/> or through the options pipeline
    /// (<c>Configure&lt;WallabyOptions&gt;</c>, configuration binding, or <c>PostConfigure</c> — the standard
    /// ordering applies). Validated as non-empty on first resolution.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;
}
