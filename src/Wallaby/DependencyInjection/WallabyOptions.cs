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
    /// Honor declared column selections at the publication (PostgreSQL publication column lists), so a
    /// table with a <c>Consumes</c>/<c>ConsumesAllExcept</c> selection publishes only those columns and
    /// the excluded ones never leave the server. Dependent-only tables, narrowed automatically to their
    /// primary key and lookup columns, are listed too. Every other table publishes whole, so its columns
    /// stay free to <c>ALTER</c> and <c>DROP</c>. Setting this to false disables column lists entirely,
    /// including declared selections. Applies to the primary publication and only when
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

    /// <summary>
    /// Purge sink destinations before the automatic re-backfill that repairs a slot-loss gap, so the
    /// recovery also removes documents whose deletes fell inside the gap. Requires sinks to implement
    /// <see cref="Wallaby.Abstractions.ISinkPurger"/> (others are skipped with a warning). While each
    /// table's re-backfill runs, its purged destinations are temporarily incomplete. Independent of
    /// this option, a single resume can request the same purge per operation via the control client's
    /// <c>ResumeAsync(purge: true)</c>.
    /// </summary>
    public bool PurgeOnSlotGapRepair { get; set; }

    /// <summary>
    /// Heal a change whose unchanged TOASTed value was not on the wire (<c>REPLICA IDENTITY DEFAULT</c>)
    /// by re-reading the row by primary key instead of halting the pipeline. The re-read returns current
    /// row state, not commit-time state; later updates to the row are themselves in the stream, so sinks
    /// converge forward. A vanished row's change is dropped (its delete follows later in the stream).
    /// Each healed change logs a warning naming the <c>REPLICA IDENTITY FULL</c> DDL that removes the cost.
    /// </summary>
    public bool ReselectUnavailableValues { get; set; } = true;

    /// <summary>Retry policy for sink delivery (attempts, base delay, delay ceiling).</summary>
    public SinkRetryOptions SinkRetry { get; set; } = new();

    /// <summary>
    /// Deploy-time suspension flag (see <see cref="WallabyBuilder.Suspend"/>). While set, this node drops
    /// every managed replication slot and idles instead of streaming — so a platform blocked by logical
    /// slots (e.g. an RDS/Aurora major-version upgrade) can proceed. A node deployed without the flag
    /// automatically resumes a flag-driven suspension; a runtime-requested one (Wallaby.Client's
    /// <c>SuspendAsync</c>) persists until an explicit resume.
    /// </summary>
    public bool Suspended { get; set; }

    /// <summary>Free-text reason recorded with a <see cref="Suspended"/> flag-driven suspension.</summary>
    public string? SuspensionReason { get; set; }

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
