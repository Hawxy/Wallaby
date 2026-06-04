namespace Wallaby.DependencyInjection;

/// <summary>What to do when a sink permanently fails (or exhausts retries) for a batch.</summary>
public enum CdcDeadLetterPolicy
{
    /// <summary>Stop the pipeline (default); the batch is retried after the leader session restarts.</summary>
    Halt,

    /// <summary>Log and drop the failed batch, then continue (acknowledging the transaction).</summary>
    Skip,
}

/// <summary>Configuration for a CDC instance, set via the fluent builder / <c>ConfigureOptions</c>.</summary>
public sealed class CdcOptions
{
    /// <summary>Logical replication slot name.</summary>
    public string SlotName { get; set; } = "efcore_cdc_slot";

    /// <summary>Publication name.</summary>
    public string PublicationName { get; set; } = "efcore_cdc_pub";

    /// <summary>Backfill keyset page size.</summary>
    public int ChunkSize { get; set; } = 500;

    /// <summary>
    /// Maximum number of records handed to a sink (and to a transform) in a single batch. Bounds the
    /// working set for large live transactions, dependent fan-out, and backfill alike: the pipeline
    /// slices each dispatch into windows of at most this many records. It also caps the inline portion
    /// of a dependent fan-out — a wider fan-out's tail is offloaded to a scoped backfill job.
    /// </summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Safety ceiling on how many changes a single transaction may buffer in memory before processing.
    /// With pgoutput v2 streaming, transactions larger than the server's <c>logical_decoding_work_mem</c> are
    /// streamed and buffered until their commit; this caps that buffer so a pathological transaction fails fast
    /// with an actionable error instead of exhausting memory. (A future disk/DB spill removes this ceiling for
    /// arbitrarily large transactions.) Must be greater than zero.
    /// </summary>
    public int MaxBufferedChangesPerTransaction { get; set; } = 1_000_000;

    /// <summary>Reconcile an existing publication's table set to match the captured model.</summary>
    public bool ManagePublicationTables { get; set; } = true;

    /// <summary>Fail (instead of warn) when a table needs <c>REPLICA IDENTITY FULL</c> but lacks it.</summary>
    public bool RequireFullReplicaIdentity { get; set; }

    /// <summary>Automatically backfill a newly declared table.</summary>
    public bool AutoBackfillNewTables { get; set; } = true;

    /// <summary>Automatically re-backfill a table when its declared transform version changes.</summary>
    public bool AutoBackfillOnVersionChange { get; set; } = true;

    /// <summary>How long a standby node waits before retrying to acquire leadership.</summary>
    public TimeSpan StandbyRetryInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How long to wait before retrying after a failed leader session.</summary>
    public TimeSpan LeaderRetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often the leader verifies it still holds the cluster lock while streaming. If the lock's
    /// connection has dropped (so Postgres auto-released it), the leader steps down within roughly this
    /// interval and re-elects, instead of running on with a stale lock.
    /// </summary>
    public TimeSpan LeaderHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How often, while a single transaction is being processed, Wallaby sends a replication status
    /// update to keep the connection alive — covering slow transforms/sinks when the consumer isn't
    /// reading the stream (so Npgsql can't answer the server's keepalives). Keep it well under the
    /// server's <c>wal_sender_timeout</c> (default 60s).
    /// </summary>
    public TimeSpan KeepaliveInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Postgres connection string. Set via <see cref="CdcBuilder.UseConnectionString"/>.</summary>
    public string ConnectionString { get; internal set; } = string.Empty;

    /// <summary>What to do when a sink permanently fails (or exhausts retries) for a batch.</summary>
    public CdcDeadLetterPolicy DeadLetterPolicy { get; set; } = CdcDeadLetterPolicy.Halt;
}
