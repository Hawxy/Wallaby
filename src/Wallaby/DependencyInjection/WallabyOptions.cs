namespace Wallaby.DependencyInjection;

/// <summary>What to do when a sink permanently fails (or exhausts retries) for a batch.</summary>
public enum WallabyDeadLetterPolicy
{
    /// <summary>Stop the pipeline (default); the batch is retried after the leader session restarts.</summary>
    Halt,

    /// <summary>Log and drop the failed batch, then continue (acknowledging the transaction).</summary>
    Skip,
}

/// <summary>Configuration for a Wallaby instance, set via the fluent builder / <c>ConfigureOptions</c>.</summary>
public sealed class WallabyOptions
{
    /// <summary>Logical replication slot name.</summary>
    public string SlotName { get; set; } = "wallaby_cdc_slot";

    /// <summary>Publication name.</summary>
    public string PublicationName { get; set; } = "wallaby_cdc_pub";

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
    /// Safety ceiling on how many changes a single <em>non-streamed</em> transaction may buffer in memory
    /// before processing. Transactions larger than the server's <c>logical_decoding_work_mem</c> are
    /// streamed and spilled out of memory (see <see cref="WallabyBuilder.SpillToDatabase"/> and friends), so
    /// they never hit this ceiling; it exists so a pathological transaction the server did not stream
    /// fails fast with an actionable error instead of exhausting memory. Must be greater than zero.
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

    /// <summary>
    /// Postgres connection string used for replication, checkpoint storage, advisory locks, and backfill
    /// reads. Supply it via <see cref="WallabyBuilder.UseConnectionString"/> or through the options pipeline
    /// (<c>Configure&lt;WallabyOptions&gt;</c>, configuration binding, or <c>PostConfigure</c> — the standard
    /// ordering applies). Validated as non-empty on first resolution.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>What to do when a sink permanently fails (or exhausts retries) for a batch.</summary>
    public WallabyDeadLetterPolicy DeadLetterPolicy { get; set; } = WallabyDeadLetterPolicy.Halt;
}
