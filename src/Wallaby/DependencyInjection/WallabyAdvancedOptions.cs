namespace Wallaby.DependencyInjection;

/// <summary>
/// Internal tuning knobs. The defaults are safe for almost all deployments.
/// You shouldn't modify these unless you know what you're doing.
/// </summary>
public sealed class WallabyAdvancedOptions
{
    /// <summary>
    /// Safety ceiling on how many changes a single <em>non-streamed</em> transaction may buffer in memory
    /// before processing. Transactions larger than the server's <c>logical_decoding_work_mem</c> are
    /// streamed and spilled out of memory (see <see cref="WallabyBuilder.SpillToDatabase"/> and friends), so
    /// they never hit this ceiling; it exists so a pathological transaction the server did not stream
    /// fails fast with an actionable error instead of exhausting memory. Must be greater than zero.
    /// </summary>
    public int MaxBufferedChangesPerTransaction { get; set; } = 1_000_000;

    /// <summary>
    /// Maximum number of committed transactions coalesced into one delivery batch: one sink dispatch
    /// and one acknowledgement at the last transaction's LSN. Coalescing is opportunistic: transactions
    /// are added only while the stream already has more buffered, so a quiet slot delivers each
    /// transaction immediately with no added latency. The record cap is <see cref="WallabyOptions.MaxBatchSize"/>.
    /// A delivery failure acknowledges nothing and the whole batch is redelivered on the next leader
    /// session (at-least-once; idempotent sinks converge). 1 disables coalescing.
    /// </summary>
    public int MaxTransactionsPerBatch { get; set; } = 100;

    /// <summary>
    /// Safety valve on how many distinct dependent-lookup keys one transaction may fan out for a single
    /// <c>DependsOn</c> binding. A wide fan-out is offloaded to the queue in bounded chunk jobs as the
    /// keys accumulate, so memory stays flat regardless of size; past this cap the transaction has
    /// effectively rewritten the dependent table, and the binding's whole primary table is re-snapshotted
    /// instead (backfill is upsert-only, so the wider scan converges to the same result). Must be greater
    /// than zero.
    /// </summary>
    public int MaxFanoutKeysPerTransaction { get; set; } = 1_000_000;

    /// <summary>How long a standby node waits before retrying to acquire leadership.</summary>
    public TimeSpan StandbyRetryInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How long to wait before retrying after a failed leader session.</summary>
    public TimeSpan LeaderRetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often, while a single transaction is being processed, Wallaby sends a replication status
    /// update to keep the connection alive, covering slow transforms/sinks when the consumer isn't
    /// reading the stream (so Npgsql can't answer the server's keepalives). Keep it well under the
    /// server's <c>wal_sender_timeout</c> (default 60s).
    /// </summary>
    public TimeSpan KeepaliveInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Fallback poll interval for the dependent fan-out queue. The worker is primarily woken on demand via
    /// LISTEN/NOTIFY the instant a job is enqueued; this interval is only a safety net that re-checks the queue
    /// in case a notification is ever missed (e.g. a dropped listening connection). Lower it for tighter
    /// worst-case fan-out latency at the cost of more idle queue polls.
    /// </summary>
    public TimeSpan FanoutPollInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Fallback poll interval for manual backfill requests. The leader's scheduler is primarily woken on
    /// demand via LISTEN/NOTIFY the instant a request is persisted; this interval is only a safety net that
    /// re-checks for requests in case a notification is ever missed (e.g. a dropped listening connection).
    /// </summary>
    public TimeSpan BackfillPollInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Fallback poll interval for the suspend/resume control state: how often the leader re-checks for a
    /// suspension request and a suspended node re-checks for a resume. Both are primarily woken on demand
    /// via LISTEN/NOTIFY the instant the control row changes; this interval is only a safety net in case a
    /// notification is ever missed (e.g. a dropped listening connection).
    /// </summary>
    public TimeSpan ControlPollInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Floor on how long a flag-less node waits before auto-resuming a configuration-origin suspension
    /// whose liveness heartbeat has gone quiet. The effective grace is
    /// <c>max(ControlPollInterval * 4, SuspensionAutoResumeGraceFloor)</c>: flag-carrying nodes refresh
    /// the heartbeat every control poll, so a mixed rolling deployment stays suspended instead of
    /// flip-flopping slots (each flap forces a full re-backfill), while the grace bounds the dead time a
    /// fully flag-less deployment waits before resuming.
    /// </summary>
    public TimeSpan SuspensionAutoResumeGraceFloor { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How often the leader emits a tiny transactional heartbeat message (<c>pg_logical_emit_message</c>)
    /// while the pipeline is idle, so the slot's <c>confirmed_flush_lsn</c> keeps advancing even when the
    /// mapped tables are quiet while other tables churn WAL, preventing unbounded WAL retention (and
    /// eventual <c>max_slot_wal_keep_size</c> slot invalidation) on shared databases. Suppressed while
    /// real traffic is being acknowledged. <see cref="TimeSpan.Zero"/> disables the heartbeat.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum interval between writes of the <c>wallaby.checkpoint</c> row. The row backs slot-loss gap
    /// detection and observability; the authoritative resume position is the slot's
    /// <c>confirmed_flush_lsn</c>, so a seconds-stale checkpoint is safe (a stale value only widens a
    /// detected gap, and the repair is a re-backfill either way). <see cref="TimeSpan.Zero"/> writes on
    /// every acknowledged transaction.
    /// </summary>
    public TimeSpan CheckpointSaveInterval { get; set; } = TimeSpan.FromSeconds(5);
}
