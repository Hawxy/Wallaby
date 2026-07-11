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

    /// <summary>How long a standby node waits before retrying to acquire leadership.</summary>
    public TimeSpan StandbyRetryInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How long to wait before retrying after a failed leader session.</summary>
    public TimeSpan LeaderRetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often, while a single transaction is being processed, Wallaby sends a replication status
    /// update to keep the connection alive — covering slow transforms/sinks when the consumer isn't
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
    /// Minimum interval between writes of the <c>wallaby.checkpoint</c> row. The row backs slot-loss gap
    /// detection and observability; the authoritative resume position is the slot's
    /// <c>confirmed_flush_lsn</c>, so a seconds-stale checkpoint is safe (a stale value only widens a
    /// detected gap, and the repair is a re-backfill either way). <see cref="TimeSpan.Zero"/> writes on
    /// every acknowledged transaction.
    /// </summary>
    public TimeSpan CheckpointSaveInterval { get; set; } = TimeSpan.FromSeconds(5);
}
