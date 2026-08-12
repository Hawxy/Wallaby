namespace Wallaby.Abstractions;

/// <summary>The role a node is currently playing in the Wallaby cluster.</summary>
public enum WallabyNodeRole
{
    /// <summary>Starting up; leadership not yet decided.</summary>
    Starting,

    /// <summary>This node holds leadership and runs the replication pipeline.</summary>
    Leader,

    /// <summary>Another node is the leader; this node is idle and ready to take over.</summary>
    Standby,

    /// <summary>The Wallaby background service has stopped (graceful shutdown or fatal fault).</summary>
    Stopped,

    /// <summary>
    /// The installation is suspended: every managed replication slot is dropped (e.g. for a database
    /// major-version upgrade) and this node idles until an explicit resume.
    /// </summary>
    Suspended,
}

/// <summary>An immutable point-in-time view of a node's Wallaby status.</summary>
public sealed record WallabyStatusSnapshot
{
    /// <summary>The node's current role.</summary>
    public required WallabyNodeRole Role { get; init; }

    /// <summary>True when the Wallaby background service terminated with a fatal error.</summary>
    public bool Faulted { get; init; }

    /// <summary>The most recent error (exception type + message), if any.</summary>
    public string? LastError { get; init; }

    /// <summary>When this node's Wallaby runtime started.</summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>When this node acquired leadership; null unless <see cref="Role"/> is <see cref="WallabyNodeRole.Leader"/>.</summary>
    public DateTimeOffset? LeaderSince { get; init; }

    /// <summary>The highest commit LSN acknowledged to Postgres.</summary>
    public ulong LastAcknowledgedLsn { get; init; }

    /// <summary>When the last transaction was acknowledged; null if none yet this session.</summary>
    public DateTimeOffset? LastProgressAt { get; init; }

    /// <summary>The most recent ingestion lag (now − source commit timestamp) in seconds; <c>-1</c> if unknown.</summary>
    public double LastIngestionLagSeconds { get; init; } = -1;

    /// <summary>
    /// Consecutive failed leader sessions. Reset when a transaction is fully delivered and acknowledged,
    /// on a clean step-down (lost lock), or on becoming a standby — never merely because a failing session
    /// survived for a while first. A steadily climbing value therefore indicates a crash-looping leader
    /// (e.g. a sink that permanently rejects a batch), even when each session streams before dying.
    /// </summary>
    public int ConsecutiveLeaderFailures { get; init; }

    /// <summary>
    /// The worst pending fan-out job's persisted failure streak (its <c>attempts</c> column). Nonzero
    /// means a job is failing and retrying with backoff (e.g. a poison scoped re-snapshot) while the rest
    /// of the queue drains and live replication is unaffected. Cleared when the job finally completes.
    /// </summary>
    public int ConsecutiveFanoutFailures { get; init; }

    /// <summary>
    /// Consecutive failed fan-out drain passes: the queue itself was unreachable, as opposed to one
    /// job failing (<see cref="ConsecutiveFanoutFailures"/>). In-memory; reset by a clean pass.
    /// </summary>
    public int ConsecutiveFanoutPassFailures { get; init; }

    /// <summary>
    /// The worst failing table's consecutive backfill failures. Nonzero means at least one table's
    /// backfill is stuck retrying with backoff (its persisted attempt count); other tables and live
    /// replication are unaffected. Cleared when the failing table's run finally starts fresh or completes.
    /// </summary>
    public int ConsecutiveBackfillFailures { get; init; }

    /// <summary>
    /// Consecutive failed backfill scheduler passes: the state store itself was unreachable, as opposed
    /// to one table failing (<see cref="ConsecutiveBackfillFailures"/>). In-memory; reset by a clean pass.
    /// </summary>
    public int ConsecutiveBackfillPassFailures { get; init; }

    /// <summary>When the current suspension was requested; null unless <see cref="Role"/> is <see cref="WallabyNodeRole.Suspended"/>.</summary>
    public DateTimeOffset? SuspendedSince { get; init; }

    /// <summary>The reason recorded with the current suspension, if any.</summary>
    public string? SuspensionReason { get; init; }

    /// <summary>
    /// True while managed publications are temporarily widened to whole-table membership (so schema
    /// migrations blocked by publication column lists can run). Capture is fully functional, but
    /// deliberately excluded columns are being published until a restore.
    /// </summary>
    public bool PublicationsWidened { get; init; }

    /// <summary>When the current publication widening was requested; null when not widened.</summary>
    public DateTimeOffset? PublicationsWidenedAt { get; init; }

    /// <summary>The replication slot name.</summary>
    public string SlotName { get; init; } = "";

    /// <summary>Per sink, when it last accepted a batch this session. Empty until a first delivery.</summary>
    public IReadOnlyDictionary<string, DateTimeOffset> LastSinkDeliveryAt { get; init; } =
        System.Collections.ObjectModel.ReadOnlyDictionary<string, DateTimeOffset>.Empty;
}

/// <summary>
/// A read-only view of this node's Wallaby status, for diagnostics and health checks. Registered as a singleton
/// by <c>AddWallaby</c>; the runtime, pipeline, and background service update it at lifecycle points.
/// </summary>
public interface IWallabyStatus
{
    /// <summary>The latest status snapshot (lock-free, tear-free read).</summary>
    WallabyStatusSnapshot Current { get; }
}
