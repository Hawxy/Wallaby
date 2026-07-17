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
    /// Consecutive failed fan-out drain passes. Nonzero means the fan-out worker is stuck retrying (e.g. a
    /// poison scoped re-snapshot) with backoff; live replication is unaffected. Reset on a healthy pass.
    /// </summary>
    public int ConsecutiveFanoutFailures { get; init; }

    /// <summary>When the current suspension was requested; null unless <see cref="Role"/> is <see cref="WallabyNodeRole.Suspended"/>.</summary>
    public DateTimeOffset? SuspendedSince { get; init; }

    /// <summary>The reason recorded with the current suspension, if any.</summary>
    public string? SuspensionReason { get; init; }

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
