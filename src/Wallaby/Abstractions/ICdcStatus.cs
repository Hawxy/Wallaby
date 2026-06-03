namespace Wallaby.Abstractions;

/// <summary>The role a node is currently playing in the CDC cluster.</summary>
public enum CdcNodeRole
{
    /// <summary>Starting up; leadership not yet decided.</summary>
    Starting,

    /// <summary>This node holds leadership and runs the replication pipeline.</summary>
    Leader,

    /// <summary>Another node is the leader; this node is idle and ready to take over.</summary>
    Standby,

    /// <summary>The CDC background service has stopped (graceful shutdown or fatal fault).</summary>
    Stopped,
}

/// <summary>An immutable point-in-time view of a node's CDC status.</summary>
public sealed record CdcStatusSnapshot
{
    /// <summary>The node's current role.</summary>
    public required CdcNodeRole Role { get; init; }

    /// <summary>True when the CDC background service terminated with a fatal error.</summary>
    public bool Faulted { get; init; }

    /// <summary>The most recent error (exception type + message), if any.</summary>
    public string? LastError { get; init; }

    /// <summary>When this node's CDC runtime started.</summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>When this node acquired leadership; null unless <see cref="Role"/> is <see cref="CdcNodeRole.Leader"/>.</summary>
    public DateTimeOffset? LeaderSince { get; init; }

    /// <summary>The highest commit LSN acknowledged to Postgres.</summary>
    public ulong LastAcknowledgedLsn { get; init; }

    /// <summary>When the last transaction was acknowledged; null if none yet this session.</summary>
    public DateTimeOffset? LastProgressAt { get; init; }

    /// <summary>The most recent ingestion lag (now − source commit timestamp) in seconds; <c>-1</c> if unknown.</summary>
    public double LastIngestionLagSeconds { get; init; } = -1;

    /// <summary>Consecutive failed leader sessions (reset on a healthy-length session or on becoming a standby).</summary>
    public int ConsecutiveLeaderFailures { get; init; }

    /// <summary>The replication slot name.</summary>
    public string SlotName { get; init; } = "";
}

/// <summary>
/// A read-only view of this node's CDC status, for diagnostics and health checks. Registered as a singleton
/// by <c>AddWallaby</c>; the runtime, pipeline, and background service update it at lifecycle points.
/// </summary>
public interface ICdcStatus
{
    /// <summary>The latest status snapshot (lock-free, tear-free read).</summary>
    CdcStatusSnapshot Current { get; }
}
