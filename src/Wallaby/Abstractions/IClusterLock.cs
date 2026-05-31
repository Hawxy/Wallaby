namespace Wallaby.Abstractions;

/// <summary>
/// Distributed lock used for leader election so that exactly one node owns the replication slot
/// and runs backfills. The default implementation is a Postgres session-level advisory lock keyed
/// on the slot name; it is replaceable (e.g. Redis/ZooKeeper) without touching the pipeline.
/// </summary>
public interface IClusterLock
{
    /// <summary>
    /// Attempt to acquire leadership for <paramref name="key"/>. Returns a handle if acquired, or
    /// <c>null</c> if another node currently holds it. The handle is released on dispose, and the
    /// underlying lock is expected to release automatically if the holder's connection/session drops.
    /// </summary>
    Task<IClusterLockHandle?> TryAcquireAsync(string key, CancellationToken ct);
}

/// <summary>A held cluster lock. Disposing releases leadership.</summary>
public interface IClusterLockHandle : IAsyncDisposable
{
    /// <summary>True while leadership is still held.</summary>
    bool IsHeld { get; }
}
