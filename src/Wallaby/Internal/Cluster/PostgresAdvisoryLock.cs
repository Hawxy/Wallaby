using System.Collections.Concurrent;
using Medallion.Threading;
using Medallion.Threading.Postgres;
using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Cluster;

/// <summary>
/// Default <see cref="IClusterLock"/> backed by DistributedLock.Postgres: a transaction-scoped advisory
/// lock on a dedicated connection the library owns and monitors. 
/// </summary>
internal sealed class PostgresAdvisoryLock(NpgsqlDataSource dataSource) : IClusterLock
{
    // One long-lived lock object per key, reused across every acquisition attempt.
    private readonly ConcurrentDictionary<string, PostgresDistributedLock> _locks = new(StringComparer.Ordinal);

    public async Task<IClusterLockHandle?> TryAcquireAsync(string key, CancellationToken ct)
    {
        var advisoryLock = _locks.GetOrAdd(
            key,
            static (k, ds) => new PostgresDistributedLock(
                new PostgresAdvisoryLockKey(StableKey(k)),
                ds,
                o => o.UseTransaction()),
            dataSource);

        var handle = await advisoryLock.TryAcquireAsync(TimeSpan.Zero, ct);
        return handle is null ? null : new Handle(handle);
    }

    /// <summary>
    /// Deterministic 64-bit key derived from the name (FNV-1a), used as the advisory lock id. Every node
    /// version must derive the same key for the same name — the cluster's mutual exclusion depends on it —
    /// so this hash is pinned by a test and must never change.
    /// </summary>
    internal static long StableKey(string value)
    {
        const ulong offset = 14695981039346656037;
        const ulong prime = 1099511628211;
        var hash = offset;
        foreach (var b in System.Text.Encoding.UTF8.GetBytes(value))
        {
            hash ^= b;
            hash *= prime;
        }
        return unchecked((long)hash);
    }

    private sealed class Handle : IClusterLockHandle
    {
        private readonly IDistributedSynchronizationHandle _handle;
        private bool _disposed;

        public Handle(IDistributedSynchronizationHandle handle)
        {
            _handle = handle;
            // Reading HandleLostToken starts the library's connection monitoring.
            Lost = handle.HandleLostToken;
        }

        public bool IsHeld => !_disposed && !Lost.IsCancellationRequested;

        public CancellationToken Lost { get; }

        public async ValueTask DisposeAsync()
        {
            _disposed = true;
            try
            {
                await _handle.DisposeAsync();
            }
            catch
            {
                // The explicit unlock fails when the session is already gone (e.g. a terminated backend).
                // The library disposes its connection in a finally either way, and closing the session
                // releases the advisory lock regardless.
            }
        }
    }
}
