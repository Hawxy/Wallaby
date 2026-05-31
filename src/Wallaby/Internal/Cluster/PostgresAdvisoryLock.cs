using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Cluster;

/// <summary>
/// Default <see cref="IClusterLock"/> using a Postgres session-level advisory lock on a dedicated
/// connection. Because the lock is session-scoped, it is released automatically if the holder's
/// connection drops (process crash, network partition) — giving fast, dependency-free failover.
/// </summary>
internal sealed class PostgresAdvisoryLock(NpgsqlDataSource dataSource) : IClusterLock
{
    public async Task<IClusterLockHandle?> TryAcquireAsync(string key, CancellationToken ct)
    {
        var lockKey = StableKey(key);
        var connection = await dataSource.OpenConnectionAsync(ct);
        try
        {
            var acquired = await PgExec.ScalarBoolAsync(connection, "SELECT pg_try_advisory_lock(@k)", ct, ("k", lockKey));
            if (!acquired)
            {
                await connection.DisposeAsync();
                return null;
            }

            return new Handle(connection, lockKey);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>Deterministic 64-bit key derived from the name (FNV-1a), used as the advisory lock id.</summary>
    private static long StableKey(string value)
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

    private sealed class Handle(NpgsqlConnection connection, long lockKey) : IClusterLockHandle
    {
        public bool IsHeld { get; private set; } = true;

        public async ValueTask DisposeAsync()
        {
            if (!IsHeld)
            {
                return;
            }
            IsHeld = false;

            try
            {
                await PgExec.ExecuteAsync(connection, "SELECT pg_advisory_unlock(@k)", CancellationToken.None, ("k", lockKey));
            }
            catch
            {
                // Closing the session releases the advisory lock regardless.
            }

            await connection.DisposeAsync();
        }
    }
}
