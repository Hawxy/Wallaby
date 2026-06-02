using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Cluster;

/// <summary>
/// Default <see cref="IClusterLock"/> using a Postgres session-level advisory lock on a dedicated
/// connection. Because the lock is session-scoped, it is released automatically if the holder's
/// connection drops (process crash, network partition) — giving fast, dependency-free failover. The
/// returned handle also heartbeats that connection so a <em>silent</em> drop surfaces promptly via
/// <see cref="IClusterLockHandle.Lost"/>.
/// </summary>
internal sealed class PostgresAdvisoryLock(NpgsqlDataSource dataSource, TimeSpan heartbeatInterval = default) : IClusterLock
{
    private readonly TimeSpan _heartbeatInterval =
        heartbeatInterval > TimeSpan.Zero ? heartbeatInterval : TimeSpan.FromSeconds(10);

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

            return new Handle(connection, lockKey, _heartbeatInterval);
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

    private sealed class Handle : IClusterLockHandle
    {
        private readonly NpgsqlConnection _connection;
        private readonly long _lockKey;
        private readonly CancellationTokenSource _lost = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _heartbeat;

        public Handle(NpgsqlConnection connection, long lockKey, TimeSpan heartbeatInterval)
        {
            _connection = connection;
            _lockKey = lockKey;

            // Heartbeat the dedicated lock connection. The session advisory lock lives as long as the
            // connection, so a failed probe means the session (and lock) is gone — cancel Lost so the
            // leader steps down. (The probe also keeps the otherwise-idle connection alive.)
            _heartbeat = LeadershipMonitor.WatchAsync(
                ProbeAsync,
                heartbeatInterval,
                onLost: () => { IsHeld = false; return _lost.CancelAsync(); },
                _stop.Token);
        }

        public bool IsHeld { get; private set; } = true;

        public CancellationToken Lost => _lost.Token;

        private async Task<bool> ProbeAsync(CancellationToken ct)
        {
            try
            {
                await PgExec.ScalarAsync(_connection, "SELECT 1", ct);
                return true;
            }
            catch (OperationCanceledException)
            {
                throw; // we're stopping, not losing the lock
            }
            catch
            {
                return false; // connection broken => session gone => lock lost
            }
        }

        public async ValueTask DisposeAsync()
        {
            // Stop the heartbeat first so it can't touch the connection while we release/close it.
            await _stop.CancelAsync();
            try { await _heartbeat; } catch { /* monitor swallows cancellation */ }

            var wasHeld = IsHeld;
            IsHeld = false;
            try
            {
                // Skip the explicit unlock if the session is already gone (it released the lock for us).
                if (wasHeld)
                {
                    await PgExec.ExecuteAsync(_connection, "SELECT pg_advisory_unlock(@k)", CancellationToken.None, ("k", _lockKey));
                }
            }
            catch
            {
                // Closing the session releases the advisory lock regardless.
            }

            await _connection.DisposeAsync(); // always dispose, even after a detected loss, so it can't leak
            _stop.Dispose();
            _lost.Dispose();
        }
    }
}
