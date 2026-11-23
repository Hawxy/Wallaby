using System.Data;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Internal.Cluster;

/// <summary>
/// Default <see cref="IClusterLock"/>: a Postgres advisory lock held on a dedicated connection, either
/// session-scoped (<c>pg_try_advisory_lock</c>) or, when <c>transactional</c> is set,
/// transaction-scoped (<c>pg_try_advisory_xact_lock</c>) inside a transaction held open for the lock's
/// lifetime.
/// </summary>
/// <remarks>
/// While held, the connection is monitored passively with <see cref="NpgsqlConnection.WaitAsync(TimeSpan, CancellationToken)"/>:
/// a socket read with no query running, so a terminated backend or broken socket fires
/// <see cref="IClusterLockHandle.Lost"/> as soon as the socket fails, and the session shows as idle. After
/// each wait a keepalive query keeps idle-session reapers off the session and, through its timeout,
/// surfaces a silent partition. When the connection string enables Npgsql <c>KeepAlive</c> (interrupting a
/// wait mid-keepalive breaks the connection) or <c>Multiplexing</c> (no wait support), monitoring instead
/// parks back-to-back <c>pg_sleep</c> queries on the connection. Multiplexing requires
/// <c>transactional</c>: outside a transaction its statements don't stay on one session.
/// </remarks>
internal sealed class PostgresAdvisoryLock : IClusterLock
{
    // Length of one monitoring wait (or sleep), so also the keepalive cadence.
    private const int MonitorSliceSeconds = 30;

    // How long a keepalive, or a fallback sleep past its slice, may run before the connection is considered gone.
    private const int MonitorGraceSeconds = 15;

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _transactional;
    private readonly bool _passive;
    private readonly ILogger _logger;

    public PostgresAdvisoryLock(NpgsqlDataSource dataSource, bool transactional = false, ILogger? logger = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
        // Read by keyword because Npgsql 11 removed the property.
        var multiplexing = builder.TryGetValue("Multiplexing", out var value) && value is true;
        if (multiplexing && !transactional)
        {
            throw new NotSupportedException(
                "The cluster lock can't hold a session-scoped advisory lock over an Npgsql Multiplexing connection, "
                + "whose statements don't stay on one session. Set WallabyOptions.Advanced.TransactionalClusterLock "
                + "to true, or disable Multiplexing.");
        }

        _dataSource = dataSource;
        _transactional = transactional;
        _passive = builder.KeepAlive == 0 && !multiplexing;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>Whether held locks are monitored with <c>WaitAsync</c> rather than <c>pg_sleep</c>.</summary>
    internal bool MonitorsPassively => _passive;

    public async Task<IClusterLockHandle?> TryAcquireAsync(string key, CancellationToken ct)
    {
        var lockKey = StableKey(key);
        var connection = await _dataSource.OpenConnectionAsync(ct);
        NpgsqlTransaction? transaction = null;
        try
        {
            if (_transactional)
            {
                transaction = await connection.BeginTransactionAsync(ct);
            }

            if (await PgExec.ScalarBoolAsync(
                    connection,
                    _transactional ? "SELECT pg_try_advisory_xact_lock(@key)" : "SELECT pg_try_advisory_lock(@key)",
                    ct,
                    ("key", lockKey)))
            {
                if (transaction is not null && HasTransactionTimeout(connection))
                {
                    // The lock's transaction stays open for as long as the lock is held.
                    await PgExec.ExecuteAsync(connection, "SELECT set_config('transaction_timeout', '0', true)", ct);
                }
                return new Handle(this, key, connection, transaction, lockKey);
            }
        }
        catch
        {
            // A cancelled acquire may still have been granted server side; release before pooling.
            await ReleaseAsync(key, connection, transaction, lockKey);
            throw;
        }

        await ReleaseAsync(key, connection, transaction, unlockKey: null);
        return null;
    }

    // transaction_timeout (PostgreSQL 17+) ends the session, not just the transaction; earlier servers
    // reject the setting.
    private static bool HasTransactionTimeout(NpgsqlConnection connection) => connection.PostgreSqlVersion.Major >= 17;

    /// <summary>
    /// Deterministic 64-bit key derived from the name (FNV-1a), used as the advisory lock id. Every node
    /// version must derive the same key for the same name (the cluster's mutual exclusion depends on it),
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

    /// <summary>
    /// Release the lock and close the connection. The explicit session unlock matters: a pooled connection
    /// keeps its session state (and so a session lock) until it is next reused. A broken connection isn't
    /// returned to the pool, and its closed session released the lock server side; a release that fails on
    /// a live connection clears the pool so that connection is closed rather than pooled with the lock.
    /// </summary>
    private async Task ReleaseAsync(string key, NpgsqlConnection connection, NpgsqlTransaction? transaction, long? unlockKey)
    {
        try
        {
            if (transaction is not null)
            {
                await transaction.RollbackAsync(CancellationToken.None);
            }
            else if (unlockKey is { } lockKey)
            {
                await PgExec.ExecuteAsync(connection, "SELECT pg_advisory_unlock(@key)", CancellationToken.None, ("key", lockKey));
            }
        }
        catch (Exception ex)
        {
            if (connection.State == ConnectionState.Open)
            {
                _logger.ReleaseFailed(key, ex);
                _dataSource.Clear();
            }
        }

        try
        {
            await connection.DisposeAsync();
        }
        catch
        {
            // Nothing left to release.
        }
    }

    private sealed class Handle : IClusterLockHandle
    {
        private readonly PostgresAdvisoryLock _owner;
        private readonly string _key;
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction? _transaction;
        private readonly long _lockKey;
        private readonly CancellationTokenSource _lost = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _monitor;
        private int _disposed;

        public Handle(PostgresAdvisoryLock owner, string key, NpgsqlConnection connection, NpgsqlTransaction? transaction, long lockKey)
        {
            _owner = owner;
            _key = key;
            _connection = connection;
            _transaction = transaction;
            _lockKey = lockKey;
            _monitor = Task.Run(MonitorAsync);
        }

        public bool IsHeld => Volatile.Read(ref _disposed) == 0 && !_lost.IsCancellationRequested;

        public CancellationToken Lost => _lost.Token;

        private async Task MonitorAsync()
        {
            try
            {
                while (true)
                {
                    if (_owner._passive)
                    {
                        await WaitPassivelyAsync();
                        await KeepaliveAsync();
                    }
                    else
                    {
                        await SleepAsync();
                    }
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Any other failure means the lock can no longer be vouched for.
                _owner._logger.LockLost(_key, ex);
            }

            try
            {
                _lost.Cancel();
            }
            catch
            {
                // A consumer's Lost callback threw; leadership is reported lost either way.
            }
        }

        private async Task WaitPassivelyAsync()
        {
            // WaitAsync also returns (true) whenever any message arrives, so wait out the rest of the slice.
            // It takes whole milliseconds and treats 0 as infinite, so a sub-millisecond remainder ends the slice.
            var start = Stopwatch.GetTimestamp();
            var slice = TimeSpan.FromSeconds(MonitorSliceSeconds);
            var remaining = slice;
            while (remaining >= TimeSpan.FromMilliseconds(1) && await _connection.WaitAsync(remaining, _stop.Token))
            {
                remaining = slice - Stopwatch.GetElapsedTime(start);
            }
        }

        private async Task KeepaliveAsync()
        {
            await using var command = new NpgsqlCommand("SELECT 0 /* Wallaby cluster lock keepalive */", _connection, _transaction)
            {
                CommandTimeout = MonitorGraceSeconds,
            };
            await command.ExecuteNonQueryAsync(_stop.Token);
        }

        private async Task SleepAsync()
        {
            // One batch runs as one implicit transaction, so the local settings lift any statement_timeout
            // and transaction_timeout for the sleep without leaking into the pooled session.
            await using var batch = new NpgsqlBatch(_connection, _transaction)
            {
                BatchCommands =
                {
                    new NpgsqlBatchCommand(HasTransactionTimeout(_connection)
                        ? "SELECT set_config('statement_timeout', '0', true), set_config('transaction_timeout', '0', true)"
                        : "SELECT set_config('statement_timeout', '0', true)"),
                    new NpgsqlBatchCommand($"SELECT pg_sleep({MonitorSliceSeconds})"),
                },
                Timeout = MonitorSliceSeconds + MonitorGraceSeconds,
            };
            await batch.ExecuteNonQueryAsync(_stop.Token);
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            // Cancelling the in-flight wait or query frees the connection for the unlock. A cancelled query
            // aborts the lock's transaction, which the rollback below then ends.
            await _stop.CancelAsync();
            await _monitor;
            await _owner.ReleaseAsync(_key, _connection, _transaction, _lockKey);
            _stop.Dispose();
        }
    }
}

internal static partial class PostgresAdvisoryLockLog
{
    [LoggerMessage(Level = LogLevel.Warning, Message = "Lost the cluster lock {Key}: monitoring its connection failed.")]
    internal static partial void LockLost(this ILogger logger, string key, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not release the cluster lock {Key} on its live connection; clearing the connection pool so a pooled connection can't keep holding it.")]
    internal static partial void ReleaseFailed(this ILogger logger, string key, Exception exception);
}
