using Npgsql;

namespace Wallaby.Internal.State;

/// <summary>
/// A dedicated <c>LISTEN</c> connection on one notify channel, opened lazily on first wait and held for
/// the subscriber's lifetime. <see cref="WaitAsync"/> returns on a notification (immediate wake) or after
/// the fallback timeout (safety poll).
/// </summary>
internal sealed class PostgresChannelSubscription(NpgsqlDataSource dataSource, string channel) : INotifySubscription
{
    private NpgsqlConnection? _connection;

    public async Task WaitAsync(TimeSpan fallbackTimeout, CancellationToken ct)
    {
        try
        {
            var connection = await EnsureListeningAsync(ct);
            // Returns true if a notification arrived, false on timeout — either way the caller re-checks.
            await connection.WaitAsync(fallbackTimeout, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // The listening connection faulted (e.g. server restart/failover). Drop it so the next wait
            // reopens and re-listens; return now so the caller re-checks in case a notification was missed.
            await DisposeConnectionAsync();
        }
    }

    private async Task<NpgsqlConnection> EnsureListeningAsync(CancellationToken ct)
    {
        if (_connection is { State: System.Data.ConnectionState.Open } open)
        {
            return open;
        }

        await DisposeConnectionAsync();
        var connection = await dataSource.OpenConnectionAsync(ct);
        await using (var listen = new NpgsqlCommand($"LISTEN {channel}", connection))
        {
            await listen.ExecuteNonQueryAsync(ct);
        }

        _connection = connection;
        return connection;
    }

    private async ValueTask DisposeConnectionAsync()
    {
        if (_connection is { } connection)
        {
            _connection = null;
            await connection.DisposeAsync();
        }
    }

    public ValueTask DisposeAsync() => DisposeConnectionAsync();
}
