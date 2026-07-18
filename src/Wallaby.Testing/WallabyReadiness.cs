using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Testing;

/// <summary>
/// Readiness gates for end-to-end Wallaby tests. Rows written before the replication slot exists are never
/// captured, so a test must wait for the pipeline to be actually streaming before it seeds data.
/// </summary>
public static class WallabyReadiness
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Wait until the Wallaby pipeline in <paramref name="services"/> is capturing changes: the node has
    /// become the leader (<see cref="IWallabyStatus"/>) and the configured replication slot exists with an
    /// attached walsender (<c>pg_replication_slots.active</c>). Throws if the background service faults.
    /// </summary>
    /// <param name="services">The host's service provider (e.g. <c>WebApplicationFactory.Services</c>).</param>
    /// <param name="timeout">How long to wait before giving up; defaults to 30 seconds.</param>
    /// <param name="ct">Cancels the wait.</param>
    /// <exception cref="InvalidOperationException">The Wallaby background service faulted while waiting.</exception>
    /// <exception cref="TimeoutException">The pipeline did not become ready in time.</exception>
    public static async Task WaitForStreamingAsync(
        IServiceProvider services, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var status = services.GetRequiredService<IWallabyStatus>();
        var options = services.GetRequiredService<WallabyOptions>();

        var effectiveTimeout = timeout ?? DefaultTimeout;
        var deadline = DateTime.UtcNow + effectiveTimeout;

        // Phase 1: leadership. The leader runs self-config (publication + slot) before streaming.
        while (status.Current.Role != WallabyNodeRole.Leader)
        {
            ThrowIfFaulted(status.Current);
            await DelayOrTimeout(deadline, effectiveTimeout, "role to become Leader", status, ct);
        }

        // Phase 2: the slot exists and a walsender is attached — changes from here on are captured.
        await using var dataSource = NpgsqlDataSource.Create(options.ConnectionString);
        while (true)
        {
            ThrowIfFaulted(status.Current);

            await using (var command = dataSource.CreateCommand(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1"))
            {
                command.Parameters.AddWithValue(options.SlotName);
                if (await command.ExecuteScalarAsync(ct) is true)
                {
                    return;
                }
            }

            await DelayOrTimeout(
                deadline, effectiveTimeout, $"replication slot '{options.SlotName}' to become active", status, ct);
        }
    }

    /// <summary>
    /// Wait until the Wallaby node in <paramref name="services"/> is fully suspended: the node reports
    /// <see cref="WallabyNodeRole.Suspended"/> and no slot Wallaby manages remains on the server. Throws
    /// if the background service faults.
    /// </summary>
    /// <param name="services">The host's service provider.</param>
    /// <param name="timeout">How long to wait before giving up; defaults to 30 seconds.</param>
    /// <param name="ct">Cancels the wait.</param>
    /// <exception cref="InvalidOperationException">The Wallaby background service faulted while waiting.</exception>
    /// <exception cref="TimeoutException">The node did not suspend in time.</exception>
    public static async Task WaitForSuspendedAsync(
        IServiceProvider services, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var status = services.GetRequiredService<IWallabyStatus>();
        var options = services.GetRequiredService<WallabyOptions>();

        var effectiveTimeout = timeout ?? DefaultTimeout;
        var deadline = DateTime.UtcNow + effectiveTimeout;

        while (status.Current.Role != WallabyNodeRole.Suspended)
        {
            ThrowIfFaulted(status.Current);
            await DelayOrTimeout(deadline, effectiveTimeout, "role to become Suspended", status, ct);
        }

        // Every registry-tracked slot is verified gone — the state a platform's upgrade precheck sees.
        await using var dataSource = NpgsqlDataSource.Create(options.ConnectionString);
        while (true)
        {
            ThrowIfFaulted(status.Current);

            try
            {
                await using var command = dataSource.CreateCommand(
                    "SELECT count(*) FROM wallaby.slot_registry r JOIN pg_replication_slots s USING (slot_name)");
                if (await command.ExecuteScalarAsync(ct) is 0L)
                {
                    return;
                }
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                // No registry table: no host ever provisioned a slot (e.g. a Suspend()-flagged node on a
                // fresh database), so none can exist.
                return;
            }

            await DelayOrTimeout(deadline, effectiveTimeout, "managed replication slots to be dropped", status, ct);
        }
    }

    private static void ThrowIfFaulted(WallabyStatusSnapshot snapshot)
    {
        if (snapshot.Faulted)
        {
            throw new InvalidOperationException(
                $"The Wallaby background service faulted while waiting for streaming to start: {snapshot.LastError ?? "(no error recorded)"}");
        }
    }

    private static async Task DelayOrTimeout(
        DateTime deadline, TimeSpan timeout, string waitingFor, IWallabyStatus status, CancellationToken ct)
    {
        if (DateTime.UtcNow >= deadline)
        {
            // A retrying (non-faulted) node reports why it isn't ready — a crash-looping leader's error
            // would otherwise be invisible here.
            var snapshot = status.Current;
            throw new TimeoutException(
                $"Timed out after {timeout} waiting for {waitingFor}. " +
                $"Role: {snapshot.Role}, consecutive leader failures: {snapshot.ConsecutiveLeaderFailures}, " +
                $"last error: {snapshot.LastError ?? "(none)"}");
        }
        await Task.Delay(PollInterval, ct);
    }
}
