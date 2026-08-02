using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Client.Internal;

/// <summary>The <c>wallaby.control</c> row; <c>null</c> from a read means no suspension has ever been recorded.</summary>
internal sealed record ControlRow(
    string State,
    string Origin,
    string? Reason,
    string? RequestedBy,
    DateTimeOffset? RequestedAt,
    DateTimeOffset? SuspendedAt,
    DateTimeOffset? ResumedAt);

/// <summary>A <c>wallaby.slot_registry</c> entry joined against the server's live slot catalog.</summary>
internal sealed record ManagedSlotRow(
    string SlotName, string Publication, string Kind, bool ExistsOnServer, bool Active, long? RetainedWalBytes);

/// <summary>
/// Self-contained SQL operations on the wallaby control plane, shared verbatim between the host and the
/// remote client (compile-linked; see <see cref="ControlContract"/>). Every state transition is a guarded
/// UPDATE, so all operations are idempotent and safe to run concurrently from multiple actors, and every
/// transition emits a NOTIFY so waiters wake immediately.
/// </summary>
internal static class ControlOperations
{
    private const string ObjectInUse = "55006";
    private const string UndefinedObject = "42704";
    private const string UndefinedTable = "42P01";
    private const string UndefinedColumn = "42703";

    private const string Notify = $"SELECT pg_notify('{ControlContract.NotifyChannel}', '');";

    /// <summary>
    /// Read the control row. Returns <c>null</c> when the row or the table doesn't exist (a database no
    /// Wallaby version with suspension support has touched) — both mean "running".
    /// </summary>
    public static async Task<ControlRow?> ReadAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"""
                 SELECT state, origin, reason, requested_by, requested_at, suspended_at, resumed_at
                 FROM {ControlContract.Table} WHERE scope = '{ControlContract.Scope}'
                 """);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return null;
            }

            return new ControlRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
                reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
                reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6));
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTable)
        {
            return null;
        }
    }

    /// <summary>
    /// Transition Running → SuspendRequested. A suspension already requested or in force is left
    /// untouched (including its origin, so a configuration flag never converts a client suspension
    /// into an auto-resumable one, and vice versa). Returns true when this call made the transition.
    /// A configuration-origin request also stamps <c>configuration_asserted_at</c> in the same
    /// statement, so a flag-less node's grace-guarded auto-resume can never observe the request
    /// without its liveness heartbeat. Only the host asserts that origin (after ensuring the state
    /// schema), so client-origin calls never reference the column and stay compatible with databases
    /// bootstrapped by older hosts.
    /// </summary>
    public static async Task<bool> RequestSuspendAsync(
        NpgsqlDataSource dataSource, string origin, string? reason, string? requestedBy, CancellationToken ct)
    {
        var (assertColumn, assertValue, assertUpdate) = origin == ControlContract.OriginConfiguration
            ? (", configuration_asserted_at", ", now()", ", configuration_asserted_at = now()")
            : ("", "", "");
        await using var cmd = dataSource.CreateCommand(
            $"""
             INSERT INTO {ControlContract.Table} (scope, state, origin, reason, requested_by, requested_at, updated_at{assertColumn})
             VALUES ('{ControlContract.Scope}', '{ControlContract.StateSuspendRequested}', @origin, @reason, @by, now(), now(){assertValue})
             ON CONFLICT (scope) DO UPDATE
                 SET state = EXCLUDED.state, origin = EXCLUDED.origin, reason = EXCLUDED.reason,
                     requested_by = EXCLUDED.requested_by, requested_at = EXCLUDED.requested_at,
                     resumed_at = NULL, updated_at = EXCLUDED.updated_at{assertUpdate}
                 WHERE control.state = '{ControlContract.StateRunning}';
             {Notify}
             """);
        cmd.Parameters.AddWithValue("origin", origin);
        cmd.Parameters.AddWithValue("reason", (object?)reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("by", (object?)requestedBy ?? DBNull.Value);
        return await cmd.ExecuteNonQueryAsync(ct) > 0;
    }

    /// <summary>
    /// Stamp the configuration-assertion heartbeat: a flag-carrying node refreshes it on every gate
    /// pass while a configuration-origin suspension is in force, keeping the grace-guarded auto-resume
    /// at bay. Deliberately does not NOTIFY (a heartbeat is not a state transition and must not wake
    /// every idle node). Host-only; callers ensure the state schema is current.
    /// </summary>
    public static async Task<bool> HeartbeatConfigurationAssertionAsync(
        NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"""
             UPDATE {ControlContract.Table}
             SET configuration_asserted_at = now()
             WHERE scope = '{ControlContract.Scope}'
               AND origin = '{ControlContract.OriginConfiguration}'
               AND state <> '{ControlContract.StateRunning}'
             """);
        return await cmd.ExecuteNonQueryAsync(ct) > 0;
    }

    /// <summary>
    /// Transition SuspendRequested/Suspended → Running. With <paramref name="configurationOriginOnly"/>
    /// (the flag-less host's auto-resume) a client-origin suspension is left in force, and
    /// <paramref name="assertionGrace"/> additionally refuses the resume while a flag-carrying node's
    /// <c>configuration_asserted_at</c> heartbeat is fresher than the grace (a never-stamped row resumes
    /// immediately). The staleness predicate lives inside the single guarded UPDATE, so two racing
    /// flag-less nodes cannot both resume off a stale read, and the NOTIFY fires only when a row
    /// actually transitioned (a refused resume must not wake the cluster). Returns true when this call
    /// made the transition; false includes the table not existing (nothing to resume).
    /// </summary>
    public static async Task<bool> ResumeAsync(
        NpgsqlDataSource dataSource, bool configurationOriginOnly, CancellationToken ct,
        TimeSpan? assertionGrace = null)
    {
        var originGuard = configurationOriginOnly
            ? $" AND origin = '{ControlContract.OriginConfiguration}'"
            : "";
        var graceGuard = assertionGrace is not null
            ? " AND (configuration_asserted_at IS NULL OR now() - configuration_asserted_at > @grace)"
            : "";
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"""
                 WITH resumed AS (
                     UPDATE {ControlContract.Table}
                     SET state = '{ControlContract.StateRunning}', resumed_at = now(), updated_at = now()
                     WHERE scope = '{ControlContract.Scope}'
                       AND state IN ('{ControlContract.StateSuspendRequested}', '{ControlContract.StateSuspended}'){originGuard}{graceGuard}
                     RETURNING 1
                 )
                 SELECT count(*), CASE WHEN count(*) > 0 THEN pg_notify('{ControlContract.NotifyChannel}', '') END
                 FROM resumed
                 """);
            if (assertionGrace is { } grace)
            {
                cmd.Parameters.AddWithValue("grace", grace);
            }
            return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct)) > 0;
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTable)
        {
            return false;
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedColumn && assertionGrace is not null)
        {
            // The heartbeat column doesn't exist (a database last touched by an older host), so no live
            // asserter can be stamping it; resume with the pre-heartbeat semantics.
            return await ResumeAsync(dataSource, configurationOriginOnly, ct);
        }
    }

    /// <summary>
    /// Transition SuspendRequested → Suspended. The only transition into Suspended, made after the
    /// managed slots are verified gone, so a crash mid-finalize re-converges from SuspendRequested.
    /// </summary>
    public static async Task<bool> TryMarkSuspendedAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"""
             UPDATE {ControlContract.Table}
             SET state = '{ControlContract.StateSuspended}', suspended_at = now(), updated_at = now()
             WHERE scope = '{ControlContract.Scope}' AND state = '{ControlContract.StateSuspendRequested}';
             {Notify}
             """);
        return await cmd.ExecuteNonQueryAsync(ct) > 0;
    }

    /// <summary>
    /// Every slot Wallaby manages (<c>wallaby.slot_registry</c>) joined with whether it currently exists
    /// on the server and is being streamed. Empty when the registry table doesn't exist.
    /// </summary>
    public static async Task<IReadOnlyList<ManagedSlotRow>> ListManagedSlotsAsync(
        NpgsqlDataSource dataSource, CancellationToken ct)
    {
        try
        {
            // The retained-WAL diff is guarded: pg_current_wal_lsn() errors on a standby in recovery,
            // and a slot missing from the server has no restart_lsn.
            await using var cmd = dataSource.CreateCommand(
                """
                SELECT r.slot_name, r.publication, r.kind,
                       s.slot_name IS NOT NULL AS exists_on_server, COALESCE(s.active, false) AS active,
                       CASE WHEN s.restart_lsn IS NOT NULL AND NOT pg_is_in_recovery()
                            THEN pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint
                       END AS retained_wal_bytes
                FROM wallaby.slot_registry r
                LEFT JOIN pg_replication_slots s USING (slot_name)
                ORDER BY r.slot_name
                """);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var slots = new List<ManagedSlotRow>();
            while (await reader.ReadAsync(ct))
            {
                slots.Add(new ManagedSlotRow(
                    reader.GetString(0), reader.GetString(1), reader.GetString(2),
                    reader.GetBoolean(3), reader.GetBoolean(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5)));
            }
            return slots;
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTable)
        {
            return [];
        }
    }

    /// <summary>
    /// Drop every registry-tracked slot still on the server, then mark the suspension finalized.
    /// A slot busy with an active consumer (<c>55006</c>) is retried on <paramref name="busyRetryDelay"/>
    /// until it frees or <paramref name="ct"/> cancels; a concurrently dropped slot is ignored. A resume
    /// observed mid-finalize stops the drops immediately — the waking hosts are recreating the slots.
    /// Returns true when this call made the SuspendRequested → Suspended transition (false: another actor
    /// won, or the request was resumed underneath us).
    /// </summary>
    public static async Task<bool> FinalizeSuspensionAsync(
        NpgsqlDataSource dataSource, TimeSpan busyRetryDelay, ILogger logger, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var row = await ReadAsync(dataSource, ct);
            if (row is null || row.State == ControlContract.StateRunning)
            {
                return false;
            }

            var present = (await ListManagedSlotsAsync(dataSource, ct)).Where(s => s.ExistsOnServer).ToList();
            if (present.Count == 0)
            {
                break;
            }

            var anyBusy = false;
            foreach (var slot in present)
            {
                try
                {
                    await using var cmd = dataSource.CreateCommand("SELECT pg_drop_replication_slot(@s)");
                    cmd.Parameters.AddWithValue("s", slot.SlotName);
                    await cmd.ExecuteNonQueryAsync(ct);
                    logger.ManagedSlotDropped(slot.SlotName, slot.Kind);
                }
                catch (PostgresException ex) when (ex.SqlState == UndefinedObject)
                {
                    // Another finalizer dropped it between the list and the drop.
                }
                catch (PostgresException ex) when (ex.SqlState == ObjectInUse)
                {
                    anyBusy = true;
                    logger.ManagedSlotBusy(slot.SlotName);
                }
            }

            if (anyBusy)
            {
                await Task.Delay(busyRetryDelay, ct);
            }
            // Loop to re-list: verifies every drop landed before the state is marked Suspended.
        }

        return await TryMarkSuspendedAsync(dataSource, ct);
    }
}

/// <summary>Source-generated log messages for <see cref="ControlOperations"/>.</summary>
internal static partial class ControlOperationsLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped managed replication slot {Slot} (kind={Kind}) for suspension.")]
    internal static partial void ManagedSlotDropped(this ILogger logger, string slot, string kind);

    [LoggerMessage(Level = LogLevel.Information, Message = "Replication slot {Slot} is in use by an active consumer; retrying the drop.")]
    internal static partial void ManagedSlotBusy(this ILogger logger, string slot);
}
