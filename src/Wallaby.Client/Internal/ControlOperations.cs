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
    DateTimeOffset? ResumedAt,
    bool PublicationsWidened = false,
    DateTimeOffset? WidenedAt = null,
    string? WidenedBy = null);

/// <summary>A <c>wallaby.slot_registry</c> entry joined against the server's live slot catalog.</summary>
internal sealed record ManagedSlotRow(
    string SlotName, string Publication, string Kind, bool ExistsOnServer, bool Active, long? RetainedWalBytes,
    bool PublicationManaged, bool PublicationNarrowed);

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
    private const string InvalidSchemaName = "3F000";

    private const string Notify = $"SELECT pg_notify('{ControlContract.NotifyChannel}', '');";

    /// <summary>
    /// The version the host's schema migrations have brought this database to, read from the
    /// <c>wallaby.schema_version</c> ledger. 0 when the ledger (or the wallaby schema) doesn't exist: a
    /// database no ledger-maintaining Wallaby host has ever run against. Drives every version-dependent
    /// decision — column-set selection for reads, and the client's refusal of writes the schema cannot
    /// serve — replacing per-column 42703 probing.
    /// </summary>
    public static async Task<int> GetSchemaVersionAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"SELECT coalesce(max(version), 0) FROM {ControlContract.SchemaVersionLedger}");
            return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedTable or InvalidSchemaName)
        {
            return 0;
        }
    }

    /// <summary>
    /// Read the control row at the current schema version. Returns <c>null</c> when the row or the
    /// table doesn't exist (a database no Wallaby host has touched) — both mean "running". Throws
    /// 42703 against a schema older than the widening columns; the host heals that by migrating and
    /// retrying, the client by passing the ledger-derived <c>includeWidening</c> to the overload.
    /// </summary>
    public static Task<ControlRow?> ReadAsync(NpgsqlDataSource dataSource, CancellationToken ct)
        => ReadAsync(dataSource, includeWidening: true, ct);

    /// <summary>
    /// Read the control row. With <paramref name="includeWidening"/> false the pre-widening column set
    /// is selected (the flag reads false), serving schemas older than
    /// <see cref="ControlContract.WideningSchemaVersion"/>.
    /// </summary>
    public static async Task<ControlRow?> ReadAsync(
        NpgsqlDataSource dataSource, bool includeWidening, CancellationToken ct)
    {
        var wideningColumns = includeWidening
            ? "publications_widened, widened_at, widened_by"
            : "false, NULL::timestamptz, NULL::text";
        await using var cmd = dataSource.CreateCommand(
            $"""
             SELECT state, origin, reason, requested_by, requested_at, suspended_at, resumed_at,
                    {wideningColumns}
             FROM {ControlContract.Table} WHERE scope = '{ControlContract.Scope}'
             """);
        try
        {
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
                reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                reader.GetBoolean(7),
                reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
                reader.IsDBNull(9) ? null : reader.GetString(9));
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
    /// The transition also ends any publication widening — the finalize drops the managed publications
    /// outright, and resume recreates them with their configured narrow lists — so a widened flag
    /// never survives into (or past) a suspension.
    /// A configuration-origin request also stamps <c>configuration_asserted_at</c> in the same
    /// statement, so a flag-less node's grace-guarded auto-resume can never observe the request
    /// without its liveness heartbeat. Callers ensure the schema is current (the host bootstraps
    /// before asserting; the client gates on the ledger version).
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
                     resumed_at = NULL, publications_widened = false, widened_at = NULL, widened_by = NULL,
                     updated_at = EXCLUDED.updated_at{assertUpdate}
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
    /// With <paramref name="purge"/>, the same transition stamps <c>purge_on_resume</c>, asking the
    /// slot-gap repair that serves the resume to purge sink destinations before its re-backfills.
    /// The resume references only columns every deployed schema carries, so an old installation can
    /// always be unsuspended.
    /// </summary>
    public static async Task<bool> ResumeAsync(
        NpgsqlDataSource dataSource, bool configurationOriginOnly, CancellationToken ct,
        TimeSpan? assertionGrace = null, bool purge = false)
    {
        var originGuard = configurationOriginOnly
            ? $" AND origin = '{ControlContract.OriginConfiguration}'"
            : "";
        var graceGuard = assertionGrace is not null
            ? " AND (configuration_asserted_at IS NULL OR now() - configuration_asserted_at > @grace)"
            : "";
        var purgeSet = purge ? ", purge_on_resume = true" : "";
        try
        {
            await using var cmd = dataSource.CreateCommand(
                $"""
                 WITH resumed AS (
                     UPDATE {ControlContract.Table}
                     SET state = '{ControlContract.StateRunning}', resumed_at = now(), updated_at = now(){purgeSet}
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
    }

    /// <summary>
    /// Whether the last resume asked the slot-gap repair to purge sink destinations before its
    /// re-backfills. Host-only; callers ensure the state schema is current.
    /// </summary>
    public static async Task<bool> ReadPurgeOnResumeAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"SELECT purge_on_resume FROM {ControlContract.Table} WHERE scope = '{ControlContract.Scope}'");
        return await cmd.ExecuteScalarAsync(ct) is true;
    }

    /// <summary>
    /// Clear the resume purge flag. The repair calls this once its purge marks are durable, so a crash
    /// before the marks re-reads the flag while a later unrelated repair does not purge unrequested.
    /// Deliberately does not NOTIFY (consuming the flag is not a state transition). Host-only.
    /// </summary>
    public static async Task ClearPurgeOnResumeAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"UPDATE {ControlContract.Table} SET purge_on_resume = false WHERE scope = '{ControlContract.Scope}'");
        await cmd.ExecuteNonQueryAsync(ct);
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
        // The retained-WAL diff is guarded: pg_current_wal_lsn() errors on a standby in recovery,
        // and a slot missing from the server has no restart_lsn.
        await using var cmd = dataSource.CreateCommand(
            """
             SELECT r.slot_name, r.publication, r.kind,
                    s.slot_name IS NOT NULL AS exists_on_server, COALESCE(s.active, false) AS active,
                    CASE WHEN s.restart_lsn IS NOT NULL AND NOT pg_is_in_recovery()
                         THEN pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint
                    END AS retained_wal_bytes,
                    r.publication_managed,
                    EXISTS (SELECT 1 FROM pg_publication p
                            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
                            WHERE p.pubname = r.publication
                              AND (pr.prattrs IS NOT NULL OR pr.prqual IS NOT NULL)) AS publication_narrowed
             FROM wallaby.slot_registry r
             LEFT JOIN pg_replication_slots s USING (slot_name)
             ORDER BY r.slot_name
             """);
        try
        {
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var slots = new List<ManagedSlotRow>();
            while (await reader.ReadAsync(ct))
            {
                slots.Add(new ManagedSlotRow(
                    reader.GetString(0), reader.GetString(1), reader.GetString(2),
                    reader.GetBoolean(3), reader.GetBoolean(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.GetBoolean(6), reader.GetBoolean(7)));
            }
            return slots;
        }
        catch (PostgresException ex) when (ex.SqlState == UndefinedTable)
        {
            return [];
        }
    }

    /// <summary>
    /// Request publication widening: set <c>publications_widened</c> while Running (a suspension already
    /// drops the managed publications, so widening one is meaningless — refused by the guard). Inserts
    /// the control row when the table exists but no suspension was ever recorded. Returns true when
    /// this call set the flag (false: already widened, or not Running). Callers gate on the ledger
    /// version first, so the widening columns exist.
    /// </summary>
    public static async Task<bool> RequestWidenAsync(
        NpgsqlDataSource dataSource, string? requestedBy, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"""
             WITH widened AS (
                 INSERT INTO {ControlContract.Table} (scope, publications_widened, widened_at, widened_by, updated_at)
                 VALUES ('{ControlContract.Scope}', true, now(), @by, now())
                 ON CONFLICT (scope) DO UPDATE
                     SET publications_widened = true, widened_at = now(), widened_by = EXCLUDED.widened_by,
                         updated_at = now()
                     WHERE control.state = '{ControlContract.StateRunning}' AND NOT control.publications_widened
                 RETURNING 1
             )
             SELECT count(*), CASE WHEN count(*) > 0 THEN pg_notify('{ControlContract.NotifyChannel}', '') END
             FROM widened
             """);
        cmd.Parameters.AddWithValue("by", (object?)requestedBy ?? DBNull.Value);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct)) > 0;
    }

    /// <summary>
    /// Clear the widening flag; the next leader term's reconcile re-narrows the publications (nothing
    /// blocks on it). Returns true when this call cleared it. Callers gate on the ledger version first.
    /// </summary>
    public static async Task<bool> RestoreWidenAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            $"""
             WITH restored AS (
                 UPDATE {ControlContract.Table}
                 SET publications_widened = false, widened_at = NULL, widened_by = NULL, updated_at = now()
                 WHERE scope = '{ControlContract.Scope}' AND publications_widened
                 RETURNING 1
             )
             SELECT count(*), CASE WHEN count(*) > 0 THEN pg_notify('{ControlContract.NotifyChannel}', '') END
             FROM restored
             """);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct)) > 0;
    }

    /// <summary>
    /// Every Wallaby-managed publication that still carries a column list or row filter on the server —
    /// the set an <c>ALTER COLUMN ... TYPE</c> migration would still be refused over. Callers gate on
    /// the ledger version first.
    /// </summary>
    public static async Task<IReadOnlyList<string>> ListNarrowedPublicationsAsync(
        NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(
            """
            SELECT DISTINCT r.publication
            FROM wallaby.slot_registry r
            JOIN pg_publication p ON p.pubname = r.publication
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            WHERE r.publication_managed AND (pr.prattrs IS NOT NULL OR pr.prqual IS NOT NULL)
            ORDER BY r.publication
            """);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var publications = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            publications.Add(reader.GetString(0));
        }
        return publications;
    }

    /// <summary>
    /// Widen still-narrowed managed publications directly — the no-host fallback. The client has no
    /// entity model, so each publication's current membership is read from the catalog and re-issued as
    /// one atomic <c>SET TABLE</c> without column lists or row filters (legal under a live slot).
    /// Idempotent; a publication with no members is skipped.
    /// </summary>
    public static async Task WidenPublicationsDirectAsync(
        NpgsqlDataSource dataSource, ILogger logger, CancellationToken ct)
    {
        foreach (var pub in await ListNarrowedPublicationsAsync(dataSource, ct))
        {
            var tables = new List<string>();
            await using (var list = dataSource.CreateCommand(
                """
                SELECT n.nspname, c.relname
                FROM pg_publication p
                JOIN pg_publication_rel pr ON pr.prpubid = p.oid
                JOIN pg_class c ON c.oid = pr.prrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE p.pubname = @p
                ORDER BY 1, 2
                """))
            {
                list.Parameters.AddWithValue("p", pub);
                await using var reader = await list.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    tables.Add($"{QuoteIdentifier(reader.GetString(0))}.{QuoteIdentifier(reader.GetString(1))}");
                }
            }
            if (tables.Count == 0)
            {
                continue;
            }

            await using var cmd = dataSource.CreateCommand(
                $"ALTER PUBLICATION {QuoteIdentifier(pub)} SET TABLE {string.Join(", ", tables)}");
            await cmd.ExecuteNonQueryAsync(ct);
            logger.ManagedPublicationWidened(pub, tables.Count);
        }
    }

    /// <summary>
    /// Drop every registry-tracked slot still on the server, then every Wallaby-managed publication
    /// (recreated from configuration on resume; slots first, so nothing is decoding through a
    /// publication being removed), then mark the suspension finalized. With both gone, the installation
    /// is fully quiesced: the upgrade precheck passes and schema migrations blocked by publication
    /// column lists or row filters (<c>ALTER COLUMN ... TYPE</c>) run freely. Unmanaged publications
    /// (<c>ManagePublicationTables=false</c>) are never touched — Wallaby cannot recreate them.
    /// A slot busy with an active consumer (<c>55006</c>) is retried on <paramref name="busyRetryDelay"/>
    /// until it frees or <paramref name="ct"/> cancels; a concurrently dropped slot is ignored. A resume
    /// observed mid-finalize stops the drops immediately — the waking hosts are recreating the slots.
    /// Returns true when this call made the SuspendRequested → Suspended transition (false: another actor
    /// won, or the request was resumed underneath us).
    /// </summary>
    public static async Task<bool> FinalizeSuspensionAsync(
        NpgsqlDataSource dataSource, TimeSpan busyRetryDelay, ILogger logger, CancellationToken ct)
    {
        IReadOnlyList<ManagedSlotRow> registry;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var row = await ReadAsync(dataSource, ct);
            if (row is null || row.State == ControlContract.StateRunning)
            {
                return false;
            }

            registry = await ListManagedSlotsAsync(dataSource, ct);
            var present = registry.Where(s => s.ExistsOnServer).ToList();
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

        foreach (var pub in registry.Where(s => s.PublicationManaged).Select(s => s.Publication).Distinct())
        {
            await using var cmd = dataSource.CreateCommand(
                $"DROP PUBLICATION IF EXISTS {QuoteIdentifier(pub)}");
            await cmd.ExecuteNonQueryAsync(ct);
            logger.ManagedPublicationDropped(pub);
        }

        return await TryMarkSuspendedAsync(dataSource, ct);
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\"\"") + "\"";
}

/// <summary>Source-generated log messages for <see cref="ControlOperations"/>.</summary>
internal static partial class ControlOperationsLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped managed replication slot {Slot} (kind={Kind}) for suspension.")]
    internal static partial void ManagedSlotDropped(this ILogger logger, string slot, string kind);

    [LoggerMessage(Level = LogLevel.Information, Message = "Replication slot {Slot} is in use by an active consumer; retrying the drop.")]
    internal static partial void ManagedSlotBusy(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped managed publication {Publication} for suspension; it is recreated from configuration on resume.")]
    internal static partial void ManagedPublicationDropped(this ILogger logger, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Widened managed publication {Publication} to whole-table membership ({TableCount} table(s)); column lists are restored by the next leader term after RestorePublicationsAsync.")]
    internal static partial void ManagedPublicationWidened(this ILogger logger, string publication, int tableCount);
}
