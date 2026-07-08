using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Default <see cref="ISelfConfigurator"/>: validates the server, ensures the <c>wallaby</c> state
/// schema, and creates/reconciles the publication and pgoutput replication slot derived from the
/// captured model. Uses a normal (non-replication) connection.
/// </summary>
internal sealed class PostgresSelfConfigurator(
    NpgsqlDataSource dataSource,
    SelfConfigOptions options,
    ILogger logger,
    WallabyInstrumentation? instrumentation = null) : ISelfConfigurator
{
    private readonly ServerValidator _validator = new(logger);
    private readonly StateSchemaBootstrapper _stateSchema = new();
    private readonly WallabyInstrumentation _instr = instrumentation ?? WallabyInstrumentation.NoOp;

    public async Task<SelfConfigResult> EnsureConfiguredAsync(WallabyModel model, CancellationToken ct)
    {
        using var activity = _instr.StartSelfConfig();
        activity?.SetTag(WallabyInstrumentation.SlotTag, options.SlotName);
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct);

            // Validate headroom for every slot we intend to create (primary + external).
            var intendedSlots = new List<string>(1 + options.ExternalSlots.Count) { options.SlotName };
            intendedSlots.AddRange(options.ExternalSlots.Select(s => s.SlotName));
            await _validator.ValidateAsync(connection, intendedSlots, ct);

            await _stateSchema.EnsureAsync(connection, ct);

            var publicationCreated = await EnsurePublicationAsync(
                connection, options.PublicationName, DesiredTables(model).ToList(), options.ManagePublicationTables, ct);
            var (slotCreated, consistentPoint) = await EnsureSlotAsync(
                connection, options.SlotName, options.PublicationName, kind: "primary", ct);
            var warnings = await ValidateReplicaIdentityAsync(connection, model, ct);
            var externalResults = await EnsureExternalSlotsAsync(connection, ct);

            logger.SelfConfigComplete(options.PublicationName, publicationCreated, options.SlotName, slotCreated);

            return new SelfConfigResult(
                options.PublicationName, options.SlotName, publicationCreated, slotCreated, consistentPoint, warnings,
                externalResults);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    /// <summary>
    /// Provision-only entry point: validate the server and ensure the declared external slots/publications
    /// without creating a primary slot or publication. Used by the provision-only hosted service when the
    /// consumer declares external slots but no capture (no sink/mappings). Leader-only and idempotent.
    /// </summary>
    public async Task<IReadOnlyList<ExternalSlotResult>> EnsureExternalSlotsOnlyAsync(CancellationToken ct)
    {
        using var activity = _instr.StartSelfConfig();
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        // Only the external slots are created here, so only they consume slot headroom.
        var slotNames = options.ExternalSlots.Select(s => s.SlotName).ToList();
        await _validator.ValidateAsync(connection, slotNames, ct);

        await _stateSchema.EnsureAsync(connection, ct);

        return await EnsureExternalSlotsAsync(connection, ct);
    }

    // Provisions each declared external publication+slot. External publications always reconcile to their
    // declared table set (Wallaby owns it); the slot is created with pgoutput but never opened by Wallaby.
    private async Task<IReadOnlyList<ExternalSlotResult>> EnsureExternalSlotsAsync(
        NpgsqlConnection connection, CancellationToken ct)
    {
        if (options.ExternalSlots.Count == 0)
        {
            return [];
        }

        var results = new List<ExternalSlotResult>(options.ExternalSlots.Count);
        foreach (var spec in options.ExternalSlots)
        {
            var pubCreated = await EnsurePublicationAsync(
                connection, spec.PublicationName, spec.Tables, reconcile: true, ct);
            var (slotCreated, _) = await EnsureSlotAsync(
                connection, spec.SlotName, spec.PublicationName, kind: "external", ct);
            logger.ExternalSlotConfigured(spec.SlotName, spec.PublicationName);
            results.Add(new ExternalSlotResult(spec.SlotName, spec.PublicationName, pubCreated, slotCreated));
        }

        return results;
    }

    private async Task<bool> EnsurePublicationAsync(
        NpgsqlConnection connection,
        string pub,
        IReadOnlyList<(string Schema, string Table)> desiredTables,
        bool reconcile,
        CancellationToken ct)
    {
        var exists = await PgExec.ScalarLongAsync(
            connection, "SELECT count(*) FROM pg_publication WHERE pubname = @p", ct, ("p", pub)) > 0;

        if (!exists)
        {
            var tableList = string.Join(", ", desiredTables.Select(t => PgExec.QuoteTable(t.Schema, t.Table)));
            await PgExec.ExecuteAsync(
                connection, $"CREATE PUBLICATION {PgExec.QuoteIdentifier(pub)} FOR TABLE {tableList}", ct);
            logger.PublicationCreated(pub, desiredTables.Count);
            return true;
        }

        if (reconcile)
        {
            await ReconcilePublicationTablesAsync(connection, pub, desiredTables, ct);
        }

        return false;
    }

    private async Task ReconcilePublicationTablesAsync(
        NpgsqlConnection connection,
        string pub,
        IReadOnlyList<(string Schema, string Table)> desiredTables,
        CancellationToken ct)
    {
        var current = new HashSet<(string Schema, string Table)>();

        await using (var cmd = new NpgsqlCommand(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = @p", connection))
        {
            cmd.Parameters.AddWithValue("p", pub);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                current.Add((reader.GetString(0), reader.GetString(1)));
            }
        }

        var desired = desiredTables.ToHashSet();

        foreach (var (schema, table) in desired.Where(d => !current.Contains(d)))
        {
            await PgExec.ExecuteAsync(
                connection,
                $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} ADD TABLE {PgExec.QuoteTable(schema, table)}", ct);
            logger.TableAddedToPublication($"{schema}.{table}", pub);
        }

        foreach (var (schema, table) in current.Where(c => !desired.Contains(c)))
        {
            await PgExec.ExecuteAsync(
                connection,
                $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} DROP TABLE {PgExec.QuoteTable(schema, table)}", ct);
            logger.TableDroppedFromPublication($"{schema}.{table}", pub);
        }
    }

    private static IEnumerable<(string Schema, string Table)> DesiredTables(WallabyModel model)
    {
        foreach (var table in model.Tables)
        {
            yield return (table.Schema, table.TableName);
        }
    }

    private async Task<(bool Created, string? ConsistentPoint)> EnsureSlotAsync(
        NpgsqlConnection connection, string slot, string publication, string kind, CancellationToken ct)
    {
        var existing = await GetSlotAsync(connection, slot, ct);
        if (existing is not null)
        {
            var (slotType, plugin, walStatus) = existing.Value;

            // Adopt a slot we didn't create this run. It must be a pgoutput logical slot — anything else
            // (a physical slot, or a logical slot on a different output plugin) can't serve this slot's
            // purpose, so fail fast rather than silently assuming it matches the declaration.
            if (!string.Equals(slotType, "logical", StringComparison.Ordinal) ||
                !string.Equals(plugin, "pgoutput", StringComparison.Ordinal))
            {
                throw new WallabyConfigurationException(
                    $"Replication slot '{slot}' already exists but is not a pgoutput logical slot " +
                    $"(slot_type='{slotType}', plugin='{plugin ?? "<none>"}'). Wallaby requires a logical/pgoutput " +
                    $"slot. Drop it with SELECT pg_drop_replication_slot('{slot}'); or use a different slot name.");
            }

            if (!string.Equals(walStatus, "lost", StringComparison.Ordinal))
            {
                // Record the adopted slot so wallaby.slot_registry reflects reality (we don't know its original
                // consistent point, so keep any value already recorded).
                await UpsertSlotRegistryAsync(connection, slot, publication, consistentPoint: null, kind, ct);
                return (false, null);
            }

            // The server invalidated the slot (e.g. max_slot_wal_keep_size exceeded); its WAL is gone and
            // streaming from it can never resume. Recreate it — the caller repairs the missed window via
            // checkpoint gap detection and re-backfill.
            logger.SlotInvalidated(slot);
            await PgExec.ExecuteAsync(connection, "SELECT pg_drop_replication_slot(@s)", ct, ("s", slot));
        }

        var consistentPoint = await PgExec.ScalarStringAsync(
            connection, "SELECT lsn::text FROM pg_create_logical_replication_slot(@s, 'pgoutput')", ct, ("s", slot));

        await UpsertSlotRegistryAsync(connection, slot, publication, consistentPoint, kind, ct);

        logger.SlotCreated(slot, consistentPoint);
        return (true, consistentPoint);
    }

    private static async Task<(string SlotType, string? Plugin, string? WalStatus)?> GetSlotAsync(
        NpgsqlConnection connection, string slot, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT slot_type, plugin, wal_status::text FROM pg_replication_slots WHERE slot_name = @s", connection);
        cmd.Parameters.AddWithValue("s", slot);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        var slotType = reader.GetString(0);
        var plugin = reader.IsDBNull(1) ? null : reader.GetString(1);
        var walStatus = reader.IsDBNull(2) ? null : reader.GetString(2);
        return (slotType, plugin, walStatus);
    }

    private static Task UpsertSlotRegistryAsync(
        NpgsqlConnection connection, string slot, string publication, string? consistentPoint, string kind, CancellationToken ct)
        => PgExec.ExecuteAsync(
            connection,
            """
            INSERT INTO wallaby.slot_registry (slot_name, publication, consistent_point, kind)
            VALUES (@s, @p, @cp::pg_lsn, @k)
            ON CONFLICT (slot_name) DO UPDATE
                SET publication = EXCLUDED.publication,
                    consistent_point = COALESCE(EXCLUDED.consistent_point, slot_registry.consistent_point),
                    kind = EXCLUDED.kind
            """,
            ct,
            ("s", slot), ("p", publication), ("cp", consistentPoint), ("k", kind));

    private async Task<IReadOnlyList<string>> ValidateReplicaIdentityAsync(
        NpgsqlConnection connection, WallabyModel model, CancellationToken ct)
    {
        var warnings = new List<string>();

        foreach (var table in model.Tables.Where(t => t.RequiresFullReplicaIdentity))
        {
            var relReplIdent = await PgExec.ScalarStringAsync(
                connection,
                """
                SELECT c.relreplident::text
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = @s AND c.relname = @t
                """,
                ct,
                ("s", table.Schema), ("t", table.TableName));

            // relreplident: 'd' default, 'n' nothing, 'f' full, 'i' index.
            if (relReplIdent != "f")
            {
                var ddl = $"ALTER TABLE {PgExec.QuoteTable(table.Schema, table.TableName)} REPLICA IDENTITY FULL;";
                if (options.RequireFullReplicaIdentity)
                {
                    throw new WallabyConfigurationException(
                        $"Table {table.QualifiedName} requires REPLICA IDENTITY FULL for its transform but has '{relReplIdent}'. Run: {ddl}");
                }

                warnings.Add(
                    $"Table {table.QualifiedName} has REPLICA IDENTITY '{relReplIdent}'; old values and unchanged-TOAST " +
                    $"columns may be unavailable on UPDATE/DELETE. To capture full rows, run: {ddl}");
            }
        }

        foreach (var warning in warnings)
        {
            logger.ConfigurationWarning(warning);
        }

        return warnings;
    }
}

/// <summary>Source-generated log messages for <see cref="PostgresSelfConfigurator"/>.</summary>
internal static partial class PostgresSelfConfiguratorLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby self-config complete: publication '{Publication}' (created={PubCreated}), slot '{Slot}' (created={SlotCreated}).")]
    internal static partial void SelfConfigComplete(this ILogger logger, string publication, bool pubCreated, string slot, bool slotCreated);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created publication '{Publication}' for {TableCount} table(s).")]
    internal static partial void PublicationCreated(this ILogger logger, string publication, int tableCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Added table {Table} to publication '{Publication}'.")]
    internal static partial void TableAddedToPublication(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped table {Table} from publication '{Publication}'.")]
    internal static partial void TableDroppedFromPublication(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created pgoutput replication slot '{Slot}' at {ConsistentPoint}.")]
    internal static partial void SlotCreated(this ILogger logger, string slot, string? consistentPoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Replication slot '{Slot}' was invalidated by the server (wal_status=lost); dropping and recreating it.")]
    internal static partial void SlotInvalidated(this ILogger logger, string slot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Configured external slot '{Slot}' (publication '{Publication}') for a third-party consumer.")]
    internal static partial void ExternalSlotConfigured(this ILogger logger, string slot, string publication);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Warning}")]
    internal static partial void ConfigurationWarning(this ILogger logger, string warning);
}
