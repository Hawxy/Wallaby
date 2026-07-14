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

            var warnings = new List<string>();
            var publicationCreated = await EnsurePublicationAsync(
                connection, options.PublicationName, DesiredTables(model).ToList(), options.ManagePublicationTables,
                warnings, ct);
            var (slotCreated, consistentPoint) = await EnsureSlotAsync(
                connection, options.SlotName, options.PublicationName, kind: "primary", ct);
            await ValidateReplicaIdentityAsync(connection, model, warnings, ct);
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
            // External publications always publish whole tables: their consumers are third-party tools
            // that expect full rows, not Wallaby's capture model.
            var tables = spec.Tables
                .Select(t => PublicationTableSpec.WholeTable(t.Schema, t.Table))
                .ToList();
            var pubCreated = await EnsurePublicationAsync(
                connection, spec.PublicationName, tables, reconcile: true, warnings: null, ct);
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
        IReadOnlyList<PublicationTableSpec> desiredTables,
        bool reconcile,
        List<string>? warnings,
        CancellationToken ct)
    {
        // null = publication absent; otherwise its puballtables flag.
        var allTables = (bool?)await PgExec.ScalarAsync(
            connection, "SELECT puballtables FROM pg_publication WHERE pubname = @p", ct, ("p", pub));

        var resolved = await ResolveColumnListsAsync(connection, desiredTables, warnings, ct);

        if (allTables is null)
        {
            var tableList = string.Join(", ", resolved.Select(FormatTableClause));
            await ExecutePublicationDdlAsync(
                connection, pub, $"CREATE PUBLICATION {PgExec.QuoteIdentifier(pub)} FOR TABLE {tableList}", ct);
            logger.PublicationCreated(pub, resolved.Count);
            return true;
        }

        if (reconcile)
        {
            if (allTables == true)
            {
                throw new WallabyConfigurationException(
                    $"Publication '{pub}' is FOR ALL TABLES; Wallaby cannot reconcile its membership. " +
                    "Recreate it as FOR TABLE, or set ManagePublicationTables=false to use it as-is.");
            }
            await ReconcilePublicationTablesAsync(connection, pub, resolved, ct);
        }

        return false;
    }

    /// <summary>
    /// Resolve candidate column lists against live catalog state (replica identity, generated columns),
    /// demoting to whole-table where a list would be unsafe. No-op when every candidate is whole-table.
    /// </summary>
    private async Task<IReadOnlyList<PublicationTableSpec>> ResolveColumnListsAsync(
        NpgsqlConnection connection,
        IReadOnlyList<PublicationTableSpec> candidates,
        List<string>? warnings,
        CancellationToken ct)
    {
        if (candidates.All(c => c.Columns is null))
        {
            return candidates;
        }

        var catalog = new Dictionary<(string Schema, string Table), TableCatalogInfo>();
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT n.nspname, c.relname, c.relreplident::text,
                   COALESCE((SELECT array_agg(a.attname::text)
                             FROM pg_index i
                             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (i.indkey)
                             WHERE i.indrelid = c.oid AND i.indisreplident), '{}') AS replident_index_cols,
                   COALESCE((SELECT array_agg(a.attname::text)
                             FROM pg_attribute a
                             WHERE a.attrelid = c.oid AND a.attnum > 0
                               AND NOT a.attisdropped AND a.attgenerated <> ''), '{}') AS generated_cols
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN unnest(@schemas, @tables) AS d(s, t) ON n.nspname = d.s AND c.relname = d.t
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("schemas", candidates.Select(c => c.Schema).ToArray());
            cmd.Parameters.AddWithValue("tables", candidates.Select(c => c.Table).ToArray());
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                catalog[(reader.GetString(0), reader.GetString(1))] = new TableCatalogInfo(
                    reader.GetString(2),
                    reader.GetFieldValue<string[]>(3),
                    reader.GetFieldValue<string[]>(4));
            }
        }

        var resolved = new List<PublicationTableSpec>(candidates.Count);
        foreach (var candidate in candidates)
        {
            var (effective, warning, omittedGenerated) = ColumnListPlanner.Plan(
                candidate, catalog.GetValueOrDefault((candidate.Schema, candidate.Table)));
            if (warning is not null)
            {
                warnings?.Add(warning);
                logger.ConfigurationWarning(warning);
            }
            foreach (var column in omittedGenerated)
            {
                logger.GeneratedColumnOmitted(column, candidate.QualifiedName);
            }
            resolved.Add(effective);
        }
        return resolved;
    }

    private async Task ReconcilePublicationTablesAsync(
        NpgsqlConnection connection,
        string pub,
        IReadOnlyList<PublicationTableSpec> desiredTables,
        CancellationToken ct)
    {
        // prattrs (not pg_publication_tables.attnames) is the source of truth: attnames expands to all
        // columns for a whole-table member, hiding the difference from an explicit all-columns list.
        var current = new Dictionary<(string Schema, string Table), HashSet<string>?>();
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT n.nspname, c.relname,
                   CASE WHEN pr.prattrs IS NULL THEN NULL
                        ELSE (SELECT array_agg(a.attname::text)
                              FROM pg_attribute a
                              WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs))
                   END AS columns
            FROM pg_publication p
            JOIN pg_publication_rel pr ON pr.prpubid = p.oid
            JOIN pg_class c ON c.oid = pr.prrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE p.pubname = @p
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("p", pub);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                current[(reader.GetString(0), reader.GetString(1))] = reader.IsDBNull(2)
                    ? null
                    : new HashSet<string>(reader.GetFieldValue<string[]>(2), StringComparer.Ordinal);
            }
        }

        var desired = desiredTables.ToDictionary(d => (d.Schema, d.Table));
        var toAdd = desiredTables.Where(d => !current.ContainsKey((d.Schema, d.Table))).ToList();
        var toDrop = current.Keys.Where(c => !desired.ContainsKey(c)).ToList();

        // Column comparison is by set: prattrs order is DDL order, not model order, and re-issuing
        // DDL on mere ordering differences would break reconcile idempotency.
        var columnDrift = new List<PublicationTableSpec>();
        foreach (var spec in desiredTables)
        {
            if (!current.TryGetValue((spec.Schema, spec.Table), out var currentColumns))
            {
                continue;
            }
            var drifted = spec.Columns is null
                ? currentColumns is not null
                : currentColumns is null || !currentColumns.SetEquals(spec.Columns);
            if (drifted)
            {
                columnDrift.Add(spec);
            }
        }

        if (columnDrift.Count == 0)
        {
            foreach (var spec in toAdd)
            {
                await ExecutePublicationDdlAsync(
                    connection, pub,
                    $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} ADD TABLE {FormatTableClause(spec)}", ct);
                logger.TableAddedToPublication(spec.QualifiedName, pub);
            }
            foreach (var (schema, table) in toDrop)
            {
                await PgExec.ExecuteAsync(
                    connection,
                    $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} DROP TABLE {PgExec.QuoteTable(schema, table)}", ct);
                logger.TableDroppedFromPublication($"{schema}.{table}", pub);
            }
            return;
        }

        // Changing a member's column list must NOT be done as DROP TABLE + ADD TABLE: pgoutput filters
        // each change by the catalog state at its commit, so a transaction committed inside the gap is
        // silently never published — an at-least-once violation. SET TABLE replaces the whole set
        // atomically in one statement.
        var setList = string.Join(", ", desiredTables.Select(FormatTableClause));
        await ExecutePublicationDdlAsync(
            connection, pub, $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} SET TABLE {setList}", ct);

        foreach (var spec in toAdd)
        {
            logger.TableAddedToPublication(spec.QualifiedName, pub);
        }
        foreach (var (schema, table) in toDrop)
        {
            logger.TableDroppedFromPublication($"{schema}.{table}", pub);
        }
        foreach (var spec in columnDrift)
        {
            if (spec.Columns is null)
            {
                logger.PublicationColumnListRemoved(spec.QualifiedName, pub);
            }
            else
            {
                logger.PublicationColumnListChanged(spec.QualifiedName, pub, spec.Columns.Count);
            }
        }
    }

    private static string FormatTableClause(PublicationTableSpec spec)
        => PgExec.QuoteTable(spec.Schema, spec.Table) + (spec.Columns is null
            ? ""
            : " (" + string.Join(", ", spec.Columns.Select(PgExec.QuoteIdentifier)) + ")");

    // 42P01 undefined_table / 42703 undefined_column: the captured model is ahead of the database.
    private static async Task ExecutePublicationDdlAsync(
        NpgsqlConnection connection, string pub, string sql, CancellationToken ct)
    {
        try
        {
            await PgExec.ExecuteAsync(connection, sql, ct);
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01" or "42703")
        {
            throw new WallabyConfigurationException(
                $"Publication DDL for '{pub}' failed: {ex.MessageText}. The captured model references a " +
                "table or column that does not exist yet — run your migrations before starting Wallaby.", ex);
        }
    }

    private IEnumerable<PublicationTableSpec> DesiredTables(WallabyModel model)
    {
        // RequiresFullReplicaIdentity tables are never listed, regardless of current relreplident: the
        // user is being told to flip them to FULL, and a list would turn that flip into publisher-side
        // DML errors on the application's own UPDATE/DELETE statements.
        var listEligible = options.PublicationColumnLists && options.ManagePublicationTables;
        foreach (var table in model.Tables)
        {
            yield return listEligible && !table.RequiresFullReplicaIdentity
                ? new PublicationTableSpec(
                    table.Schema, table.TableName, [.. table.Columns.Select(c => c.ColumnName)])
                : PublicationTableSpec.WholeTable(table.Schema, table.TableName);
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

    private async Task ValidateReplicaIdentityAsync(
        NpgsqlConnection connection, WallabyModel model, List<string> warnings, CancellationToken ct)
    {
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

                var warning =
                    $"Table {table.QualifiedName} has REPLICA IDENTITY '{relReplIdent}'; old values and unchanged-TOAST " +
                    $"columns may be unavailable on UPDATE/DELETE. To capture full rows, run: {ddl}";
                warnings.Add(warning);
                logger.ConfigurationWarning(warning);
            }
        }
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Updated column list for table {Table} in publication '{Publication}' ({ColumnCount} column(s)).")]
    internal static partial void PublicationColumnListChanged(this ILogger logger, string table, string publication, int columnCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Table {Table} reverted to publishing all columns in publication '{Publication}'.")]
    internal static partial void PublicationColumnListRemoved(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Generated column {Column} on {Table} omitted from the publication column list (never published by pgoutput).")]
    internal static partial void GeneratedColumnOmitted(this ILogger logger, string column, string table);
}
