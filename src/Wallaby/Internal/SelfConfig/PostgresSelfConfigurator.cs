using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Diagnostics;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Default <see cref="ISelfConfigurator"/>: validates the server, ensures the <c>wallaby</c> state
/// schema, and delegates publication and slot provisioning to <see cref="PublicationReconciler"/> and
/// <see cref="SlotProvisioner"/> for the primary and every declared external slot. Uses a normal
/// (non-replication) connection.
/// </summary>
internal sealed class PostgresSelfConfigurator(
    NpgsqlDataSource dataSource,
    SelfConfigOptions options,
    ILogger logger,
    WallabyInstrumentation? instrumentation = null) : ISelfConfigurator
{
    private readonly ServerValidator _validator = new(logger);
    private readonly StateSchemaBootstrapper _stateSchema = new(logger);
    private readonly PublicationReconciler _publications = new(logger);
    private readonly SlotProvisioner _slots = new(logger);
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
            var publication = await _publications.EnsureAsync(
                connection, options.PublicationName, DesiredTables(model).ToList(), options.ManagePublicationTables,
                warnings, ct);
            if (!publication.ViaRoot)
            {
                await ValidatePartitionedCapturesAsync(connection, model, ct);
            }
            var (slotCreated, consistentPoint) = await _slots.EnsureAsync(
                connection, options.SlotName, options.PublicationName, kind: "primary", ct);
            await ValidateReplicaIdentityAsync(connection, model, warnings, ct);
            var externalResults = await EnsureExternalSlotsAsync(connection, ct);

            logger.SelfConfigComplete(options.PublicationName, publication.Created, options.SlotName, slotCreated);

            return new SelfConfigResult(
                options.PublicationName, options.SlotName, publication.Created, slotCreated, consistentPoint, warnings,
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
            var publication = await _publications.EnsureAsync(
                connection, spec.PublicationName, tables, reconcile: true, warnings: null, ct);
            var (slotCreated, _) = await _slots.EnsureAsync(
                connection, spec.SlotName, spec.PublicationName, kind: "external", ct);
            logger.ExternalSlotConfigured(spec.SlotName, spec.PublicationName);
            results.Add(new ExternalSlotResult(spec.SlotName, spec.PublicationName, publication.Created, slotCreated));
        }

        return results;
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

    /// <summary>
    /// A publication that does not publish via the partition root streams changes under leaf partition
    /// names, which no captured table matches — silent data loss, so this always throws. Only reachable
    /// for pre-existing unmanaged publications; managed ones are always fixed to publish via root.
    /// </summary>
    private async Task ValidatePartitionedCapturesAsync(
        NpgsqlConnection connection, WallabyModel model, CancellationToken ct)
    {
        var partitioned = new List<string>();
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN unnest(@schemas, @tables) AS d(s, t) ON n.nspname = d.s AND c.relname = d.t
            WHERE c.relkind = 'p'
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("schemas", model.Tables.Select(t => t.Schema).ToArray());
            cmd.Parameters.AddWithValue("tables", model.Tables.Select(t => t.TableName).ToArray());
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                partitioned.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
            }
        }

        if (partitioned.Count > 0)
        {
            throw new WallabyConfigurationException(
                $"Publication '{options.PublicationName}' does not publish via the partition root, so changes on " +
                $"partitioned table(s) {string.Join(", ", partitioned)} would arrive under leaf partition names " +
                "and be dropped. Run: ALTER PUBLICATION " +
                $"{PgExec.QuoteIdentifier(options.PublicationName)} SET (publish_via_partition_root = true); " +
                "or set ManagePublicationTables=true to let Wallaby manage it.");
        }
    }

    private async Task ValidateReplicaIdentityAsync(
        NpgsqlConnection connection, WallabyModel model, List<string> warnings, CancellationToken ct)
    {
        var required = model.Tables.Where(t => t.RequiresFullReplicaIdentity).ToList();
        if (required.Count == 0)
        {
            return;
        }

        // Replica identity governs WAL old-tuple content per physical relation, so a partitioned table
        // must be FULL on every leaf; the root's own setting does not propagate. pg_partition_tree is
        // empty for an ordinary table, which the relkind branch covers.
        var physicalByRoot = new Dictionary<string, List<(string Schema, string Table, string ReplIdent)>>();
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT d.s, d.t, pn.nspname, pc.relname, pc.relreplident::text
            FROM unnest(@schemas, @tables) AS d(s, t)
            JOIN pg_namespace n ON n.nspname = d.s
            JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = d.t
            CROSS JOIN LATERAL (
                SELECT pt.relid FROM pg_partition_tree(c.oid) pt WHERE pt.isleaf
                UNION ALL
                SELECT c.oid WHERE c.relkind <> 'p'
            ) leaf(relid)
            JOIN pg_class pc ON pc.oid = leaf.relid
            JOIN pg_namespace pn ON pn.oid = pc.relnamespace
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("schemas", required.Select(t => t.Schema).ToArray());
            cmd.Parameters.AddWithValue("tables", required.Select(t => t.TableName).ToArray());
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var root = $"{reader.GetString(0)}.{reader.GetString(1)}";
                if (!physicalByRoot.TryGetValue(root, out var list))
                {
                    physicalByRoot[root] = list = [];
                }
                list.Add((reader.GetString(2), reader.GetString(3), reader.GetString(4)));
            }
        }

        foreach (var table in required)
        {
            // A table missing from the catalog surfaces from publication DDL as run-your-migrations.
            if (!physicalByRoot.TryGetValue(table.QualifiedName, out var physical))
            {
                continue;
            }

            // relreplident: 'd' default, 'n' nothing, 'f' full, 'i' index.
            var notFull = physical.Where(p => p.ReplIdent != "f").ToList();
            if (notFull.Count == 0)
            {
                continue;
            }

            var isPartitioned = physical.Count > 1 ||
                (physical[0].Schema, physical[0].Table) != (table.Schema, table.TableName);
            var ddl = string.Join(" ", notFull.Select(p =>
                $"ALTER TABLE {PgExec.QuoteTable(p.Schema, p.Table)} REPLICA IDENTITY FULL;"));

            string message;
            if (isPartitioned)
            {
                var leaves = string.Join(", ", notFull.Select(p => $"{p.Schema}.{p.Table}"));
                message = options.RequireFullReplicaIdentity
                    ? $"Table {table.QualifiedName} requires REPLICA IDENTITY FULL for its transform, but " +
                      $"partition(s) {leaves} are not FULL (identity is per leaf and does not propagate " +
                      $"from the root). Run: {ddl} New partitions need the same treatment."
                    : $"Table {table.QualifiedName} has partition(s) {leaves} without REPLICA IDENTITY FULL; " +
                      $"old values and unchanged-TOAST columns may be unavailable on UPDATE/DELETE. To " +
                      $"capture full rows, run: {ddl} New partitions need the same treatment.";
            }
            else
            {
                var relReplIdent = notFull[0].ReplIdent;
                message = options.RequireFullReplicaIdentity
                    ? $"Table {table.QualifiedName} requires REPLICA IDENTITY FULL for its transform but has '{relReplIdent}'. Run: {ddl}"
                    : $"Table {table.QualifiedName} has REPLICA IDENTITY '{relReplIdent}'; old values and unchanged-TOAST " +
                      $"columns may be unavailable on UPDATE/DELETE. To capture full rows, run: {ddl}";
            }

            if (options.RequireFullReplicaIdentity)
            {
                throw new WallabyConfigurationException(message);
            }
            warnings.Add(message);
            logger.ConfigurationWarning(message);
        }
    }
}

/// <summary>Source-generated log messages for <see cref="PostgresSelfConfigurator"/>.</summary>
internal static partial class PostgresSelfConfiguratorLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Wallaby self-config complete: publication '{Publication}' (created={PubCreated}), slot '{Slot}' (created={SlotCreated}).")]
    internal static partial void SelfConfigComplete(this ILogger logger, string publication, bool pubCreated, string slot, bool slotCreated);

    [LoggerMessage(Level = LogLevel.Information, Message = "Configured external slot '{Slot}' (publication '{Publication}') for a third-party consumer.")]
    internal static partial void ExternalSlotConfigured(this ILogger logger, string slot, string publication);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Warning}")]
    internal static partial void ConfigurationWarning(this ILogger logger, string warning);
}
