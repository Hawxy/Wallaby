using EFCore.CDC.Internal.State;
using EFCore.CDC.Model;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace EFCore.CDC.Internal.SelfConfig;

/// <summary>
/// Default <see cref="ICdcSelfConfigurator"/>: validates the server, ensures the <c>cdc</c> state
/// schema, and creates/reconciles the publication and pgoutput replication slot derived from the
/// captured model. Uses a normal (non-replication) connection.
/// </summary>
internal sealed class PostgresSelfConfigurator(
    string connectionString,
    SelfConfigOptions options,
    ILogger logger) : ICdcSelfConfigurator
{
    private readonly ServerValidator _validator = new(logger);
    private readonly StateSchemaBootstrapper _stateSchema = new();

    public async Task<SelfConfigResult> EnsureConfiguredAsync(CdcModel model, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await _validator.ValidateAsync(connection, options.SlotName, ct);
        await _stateSchema.EnsureAsync(connection, ct);

        var publicationCreated = await EnsurePublicationAsync(connection, model, ct);
        var (slotCreated, consistentPoint) = await EnsureSlotAsync(connection, ct);
        var warnings = await ValidateReplicaIdentityAsync(connection, model, ct);

        logger.LogInformation(
            "CDC self-config complete: publication '{Publication}' (created={PubCreated}), slot '{Slot}' (created={SlotCreated}).",
            options.PublicationName, publicationCreated, options.SlotName, slotCreated);

        return new SelfConfigResult(
            options.PublicationName, options.SlotName, publicationCreated, slotCreated, consistentPoint, warnings);
    }

    private async Task<bool> EnsurePublicationAsync(NpgsqlConnection connection, CdcModel model, CancellationToken ct)
    {
        var pub = options.PublicationName;
        var exists = await PgExec.ScalarLongAsync(
            connection, "SELECT count(*) FROM pg_publication WHERE pubname = @p", ct, ("p", pub)) > 0;

        if (!exists)
        {
            var tableList = string.Join(", ", DesiredTables(model).Select(t => PgExec.QuoteTable(t.Schema, t.Table)));
            await PgExec.ExecuteAsync(
                connection, $"CREATE PUBLICATION {PgExec.QuoteIdentifier(pub)} FOR TABLE {tableList}", ct);
            logger.LogInformation("Created publication '{Publication}' for {TableCount} table(s).", pub, model.Tables.Count);
            return true;
        }

        if (options.ManagePublicationTables)
        {
            await ReconcilePublicationTablesAsync(connection, model, ct);
        }

        return false;
    }

    private async Task ReconcilePublicationTablesAsync(NpgsqlConnection connection, CdcModel model, CancellationToken ct)
    {
        var pub = options.PublicationName;
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

        var desired = DesiredTables(model).ToHashSet();

        foreach (var (schema, table) in desired.Where(d => !current.Contains(d)))
        {
            await PgExec.ExecuteAsync(
                connection,
                $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} ADD TABLE {PgExec.QuoteTable(schema, table)}", ct);
            logger.LogInformation("Added table {Table} to publication '{Publication}'.", $"{schema}.{table}", pub);
        }

        foreach (var (schema, table) in current.Where(c => !desired.Contains(c)))
        {
            await PgExec.ExecuteAsync(
                connection,
                $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} DROP TABLE {PgExec.QuoteTable(schema, table)}", ct);
            logger.LogInformation("Dropped table {Table} from publication '{Publication}'.", $"{schema}.{table}", pub);
        }
    }

    // The captured tables plus the internal watermark sentinel table (needed so backfill watermarks
    // flow through the same replication stream).
    private static IEnumerable<(string Schema, string Table)> DesiredTables(CdcModel model)
    {
        foreach (var table in model.Tables)
        {
            yield return (table.Schema, table.TableName);
        }
        yield return (CdcSchema.Schema, CdcSchema.WatermarkTable);
    }

    private async Task<(bool Created, string? ConsistentPoint)> EnsureSlotAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var slot = options.SlotName;
        var exists = await PgExec.ScalarLongAsync(
            connection, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = @s", ct, ("s", slot)) > 0;

        if (exists)
        {
            return (false, null);
        }

        var consistentPoint = await PgExec.ScalarStringAsync(
            connection, "SELECT lsn::text FROM pg_create_logical_replication_slot(@s, 'pgoutput')", ct, ("s", slot));

        await PgExec.ExecuteAsync(
            connection,
            """
            INSERT INTO cdc.slot_registry (slot_name, publication, consistent_point)
            VALUES (@s, @p, @cp::pg_lsn)
            ON CONFLICT (slot_name) DO UPDATE
                SET publication = EXCLUDED.publication,
                    consistent_point = EXCLUDED.consistent_point
            """,
            ct,
            ("s", slot), ("p", options.PublicationName), ("cp", consistentPoint));

        logger.LogInformation("Created pgoutput replication slot '{Slot}' at {ConsistentPoint}.", slot, consistentPoint);
        return (true, consistentPoint);
    }

    private async Task<IReadOnlyList<string>> ValidateReplicaIdentityAsync(
        NpgsqlConnection connection, CdcModel model, CancellationToken ct)
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
                    throw new CdcConfigurationException(
                        $"Table {table.QualifiedName} requires REPLICA IDENTITY FULL for its transform but has '{relReplIdent}'. Run: {ddl}");
                }

                warnings.Add(
                    $"Table {table.QualifiedName} has REPLICA IDENTITY '{relReplIdent}'; old values and unchanged-TOAST " +
                    $"columns may be unavailable on UPDATE/DELETE. To capture full rows, run: {ddl}");
            }
        }

        foreach (var warning in warnings)
        {
            logger.LogWarning("{Warning}", warning);
        }

        return warnings;
    }
}
