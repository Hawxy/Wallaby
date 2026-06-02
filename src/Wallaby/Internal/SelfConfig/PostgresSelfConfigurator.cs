using Microsoft.Extensions.Logging;
using Npgsql;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Default <see cref="ICdcSelfConfigurator"/>: validates the server, ensures the <c>wallaby</c> state
/// schema, and creates/reconciles the publication and pgoutput replication slot derived from the
/// captured model. Uses a normal (non-replication) connection.
/// </summary>
internal sealed class PostgresSelfConfigurator(
    NpgsqlDataSource dataSource,
    SelfConfigOptions options,
    ILogger logger) : ICdcSelfConfigurator
{
    private readonly ServerValidator _validator = new(logger);
    private readonly StateSchemaBootstrapper _stateSchema = new();

    public async Task<SelfConfigResult> EnsureConfiguredAsync(CdcModel model, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await _validator.ValidateAsync(connection, options.SlotName, ct);
        await _stateSchema.EnsureAsync(connection, ct);

        var publicationCreated = await EnsurePublicationAsync(connection, model, ct);
        var (slotCreated, consistentPoint) = await EnsureSlotAsync(connection, ct);
        var warnings = await ValidateReplicaIdentityAsync(connection, model, ct);

        logger.SelfConfigComplete(options.PublicationName, publicationCreated, options.SlotName, slotCreated);

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
            logger.PublicationCreated(pub, model.Tables.Count);
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

    private static IEnumerable<(string Schema, string Table)> DesiredTables(CdcModel model)
    {
        foreach (var table in model.Tables)
        {
            yield return (table.Schema, table.TableName);
        }
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
            INSERT INTO wallaby.slot_registry (slot_name, publication, consistent_point)
            VALUES (@s, @p, @cp::pg_lsn)
            ON CONFLICT (slot_name) DO UPDATE
                SET publication = EXCLUDED.publication,
                    consistent_point = EXCLUDED.consistent_point
            """,
            ct,
            ("s", slot), ("p", options.PublicationName), ("cp", consistentPoint));

        logger.SlotCreated(slot, consistentPoint);
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
            logger.ConfigurationWarning(warning);
        }

        return warnings;
    }
}

/// <summary>Source-generated log messages for <see cref="PostgresSelfConfigurator"/>.</summary>
internal static partial class PostgresSelfConfiguratorLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "CDC self-config complete: publication '{Publication}' (created={PubCreated}), slot '{Slot}' (created={SlotCreated}).")]
    internal static partial void SelfConfigComplete(this ILogger logger, string publication, bool pubCreated, string slot, bool slotCreated);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created publication '{Publication}' for {TableCount} table(s).")]
    internal static partial void PublicationCreated(this ILogger logger, string publication, int tableCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Added table {Table} to publication '{Publication}'.")]
    internal static partial void TableAddedToPublication(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped table {Table} from publication '{Publication}'.")]
    internal static partial void TableDroppedFromPublication(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created pgoutput replication slot '{Slot}' at {ConsistentPoint}.")]
    internal static partial void SlotCreated(this ILogger logger, string slot, string? consistentPoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Warning}")]
    internal static partial void ConfigurationWarning(this ILogger logger, string warning);
}
