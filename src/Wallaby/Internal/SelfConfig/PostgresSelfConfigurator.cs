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
    private readonly StateSchemaBootstrapper _stateSchema = new();
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
            var publicationCreated = await _publications.EnsureAsync(
                connection, options.PublicationName, DesiredTables(model).ToList(), options.ManagePublicationTables,
                warnings, ct);
            var (slotCreated, consistentPoint) = await _slots.EnsureAsync(
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
            var pubCreated = await _publications.EnsureAsync(
                connection, spec.PublicationName, tables, reconcile: true, warnings: null, ct);
            var (slotCreated, _) = await _slots.EnsureAsync(
                connection, spec.SlotName, spec.PublicationName, kind: "external", ct);
            logger.ExternalSlotConfigured(spec.SlotName, spec.PublicationName);
            results.Add(new ExternalSlotResult(spec.SlotName, spec.PublicationName, pubCreated, slotCreated));
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Configured external slot '{Slot}' (publication '{Publication}') for a third-party consumer.")]
    internal static partial void ExternalSlotConfigured(this ILogger logger, string slot, string publication);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Warning}")]
    internal static partial void ConfigurationWarning(this ILogger logger, string warning);
}
