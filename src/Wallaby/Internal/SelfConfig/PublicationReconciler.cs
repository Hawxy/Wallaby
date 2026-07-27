using Microsoft.Extensions.Logging;
using Npgsql;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// Outcome of ensuring a publication: whether it was created, and whether it publishes via the
/// partition root (always true for a managed publication after the ensure).
/// </summary>
internal readonly record struct PublicationEnsureResult(bool Created, bool ViaRoot);

/// <summary>
/// Creates a publication for the desired table set, or reconciles an existing one: membership drift is
/// applied per table, column-list drift atomically via <c>SET TABLE</c>. Candidate column lists are
/// resolved against live catalog state (<see cref="ColumnListPlanner"/>) before any DDL. Managed
/// publications always set <c>publish_via_partition_root</c> so partitioned tables publish under the
/// root's name and schema.
/// </summary>
internal sealed class PublicationReconciler(ILogger logger)
{
    /// <summary>Ensure <paramref name="pub"/> exists with the desired tables.</summary>
    public async Task<PublicationEnsureResult> EnsureAsync(
        NpgsqlConnection connection,
        string pub,
        IReadOnlyList<PublicationTableSpec> desiredTables,
        bool reconcile,
        List<string>? warnings,
        CancellationToken ct)
    {
        // null = publication absent.
        (bool AllTables, bool ViaRoot)? existing = null;
        await using (var probe = new NpgsqlCommand(
            "SELECT puballtables, pubviaroot FROM pg_publication WHERE pubname = @p", connection))
        {
            probe.Parameters.AddWithValue("p", pub);
            await using var reader = await probe.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                existing = (reader.GetBoolean(0), reader.GetBoolean(1));
            }
        }

        var resolved = await ResolveColumnListsAsync(connection, desiredTables, warnings, ct);

        if (existing is null)
        {
            var tableList = string.Join(", ", resolved.Select(FormatTableClause));
            await ExecutePublicationDdlAsync(
                connection, pub,
                $"CREATE PUBLICATION {PgExec.QuoteIdentifier(pub)} FOR TABLE {tableList} " +
                "WITH (publish_via_partition_root = true)", ct);
            logger.PublicationCreated(pub, resolved.Count);
            return new PublicationEnsureResult(Created: true, ViaRoot: true);
        }

        if (reconcile)
        {
            if (existing.Value.AllTables)
            {
                throw new WallabyConfigurationException(
                    $"Publication '{pub}' is FOR ALL TABLES; Wallaby cannot reconcile its membership. " +
                    "Recreate it as FOR TABLE, or set ManagePublicationTables=false to use it as-is.");
            }
            if (!existing.Value.ViaRoot)
            {
                // Safe with an existing slot: transactions committed before this ALTER decode under the
                // historic catalog (leaf names for partitioned tables), which no captured table matches
                // anyway; everything after decodes under the root.
                await PgExec.ExecuteAsync(
                    connection,
                    $"ALTER PUBLICATION {PgExec.QuoteIdentifier(pub)} SET (publish_via_partition_root = true)", ct);
                logger.PublicationViaRootEnabled(pub);
            }
            await ReconcileTablesAsync(connection, pub, resolved, ct);
            return new PublicationEnsureResult(Created: false, ViaRoot: true);
        }

        return new PublicationEnsureResult(Created: false, ViaRoot: existing.Value.ViaRoot);
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
            SELECT n.nspname, c.relname, c.relkind::text, c.relreplident::text,
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
                    reader.GetString(3),
                    reader.GetFieldValue<string[]>(4),
                    reader.GetFieldValue<string[]>(5));
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

    private async Task ReconcileTablesAsync(
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
}

/// <summary>Source-generated log messages for <see cref="PublicationReconciler"/>.</summary>
internal static partial class PublicationReconcilerLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Created publication {Publication} for {TableCount} table(s).")]
    internal static partial void PublicationCreated(this ILogger logger, string publication, int tableCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Enabled publish_via_partition_root on publication {Publication}.")]
    internal static partial void PublicationViaRootEnabled(this ILogger logger, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Added table {Table} to publication {Publication}.")]
    internal static partial void TableAddedToPublication(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped table {Table} from publication {Publication}.")]
    internal static partial void TableDroppedFromPublication(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Information, Message = "Updated column list for table {Table} in publication {Publication} ({ColumnCount} column(s)).")]
    internal static partial void PublicationColumnListChanged(this ILogger logger, string table, string publication, int columnCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Table {Table} reverted to publishing all columns in publication {Publication}.")]
    internal static partial void PublicationColumnListRemoved(this ILogger logger, string table, string publication);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Generated column {Column} on {Table} omitted from the publication column list (never published by pgoutput).")]
    internal static partial void GeneratedColumnOmitted(this ILogger logger, string column, string table);
}
