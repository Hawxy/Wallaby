namespace Wallaby.Internal.SelfConfig;

/// <summary>Catalog facts about one physical table, read at reconcile time.</summary>
/// <param name="RelKind"><c>pg_class.relkind</c>: 'r' ordinary, 'p' partitioned.</param>
/// <param name="RelReplIdent"><c>pg_class.relreplident</c>: 'd' default, 'n' nothing, 'f' full, 'i' index.</param>
/// <param name="ReplicaIdentityIndexColumns">Columns of the replica-identity index; empty unless 'i'.</param>
/// <param name="GeneratedColumns">Stored generated columns (<c>attgenerated &lt;&gt; ''</c>).</param>
internal sealed record TableCatalogInfo(
    string RelKind,
    string RelReplIdent,
    IReadOnlyList<string> ReplicaIdentityIndexColumns,
    IReadOnlyList<string> GeneratedColumns);

/// <summary>
/// Resolves a candidate publication column list against live catalog state. A list that does not cover
/// the table's replica identity would make the application's own UPDATE/DELETE statements error at DML
/// time on the publisher, so such candidates are demoted to whole-table rather than emitted.
/// </summary>
internal static class ColumnListPlanner
{
    /// <summary>
    /// Returns the effective spec (possibly demoted to whole-table), a warning when demoted, and the
    /// generated columns removed from the list (never publishable, rejected by column-list DDL).
    /// </summary>
    public static (PublicationTableSpec Effective, string? Warning, IReadOnlyList<string> OmittedGenerated) Plan(
        PublicationTableSpec candidate, TableCatalogInfo? catalog)
    {
        // Whole-table candidates need no catalog input; a table missing from the catalog (model ahead
        // of migrations) passes through so the subsequent DDL fails fast like a missing table today.
        if (candidate.Columns is null || catalog is null)
        {
            return (candidate, null, []);
        }

        // Replica identity is governed per leaf partition, which this root-level pass cannot see; a
        // list missing a FULL leaf's identity errors the application's own UPDATE/DELETE at DML time.
        if (catalog.RelKind == "p")
        {
            return (candidate with { Columns = null },
                $"Table {candidate.QualifiedName} is partitioned; publishing all columns " +
                "(a column list cannot be validated against each partition's replica identity).",
                []);
        }

        if (catalog.RelReplIdent == "f")
        {
            return (candidate with { Columns = null },
                $"Table {candidate.QualifiedName} has REPLICA IDENTITY FULL; publishing all columns " +
                "(a publication column list must cover the replica identity, and FULL covers every column).",
                []);
        }

        var columns = candidate.Columns;
        var omittedGenerated = catalog.GeneratedColumns.Count == 0
            ? []
            : catalog.GeneratedColumns.Where(g => columns.Contains(g, StringComparer.Ordinal)).ToList();
        if (omittedGenerated.Count > 0)
        {
            columns = columns.Where(c => !omittedGenerated.Contains(c, StringComparer.Ordinal)).ToList();
        }

        // Degenerate in practice (a key column is never generated), but an empty list is invalid DDL.
        if (columns.Count == 0)
        {
            return (candidate with { Columns = null }, null, omittedGenerated);
        }

        if (catalog.RelReplIdent == "i" &&
            catalog.ReplicaIdentityIndexColumns.Any(ic => !columns.Contains(ic, StringComparer.Ordinal)))
        {
            var uncovered = catalog.ReplicaIdentityIndexColumns
                .Where(ic => !columns.Contains(ic, StringComparer.Ordinal));
            return (candidate with { Columns = null },
                $"Table {candidate.QualifiedName} uses REPLICA IDENTITY USING INDEX with column(s) " +
                $"{string.Join(", ", uncovered)} outside the captured set; publishing all columns.",
                omittedGenerated);
        }

        // 'd': the captured set always includes the primary key. 'n': UPDATE/DELETE on a published
        // table already error at DML time regardless of column lists — a list adds no new failure mode.
        return (candidate with { Columns = columns }, null, omittedGenerated);
    }
}
