namespace Wallaby.Internal.Backfill;

/// <summary>
/// Composes the per-table backfill version from the versions declared by every mapping on that table.
/// Backfill state is stored per table, so a table mapped to several sinks has ONE version key: any single
/// mapping's <c>WithBackfillVersion</c> bump changes the composite and re-backfills the table — the
/// snapshot flows through the router, so every sink mapped to the table receives it again (idempotent
/// upserts make that convergent over-delivery).
/// </summary>
internal static class BackfillVersioning
{
    /// <summary>Distinct declared versions sorted ordinally and '+'-joined; null when no mapping declares one.</summary>
    public static string? Compose(IEnumerable<string> versions)
    {
        var distinct = versions.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList();
        return distinct.Count == 0 ? null : string.Join('+', distinct);
    }
}
