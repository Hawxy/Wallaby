using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// One (sink, destination) pair a table's mappings deliver to, as a purge target. A scoped mapping's
/// destinations are computed per record and cannot be enumerated, so it is marked and skipped with a
/// warning instead of purged.
/// </summary>
internal sealed record SinkPurgeTarget(string SinkName, string? Destination, bool Scoped);

/// <summary>
/// A mapped table as the backfill scheduler sees it: the composite of its mappings' declared backfill
/// versions, whether any mapping opted into purging on a version change, and the purge targets its
/// mappings deliver to.
/// </summary>
internal sealed record BackfillTable(
    CapturedTable Table,
    string? TransformVersion,
    bool PurgeOnVersionChange,
    IReadOnlyList<SinkPurgeTarget> PurgeTargets);
