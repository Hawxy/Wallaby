namespace Wallaby.Internal.SelfConfig;

/// <summary>Inputs to self-configuration. Populated from the consumer's Wallaby options at startup.</summary>
internal sealed class SelfConfigOptions
{
    /// <summary>
    /// The primary logical replication slot name to create/reuse. Unused in provision-only mode
    /// (<see cref="ExternalSlots"/> only, via <c>EnsureExternalSlotsOnlyAsync</c>), so it defaults to empty.
    /// </summary>
    public string SlotName { get; init; } = "";

    /// <summary>The primary publication name to create/reuse. Unused in provision-only mode (defaults to empty).</summary>
    public string PublicationName { get; init; } = "";

    /// <summary>
    /// When true, an existing publication's table set is reconciled to match the captured model
    /// (ADD/DROP TABLE). When false, an existing publication is used as-is.
    /// </summary>
    public bool ManagePublicationTables { get; init; } = true;

    /// <summary>
    /// When true, a table that needs <c>REPLICA IDENTITY FULL</c> but doesn't have it causes a hard
    /// failure instead of a warning.
    /// </summary>
    public bool RequireFullReplicaIdentity { get; init; }

    /// <summary>
    /// When true (and <see cref="ManagePublicationTables"/> is true), the primary publication gives a
    /// PG15 column list to each table whose capture set was deliberately narrowed
    /// (<see cref="Model.CapturedTable.ColumnsNarrowed"/>); every other table publishes whole rows, as
    /// do tables requiring <c>REPLICA IDENTITY FULL</c>. External publications are unaffected.
    /// </summary>
    public bool PublicationColumnLists { get; init; } = true;

    /// <summary>
    /// Additional pgoutput publication+slot pairs to provision for third-party consumers (e.g. an ELT
    /// tool). Wallaby creates them, reconciles their table sets, and records them in
    /// <c>wallaby.slot_registry</c> with <c>kind='external'</c>, but never opens them itself.
    /// </summary>
    public IReadOnlyList<ExternalSlotSpec> ExternalSlots { get; init; } = [];
}
