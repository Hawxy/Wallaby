using Wallaby.Model;

namespace Wallaby.Internal.SelfConfig;

/// <summary>The outcome of a self-configuration run.</summary>
/// <param name="PublicationName">The publication that was ensured.</param>
/// <param name="SlotName">The replication slot that was ensured.</param>
/// <param name="PublicationCreated">True if the publication was created during this run.</param>
/// <param name="SlotCreated">True if the slot was created during this run.</param>
/// <param name="ConsistentPoint">The slot's consistent point LSN (text), when the slot was just created.</param>
/// <param name="SlotRecreated">
/// True when the slot was created this run but its <c>wallaby.slot_registry</c> row already existed:
/// the installation had a slot before, so changes committed while it was gone were never streamed.
/// </param>
/// <param name="Warnings">Non-fatal advisories (e.g. REPLICA IDENTITY recommendations).</param>
/// <param name="ExternalSlots">Per-external-slot outcomes (empty when none are declared).</param>
internal sealed record SelfConfigResult(
    string PublicationName,
    string SlotName,
    bool PublicationCreated,
    bool SlotCreated,
    string? ConsistentPoint,
    bool SlotRecreated,
    IReadOnlyList<string> Warnings,
    IReadOnlyList<ExternalSlotResult> ExternalSlots);

/// <summary>
/// Brings the source Postgres database into a state where Wallaby can run: validates server settings,
/// ensures the <c>wallaby</c> state schema, and creates/reconciles the publication and replication slot.
/// </summary>
internal interface ISelfConfigurator
{
    /// <summary>
    /// With <paramref name="widenPublications"/> every managed publication reconciles to plain
    /// whole-table membership (no column lists), so schema migrations blocked by publication column
    /// lists can run; the next reconcile without the flag restores the narrow lists.
    /// </summary>
    Task<SelfConfigResult> EnsureConfiguredAsync(
        WallabyModel model, CancellationToken ct, bool widenPublications = false);
}
