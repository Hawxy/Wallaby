using Wallaby.Model;

namespace Wallaby.Internal.SelfConfig;

/// <summary>The outcome of a self-configuration run.</summary>
/// <param name="PublicationName">The publication that was ensured.</param>
/// <param name="SlotName">The replication slot that was ensured.</param>
/// <param name="PublicationCreated">True if the publication was created during this run.</param>
/// <param name="SlotCreated">True if the slot was created during this run.</param>
/// <param name="ConsistentPoint">The slot's consistent point LSN (text), when the slot was just created.</param>
/// <param name="Warnings">Non-fatal advisories (e.g. REPLICA IDENTITY recommendations).</param>
internal sealed record SelfConfigResult(
    string PublicationName,
    string SlotName,
    bool PublicationCreated,
    bool SlotCreated,
    string? ConsistentPoint,
    IReadOnlyList<string> Warnings);

/// <summary>
/// Brings the source Postgres database into a state where CDC can run: validates server settings,
/// ensures the <c>wallaby</c> state schema, and creates/reconciles the publication and replication slot.
/// </summary>
internal interface ICdcSelfConfigurator
{
    Task<SelfConfigResult> EnsureConfiguredAsync(CdcModel model, CancellationToken ct);
}
