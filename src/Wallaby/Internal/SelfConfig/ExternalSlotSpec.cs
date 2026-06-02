namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// A resolved external replication slot to provision: a pgoutput slot backed by a publication for a fixed
/// set of tables. Wallaby creates and reconciles these for a third-party CDC consumer (e.g. an ELT tool)
/// but never opens them itself. Produced from the declared registrations by <see cref="ExternalSlotResolver"/>.
/// </summary>
/// <param name="SlotName">The logical replication slot to create/reuse.</param>
/// <param name="PublicationName">The publication backing the slot (created/reconciled for <paramref name="Tables"/>).</param>
/// <param name="Tables">The schema-qualified tables the publication should contain.</param>
internal sealed record ExternalSlotSpec(
    string SlotName,
    string PublicationName,
    IReadOnlyList<(string Schema, string Table)> Tables);

/// <summary>Per-external-slot outcome of a self-configuration run.</summary>
/// <param name="SlotName">The external slot that was ensured.</param>
/// <param name="PublicationName">The external publication that was ensured.</param>
/// <param name="PublicationCreated">True if the publication was created during this run.</param>
/// <param name="SlotCreated">True if the slot was created during this run.</param>
internal sealed record ExternalSlotResult(
    string SlotName,
    string PublicationName,
    bool PublicationCreated,
    bool SlotCreated);
