using EFCore.CDC.Abstractions;

namespace EFCore.CDC.Internal.State;

/// <summary>Persists the per-slot replication checkpoint (our durable mirror of confirmed progress).</summary>
internal interface ICheckpointStore
{
    Task<Checkpoint?> GetAsync(string slotName, CancellationToken ct);

    Task SaveAsync(string slotName, Checkpoint checkpoint, CancellationToken ct);
}
