using Wallaby.Abstractions;

namespace Wallaby.Internal.State;

/// <summary>Persists per-table backfill state in <c>cdc.backfill_state</c>.</summary>
internal interface IBackfillStateStore
{
    Task<BackfillState?> GetAsync(string tableQualifiedName, CancellationToken ct);

    Task SaveAsync(BackfillState state, CancellationToken ct);

    Task<IReadOnlyList<BackfillState>> ListAsync(CancellationToken ct);
}
