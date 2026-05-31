using Wallaby.Abstractions;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Default <see cref="ICdcBackfillManager"/>: persists a backfill request by marking the table's
/// <c>cdc.backfill_state</c> row as <see cref="BackfillStatus.Requested"/>, so the leader's scheduler
/// picks it up (works regardless of which node received the request).
/// </summary>
internal sealed class DefaultBackfillManager(CdcModel model, IBackfillStateStore store) : ICdcBackfillManager
{
    public Task RequestBackfillAsync<TEntity>(CancellationToken ct = default) where TEntity : class
        => RequestBackfillAsync(typeof(TEntity), ct);

    public async Task RequestBackfillAsync(Type entityClrType, CancellationToken ct = default)
    {
        var table = model.FindByClrType(entityClrType)
            ?? throw new CdcConfigurationException(
                $"Cannot request a backfill for '{entityClrType.FullName}': it is not a captured table.");

        var existing = await store.GetAsync(table.QualifiedName, ct);
        await store.SaveAsync(
            new BackfillState(table.QualifiedName, BackfillStatus.Requested, existing?.TransformVersion, null, 0, DateTimeOffset.UtcNow),
            ct);
    }

    public Task<IReadOnlyList<BackfillState>> GetStatusAsync(CancellationToken ct = default)
        => store.ListAsync(ct);
}
