using Wallaby.Abstractions;
using Wallaby.Internal.State;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Default <see cref="IWallabyBackfillManager"/>: persists a backfill request by marking the table's
/// <c>wallaby.backfill_state</c> row as <see cref="BackfillStatus.Requested"/> and signalling the backfill
/// notify channel, so the leader's scheduler serves it immediately (works regardless of which node
/// received the request).
/// </summary>
internal sealed class DefaultBackfillManager(WallabyModel model, IBackfillStateStore store) : IWallabyBackfillManager
{
    public Task RequestBackfillAsync<TEntity>(CancellationToken ct = default) where TEntity : class
        => RequestBackfillAsync(typeof(TEntity), purge: false, ct);

    public Task RequestBackfillAsync<TEntity>(bool purge, CancellationToken ct = default) where TEntity : class
        => RequestBackfillAsync(typeof(TEntity), purge, ct);

    public Task RequestBackfillAsync(Type entityClrType, CancellationToken ct = default)
        => RequestBackfillAsync(entityClrType, purge: false, ct);

    public async Task RequestBackfillAsync(Type entityClrType, bool purge, CancellationToken ct = default)
    {
        var table = model.FindByClrType(entityClrType)
            ?? throw new WallabyConfigurationException(
                $"Cannot request a backfill for '{entityClrType.FullName}': it is not a captured table.");

        var existing = await store.GetAsync(table.QualifiedName, ct);
        await store.RequestAsync(table.QualifiedName, existing?.TransformVersion, purge, ct);
    }

    public Task<bool> CancelBackfillAsync<TEntity>(CancellationToken ct = default) where TEntity : class
        => CancelBackfillAsync(typeof(TEntity), ct);

    public Task<bool> CancelBackfillAsync(Type entityClrType, CancellationToken ct = default)
    {
        var table = model.FindByClrType(entityClrType)
            ?? throw new WallabyConfigurationException(
                $"Cannot cancel a backfill for '{entityClrType.FullName}': it is not a captured table.");
        return store.CancelRequestAsync(table.QualifiedName, ct);
    }

    public Task<bool> CancelBackfillAsync(string tableQualifiedName, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableQualifiedName);
        return store.CancelRequestAsync(tableQualifiedName, ct);
    }

    public Task<IReadOnlyList<BackfillState>> GetStatusAsync(CancellationToken ct = default)
        => store.ListAsync(ct);
}
