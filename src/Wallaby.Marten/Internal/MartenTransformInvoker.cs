using Wallaby.Abstractions;
using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>Wraps an <see cref="IWallabyMartenTransform{TEntity}"/> as an <see cref="IWallabyTransformInvoker"/>.</summary>
internal sealed class MartenTransformInvoker<TEntity>(IWallabyMartenTransform<TEntity> transform)
    : IWallabyTransformInvoker
    where TEntity : class
{
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
        object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        => transform.TransformAsync(session, ChangeEventBatch.Cast<TEntity>(changes), ct);
}
