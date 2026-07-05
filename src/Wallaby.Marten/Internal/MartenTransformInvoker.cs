using Marten;
using Wallaby.Abstractions;
using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>
/// Wraps a strongly-typed <see cref="IWallabyMartenTransform{TEntity}"/> as an
/// <see cref="IWallabyTransformInvoker"/>, downcasting the leased session to the
/// <see cref="IQuerySession"/> the transform expects.
/// </summary>
internal sealed class MartenTransformInvoker<TEntity>(IWallabyMartenTransform<TEntity> transform)
    : IWallabyTransformInvoker
    where TEntity : class
{
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
        object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        => transform.TransformAsync((IQuerySession)session, ChangeEventBatch.Cast<TEntity>(changes), ct);
}
