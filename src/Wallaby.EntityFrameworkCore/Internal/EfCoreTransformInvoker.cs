using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Providers;

namespace Wallaby.EntityFrameworkCore.Internal;

/// <summary>
/// Wraps a strongly-typed <see cref="IWallabyEfTransform{TEntity}"/> as an <see cref="IWallabyTransformInvoker"/>,
/// downcasting the leased session to the <see cref="DbContext"/> the transform expects.
/// </summary>
internal sealed class EfCoreTransformInvoker<TEntity>(IWallabyEfTransform<TEntity> transform)
    : IWallabyTransformInvoker
    where TEntity : class
{
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
        object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        => transform.TransformAsync((DbContext)session, ChangeEventBatch.Cast<TEntity>(changes), ct);
}
