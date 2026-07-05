using Marten;
using Wallaby.Abstractions;

namespace Wallaby.Marten;

/// <summary>
/// Adapts a lambda to <see cref="IWallabyMartenTransform{TEntity}"/> for the trivial cases that don't
/// warrant a dedicated class (e.g. projecting straight from the change with no enrichment).
/// </summary>
public sealed class DelegateTransform<TEntity>(
    Func<IQuerySession, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
    : IWallabyMartenTransform<TEntity>
    where TEntity : class
{
    /// <inheritdoc />
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        IQuerySession querySession, IReadOnlyList<ChangeEvent<TEntity>> changes, CancellationToken ct)
        => handler(querySession, changes, ct);
}
