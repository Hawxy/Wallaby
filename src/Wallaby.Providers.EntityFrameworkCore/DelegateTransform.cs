using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;

namespace Wallaby.Providers.EntityFrameworkCore;

/// <summary>
/// Adapts a lambda to <see cref="IWallabyEfTransform{TEntity}"/> for the trivial cases that don't
/// warrant a dedicated class (e.g. projecting straight from the change with no enrichment).
/// </summary>
public sealed class DelegateTransform<TEntity>(
    Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
    : IWallabyEfTransform<TEntity>
    where TEntity : class
{
    /// <inheritdoc />
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<TEntity>> changes, CancellationToken ct)
        => handler(db, changes, ct);
}
