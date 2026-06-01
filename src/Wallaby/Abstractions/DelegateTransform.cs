using Microsoft.EntityFrameworkCore;

namespace Wallaby.Abstractions;

/// <summary>
/// Adapts a lambda to <see cref="ICdcTransform{TEntity}"/> for the trivial cases that don't
/// warrant a dedicated class (e.g. projecting straight from the change with no enrichment).
/// </summary>
public sealed class DelegateTransform<TEntity>(
    Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, CdcDocument?>>> handler)
    : ICdcTransform<TEntity>
    where TEntity : class
{
    /// <inheritdoc />
    public Task<IReadOnlyDictionary<DocumentKey, CdcDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<TEntity>> changes, CancellationToken ct)
        => handler(db, changes, ct);
}
