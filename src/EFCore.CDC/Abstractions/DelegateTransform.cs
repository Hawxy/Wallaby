using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.Abstractions;

/// <summary>
/// Adapts a lambda to <see cref="ICdcTransform{TEntity,TDocument}"/> for the trivial cases that don't
/// warrant a dedicated class (e.g. projecting straight from the change with no enrichment).
/// </summary>
public sealed class DelegateTransform<TEntity, TDocument>(
    Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, TDocument?>>> handler)
    : ICdcTransform<TEntity, TDocument>
    where TEntity : class
{
    /// <inheritdoc />
    public Task<IReadOnlyDictionary<DocumentKey, TDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<TEntity>> changes, CancellationToken ct)
        => handler(db, changes, ct);
}
