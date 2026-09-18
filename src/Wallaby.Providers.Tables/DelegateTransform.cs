using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Providers.Tables;

/// <summary>
/// Adapts a lambda to <see cref="IWallabyTablesTransform{TEntity}"/> for the trivial cases that don't
/// warrant a dedicated class (e.g. projecting straight from the change with no enrichment).
/// </summary>
internal sealed class DelegateTransform<TEntity>(
    Func<NpgsqlDataSource, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
    : IWallabyTablesTransform<TEntity>
    where TEntity : class
{
    /// <inheritdoc />
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        NpgsqlDataSource dataSource, IReadOnlyList<ChangeEvent<TEntity>> changes, CancellationToken ct)
        => handler(dataSource, changes, ct);
}
