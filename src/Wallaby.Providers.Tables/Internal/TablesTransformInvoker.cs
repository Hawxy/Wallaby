using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>
/// Wraps a strongly-typed <see cref="IWallabyTablesTransform{TEntity}"/> as an
/// <see cref="IWallabyTransformInvoker"/>, downcasting the leased session to the
/// <see cref="NpgsqlDataSource"/> the transform expects.
/// </summary>
internal sealed class TablesTransformInvoker<TEntity>(IWallabyTablesTransform<TEntity> transform)
    : IWallabyTransformInvoker
    where TEntity : class
{
    public Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> InvokeAsync(
        object session, IReadOnlyList<ChangeEvent> changes, CancellationToken ct)
        => transform.TransformAsync((NpgsqlDataSource)session, ChangeEventBatch.Cast<TEntity>(changes), ct);
}
