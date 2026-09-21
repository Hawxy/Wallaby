using Npgsql;
using Wallaby.Abstractions;

namespace Wallaby.Providers.Tables;

/// <summary>
/// Plain-table transform: given a batch of changes for one mapped type plus the provider's
/// <see cref="NpgsqlDataSource"/>, produce the output document per source key. Open a pooled connection
/// from the data source for any enrichment query (Dapper, raw Npgsql, anything that takes a connection).
/// </summary>
/// <typeparam name="TEntity">The mapped CLR type for the source table.</typeparam>
public interface IWallabyTablesTransform<TEntity>
    where TEntity : class
{
    /// <summary>
    /// Produce a <see cref="WallabyDocument"/> per source key. Omit a key from the result (or map it to
    /// <c>null</c>) to emit a deletion for that key at the sink.
    /// </summary>
    /// <param name="dataSource">The data source enrichment queries open connections from.</param>
    /// <param name="changes">The batch of insert/update/read changes for <typeparamref name="TEntity"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        NpgsqlDataSource dataSource,
        IReadOnlyList<ChangeEvent<TEntity>> changes,
        CancellationToken ct);
}
