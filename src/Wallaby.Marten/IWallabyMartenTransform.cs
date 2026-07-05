using Wallaby.Abstractions;

namespace Wallaby.Marten;

/// <summary>
/// Marten-flavored transform: given a batch of changes for one document type plus a leased query
/// session, produce the output document per source key. The session parameter is typed
/// <see cref="object"/> in this preview; it becomes Marten's <c>IQuerySession</c> once the provider
/// takes a Marten dependency.
/// </summary>
/// <typeparam name="TEntity">The mapped document type for the source table.</typeparam>
public interface IWallabyMartenTransform<TEntity>
    where TEntity : class
{
    /// <summary>
    /// Produce a <see cref="WallabyDocument"/> per source key. Omit a key from the result (or map it to
    /// <c>null</c>) to emit a deletion for that key at the sink.
    /// </summary>
    /// <param name="querySession">The leased Marten query session (typed <c>IQuerySession</c> once the provider is functional).</param>
    /// <param name="changes">The batch of insert/update/read changes for <typeparamref name="TEntity"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        object querySession,
        IReadOnlyList<ChangeEvent<TEntity>> changes,
        CancellationToken ct);
}
