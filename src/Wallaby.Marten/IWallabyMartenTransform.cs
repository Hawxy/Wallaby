using Marten;
using Wallaby.Abstractions;

namespace Wallaby.Marten;

/// <summary>
/// Marten-flavored transform: given a batch of changes for one document type plus a leased query
/// session, produce the output document per source key. Sessions come from the store's
/// <c>QuerySession()</c> — tenant-scoped when the mapping declares <c>ScopedByTenant()</c> and
/// <c>UseTenantSessions()</c> is registered.
/// </summary>
/// <typeparam name="TEntity">The mapped document type for the source table.</typeparam>
public interface IWallabyMartenTransform<TEntity>
    where TEntity : class
{
    /// <summary>
    /// Produce a <see cref="WallabyDocument"/> per source key. Omit a key from the result (or map it to
    /// <c>null</c>) to emit a deletion for that key at the sink.
    /// </summary>
    /// <param name="querySession">The leased Marten query session for enrichment lookups.</param>
    /// <param name="changes">The batch of insert/update/read changes for <typeparamref name="TEntity"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        IQuerySession querySession,
        IReadOnlyList<ChangeEvent<TEntity>> changes,
        CancellationToken ct);
}
