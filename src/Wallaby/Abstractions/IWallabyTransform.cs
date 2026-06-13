using Microsoft.EntityFrameworkCore;

namespace Wallaby.Abstractions;

/// <summary>
/// The single interface for all enrichment/transformation. Given a batch of changes for one
/// entity type plus a scoped <see cref="DbContext"/>, produce the output document per source key.
/// </summary>
/// <remarks>
/// It is batch-invoked so a backfill chunk or a transaction batch resolves many keys
/// in a single round-trip. 
/// <para>
/// Only insert/update/read changes are passed here. Deletes are handled by the engine, which
/// removes the document by key (the row is already gone), so a transform never sees a delete.
/// </para>
/// </remarks>
/// <typeparam name="TEntity">The mapped entity type for the source table.</typeparam>
public interface IWallabyTransform<TEntity>
    where TEntity : class
{
    /// <summary>
    /// Produce a <see cref="WallabyDocument"/> per source key. Omit a key from the result (or map it to
    /// <c>null</c>) to emit a deletion for that key at the sink.
    /// </summary>
    /// <param name="db">A scoped <see cref="DbContext"/> usable for enrichment queries.</param>
    /// <param name="changes">The batch of insert/update/read changes for <typeparamref name="TEntity"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        DbContext db,
        IReadOnlyList<ChangeEvent<TEntity>> changes,
        CancellationToken ct);
}
