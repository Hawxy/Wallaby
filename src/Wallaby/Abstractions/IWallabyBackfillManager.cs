namespace Wallaby.Abstractions;

/// <summary>
/// Consumer-facing service to inspect and trigger backfills at runtime (e.g. from an admin
/// endpoint). Requests are persisted, so they survive restarts and are executed by whichever
/// node currently holds leadership.
/// </summary>
public interface IWallabyBackfillManager
{
    /// <summary>Request a (re)backfill of <typeparamref name="TEntity"/>.</summary>
    Task RequestBackfillAsync<TEntity>(CancellationToken ct = default) where TEntity : class;

    /// <summary>
    /// Request a (re)backfill of <typeparamref name="TEntity"/>, optionally purging sink destinations
    /// first (see <see cref="ISinkPurger"/>) so the backfill converges them to exactly the current
    /// table contents.
    /// </summary>
    Task RequestBackfillAsync<TEntity>(bool purge, CancellationToken ct = default) where TEntity : class;

    /// <summary>Request a (re)backfill of the table mapped to <paramref name="entityClrType"/>.</summary>
    Task RequestBackfillAsync(Type entityClrType, CancellationToken ct = default);

    /// <summary>
    /// Request a (re)backfill of the table mapped to <paramref name="entityClrType"/>, optionally
    /// purging sink destinations first (see <see cref="ISinkPurger"/>) so the backfill converges them
    /// to exactly the current table contents.
    /// </summary>
    Task RequestBackfillAsync(Type entityClrType, bool purge, CancellationToken ct = default);

    /// <summary>
    /// Cancel a queued backfill request for <typeparamref name="TEntity"/> before the leader serves
    /// it, clearing any pending purge mark with it. Returns false when no request is queued. A
    /// backfill already running is not interrupted (though a re-run request queued behind it is
    /// withdrawn), and a request the leader has already begun serving proceeds.
    /// </summary>
    Task<bool> CancelBackfillAsync<TEntity>(CancellationToken ct = default) where TEntity : class;

    /// <summary>
    /// Cancel a queued backfill request for the table mapped to <paramref name="entityClrType"/>
    /// (see <see cref="CancelBackfillAsync{TEntity}"/>).
    /// </summary>
    Task<bool> CancelBackfillAsync(Type entityClrType, CancellationToken ct = default);

    /// <summary>
    /// Cancel a queued backfill request by schema-qualified table name (e.g. <c>public.orders</c>).
    /// Unlike the typed overloads the name is not validated against the model, so it can withdraw a
    /// request for a table Wallaby does not capture (e.g. a mistyped remote request).
    /// </summary>
    Task<bool> CancelBackfillAsync(string tableQualifiedName, CancellationToken ct = default);

    /// <summary>Get the current backfill state for every tracked table.</summary>
    Task<IReadOnlyList<BackfillState>> GetStatusAsync(CancellationToken ct = default);
}
