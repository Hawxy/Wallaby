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

    /// <summary>Get the current backfill state for every tracked table.</summary>
    Task<IReadOnlyList<BackfillState>> GetStatusAsync(CancellationToken ct = default);
}
