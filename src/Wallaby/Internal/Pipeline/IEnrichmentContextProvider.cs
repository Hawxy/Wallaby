using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// A leased enrichment <see cref="DbContext"/>, optionally with an owning DI scope. Disposing the lease disposes
/// the context and then the scope (when present) in the same lifetime, so any scoped services the context or a
/// consumer factory resolved are released with it. Disposing the context first is safe even when the scope also
/// owns it — <see cref="DbContext.DisposeAsync"/> is idempotent. When there is no scope (an
/// <c>IDbContextFactory</c> or a plain factory), only the context is disposed.
/// </summary>
internal readonly struct EnrichmentContextLease(DbContext context, AsyncServiceScope? scope) : IAsyncDisposable
{
    public DbContext Context => context;

    public async ValueTask DisposeAsync()
    {
        await context.DisposeAsync();
        if (scope is { } owned)
        {
            await owned.DisposeAsync();
        }
    }
}

/// <summary>
/// Supplies the <see cref="DbContext"/> a transform uses for enrichment, optionally scoped by a per-row
/// key (e.g. tenant id). The router caches one lease per distinct scope key within a batch and disposes them
/// at the end of the batch.
/// </summary>
internal interface IEnrichmentContextProvider
{
    /// <summary>True when contexts differ by scope key (so the router caches per key rather than once per batch).</summary>
    bool IsScoped { get; }

    /// <summary>Lease a context for the given scope key (ignored when <see cref="IsScoped"/> is false).</summary>
    EnrichmentContextLease Create(object? scopeKey);
}

/// <summary>Unscoped provider: leases the source context (factory- or scope-resolved), ignoring the key.</summary>
internal sealed class DefaultEnrichmentContextProvider(Func<EnrichmentContextLease> lease) : IEnrichmentContextProvider
{
    /// <summary>Convenience for callers that already have a plain context factory with no DI scope to dispose (tests, harness).</summary>
    public DefaultEnrichmentContextProvider(Func<DbContext> contextFactory)
        : this(() => new EnrichmentContextLease(contextFactory(), scope: null)) { }

    public bool IsScoped => false;
    public EnrichmentContextLease Create(object? scopeKey) => lease();
}

/// <summary>
/// Scoped provider: builds a context from the scope key via a consumer-supplied factory. Each call opens a fresh
/// DI scope and hands the factory that scope's <see cref="IServiceProvider"/> (so it may resolve scoped services
/// such as <c>DbContextOptions</c>); the scope is owned by the returned lease and disposed together with the
/// context. The router caches one lease per distinct scope key per batch, so one scope is opened per key per batch.
/// </summary>
internal sealed class ScopedEnrichmentContextProvider(
    Func<object?, IServiceProvider, DbContext> factory, IServiceProvider services) : IEnrichmentContextProvider
{
    public bool IsScoped => true;

    public EnrichmentContextLease Create(object? scopeKey)
    {
        var scope = services.CreateAsyncScope();
        try
        {
            return new EnrichmentContextLease(factory(scopeKey, scope.ServiceProvider), scope);
        }
        catch
        {
            // The factory threw before a lease could take ownership of the scope — don't leak it.
            scope.Dispose();
            throw;
        }
    }
}
