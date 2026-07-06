using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Providers;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// A leased enrichment <see cref="DbContext"/>, optionally with an owning DI scope. Disposing the lease disposes
/// the context and then the scope (when present) in the same lifetime, so any scoped services the context or a
/// consumer factory resolved are released with it. Disposing the context first is safe even when the scope also
/// owns it — <see cref="DbContext.DisposeAsync"/> is idempotent. When there is no scope (an
/// <c>IDbContextFactory</c> or a plain factory), only the context is disposed.
/// </summary>
internal sealed class DbContextEnrichmentSession(DbContext context, AsyncServiceScope? scope) : IEnrichmentSession
{
    public DbContext Context => context;

    public object Session => context;

    public async ValueTask DisposeAsync()
    {
        await context.DisposeAsync();
        if (scope is { } owned)
        {
            await owned.DisposeAsync();
        }
    }
}

/// <summary>Unscoped provider: leases the source context (factory- or scope-resolved), ignoring the key.</summary>
internal sealed class DbContextEnrichmentSessionProvider(Func<DbContextEnrichmentSession> lease) : IEnrichmentSessionProvider
{
    /// <summary>Convenience for callers that already have a plain context factory with no DI scope to dispose (tests, harness).</summary>
    public DbContextEnrichmentSessionProvider(Func<DbContext> contextFactory)
        : this(() => new DbContextEnrichmentSession(contextFactory(), scope: null)) { }

    public bool IsScoped => false;
    public IEnrichmentSession Lease(object? scopeKey) => lease();
}

/// <summary>
/// Scoped provider: builds a context from the scope key via a consumer-supplied factory. Each call opens a fresh
/// DI scope and hands the factory that scope's <see cref="IServiceProvider"/> (so it may resolve scoped services
/// such as <c>DbContextOptions</c>); the scope is owned by the returned lease and disposed together with the
/// context. The router caches one lease per distinct scope key per batch, so one scope is opened per key per batch.
/// </summary>
internal sealed class ScopedDbContextEnrichmentSessionProvider(
    Func<object?, IServiceProvider, DbContext> factory, IServiceProvider services) : IEnrichmentSessionProvider
{
    public bool IsScoped => true;

    public IEnrichmentSession Lease(object? scopeKey)
    {
        var scope = services.CreateAsyncScope();
        try
        {
            return new DbContextEnrichmentSession(factory(scopeKey, scope.ServiceProvider), scope);
        }
        catch
        {
            // The factory threw before a lease could take ownership of the scope — don't leak it.
            scope.Dispose();
            throw;
        }
    }
}
