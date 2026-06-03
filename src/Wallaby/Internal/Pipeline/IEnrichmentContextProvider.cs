using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// A leased enrichment <see cref="DbContext"/>. When the context was resolved from a DI scope (the consumer
/// registered <c>AddDbContext</c> rather than a factory), disposing the lease disposes the owning scope — which
/// in turn disposes the context. When it came from an <c>IDbContextFactory</c> or a consumer-supplied factory,
/// there is no scope and the context is disposed directly.
/// </summary>
internal readonly struct EnrichmentContextLease(DbContext context, AsyncServiceScope? scope) : IAsyncDisposable
{
    public DbContext Context => context;

    public ValueTask DisposeAsync() => scope is { } owned ? owned.DisposeAsync() : context.DisposeAsync();
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

/// <summary>Scoped provider: builds a context from the scope key via a consumer-supplied factory (consumer-owned, no scope).</summary>
internal sealed class ScopedEnrichmentContextProvider(
    Func<object?, IServiceProvider, DbContext> factory, IServiceProvider services) : IEnrichmentContextProvider
{
    public bool IsScoped => true;
    public EnrichmentContextLease Create(object? scopeKey) => new(factory(scopeKey, services), scope: null);
}
