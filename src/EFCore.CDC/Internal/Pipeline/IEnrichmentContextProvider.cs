using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.Internal.Pipeline;

/// <summary>
/// Supplies the <see cref="DbContext"/> a transform uses for enrichment, optionally scoped by a per-row
/// key (e.g. tenant id). The router caches one context per distinct scope key within a batch.
/// </summary>
internal interface IEnrichmentContextProvider
{
    /// <summary>True when contexts differ by scope key (so the router caches per key rather than once per batch).</summary>
    bool IsScoped { get; }

    /// <summary>Create a context for the given scope key (ignored when <see cref="IsScoped"/> is false).</summary>
    DbContext Create(object? scopeKey);
}

/// <summary>Unscoped provider: returns the source context from the consumer's factory, ignoring the key.</summary>
internal sealed class DefaultEnrichmentContextProvider(Func<DbContext> factory) : IEnrichmentContextProvider
{
    public bool IsScoped => false;
    public DbContext Create(object? scopeKey) => factory();
}

/// <summary>Scoped provider: builds a context from the scope key via a consumer-supplied factory.</summary>
internal sealed class ScopedEnrichmentContextProvider(
    Func<object?, IServiceProvider, DbContext> factory, IServiceProvider services) : IEnrichmentContextProvider
{
    public bool IsScoped => true;
    public DbContext Create(object? scopeKey) => factory(scopeKey, services);
}
