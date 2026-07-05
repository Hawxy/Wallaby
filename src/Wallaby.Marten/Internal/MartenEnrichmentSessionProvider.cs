using Marten;
using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>A leased Marten query session; disposing the lease disposes the session.</summary>
internal sealed class MartenEnrichmentSession(IQuerySession session) : IEnrichmentSession
{
    public object Session => session;

    public ValueTask DisposeAsync() => session.DisposeAsync();
}

/// <summary>Unscoped provider: leases a default-tenant query session from the store, ignoring the key.</summary>
internal sealed class MartenEnrichmentSessionProvider(IDocumentStore store) : IEnrichmentSessionProvider
{
    public bool IsScoped => false;

    public IEnrichmentSession Lease(object? scopeKey) => new MartenEnrichmentSession(store.QuerySession());
}

/// <summary>
/// Tenant-scoped provider (<c>UseTenantSessions</c>): leases a query session for the scope key's tenant,
/// so transforms on mappings that declare <c>ScopedByTenant()</c> query same-tenant data. A null key
/// (e.g. an unscoped mapping in the same batch) leases a default-tenant session. The router caches one
/// lease per distinct tenant per batch.
/// </summary>
internal sealed class MartenTenantSessionProvider(IDocumentStore store) : IEnrichmentSessionProvider
{
    public bool IsScoped => true;

    public IEnrichmentSession Lease(object? scopeKey)
        => new MartenEnrichmentSession(
            scopeKey?.ToString() is { } tenantId ? store.QuerySession(tenantId) : store.QuerySession());
}
