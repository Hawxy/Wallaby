using Marten;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;
using Wallaby.Providers.Marten.Internal;
using Wallaby.Providers;

namespace Wallaby.Providers.Marten;

/// <summary>Marten provider registration for the Wallaby builder.</summary>
public static class MartenWallabyBuilderExtensions
{
    internal const string ProviderName = "Marten";

    /// <summary>
    /// Drive capture from the Marten document store registered in the container (via <c>AddMarten</c>)
    /// and lease its query sessions for transform enrichment. Only documents registered with the store up
    /// front (<c>RegisterDocumentType</c>, <c>Schema.For&lt;T&gt;()</c>, …) are visible to capture —
    /// Marten's lazy first-use discovery happens too late for a capture model built at startup.
    /// </summary>
    public static WallabyBuilder UseMarten(this WallabyBuilder cdc)
        => cdc.UseMarten(sp => sp.GetRequiredService<IDocumentStore>());

    /// <summary>
    /// Drive capture from a Marten document store resolved by <paramref name="store"/> — for hosts with
    /// multiple stores or a store not registered as <see cref="IDocumentStore"/>.
    /// </summary>
    public static WallabyBuilder UseMarten(this WallabyBuilder cdc, Func<IServiceProvider, IDocumentStore> store)
    {
        ArgumentNullException.ThrowIfNull(cdc);
        ArgumentNullException.ThrowIfNull(store);
        return cdc.UseProvider(new WallabyProviderRegistration
        {
            Name = ProviderName,
            ModelProvider = sp => new MartenModelProvider(store(sp).Options),
            EnrichmentSessions = sp => new MartenEnrichmentSessionProvider(store(sp)),
        });
    }

    /// <summary>
    /// Lease tenant-scoped query sessions (<c>store.QuerySession(tenantId)</c>) for mappings that declare
    /// <c>ScopedByTenant()</c>/<c>ScopedBy(...)</c>, so transforms query same-tenant data. Call after
    /// <c>UseMarten()</c>; only the Marten provider's mappings are affected. Pass <paramref name="store"/>
    /// when the store isn't registered as <see cref="IDocumentStore"/>.
    /// </summary>
    public static WallabyBuilder UseTenantSessions(
        this WallabyBuilder cdc, Func<IServiceProvider, IDocumentStore>? store = null)
    {
        ArgumentNullException.ThrowIfNull(cdc);
        store ??= sp => sp.GetRequiredService<IDocumentStore>();
        return cdc.UseScopedEnrichmentSessions(ProviderName, sp => new MartenTenantSessionProvider(store(sp)));
    }
}
