using Microsoft.EntityFrameworkCore;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Providers;

namespace Wallaby.Providers.EntityFrameworkCore;

/// <summary>EF Core provider registration for the Wallaby builder.</summary>
public static class EfCoreWallabyBuilderExtensions
{
    /// <summary>The provider name this package registers under, for <c>Map&lt;T&gt;().FromProvider(...)</c>.</summary>
    public const string ProviderName = "EntityFrameworkCore";

    /// <summary>
    /// Drive capture from the EF Core model of <typeparamref name="TContext"/> and lease it for transform
    /// enrichment. Required whenever Wallaby streams (any sink)
    /// and to resolve <c>AddExternalSlot(...).ForEntity&lt;T&gt;()</c> table
    /// declarations. The consumer registers the context as usual — a scoped <c>AddDbContext&lt;TContext&gt;()</c>
    /// is sufficient (Wallaby uses an <see cref="IDbContextFactory{TContext}"/> if one is registered,
    /// otherwise a DI scope). Omit it entirely for a provision-only worker that declares external slots by
    /// table name only.
    /// </summary>
    public static WallabyBuilder UseEntityFrameworkCore<TContext>(this WallabyBuilder builder)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.UseProvider(new WallabyProviderRegistration
        {
            Name = ProviderName,
            ModelProvider = sp => new EfCoreModelProvider(DbContextResolver.ReadModel<TContext>(sp)),
            EnrichmentSessions = sp => new DbContextEnrichmentSessionProvider(() => DbContextResolver.Lease<TContext>(sp)),
        });
    }

    /// <summary>
    /// Build the enrichment <see cref="DbContext"/> handed to transforms from a row's scope key (e.g. tenant),
    /// e.g. by selecting a tenant connection string or a context carrying the tenant for global query filters.
    /// Used by mappings that declare <c>ScopedBy(...)</c>. Call after <c>UseEntityFrameworkCore&lt;TContext&gt;()</c>;
    /// only this provider's mappings are affected.
    /// </summary>
    public static WallabyBuilder UseScopedDbContext(
        this WallabyBuilder builder, Func<object?, IServiceProvider, DbContext> factory)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        return builder.UseScopedEnrichmentSessions(
            ProviderName, sp => new ScopedDbContextEnrichmentSessionProvider(factory, sp));
    }
}
