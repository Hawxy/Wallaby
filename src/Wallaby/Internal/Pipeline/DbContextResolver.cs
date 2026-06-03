using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace Wallaby.Internal.Pipeline;

/// <summary>
/// Obtains the consumer's <see cref="DbContext"/> without forcing them to register an
/// <see cref="IDbContextFactory{TContext}"/>: it uses a registered factory when present (the recommended setup
/// for background services), and otherwise creates a DI scope and resolves the scoped context registered by the
/// ubiquitous <c>AddDbContext&lt;TContext&gt;()</c>. The generic methods are captured by
/// <c>CdcBuilder.UseContext&lt;TContext&gt;()</c> as the model accessor and the enrichment-context lease.
/// </summary>
internal static class DbContextResolver
{
    /// <summary>Read the EF Core model. Constructs one context (no query runs), reads <c>.Model</c>, disposes it.</summary>
    public static IModel ReadModel<TContext>(IServiceProvider services) where TContext : DbContext
    {
        var factory = services.GetService<IDbContextFactory<TContext>>();
        if (factory is not null)
        {
            using var context = factory.CreateDbContext();
            return context.Model;
        }

        using var scope = services.CreateScope();
        return scope.ServiceProvider.GetRequiredService<TContext>().Model;
    }

    /// <summary>
    /// Lease a context for enrichment. Factory-created contexts are disposed directly; scope-resolved contexts
    /// are owned by the returned lease's scope (disposing the lease disposes the scope and the context).
    /// </summary>
    public static EnrichmentContextLease Lease<TContext>(IServiceProvider services) where TContext : DbContext
    {
        var factory = services.GetService<IDbContextFactory<TContext>>();
        if (factory is not null)
        {
            return new EnrichmentContextLease(factory.CreateDbContext(), scope: null);
        }

        var scope = services.CreateAsyncScope();
        return new EnrichmentContextLease(scope.ServiceProvider.GetRequiredService<TContext>(), scope);
    }
}
