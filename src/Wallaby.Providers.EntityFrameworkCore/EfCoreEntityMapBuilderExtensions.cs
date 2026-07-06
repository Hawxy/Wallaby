using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore.Internal;

namespace Wallaby.Providers.EntityFrameworkCore;

/// <summary>EF Core-typed entity-mapping extensions: transforms and dependent-table declarations.</summary>
public static class EfCoreEntityMapBuilderExtensions
{
    private const string ProviderName = "EntityFrameworkCore";

    /// <summary>Use a transform instance.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map, IWallabyEfTransform<TEntity> transform)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(transform);
        return map.UsingTransformInvoker(_ => new EfCoreTransformInvoker<TEntity>(transform), ProviderName);
    }

    /// <summary>Use a transform type resolved (or constructed) from the container.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity, TTransform>(this EntityMapBuilder<TEntity> map)
        where TEntity : class
        where TTransform : class, IWallabyEfTransform<TEntity>
        => map.UsingTransformInvoker(sp =>
            new EfCoreTransformInvoker<TEntity>(ActivatorUtilities.GetServiceOrCreateInstance<TTransform>(sp)), ProviderName);

    /// <summary>Use an inline transform lambda (the trivial, no-class case).</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map,
        Func<DbContext, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        return map.UsingTransformInvoker(
            _ => new EfCoreTransformInvoker<TEntity>(new DelegateTransform<TEntity>(handler)), ProviderName);
    }

    /// <summary>
    /// Declare that changes to the table behind <paramref name="navigation"/> should fan out and re-emit
    /// this entity. Use this when the transform reads data from related tables (a referenced principal,
    /// a many-to-many skip-navigation's join table, or an owned side table) — otherwise those changes
    /// would not reach the pipeline. The navigation expression is resolved against the EF Core model at
    /// startup; it must point at a single one-hop navigation (no chains, no method calls).
    /// </summary>
    public static EntityMapBuilder<TEntity> DependsOn<TEntity, TNav>(
        this EntityMapBuilder<TEntity> map, Expression<Func<TEntity, TNav>> navigation)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(navigation);
        return map.DependsOnNavigation(navigation);
    }
}
