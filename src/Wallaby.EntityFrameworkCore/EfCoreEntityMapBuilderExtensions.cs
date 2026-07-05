using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.EntityFrameworkCore.Internal;

namespace Wallaby.EntityFrameworkCore;

/// <summary>EF Core transform registration for entity mappings.</summary>
public static class EfCoreEntityMapBuilderExtensions
{
    private const string ProviderName = "EntityFrameworkCore";

    /// <summary>Use a transform instance.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map, IWallabyTransform<TEntity> transform)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(transform);
        return map.UsingTransformInvoker(_ => new EfCoreTransformInvoker<TEntity>(transform), ProviderName);
    }

    /// <summary>Use a transform type resolved (or constructed) from the container.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity, TTransform>(this EntityMapBuilder<TEntity> map)
        where TEntity : class
        where TTransform : class, IWallabyTransform<TEntity>
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
}
