using System.Diagnostics.CodeAnalysis;
using Marten;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.Marten.Internal;

namespace Wallaby.Providers.Marten;

/// <summary>Marten transform registration for entity mappings.</summary>
public static class MartenEntityMapBuilderExtensions
{
    /// <summary>Use a transform instance.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map, IWallabyMartenTransform<TEntity> transform)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(transform);
        return map.UsingTransformInvoker(
            _ => new MartenTransformInvoker<TEntity>(transform), MartenWallabyBuilderExtensions.ProviderName);
    }

    /// <summary>Use a transform type resolved (or constructed) from the container.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TTransform>(
        this EntityMapBuilder<TEntity> map)
        where TEntity : class
        where TTransform : class, IWallabyMartenTransform<TEntity>
        => map.UsingTransformInvoker(
            sp => new MartenTransformInvoker<TEntity>(ActivatorUtilities.GetServiceOrCreateInstance<TTransform>(sp)),
            MartenWallabyBuilderExtensions.ProviderName);

    /// <summary>Use an inline transform lambda (the trivial, no-class case).</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map,
        Func<IQuerySession, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        return map.UsingTransformInvoker(
            _ => new MartenTransformInvoker<TEntity>(new DelegateTransform<TEntity>(handler)),
            MartenWallabyBuilderExtensions.ProviderName);
    }

    /// <summary>
    /// Scope this mapping by the row's tenant id (conjoined tenancy): the transform batch is sub-grouped
    /// per tenant, <c>UseTenantSessions()</c> leases a same-tenant session for each group, and
    /// <c>ScopedDestination(...)</c> can route per tenant. The tenant id is read from the captured
    /// <c>tenant_id</c> column, so it is available on deletes too.
    /// </summary>
    public static EntityMapBuilder<TEntity> ScopedByTenant<TEntity>(this EntityMapBuilder<TEntity> map)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(map);
        return map.ScopedBy((ChangeEvent change) => change.Record.GetValueOrDefault("TenantId"));
    }
}
