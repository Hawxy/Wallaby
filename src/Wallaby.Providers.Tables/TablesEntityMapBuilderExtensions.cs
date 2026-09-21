using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables;

/// <summary>Plain-table transform registration and column selection for entity mappings.</summary>
public static class TablesEntityMapBuilderExtensions
{
    /// <summary>Use a transform instance.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map, IWallabyTablesTransform<TEntity> transform)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(transform);
        return map.UsingTransformInvoker(
            _ => new TablesTransformInvoker<TEntity>(transform), TablesWallabyBuilderExtensions.ProviderName);
    }

    /// <summary>Use a transform type resolved (or constructed) from the container.</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TTransform>(
        this EntityMapBuilder<TEntity> map)
        where TEntity : class
        where TTransform : class, IWallabyTablesTransform<TEntity>
        => map.UsingTransformInvoker(
            sp => new TablesTransformInvoker<TEntity>(ActivatorUtilities.GetServiceOrCreateInstance<TTransform>(sp)),
            TablesWallabyBuilderExtensions.ProviderName);

    /// <summary>Use an inline transform lambda (the trivial, no-class case).</summary>
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity>(
        this EntityMapBuilder<TEntity> map,
        Func<NpgsqlDataSource, IReadOnlyList<ChangeEvent<TEntity>>, CancellationToken, Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>> handler)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        return map.UsingTransformInvoker(
            _ => new TablesTransformInvoker<TEntity>(new DelegateTransform<TEntity>(handler)),
            TablesWallabyBuilderExtensions.ProviderName);
    }

    /// <summary>
    /// Declare that this mapping's transform consumes only the named properties. The entity's captured
    /// column set is the union of its mappings' selections plus the key (always captured); a mapping
    /// without a selection keeps the entity at consume-all. Unselected columns never leave the server:
    /// they are omitted from the publication column list, materialization and backfill, so the entity
    /// keeps their default values and <c>ChangeEvent.Record</c> omits them. Repeated calls accumulate.
    /// </summary>
    public static EntityMapBuilder<TEntity> Consumes<TEntity>(
        this EntityMapBuilder<TEntity> map, params Expression<Func<TEntity, object?>>[] properties)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Include, MemberNames.FromExpressions(properties, nameof(Consumes)));

    /// <summary>
    /// Declare that this mapping's transform consumes everything except the named properties. Intended
    /// for large TOAST-prone columns no transform reads: under <c>REPLICA IDENTITY DEFAULT</c> an unchanged
    /// TOASTed value is not carried in the change. Key properties cannot be excluded. The exclusion holds
    /// only while every mapping of the entity declares a selection omitting the property.
    /// </summary>
    public static EntityMapBuilder<TEntity> ConsumesAllExcept<TEntity>(
        this EntityMapBuilder<TEntity> map, params Expression<Func<TEntity, object?>>[] properties)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Exclude, MemberNames.FromExpressions(properties, nameof(ConsumesAllExcept)));

    /// <summary>
    /// <see cref="Consumes{TEntity}(EntityMapBuilder{TEntity}, Expression{Func{TEntity, object?}}[])"/> by
    /// property name, for members a lambda cannot name. Names are validated at startup.
    /// </summary>
    public static EntityMapBuilder<TEntity> Consumes<TEntity>(
        this EntityMapBuilder<TEntity> map, params string[] propertyNames)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Include, MemberNames.Validated<TEntity>(propertyNames, nameof(Consumes)));

    /// <summary>
    /// <see cref="ConsumesAllExcept{TEntity}(EntityMapBuilder{TEntity}, Expression{Func{TEntity, object?}}[])"/>
    /// by property name, for members a lambda cannot name. Names are validated at startup.
    /// </summary>
    public static EntityMapBuilder<TEntity> ConsumesAllExcept<TEntity>(
        this EntityMapBuilder<TEntity> map, params string[] propertyNames)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Exclude, MemberNames.Validated<TEntity>(propertyNames, nameof(ConsumesAllExcept)));
}
