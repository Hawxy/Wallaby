using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
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
    public static EntityMapBuilder<TEntity> UsingTransform<TEntity,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TTransform>(
        this EntityMapBuilder<TEntity> map)
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

    /// <summary>
    /// Declare that this mapping's transform consumes only the named properties. The entity's captured
    /// column set is the union of its mappings' selections, plus primary-key and <c>DependsOn(...)</c>
    /// lookup columns (always captured); a mapping without a selection keeps the entity at consume-all.
    /// Unselected columns never leave the server — they are omitted from the publication column list,
    /// materialization, and backfill — so the materialized entity keeps their default values and
    /// <c>ChangeEvent.Record</c> omits them (its indexer throws for unselected keys; an entity property
    /// read silently yields the default). Repeated calls accumulate.
    /// </summary>
    public static EntityMapBuilder<TEntity> Consumes<TEntity>(
        this EntityMapBuilder<TEntity> map, params Expression<Func<TEntity, object?>>[] properties)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Include, PropertyNames(properties, nameof(Consumes)));

    /// <summary>
    /// Declare that this mapping's transform consumes everything except the named properties. Intended
    /// for large TOAST-prone columns (long text, big jsonb) no transform reads — under
    /// <c>REPLICA IDENTITY DEFAULT</c> an unchanged TOASTed value is not carried in the change, which
    /// otherwise fails the change. Primary-key properties and columns a <c>DependsOn(...)</c> lookup
    /// resolves through cannot be excluded. The exclusion holds only while every mapping of the entity
    /// declares a selection omitting the property.
    /// </summary>
    public static EntityMapBuilder<TEntity> ConsumesAllExcept<TEntity>(
        this EntityMapBuilder<TEntity> map, params Expression<Func<TEntity, object?>>[] properties)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Exclude, PropertyNames(properties, nameof(ConsumesAllExcept)));

    /// <summary>
    /// <see cref="Consumes{TEntity}(EntityMapBuilder{TEntity}, Expression{Func{TEntity, object?}}[])"/>
    /// by EF model property name (not column name), for members a lambda cannot name: properties not
    /// visible from the calling assembly, shadow properties (readable via <c>ChangeEvent.Record</c>
    /// only), and individual owned or complex leaves as dotted paths (e.g. <c>"Address.City"</c>).
    /// Names are validated against the model at startup; the two overloads accumulate freely.
    /// </summary>
    public static EntityMapBuilder<TEntity> Consumes<TEntity>(
        this EntityMapBuilder<TEntity> map, params string[] propertyNames)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Include, ValidNames<TEntity>(propertyNames, nameof(Consumes)));

    /// <summary>
    /// <see cref="ConsumesAllExcept{TEntity}(EntityMapBuilder{TEntity}, Expression{Func{TEntity, object?}}[])"/>
    /// by EF model property name (not column name), for members a lambda cannot name: properties not
    /// visible from the calling assembly, shadow properties, and individual owned or complex leaves as
    /// dotted paths (e.g. <c>"Address.City"</c>). Names are validated against the model at startup;
    /// the two overloads accumulate freely.
    /// </summary>
    public static EntityMapBuilder<TEntity> ConsumesAllExcept<TEntity>(
        this EntityMapBuilder<TEntity> map, params string[] propertyNames)
        where TEntity : class
        => map.SelectColumns(ColumnSelectionMode.Exclude, ValidNames<TEntity>(propertyNames, nameof(ConsumesAllExcept)));

    private static string[] ValidNames<TEntity>(string[] propertyNames, string method)
    {
        ArgumentNullException.ThrowIfNull(propertyNames);
        foreach (var name in propertyNames)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new WallabyConfigurationException(
                    $"{method}<{typeof(TEntity).Name}>(...) property names must be non-empty.");
            }
        }
        return propertyNames;
    }

    private static string[] PropertyNames<TEntity>(
        Expression<Func<TEntity, object?>>[] properties, string method)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var names = new string[properties.Length];
        for (var i = 0; i < properties.Length; i++)
        {
            var body = properties[i].Body;
            while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            {
                body = unary.Operand;
            }

            if (body is not MemberExpression { Member: PropertyInfo info, Expression: ParameterExpression })
            {
                throw new WallabyConfigurationException(
                    $"{method}<{typeof(TEntity).Name}>(...) must select a property directly on the entity " +
                    $"(e.g. e => e.Payload), got: {properties[i].Body}.");
            }
            names[i] = info.Name;
        }
        return names;
    }
}
