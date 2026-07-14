using System.Linq.Expressions;
using System.Reflection;
using Wallaby.Providers.EntityFrameworkCore.Internal;

namespace Wallaby.Providers.EntityFrameworkCore;

/// <summary>Provider configuration supplied via <c>UseEntityFrameworkCore&lt;TContext&gt;(configure)</c>.</summary>
public sealed class EfCoreProviderOptions
{
    internal PropertyExclusions Exclusions { get; } = new();

    /// <summary>
    /// Drop a mapped property from capture: its column is skipped during materialization and never read
    /// during backfill, so the materialized entity keeps the property's default value and
    /// <c>ChangeEvent.Record</c> omits it. Intended for large TOAST-prone columns (long text, big jsonb)
    /// that no transform reads — under <c>REPLICA IDENTITY DEFAULT</c> an unchanged TOASTed value is not
    /// carried in the change, which otherwise fails the change. Primary-key properties and columns a
    /// <c>DependsOn(...)</c> lookup resolves through cannot be excluded.
    /// </summary>
    public EfCoreProviderOptions ExcludeProperty<TEntity>(Expression<Func<TEntity, object?>> property)
    {
        ArgumentNullException.ThrowIfNull(property);

        var body = property.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is not MemberExpression { Member: PropertyInfo info, Expression: ParameterExpression })
        {
            throw new WallabyConfigurationException(
                $"ExcludeProperty<{typeof(TEntity).Name}>(...) must select a property directly on the entity " +
                $"(e.g. e => e.Payload), got: {property.Body}.");
        }

        Exclusions.Add(typeof(TEntity), info.Name);
        return this;
    }
}
