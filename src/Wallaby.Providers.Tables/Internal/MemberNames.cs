using System.Linq.Expressions;
using System.Reflection;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>Resolves <c>e =&gt; e.Property</c> lambdas to property names for the fluent surfaces.</summary>
internal static class MemberNames
{
    public static string[] FromExpressions<TEntity>(Expression<Func<TEntity, object?>>[] properties, string method)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var names = new string[properties.Length];
        for (var i = 0; i < properties.Length; i++)
        {
            names[i] = FromExpression(properties[i], method);
        }
        return names;
    }

    public static string FromExpression<TEntity>(Expression<Func<TEntity, object?>> property, string method)
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
                $"{method}<{typeof(TEntity).Name}>(...) must select a property directly on the entity " +
                $"(e.g. e => e.Name); got '{property}'.");
        }
        return info.Name;
    }

    public static string[] Validated<TEntity>(string[] propertyNames, string method)
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
}
