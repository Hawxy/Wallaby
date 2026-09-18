using System.Linq.Expressions;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables;

/// <summary>
/// Fluent overrides for one registered type. Each call wins over the corresponding attribute
/// (<c>[Table]</c>, <c>[Column]</c>, <c>[Key]</c>, <c>[NotMapped]</c>) and the naming convention.
/// </summary>
public sealed class TableBuilder<TEntity>
    where TEntity : class
{
    private readonly TableRegistration.Source _source;

    internal TableBuilder(TableRegistration.Source source)
    {
        _source = source;
    }

    /// <summary>Map to <paramref name="table"/>, in <paramref name="schema"/> when given, else the default schema.</summary>
    public TableBuilder<TEntity> ToTable(string table, string? schema = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _source.Table = table;
        _source.Schema = schema;
        return this;
    }

    /// <summary>The primary key, in key order. Replaces <c>[Key]</c> attributes and the <c>Id</c> convention.</summary>
    public TableBuilder<TEntity> HasKey(params Expression<Func<TEntity, object?>>[] properties)
    {
        var names = MemberNames.FromExpressions(properties, nameof(HasKey));
        if (names.Length == 0)
        {
            throw new WallabyConfigurationException($"HasKey<{typeof(TEntity).Name}>(...) must name at least one property.");
        }
        _source.Key = [.. names];
        return this;
    }

    /// <summary>Map <paramref name="property"/> to <paramref name="columnName"/>.</summary>
    public TableBuilder<TEntity> Column(Expression<Func<TEntity, object?>> property, string columnName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName);
        _source.ColumnNames[MemberNames.FromExpression(property, nameof(Column))] = columnName;
        return this;
    }

    /// <summary>Leave <paramref name="property"/> out of the capture: it is neither read nor published.</summary>
    public TableBuilder<TEntity> Ignore(Expression<Func<TEntity, object?>> property)
    {
        _source.Ignored.Add(MemberNames.FromExpression(property, nameof(Ignore)));
        return this;
    }
}
