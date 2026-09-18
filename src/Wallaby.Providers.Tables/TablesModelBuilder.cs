using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables;

/// <summary>
/// Registers the POCOs the provider captures. Only registered types are handled, so a type that also
/// belongs to another provider's model is never claimed twice. Table, column and key metadata come from
/// <c>System.ComponentModel.DataAnnotations</c> attributes, the fluent <see cref="TableBuilder{TEntity}"/>
/// overrides, and the naming convention, in that order of precedence (fluent first).
/// </summary>
public sealed class TablesModelBuilder
{
    private readonly Dictionary<Type, TableRegistration.Source> _sources = new();
    private bool _snakeCase;
    private string _defaultSchema = "public";

    internal TablesModelBuilder()
    {
    }

    /// <summary>
    /// Derive unannotated table and column names in snake_case (<c>OrderLine</c> to <c>order_line</c>,
    /// <c>CustomerRef</c> to <c>customer_ref</c>). Off by default: names are used verbatim.
    /// </summary>
    public TablesModelBuilder UseSnakeCase()
    {
        _snakeCase = true;
        return this;
    }

    /// <summary>The schema for types whose table declares none. Defaults to <c>public</c>.</summary>
    public TablesModelBuilder DefaultSchema(string schema)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        _defaultSchema = schema;
        return this;
    }

    /// <summary>
    /// Register <typeparamref name="TEntity"/>. Public instance properties with a getter map to columns;
    /// the key is <c>HasKey(...)</c>, else <c>[Key]</c> members, else a property named <c>Id</c> or
    /// <c>{Type}Id</c>. The type needs a public parameterless constructor or a single constructor whose
    /// parameters match its properties (positional records).
    /// </summary>
    public TableBuilder<TEntity> Add<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicConstructors)] TEntity>()
        where TEntity : class
    {
        if (_sources.ContainsKey(typeof(TEntity)))
        {
            throw new WallabyConfigurationException($"'{typeof(TEntity).FullName}' is already registered with UseTables(...).");
        }

        var source = new TableRegistration.Source
        {
            ClrType = typeof(TEntity),
            Properties = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance),
            Constructors = typeof(TEntity).GetConstructors(BindingFlags.Public | BindingFlags.Instance),
        };
        _sources[typeof(TEntity)] = source;
        return new TableBuilder<TEntity>(source);
    }

    internal IReadOnlyDictionary<Type, TableRegistration> Build()
    {
        var registrations = new Dictionary<Type, TableRegistration>(_sources.Count);
        var tables = new Dictionary<QualifiedTable, Type>();
        foreach (var (type, source) in _sources)
        {
            var registration = TableRegistration.Build(source, _snakeCase, _defaultSchema);
            if (tables.TryGetValue(registration.QualifiedTable, out var other))
            {
                throw new WallabyConfigurationException(
                    $"'{type.FullName}' and '{other.FullName}' both map to table '{registration.Schema}.{registration.Table}'.");
            }
            tables[registration.QualifiedTable] = type;
            registrations[type] = registration;
        }
        return registrations;
    }
}
