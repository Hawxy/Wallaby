using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>One mapped property: its column, CLR type, and how a value reaches the entity.</summary>
internal sealed class MemberPlan
{
    public required string PropertyName { get; init; }

    public required string ColumnName { get; init; }

    public required Type ClrType { get; init; }

    public required PropertyInfo Property { get; init; }

    /// <summary>True when the property has any setter (init-only included); false means record-only.</summary>
    public required bool CanSet { get; init; }

    /// <summary>False for a non-nullable value type: a null column value leaves the member at its default.</summary>
    public required bool AcceptsNull { get; init; }

    public required bool IsKey { get; init; }
}

/// <summary>The constructor the materializer calls and which member feeds each parameter.</summary>
internal sealed class ConstructorPlan
{
    public required ConstructorInfo Constructor { get; init; }

    /// <summary>Per parameter, the index into the registration's member list, or -1 for an unmapped property.</summary>
    public required int[] ParameterMembers { get; init; }

    public required Type[] ParameterTypes { get; init; }
}

/// <summary>
/// The product of <c>Add&lt;T&gt;()</c>: the table, every mapped member in declaration order, the key
/// members in key order, and the constructor plan. Built once at <c>UseTables</c> time so configuration
/// errors surface before the host starts.
/// </summary>
internal sealed class TableRegistration
{
    public required Type ClrType { get; init; }

    public required string Schema { get; init; }

    public required string Table { get; init; }

    public required IReadOnlyList<MemberPlan> Members { get; init; }

    public required IReadOnlyList<MemberPlan> Key { get; init; }

    public required ConstructorPlan Constructor { get; init; }

    public QualifiedTable QualifiedTable => new(Schema, Table);

    /// <summary>The per-type inputs <c>Add&lt;T&gt;()</c> captures (reflection happens there, under the DAM annotation).</summary>
    internal sealed class Source
    {
        public required Type ClrType { get; init; }

        public required PropertyInfo[] Properties { get; init; }

        public required ConstructorInfo[] Constructors { get; init; }

        public string? Table { get; set; }

        public string? Schema { get; set; }

        public List<string>? Key { get; set; }

        public Dictionary<string, string> ColumnNames { get; } = new(StringComparer.Ordinal);

        public HashSet<string> Ignored { get; } = new(StringComparer.Ordinal);
    }

    public static TableRegistration Build(Source source, bool snakeCase, string defaultSchema)
    {
        var typeName = source.ClrType.Name;
        var tableAttribute = source.ClrType.GetCustomAttribute<TableAttribute>();
        var table = source.Table ?? tableAttribute?.Name ?? Convention(typeName, snakeCase);
        var schema = source.Schema ?? tableAttribute?.Schema ?? defaultSchema;

        var members = new List<MemberPlan>();
        var keyAttributed = new List<(MemberPlan Member, int Order, int Index)>();
        var columnNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in source.Properties)
        {
            if (property.GetIndexParameters().Length > 0
                || property.GetMethod is not { IsPublic: true }
                || property.GetCustomAttribute<NotMappedAttribute>() is not null
                || source.Ignored.Contains(property.Name))
            {
                continue;
            }

            var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
            var columnName = source.ColumnNames.GetValueOrDefault(property.Name)
                ?? columnAttribute?.Name
                ?? Convention(property.Name, snakeCase);
            if (!ScalarTypes.IsSupported(property.PropertyType))
            {
                throw new WallabyConfigurationException(
                    $"'{typeName}.{property.Name}' has type '{property.PropertyType.Name}', which does not map to a " +
                    "single column. Mark it [NotMapped] or Ignore(...) it; JSON columns are not supported in this version.");
            }
            if (!columnNames.Add(columnName))
            {
                throw new WallabyConfigurationException(
                    $"'{typeName}' maps two properties to column '{columnName}'.");
            }

            var isKey = source.Key is null && property.GetCustomAttribute<KeyAttribute>() is not null;
            var member = new MemberPlan
            {
                PropertyName = property.Name,
                ColumnName = columnName,
                ClrType = property.PropertyType,
                Property = property,
                CanSet = property.SetMethod is not null,
                AcceptsNull = !property.PropertyType.IsValueType || Nullable.GetUnderlyingType(property.PropertyType) is not null,
                IsKey = isKey,
            };
            members.Add(member);
            if (isKey)
            {
                keyAttributed.Add((member, columnAttribute?.Order ?? int.MaxValue, members.Count - 1));
            }
        }

        var key = ResolveKey(source, members, keyAttributed, typeName);
        foreach (var keyMember in key)
        {
            if (!ScalarTypes.IsSupportedKey(keyMember.ClrType))
            {
                throw new WallabyConfigurationException(
                    $"Key property '{typeName}.{keyMember.PropertyName}' has type '{keyMember.ClrType.Name}', which " +
                    "cannot be used in a backfill cursor. Keys must be numbers, strings, Guids, dates, times or byte arrays.");
            }
        }

        return new TableRegistration
        {
            ClrType = source.ClrType,
            Schema = schema,
            Table = table,
            Members = members,
            Key = key,
            Constructor = ResolveConstructor(source, members, typeName),
        };
    }

    private static string Convention(string name, bool snakeCase)
        => snakeCase ? NameConventions.ToSnakeCase(name) : name;

    private static IReadOnlyList<MemberPlan> ResolveKey(
        Source source, List<MemberPlan> members, List<(MemberPlan Member, int Order, int Index)> keyAttributed, string typeName)
    {
        if (source.Key is { } explicitKey)
        {
            var key = new List<MemberPlan>(explicitKey.Count);
            foreach (var name in explicitKey)
            {
                var index = members.FindIndex(m => m.PropertyName == name);
                if (index < 0)
                {
                    throw new WallabyConfigurationException(
                        $"HasKey(...) names '{name}', which is not a mapped property of '{typeName}'.");
                }
                members[index] = With(members[index], isKey: true);
                key.Add(members[index]);
            }
            return key;
        }

        if (keyAttributed.Count > 0)
        {
            return keyAttributed.OrderBy(k => k.Order).ThenBy(k => k.Index).Select(k => k.Member).ToList();
        }

        var conventional = members.FindIndex(m => m.PropertyName.Equals("Id", StringComparison.OrdinalIgnoreCase));
        if (conventional < 0)
        {
            conventional = members.FindIndex(m => m.PropertyName.Equals(typeName + "Id", StringComparison.OrdinalIgnoreCase));
        }
        if (conventional < 0)
        {
            throw new WallabyConfigurationException(
                $"'{typeName}' has no key: mark the key property with [Key], call HasKey(...), or name it 'Id' or '{typeName}Id'.");
        }
        members[conventional] = With(members[conventional], isKey: true);
        return [members[conventional]];
    }

    private static MemberPlan With(MemberPlan member, bool isKey) => new()
    {
        PropertyName = member.PropertyName,
        ColumnName = member.ColumnName,
        ClrType = member.ClrType,
        Property = member.Property,
        CanSet = member.CanSet,
        AcceptsNull = member.AcceptsNull,
        IsKey = isKey,
    };

    // A public parameterless constructor wins; otherwise the single public constructor whose parameters
    // all name public properties (positional records). An ignored property's parameter receives the
    // default. Anything else is ambiguous.
    private static ConstructorPlan ResolveConstructor(Source source, List<MemberPlan> members, string typeName)
    {
        var constructors = source.Constructors.Where(c => c.IsPublic).ToList();
        if (constructors.FirstOrDefault(c => c.GetParameters().Length == 0) is { } parameterless)
        {
            return new ConstructorPlan { Constructor = parameterless, ParameterMembers = [], ParameterTypes = [] };
        }

        var candidates = new List<ConstructorPlan>();
        foreach (var constructor in constructors)
        {
            var parameters = constructor.GetParameters();
            var indices = new int[parameters.Length];
            var matched = true;
            for (var i = 0; i < parameters.Length; i++)
            {
                var name = parameters[i].Name;
                var index = members.FindIndex(m => m.PropertyName.Equals(name, StringComparison.OrdinalIgnoreCase));
                if (index >= 0)
                {
                    matched = parameters[i].ParameterType.IsAssignableFrom(members[index].ClrType);
                }
                else
                {
                    matched = source.Properties.Any(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                }
                if (!matched)
                {
                    break;
                }
                indices[i] = index;
            }
            if (matched)
            {
                candidates.Add(new ConstructorPlan
                {
                    Constructor = constructor,
                    ParameterMembers = indices,
                    ParameterTypes = [.. parameters.Select(p => p.ParameterType)],
                });
            }
        }

        return candidates.Count == 1
            ? candidates[0]
            : throw new WallabyConfigurationException(
                $"'{typeName}' needs a public parameterless constructor, or exactly one public constructor whose " +
                $"parameters match its public properties by name ({candidates.Count} match).");
    }
}
