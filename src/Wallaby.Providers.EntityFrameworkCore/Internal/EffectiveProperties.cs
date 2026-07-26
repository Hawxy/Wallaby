using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// Expands an entity type's capturable surface: its own scalar properties plus, flattened in, the
/// properties of same-table owned reference navigations (recursively) and column-mapped complex
/// properties. Members whose data does not live on the entity's own rows (owned collections, owned
/// types in their own table, JSON-mapped members) are reported as uncapturable with a reason, so
/// callers can warn, honor an acknowledgment, or reject a column selection naming them. The capture
/// model builder, the materializer, and the column-consumption resolver all consume this expansion,
/// keeping the capture set, publication list, materializer plan, and backfill reads in lockstep.
/// </summary>
internal static class EffectiveProperties
{
    public static EffectivePropertySet Resolve(IEntityType entityType)
    {
        var storeObject = StoreObjectIdentifier.Table(entityType.GetTableName()!, entityType.GetSchema());
        var leaves = new List<EffectiveLeaf>();
        var instances = new List<EffectiveInstance>();
        var uncapturable = new List<UncapturableMember>();

        foreach (var property in entityType.GetProperties())
        {
            leaves.Add(new EffectiveLeaf { Property = property, Path = property.Name, InstanceIndex = -1 });
        }
        Expand(entityType, parentIndex: -1, pathPrefix: "");

        return new EffectivePropertySet { Leaves = leaves, Instances = instances, Uncapturable = uncapturable };

        void Expand(ITypeBase type, int parentIndex, string pathPrefix)
        {
            if (type is IEntityType owner)
            {
                foreach (var navigation in owner.GetNavigations())
                {
                    // Only ownership navigations declared on the owner side; regular navigations and the
                    // inverse (owned-to-owner) navigation carry no column data of their own.
                    if (!navigation.ForeignKey.IsOwnership || navigation.IsOnDependent)
                    {
                        continue;
                    }
                    ExpandOwnedNavigation(navigation, parentIndex, pathPrefix);
                }
            }

            foreach (var complexProperty in type.GetComplexProperties())
            {
                ExpandComplexProperty(complexProperty, parentIndex, pathPrefix);
            }
        }

        void ExpandOwnedNavigation(INavigation navigation, int parentIndex, string pathPrefix)
        {
            var path = pathPrefix + navigation.Name;
            var target = navigation.TargetEntityType;
            if (navigation.IsCollection)
            {
                uncapturable.Add(new UncapturableMember
                {
                    Name = path,
                    Reason = target.GetContainerColumnName() is { } jsonColumn
                        ? $"is an owned collection mapped to the JSON column '{jsonColumn}'"
                        : $"is an owned collection stored in table '{TableOf(target)}'",
                    HasSideTable = target.GetContainerColumnName() is null,
                });
                return;
            }
            if (target.GetContainerColumnName() is { } json)
            {
                uncapturable.Add(new UncapturableMember
                {
                    Name = path,
                    Reason = $"is an owned type mapped to the JSON column '{json}'",
                    HasSideTable = false,
                });
                return;
            }
            if (StoreObjectIdentifier.Create(target, StoreObjectType.Table) is not { } targetTable
                || !targetTable.Equals(storeObject))
            {
                uncapturable.Add(new UncapturableMember
                {
                    Name = path,
                    Reason = $"is an owned type stored in its own table '{TableOf(target)}'",
                    HasSideTable = true,
                });
                return;
            }

            var index = instances.Count;
            instances.Add(new EffectiveInstance
            {
                Member = navigation,
                TargetType = target,
                Path = path,
                ParentIndex = parentIndex,
                IsOptional = !navigation.ForeignKey.IsRequiredDependent,
            });
            foreach (var property in target.GetProperties())
            {
                // A same-table owned type's primary key is the shadow FK to its owner, mapped to the
                // owner's PK columns, which the root properties already capture.
                if (property.IsPrimaryKey())
                {
                    continue;
                }
                leaves.Add(new EffectiveLeaf
                {
                    Property = property, Path = $"{path}.{property.Name}", InstanceIndex = index,
                });
            }
            Expand(target, index, path + ".");
        }

        void ExpandComplexProperty(IComplexProperty complexProperty, int parentIndex, string pathPrefix)
        {
            var path = pathPrefix + complexProperty.Name;
            if (complexProperty.IsCollection)
            {
                uncapturable.Add(new UncapturableMember
                {
                    Name = path, Reason = "is a complex collection, which is not stored in table columns",
                    HasSideTable = false,
                });
                return;
            }
            if (complexProperty.ComplexType.GetContainerColumnName() is { } jsonColumn)
            {
                uncapturable.Add(new UncapturableMember
                {
                    Name = path, Reason = $"is a complex property mapped to the JSON column '{jsonColumn}'",
                    HasSideTable = false,
                });
                return;
            }

            var index = instances.Count;
            instances.Add(new EffectiveInstance
            {
                Member = complexProperty,
                TargetType = complexProperty.ComplexType,
                Path = path,
                ParentIndex = parentIndex,
                IsOptional = complexProperty.IsNullable,
            });
            foreach (var property in complexProperty.ComplexType.GetProperties())
            {
                leaves.Add(new EffectiveLeaf
                {
                    Property = property, Path = $"{path}.{property.Name}", InstanceIndex = index,
                });
            }
            Expand(complexProperty.ComplexType, index, path + ".");
        }

        static string TableOf(IEntityType entityType)
            => $"{entityType.GetSchema() ?? "public"}.{entityType.GetTableName()}";
    }
}

/// <summary>The expanded property surface of one entity type. See <see cref="EffectiveProperties"/>.</summary>
internal sealed class EffectivePropertySet
{
    /// <summary>Every property whose value is present on the entity's own rows, in model order.</summary>
    public required IReadOnlyList<EffectiveLeaf> Leaves { get; init; }

    /// <summary>Flattened owned/complex member instances, ordered parents-before-children.</summary>
    public required IReadOnlyList<EffectiveInstance> Instances { get; init; }

    /// <summary>Members whose data is not stored on the entity's rows.</summary>
    public required IReadOnlyList<UncapturableMember> Uncapturable { get; init; }

    /// <summary>The selection surface used by the column-consumption resolver.</summary>
    public EffectiveMembers ToMembers()
    {
        var leaves = Leaves.Select(l => l.Path).ToHashSet(StringComparer.Ordinal);
        var expandable = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        foreach (var instance in Instances)
        {
            if (instance.ParentIndex >= 0)
            {
                continue; // nested members are selected through their root
            }
            var prefix = instance.Path + ".";
            expandable[instance.Path] = Leaves
                .Where(l => l.Path.StartsWith(prefix, StringComparison.Ordinal))
                .Select(l => l.Path)
                .ToList();
        }
        var uncapturable = Uncapturable
            .Where(u => !u.Name.Contains('.'))
            .ToDictionary(u => u.Name, u => u.Reason, StringComparer.Ordinal);
        return new EffectiveMembers { Leaves = leaves, Expandable = expandable, Uncapturable = uncapturable };
    }
}

/// <summary>A property whose value is present on the entity's own rows.</summary>
internal sealed class EffectiveLeaf
{
    public required IProperty Property { get; init; }

    /// <summary>Dotted member path from the root entity, e.g. <c>Name</c> or <c>Address.Location.Lat</c>.</summary>
    public required string Path { get; init; }

    /// <summary>Index into <see cref="EffectivePropertySet.Instances"/>; -1 for root properties.</summary>
    public required int InstanceIndex { get; init; }
}

/// <summary>An owned or complex member instance flattened into the entity's table.</summary>
internal sealed class EffectiveInstance
{
    /// <summary>The <see cref="INavigation"/> or <see cref="IComplexProperty"/> on the parent.</summary>
    public required IPropertyBase Member { get; init; }

    /// <summary>The owned <see cref="IEntityType"/> or <see cref="IComplexType"/> being flattened.</summary>
    public required ITypeBase TargetType { get; init; }

    /// <summary>Dotted member path from the root entity, e.g. <c>Address</c> or <c>Address.Location</c>.</summary>
    public required string Path { get; init; }

    /// <summary>Index of the parent instance; -1 when the member sits on the root entity.</summary>
    public required int ParentIndex { get; init; }

    /// <summary>An optional member is left null when every leaf in its subtree is null.</summary>
    public required bool IsOptional { get; init; }
}

/// <summary>A member whose data is not stored on the entity's own rows.</summary>
internal sealed class UncapturableMember
{
    /// <summary>Dotted member path from the root entity (root-level for anything selectable).</summary>
    public required string Name { get; init; }

    /// <summary>Sentence fragment, e.g. "is an owned collection stored in table 'public.notes'".</summary>
    public required string Reason { get; init; }

    /// <summary>True when the data lives in a side table a <c>DependsOn(...)</c> declaration can watch.</summary>
    public required bool HasSideTable { get; init; }
}

/// <summary>The selection surface the column-consumption resolver validates names against.</summary>
internal sealed class EffectiveMembers
{
    /// <summary>Every capturable leaf path.</summary>
    public required IReadOnlySet<string> Leaves { get; init; }

    /// <summary>Root owned/complex member name to the leaf paths it expands to when selected as a unit.</summary>
    public required IReadOnlyDictionary<string, IReadOnlyList<string>> Expandable { get; init; }

    /// <summary>Root member name to the reason it cannot be captured with the entity.</summary>
    public required IReadOnlyDictionary<string, string> Uncapturable { get; init; }
}
