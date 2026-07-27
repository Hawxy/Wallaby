using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// Builds the per-table plan <see cref="EntityMaterializer"/> executes: a tree of construction nodes
/// (the entity itself at the root, one node per same-table owned reference or complex property) over
/// a flat per-row value buffer with one slot per effective leaf property. A node constructs via the
/// EF constructor binding when every parameter is satisfiable from column values, falling back to a
/// parameterless constructor. For captured entity types an unconstructible node fails here, at
/// startup; on other types the member's node is dropped and it stays at its default.
/// </summary>
internal static class EntityPlanBuilder
{
    public static EntityPlan Build(
        IEntityType entityType, string table, IReadOnlySet<string>? consumedProperties, bool strict)
    {
        var storeObject = StoreObjectIdentifier.Table(table, entityType.GetSchema());
        var effective = EffectiveProperties.Resolve(entityType);
        var leaves = effective.Leaves;

        // One buffer slot per leaf; constructor parameters may draw on unconsumed leaves, which then
        // stay at default(T).
        var slotOfProperty = new Dictionary<IProperty, int>(leaves.Count);
        var leafColumn = new string?[leaves.Count];
        var leafConsumed = new bool[leaves.Count];
        for (var i = 0; i < leaves.Count; i++)
        {
            var leaf = leaves[i];
            slotOfProperty[leaf.Property] = i;
            leafColumn[i] = leaf.Property.GetColumnName(storeObject);
            leafConsumed[i] = leafColumn[i] is not null
                && (consumedProperties is null || consumedProperties.Contains(leaf.Path));
        }

        var leavesOfNode = Enumerable.Range(0, leaves.Count).ToLookup(i => leaves[i].InstanceIndex);
        var childrenOf = Enumerable.Range(0, effective.Instances.Count)
            .ToLookup(i => effective.Instances[i].ParentIndex);

        var root = BuildNode(instanceIndex: -1)!;

        var byColumn = new Dictionary<string, ColumnPlan>();
        var byProperty = new Dictionary<string, ColumnPlan>();
        CollectColumns(root);

        var primaryKey = entityType.FindPrimaryKey()!.Properties
            .Select(p => byProperty[p.Name])
            .ToList();

        return new EntityPlan
        {
            ClrType = entityType.ClrType,
            Root = root,
            ColumnsByName = byColumn,
            PrimaryKey = primaryKey,
            SlotCount = leaves.Count,
        };

        NodePlan? BuildNode(int instanceIndex)
        {
            var instance = instanceIndex < 0 ? null : effective.Instances[instanceIndex];

            var children = new List<NodePlan>();
            foreach (var childIndex in childrenOf[instanceIndex])
            {
                if (BuildNode(childIndex) is { } child)
                {
                    children.Add(child);
                }
            }

            var valueSlots = leavesOfNode[instanceIndex].Where(i => leafConsumed[i]).ToArray();
            if (instance is not null && valueSlots.Length == 0 && children.Count == 0)
            {
                return null; // nothing in the subtree is consumed: the member stays at its default
            }

            ITypeBase targetType = instance is null ? entityType : instance.TargetType;
            var failure = TryResolveConstruction(targetType, slotOfProperty, out var construct, out var ctorSlots);
            Action<object, object?>? attach = null;
            if (failure is null && instance is not null)
            {
                attach = CreateAssigner(instance.Member);
                if (attach is null)
                {
                    failure = "the member has no settable accessor or backing field";
                }
            }
            if (failure is not null)
            {
                if (strict)
                {
                    var member = instance is null
                        ? entityType.ClrType.Name
                        : $"{entityType.ClrType.Name}.{instance.Path}";
                    throw new WallabyConfigurationException(
                        $"Cannot materialize '{member}': {failure}. Give the type a constructor EF Core " +
                        "can bind to mapped properties (or settable properties and a parameterless " +
                        "constructor), or drop the member from capture via the mapping's column selection.");
                }
                if (instance is not null)
                {
                    return null;
                }
                // Uncaptured table whose root cannot be constructed: defer the failure to runtime.
                construct = _ => Activator.CreateInstance(entityType.ClrType)!;
                ctorSlots = [];
            }

            var ctorConsumed = ctorSlots!.ToHashSet();
            var assignments = new List<LeafAssignment>();
            foreach (var i in leavesOfNode[instanceIndex])
            {
                if (!leafConsumed[i] || ctorConsumed.Contains(i)) continue;
                var property = leaves[i].Property;
                if (CreateAssigner(property) is not { } set) continue; // shadow: record-only
                assignments.Add(new LeafAssignment(i, AcceptsNull(property.ClrType), set));
            }

            return new NodePlan
            {
                Construct = construct!,
                Assignments = assignments.ToArray(),
                ValueSlots = valueSlots,
                Children = children.ToArray(),
                IsOptional = instance?.IsOptional ?? false,
                Attach = attach,
            };
        }

        void CollectColumns(NodePlan node)
        {
            foreach (var slot in node.ValueSlots)
            {
                var leaf = leaves[slot];
                var plan = new ColumnPlan
                {
                    ColumnName = leafColumn[slot]!,
                    PropertyName = leaf.Path,
                    ClrType = leaf.Property.ClrType,
                    Converter = leaf.Property.GetValueConverter(),
                    Slot = slot,
                };
                byColumn[plan.ColumnName] = plan;
                byProperty[leaf.Path] = plan;
            }
            foreach (var child in node.Children)
            {
                CollectColumns(child);
            }
        }
    }

    // The EF constructor binding covers ctor-bound owned/complex types and record entities; a binding
    // is satisfiable when every parameter binds a captured mapped property. An unsatisfiable binding
    // falls back to a parameterless constructor when one exists (e.g. an entity whose bound
    // constructor injects services but also has a parameterless constructor).
    private static string? TryResolveConstruction(
        ITypeBase targetType, Dictionary<IProperty, int> slotOfProperty,
        out Func<object?[], object>? construct, out int[]? ctorSlots)
    {
        construct = null;
        ctorSlots = null;
        string? failure = null;
        if (targetType.ConstructorBinding is ConstructorBinding ctorBinding)
        {
            var slots = new int[ctorBinding.ParameterBindings.Count];
            for (var i = 0; i < slots.Length; i++)
            {
                if (ctorBinding.ParameterBindings[i] is not PropertyParameterBinding
                    { ConsumedProperties: [IProperty property] })
                {
                    failure = $"constructor parameter {i + 1} of '{targetType.ClrType.Name}' is not " +
                              "bound to a mapped property";
                    break;
                }
                if (!slotOfProperty.TryGetValue(property, out var slot))
                {
                    failure = $"constructor parameter '{property.Name}' of '{targetType.ClrType.Name}' " +
                              "is not backed by a captured column";
                    break;
                }
                slots[i] = slot;
            }
            if (failure is null)
            {
                construct = CtorInvoker(ctorBinding.Constructor, slots);
                ctorSlots = slots;
                return null;
            }
        }
        else if (targetType.ConstructorBinding is not null)
        {
            failure = $"'{targetType.ClrType.Name}' uses an unsupported instantiation binding " +
                      $"({targetType.ConstructorBinding.GetType().Name})";
        }

        var parameterless = targetType.ClrType.GetConstructor(
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null, types: Type.EmptyTypes, modifiers: null);
        if (parameterless is not null)
        {
            construct = CtorInvoker(parameterless, []);
            ctorSlots = [];
            return null;
        }
        return failure
            ?? $"'{targetType.ClrType.Name}' has no parameterless constructor and no EF constructor binding";
    }

    private static Func<object?[], object> CtorInvoker(ConstructorInfo ctor, int[] ctorSlots)
    {
        if (ctorSlots.Length == 0)
        {
            var factory = Expression.Lambda<Func<object>>(
                Expression.Convert(Expression.New(ctor), typeof(object))).Compile();
            return _ => factory();
        }
        return slots =>
        {
            var args = new object?[ctorSlots.Length];
            for (var i = 0; i < args.Length; i++)
            {
                // Reflection substitutes default(T) when a value-type parameter receives null
                // (a non-identity column on DELETE/REPLICA IDENTITY DEFAULT).
                args[i] = slots[ctorSlots[i]];
            }
            return ctor.Invoke(args);
        };
    }

    // PropertyInfo assignment mutates a boxed struct in place (EF's compiled setters unbox-copy),
    // which nested complex struct construction relies on. Property-bag properties (shared-type
    // entities, e.g. skip-navigation join tables) exist only through the indexer, which EF's
    // compiled setter handles; shadow properties have no CLR member at all.
    private static Action<object, object?>? CreateAssigner(IPropertyBase member)
    {
        if (!member.IsIndexerProperty() && member.PropertyInfo is { SetMethod: not null } info)
        {
            return (target, value) => info.SetValue(target, value);
        }
        if (member.PropertyInfo is null && member.FieldInfo is null)
        {
            return null; // shadow: no CLR member to assign
        }
#pragma warning disable EF1001
        var setter = ((IRuntimePropertyBase)member).GetSetter();
#pragma warning restore EF1001
        return (target, value) => setter.SetClrValue(target, value);
    }

    private static bool AcceptsNull(Type clrType)
        => !clrType.IsValueType || Nullable.GetUnderlyingType(clrType) is not null;
}

/// <summary>The cached materialization plan for one table.</summary>
internal sealed class EntityPlan
{
    public required Type ClrType { get; init; }

    /// <summary>The entity's construction tree; children are owned/complex member nodes.</summary>
    public required NodePlan Root { get; init; }

    /// <summary>Consumed leaf columns by column name; a dropped member's columns are absent.</summary>
    public required IReadOnlyDictionary<string, ColumnPlan> ColumnsByName { get; init; }

    public required IReadOnlyList<ColumnPlan> PrimaryKey { get; init; }

    /// <summary>Size of the per-row value buffer (one slot per effective leaf).</summary>
    public required int SlotCount { get; init; }
}

/// <summary>One instance to construct per materialization: the entity or an owned/complex member.</summary>
internal sealed class NodePlan
{
    /// <summary>Constructs the instance, drawing constructor arguments from the value buffer.</summary>
    public required Func<object?[], object> Construct { get; init; }

    /// <summary>Post-construction sets for consumed leaves the constructor does not consume.</summary>
    public required LeafAssignment[] Assignments { get; init; }

    /// <summary>Consumed own-leaf slots, for the optional all-null check.</summary>
    public required int[] ValueSlots { get; init; }

    public required NodePlan[] Children { get; init; }

    /// <summary>An optional member is left null when every slot in its subtree is null.</summary>
    public required bool IsOptional { get; init; }

    /// <summary>Assigns the constructed instance to its parent's member; null on the root.</summary>
    public required Action<object, object?>? Attach { get; init; }
}

/// <summary>A consumed leaf column: its buffer slot, record key, and value conversion.</summary>
internal sealed class ColumnPlan
{
    public required string ColumnName { get; init; }

    /// <summary>Dotted member path, e.g. <c>Name</c> or <c>Address.Street</c>; the record key.</summary>
    public required string PropertyName { get; init; }

    public required Type ClrType { get; init; }
    public required int Slot { get; init; }
    public ValueConverter? Converter { get; init; }
}

/// <summary>A post-construction member set from a value-buffer slot.</summary>
internal readonly record struct LeafAssignment(int Slot, bool AcceptsNull, Action<object, object?> Set);
