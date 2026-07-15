using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Providers;

namespace Wallaby.Providers.EntityFrameworkCore.Internal;

/// <summary>
/// Computes each entity's effective consumed-property set from its mappings' column selections
/// (<c>Consumes</c>/<c>ConsumesAllExcept</c>): the union of the selections, plus primary-key and
/// <c>DependsOn(...)</c> lookup properties, which are always captured. Entities without selections are
/// absent from the result (consume-all). Validates every selected name against the model.
/// </summary>
internal static class ColumnConsumptionResolver
{
    public static IReadOnlyDictionary<Type, IReadOnlySet<string>> Resolve(IModel model, CaptureSpec spec)
    {
        var resolved = new Dictionary<Type, IReadOnlySet<string>>();
        if (spec.DeclaredColumnSelections.Count == 0)
        {
            return resolved;
        }

        var lookupProperties = ResolveLookupProperties(model, spec);
        foreach (var (clrType, selections) in spec.DeclaredColumnSelections)
        {
            var entityType = model.FindEntityType(clrType)
                ?? throw new WallabyConfigurationException(
                    $"A column selection was declared for '{clrType.Name}', which is not part of the DbContext model.");

            resolved[clrType] = ResolveEntity(
                clrType.Name,
                selections,
                mappedProperties: entityType.GetProperties().Select(p => p.Name).ToHashSet(StringComparer.Ordinal),
                primaryKeyProperties: entityType.FindPrimaryKey()?.Properties.Select(p => p.Name)
                    .ToHashSet(StringComparer.Ordinal) ?? [],
                lookupProperties: lookupProperties.GetValueOrDefault(clrType) ?? []);
        }
        return resolved;
    }

    /// <summary>The pure union/validation core (see the class doc for the rules).</summary>
    internal static IReadOnlySet<string> ResolveEntity(
        string entityName,
        IReadOnlyList<ColumnSelection> selections,
        IReadOnlySet<string> mappedProperties,
        IReadOnlySet<string> primaryKeyProperties,
        IReadOnlySet<string> lookupProperties)
    {
        var effective = new HashSet<string>(primaryKeyProperties, StringComparer.Ordinal);
        effective.UnionWith(lookupProperties);

        foreach (var selection in selections)
        {
            var method = selection.Mode == ColumnSelectionMode.Include ? "Consumes" : "ConsumesAllExcept";
            foreach (var name in selection.PropertyNames)
            {
                if (!mappedProperties.Contains(name))
                {
                    throw new WallabyConfigurationException(
                        $"{method}<{entityName}>(e => e.{name}): '{name}' is not a mapped scalar property " +
                        "of the entity (navigations cannot be selected).");
                }
                if (selection.Mode == ColumnSelectionMode.Exclude && primaryKeyProperties.Contains(name))
                {
                    throw new WallabyConfigurationException(
                        $"{method}<{entityName}>(e => e.{name}): a primary-key property cannot be excluded.");
                }
                if (selection.Mode == ColumnSelectionMode.Exclude && lookupProperties.Contains(name))
                {
                    throw new WallabyConfigurationException(
                        $"{method}<{entityName}>(e => e.{name}): a DependsOn(...) lookup resolves through " +
                        $"'{name}'; a dependency-lookup column cannot be excluded.");
                }
            }

            if (selection.Mode == ColumnSelectionMode.Include)
            {
                effective.UnionWith(selection.PropertyNames);
            }
            else
            {
                effective.UnionWith(mappedProperties.Except(selection.PropertyNames));
            }
        }
        return effective;
    }

    // DependsOn fan-out reads its lookup keys from captured columns on both sides of the navigation, so
    // those properties are pinned into every effective set the way primary keys are.
    private static Dictionary<Type, HashSet<string>> ResolveLookupProperties(IModel model, CaptureSpec spec)
    {
        var byType = new Dictionary<Type, HashSet<string>>();
        foreach (var (clrType, expressions) in spec.DeclaredDependencies)
        {
            // A missing entity type fails later, in the model builder, with its own error.
            if (model.FindEntityType(clrType) is not { } entityType)
            {
                continue;
            }
            foreach (var expression in expressions)
            {
                var resolution = DependencyAnalyzer.Analyze(entityType, expression);
                foreach (var lookup in resolution.Lookup)
                {
                    AddByColumnName(byType, entityType, lookup.PrimaryColumn);
                    AddByColumnName(byType, resolution.DependentEntityType, lookup.DependentColumn);
                }
            }
        }
        return byType;
    }

    private static void AddByColumnName(
        Dictionary<Type, HashSet<string>> byType, IEntityType entityType, string columnName)
    {
        var storeObject = StoreObjectIdentifier.Table(entityType.GetTableName()!, entityType.GetSchema());
        foreach (var property in entityType.GetProperties())
        {
            if (property.GetColumnName(storeObject) != columnName)
            {
                continue;
            }
            if (!byType.TryGetValue(entityType.ClrType, out var names))
            {
                byType[entityType.ClrType] = names = new HashSet<string>(StringComparer.Ordinal);
            }
            names.Add(property.Name);
            return;
        }
    }
}
