using System.Linq.Expressions;
using EFCore.CDC.Abstractions;
using EFCore.CDC.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.CDC.Internal.SelfConfig;

/// <summary>
/// Resolves a navigation expression declared via <c>DependsOn(...)</c> into a
/// <see cref="DependencyResolution"/> — the dependent <see cref="IEntityType"/> whose table needs to be
/// captured and the column lookup that turns its row changes into primary-key parameters for fan-out.
/// Three navigation shapes are supported (each reduces to "find the FK and the row that holds it"):
/// reference to a principal (e.g. <c>Product.Category</c>), collection to a dependent (e.g.
/// <c>Product.ProductLabels</c> or <c>Category.Products</c>), and skip-navigation (e.g.
/// <c>Product.Tags</c> backed by an implicit join entity).
/// </summary>
internal static class DependencyAnalyzer
{
    public static DependencyResolution Analyze(IEntityType primary, LambdaExpression navigation)
    {
        ArgumentNullException.ThrowIfNull(primary);
        ArgumentNullException.ThrowIfNull(navigation);

        var memberName = ExtractMemberName(navigation, primary.ClrType);

        if (primary.FindNavigation(memberName) is { } nav)
        {
            return ResolveReference(primary, nav);
        }

        if (primary.FindSkipNavigation(memberName) is { } skip)
        {
            return ResolveSkip(primary, skip);
        }

        throw new CdcConfigurationException(
            $"'{primary.ClrType.FullName}.{memberName}' is not an EF Core navigation. " +
            $"DependsOn must point at a reference, collection, or skip-navigation property.");
    }

    private static string ExtractMemberName(LambdaExpression expression, Type primaryClrType)
    {
        var body = expression.Body;
        while (body is UnaryExpression unary
               && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            body = unary.Operand;
        }

        if (body is not MemberExpression member)
        {
            throw new CdcConfigurationException(
                $"DependsOn expression on '{primaryClrType.FullName}' must be a single navigation property access " +
                $"(e.g. p => p.Category), got: {expression}.");
        }

        return member.Member.Name;
    }

    private static DependencyResolution ResolveReference(IEntityType primary, INavigation nav)
    {
        // The FK has two sides. Whichever side is NOT the primary mapping is the "dependent table" for
        // fan-out purposes: a row change there must re-read the primary.
        var fk = nav.ForeignKey;

        IEntityType dependentEntityType;          // its row changes trigger fan-out
        IReadOnlyList<IProperty> dependentCols;   // FK columns read from the changed row
        IReadOnlyList<IProperty> primaryCols;     // matching columns on the primary table

        if (nav.IsOnDependent)
        {
            // Primary holds the FK (e.g. Product.CategoryId) and the nav points at the principal
            // (Category). Fan-out triggers on Category row changes.
            dependentEntityType = fk.PrincipalEntityType;
            dependentCols = fk.PrincipalKey.Properties; // e.g. Category.Id
            primaryCols = fk.Properties;                // e.g. Product.CategoryId
        }
        else
        {
            // The nav points at the FK-bearing side (a collection on the principal, e.g.
            // Product.ProductLabels or Category.Products). Fan-out triggers on the dependent table.
            dependentEntityType = fk.DeclaringEntityType;
            dependentCols = fk.Properties;              // e.g. ProductLabel.ProductId
            primaryCols = fk.PrincipalKey.Properties;   // e.g. Product.Id
        }

        return BuildResolution(primary, dependentEntityType, dependentCols, primaryCols, nav.Name);
    }

    private static DependencyResolution ResolveSkip(IEntityType primary, ISkipNavigation skip)
    {
        // skip.ForeignKey points from the implicit join entity at the declaring primary
        // (e.g. ProductLabels.ProductId → Product.Id). The join entity's table is the dependent.
        var fk = skip.ForeignKey;
        return BuildResolution(
            primary,
            dependentEntityType: skip.JoinEntityType,
            dependentCols: fk.Properties,
            primaryCols: fk.PrincipalKey.Properties,
            navigationName: skip.Name);
    }

    private static DependencyResolution BuildResolution(
        IEntityType primary,
        IEntityType dependentEntityType,
        IReadOnlyList<IProperty> dependentCols,
        IReadOnlyList<IProperty> primaryCols,
        string navigationName)
    {
        if (dependentCols.Count != primaryCols.Count)
        {
            throw new CdcConfigurationException(
                $"Foreign key for '{primary.ClrType.Name}.{navigationName}' has mismatched column counts; cannot fan out.");
        }

        var lookup = new List<DependentLookupColumn>(dependentCols.Count);
        for (var i = 0; i < dependentCols.Count; i++)
        {
            lookup.Add(new DependentLookupColumn(
                DependentColumn: GetColumnName(dependentCols[i], dependentEntityType),
                PrimaryColumn: GetColumnName(primaryCols[i], primary)));
        }

        return new DependencyResolution(dependentEntityType, lookup);
    }

    private static string GetColumnName(IProperty property, IEntityType entityType)
    {
        var storeObject = StoreObjectIdentifier.Table(entityType.GetTableName()!, entityType.GetSchema());
        return property.GetColumnName(storeObject)
               ?? throw new CdcConfigurationException(
                   $"Property '{entityType.ClrType.Name}.{property.Name}' has no column on table " +
                   $"'{entityType.GetSchema()}.{entityType.GetTableName()}'.");
    }
}

internal sealed record DependencyResolution(IEntityType DependentEntityType, IReadOnlyList<DependentLookupColumn> Lookup);
