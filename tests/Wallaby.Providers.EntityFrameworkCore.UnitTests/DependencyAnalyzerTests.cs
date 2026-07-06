using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal.SelfConfig;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.UnitTests;

public class DependencyAnalyzerTests
{
    private static IEntityType EntityType<TEntity>()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return ctx.Model.FindEntityType(typeof(TEntity))
               ?? throw new InvalidOperationException($"{typeof(TEntity).Name} not in model.");
    }

    [Test]
    public void Reference_navigation_resolves_to_principal_table_with_fk_lookup()
    {
        var resolution = DependencyAnalyzer.Analyze(
            EntityType<Product>(),
            (Expression<Func<Product, Category?>>)(p => p.Category));

        resolution.DependentEntityType.ClrType.ShouldBe(typeof(Category));
        resolution.Lookup.Count.ShouldBe(1);
        // Read Category.Id from the changed row; match against Product.CategoryId.
        resolution.Lookup[0].DependentColumn.ShouldBe("Id");
        resolution.Lookup[0].PrimaryColumn.ShouldBe("CategoryId");
    }

    [Test]
    public void Skip_navigation_resolves_to_join_table_with_fk_back_to_primary()
    {
        var resolution = DependencyAnalyzer.Analyze(
            EntityType<Product>(),
            (Expression<Func<Product, List<Label>>>)(p => p.Labels));

        // The implicit join table name was configured via .UsingEntity(j => j.ToTable("product_labels")).
        resolution.DependentEntityType.GetTableName().ShouldBe("product_labels");
        resolution.Lookup.Count.ShouldBe(1);

        // Whatever EF Core named the FK column on the join, it must map back to Product.Id.
        resolution.Lookup[0].PrimaryColumn.ShouldBe("Id");
    }

    [Test]
    public void Collection_navigation_on_principal_side_resolves_to_dependent_table()
    {
        // Category has a collection of Products. Fan-out triggers when a Product row changes,
        // re-emitting its Category (e.g. for a "product count per category" projection).
        var resolution = DependencyAnalyzer.Analyze(
            EntityType<Category>(),
            (Expression<Func<Category, List<Product>>>)(c => c.Products));

        resolution.DependentEntityType.ClrType.ShouldBe(typeof(Product));
        resolution.Lookup.Count.ShouldBe(1);
        resolution.Lookup[0].DependentColumn.ShouldBe("CategoryId");
        resolution.Lookup[0].PrimaryColumn.ShouldBe("Id");
    }

    [Test]
    public void Non_navigation_property_is_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => DependencyAnalyzer.Analyze(
            EntityType<Product>(),
            (Expression<Func<Product, string>>)(p => p.Name)));
    }
}
