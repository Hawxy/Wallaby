using System.Linq.Expressions;
using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby;
using Wallaby.Internal.SelfConfig;

namespace EFCore.CDC.UnitTests;

public class DependencyAnalyzerTests
{
    private static IEntityType EntityType<TEntity>()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();
        return ctx.Model.FindEntityType(typeof(TEntity))
               ?? throw new InvalidOperationException($"{typeof(TEntity).Name} not in model.");
    }

    [Test]
    public async Task Reference_navigation_resolves_to_principal_table_with_fk_lookup()
    {
        var resolution = DependencyAnalyzer.Analyze(
            EntityType<Product>(),
            (Expression<Func<Product, Category?>>)(p => p.Category));

        await Assert.That(resolution.DependentEntityType.ClrType).IsEqualTo(typeof(Category));
        await Assert.That(resolution.Lookup).HasCount(1);
        // Read Category.Id from the changed row; match against Product.CategoryId.
        await Assert.That(resolution.Lookup[0].DependentColumn).IsEqualTo("Id");
        await Assert.That(resolution.Lookup[0].PrimaryColumn).IsEqualTo("CategoryId");
    }

    [Test]
    public async Task Skip_navigation_resolves_to_join_table_with_fk_back_to_primary()
    {
        var resolution = DependencyAnalyzer.Analyze(
            EntityType<Product>(),
            (Expression<Func<Product, List<Label>>>)(p => p.Labels));

        // The implicit join table name was configured via .UsingEntity(j => j.ToTable("product_labels")).
        await Assert.That(resolution.DependentEntityType.GetTableName()).IsEqualTo("product_labels");
        await Assert.That(resolution.Lookup).HasCount(1);

        // Whatever EF Core named the FK column on the join, it must map back to Product.Id.
        await Assert.That(resolution.Lookup[0].PrimaryColumn).IsEqualTo("Id");
    }

    [Test]
    public async Task Collection_navigation_on_principal_side_resolves_to_dependent_table()
    {
        // Category has a collection of Products. Fan-out triggers when a Product row changes,
        // re-emitting its Category (e.g. for a "product count per category" projection).
        var resolution = DependencyAnalyzer.Analyze(
            EntityType<Category>(),
            (Expression<Func<Category, List<Product>>>)(c => c.Products));

        await Assert.That(resolution.DependentEntityType.ClrType).IsEqualTo(typeof(Product));
        await Assert.That(resolution.Lookup).HasCount(1);
        await Assert.That(resolution.Lookup[0].DependentColumn).IsEqualTo("CategoryId");
        await Assert.That(resolution.Lookup[0].PrimaryColumn).IsEqualTo("Id");
    }

    [Test]
    public async Task Non_navigation_property_is_rejected()
    {
        await Assert.That(() => DependencyAnalyzer.Analyze(
                EntityType<Product>(),
                (Expression<Func<Product, string>>)(p => p.Name)))
            .Throws<CdcConfigurationException>();
    }
}
