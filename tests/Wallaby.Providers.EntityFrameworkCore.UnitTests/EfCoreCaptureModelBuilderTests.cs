using System.Linq.Expressions;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal.SelfConfig;
using Wallaby.Providers;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.UnitTests;

public class EfCoreCaptureModelBuilderTests
{
    private static CaptureSpec Declared(params Type[] types) => new()
    {
        DeclaredEntities = types,
    };

    [Test]
    public async Task Declared_resolves_schema_table_and_columns()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Product)));

        var product = model.FindByClrType(typeof(Product));
        product.ShouldNotBeNull();
        product!.Schema.ShouldBe("public");
        product.TableName.ShouldBe("products");
        product.QualifiedName.ShouldBe("public.products");

        // Custom column name is honored.
        var sku = product.Columns.Single(c => c.PropertyName == nameof(Product.Sku));
        sku.ColumnName.ShouldBe("product_sku");
    }

    [Test]
    public async Task Declared_resolves_single_primary_key()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Product)));
        var product = model.FindByClrType(typeof(Product))!;

        product.PrimaryKey.Count.ShouldBe(1);
        product.PrimaryKey[0].PropertyName.ShouldBe(nameof(Product.Id));
        product.PrimaryKey[0].IsPrimaryKey.ShouldBeTrue();
    }

    [Test]
    public async Task Declared_resolves_composite_primary_key_in_order()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(OrderLine)));
        var line = model.FindByClrType(typeof(OrderLine))!;

        // Order matters for a composite key.
        var pkInOrder = string.Join(",", line.PrimaryKey.Select(c => c.PropertyName));
        pkInOrder.ShouldBe($"{nameof(OrderLine.OrderId)},{nameof(OrderLine.LineNumber)}");
    }

    [Test]
    public async Task Declared_resolves_non_default_schema()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Order)));
        var order = model.FindByClrType(typeof(Order))!;

        order.Schema.ShouldBe("sales");
        order.QualifiedName.ShouldBe("sales.orders");
    }

    [Test]
    public async Task Declared_only_includes_declared_tables()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Product)));

        model.Tables.Count.ShouldBe(1);
        model.FindByClrType(typeof(Customer)).ShouldBeNull();
    }

    [Test]
    public async Task An_empty_spec_builds_an_empty_model()
    {
        // With several providers registered, one may have no mapped entities; "no mappings at all" is
        // rejected once, at WallabyBuilder.Build().
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared());

        model.Tables.ShouldBeEmpty();
        model.DependentBindings.ShouldBeEmpty();
    }

    [Test]
    public async Task Declaring_unmapped_type_fails_fast()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        Should.Throw<WallabyConfigurationException>(() => { EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(EfCoreCaptureModelBuilderTests))); });
    }

    [Test]
    public async Task A_navigation_declared_by_several_mappings_binds_once()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        // Two sinks mapping Product each declare DependsOn(p => p.Category); the third expression is a
        // different navigation and must survive the dedupe.
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Product)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>
            {
                [typeof(Product)] =
                [
                    (Expression<Func<Product, Category?>>)(p => p.Category),
                    (Expression<Func<Product, Category?>>)(p => p.Category),
                    (Expression<Func<Product, List<Label>>>)(p => p.Labels),
                ],
            },
        };

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, spec);

        model.DependentBindings.Count.ShouldBe(2);
        model.DependentBindings.Count(b => b.DependentTable.EntityClrType == typeof(Category)).ShouldBe(1);
    }

    [Test]
    public async Task RequiresFullReplicaIdentity_flag_is_propagated()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var spec = new CaptureSpec
        {
            DeclaredEntities = new[] { typeof(Product) },
            RequiresFullReplicaIdentity = new HashSet<Type> { typeof(Product) },
        };

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, spec);

        model.FindByClrType(typeof(Product))!.RequiresFullReplicaIdentity.ShouldBeTrue();
    }
}
