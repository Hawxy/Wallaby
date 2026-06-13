using Wallaby.Internal.SelfConfig;
using Wallaby.TestModel;

namespace Wallaby.UnitTests;

public class ModelToCdcModelTests
{
    private static CaptureSpec Declared(params Type[] types) => new()
    {
        CaptureAllMapped = false,
        DeclaredEntities = types,
    };

    [Test]
    public async Task Declared_resolves_schema_table_and_columns()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Product)));

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

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Product)));
        var product = model.FindByClrType(typeof(Product))!;

        product.PrimaryKey.Count.ShouldBe(1);
        product.PrimaryKey[0].PropertyName.ShouldBe(nameof(Product.Id));
        product.PrimaryKey[0].IsPrimaryKey.ShouldBeTrue();
    }

    [Test]
    public async Task Declared_resolves_composite_primary_key_in_order()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(OrderLine)));
        var line = model.FindByClrType(typeof(OrderLine))!;

        // Order matters for a composite key.
        var pkInOrder = string.Join(",", line.PrimaryKey.Select(c => c.PropertyName));
        pkInOrder.ShouldBe($"{nameof(OrderLine.OrderId)},{nameof(OrderLine.LineNumber)}");
    }

    [Test]
    public async Task Declared_resolves_non_default_schema()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Order)));
        var order = model.FindByClrType(typeof(Order))!;

        order.Schema.ShouldBe("sales");
        order.QualifiedName.ShouldBe("sales.orders");
    }

    [Test]
    public async Task Declared_only_includes_declared_tables()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Product)));

        model.Tables.Count.ShouldBe(1);
        model.FindByClrType(typeof(Customer)).ShouldBeNull();
    }

    [Test]
    public async Task CaptureAllMapped_includes_every_keyed_table()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, new CaptureSpec { CaptureAllMapped = true });

        model.Tables.Select(t => t.QualifiedName).OrderBy(n => n).ToList()
            .ShouldBe(new[]
            {
                "public.categories",
                "public.customers",
                "public.labels",
                "public.product_labels",
                "public.products",
                "sales.order_lines",
                "sales.orders",
            }, ignoreOrder: true);
    }

    [Test]
    public async Task No_declaration_fails_fast()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        Should.Throw<CdcConfigurationException>(() => { ModelToCdcModel.Build(ctx.Model, Declared()); });
    }

    [Test]
    public async Task Declaring_unmapped_type_fails_fast()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        Should.Throw<CdcConfigurationException>(() => { ModelToCdcModel.Build(ctx.Model, Declared(typeof(ModelToCdcModelTests))); });
    }

    [Test]
    public async Task RequiresFullReplicaIdentity_flag_is_propagated()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var spec = new CaptureSpec
        {
            CaptureAllMapped = false,
            DeclaredEntities = new[] { typeof(Product) },
            RequiresFullReplicaIdentity = new HashSet<Type> { typeof(Product) },
        };

        var model = ModelToCdcModel.Build(ctx.Model, spec);

        model.FindByClrType(typeof(Product))!.RequiresFullReplicaIdentity.ShouldBeTrue();
    }
}
