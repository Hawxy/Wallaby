using EFCore.CDC.Internal.SelfConfig;
using EFCore.CDC.Model;
using EFCore.CDC.TestModel;

namespace EFCore.CDC.UnitTests;

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
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Product)));

        var product = model.FindByClrType(typeof(Product));
        await Assert.That(product).IsNotNull();
        await Assert.That(product!.Schema).IsEqualTo("public");
        await Assert.That(product.TableName).IsEqualTo("products");
        await Assert.That(product.QualifiedName).IsEqualTo("public.products");

        // Custom column name is honored.
        var sku = product.Columns.Single(c => c.PropertyName == nameof(Product.Sku));
        await Assert.That(sku.ColumnName).IsEqualTo("product_sku");
    }

    [Test]
    public async Task Declared_resolves_single_primary_key()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Product)));
        var product = model.FindByClrType(typeof(Product))!;

        await Assert.That(product.PrimaryKey.Count).IsEqualTo(1);
        await Assert.That(product.PrimaryKey[0].PropertyName).IsEqualTo(nameof(Product.Id));
        await Assert.That(product.PrimaryKey[0].IsPrimaryKey).IsTrue();
    }

    [Test]
    public async Task Declared_resolves_composite_primary_key_in_order()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(OrderLine)));
        var line = model.FindByClrType(typeof(OrderLine))!;

        // Order matters for a composite key.
        var pkInOrder = string.Join(",", line.PrimaryKey.Select(c => c.PropertyName));
        await Assert.That(pkInOrder).IsEqualTo($"{nameof(OrderLine.OrderId)},{nameof(OrderLine.LineNumber)}");
    }

    [Test]
    public async Task Declared_resolves_non_default_schema()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Order)));
        var order = model.FindByClrType(typeof(Order))!;

        await Assert.That(order.Schema).IsEqualTo("sales");
        await Assert.That(order.QualifiedName).IsEqualTo("sales.orders");
    }

    [Test]
    public async Task Declared_only_includes_declared_tables()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, Declared(typeof(Product)));

        await Assert.That(model.Tables.Count).IsEqualTo(1);
        await Assert.That(model.FindByClrType(typeof(Customer))).IsNull();
    }

    [Test]
    public async Task CaptureAllMapped_includes_every_keyed_table()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = ModelToCdcModel.Build(ctx.Model, new CaptureSpec { CaptureAllMapped = true });

        await Assert.That(model.Tables.Select(t => t.QualifiedName).OrderBy(n => n).ToList())
            .IsEquivalentTo(new[]
            {
                "public.categories",
                "public.customers",
                "public.products",
                "sales.order_lines",
                "sales.orders",
            });
    }

    [Test]
    public async Task No_declaration_fails_fast()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        await Assert.That(() => { ModelToCdcModel.Build(ctx.Model, Declared()); })
            .Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task Declaring_unmapped_type_fails_fast()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        await Assert.That(() => { ModelToCdcModel.Build(ctx.Model, Declared(typeof(ModelToCdcModelTests))); })
            .Throws<CdcConfigurationException>();
    }

    [Test]
    public async Task RequiresFullReplicaIdentity_flag_is_propagated()
    {
        using var ctx = TestModelFactory.CreateModelOnlyContext();

        var spec = new CaptureSpec
        {
            CaptureAllMapped = false,
            DeclaredEntities = new[] { typeof(Product) },
            RequiresFullReplicaIdentity = new HashSet<Type> { typeof(Product) },
        };

        var model = ModelToCdcModel.Build(ctx.Model, spec);

        await Assert.That(model.FindByClrType(typeof(Product))!.RequiresFullReplicaIdentity).IsTrue();
    }
}
