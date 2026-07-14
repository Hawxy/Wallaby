using System.Linq.Expressions;
using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Providers;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

public class PropertyExclusionTests
{
    private static PropertyExclusions Exclusions(Action<EfCoreProviderOptions> configure)
    {
        var options = new EfCoreProviderOptions();
        configure(options);
        return options.Exclusions;
    }

    private static RawColumn Col(string name, object? value) => new() { ColumnName = name, Value = value };

    private static RawColumn Toast(string name) => new() { ColumnName = name, Value = null, IsUnchangedToast = true };

    private static RawChange ProductInsert() => new()
    {
        RelationId = 1,
        Schema = "public",
        TableName = "products",
        Action = ChangeAction.Insert,
        NewValues =
        [
            Col("Id", 42),
            Col("Name", "Widget"),
            Col("Price", 9.99m),
            Col("product_sku", "W-1"),
            Col("Status", "Active"),
            Col("Tags", "[\"a\",\"b\"]"),
            Col("Description", "a widget"),
            Col("CategoryId", 7),
        ],
    };

    [Test]
    public async Task Excluded_property_is_skipped_on_insert()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(
            ctx.Model, Exclusions(o => o.ExcludeProperty<Product>(p => p.Description)));

        materializer.TryMaterialize(ProductInsert(), out var row);

        ((Product)row!.Entity!).Description.ShouldBe("");
        row.Record.ContainsKey(nameof(Product.Description)).ShouldBeFalse();
        row.Record[nameof(Product.Name)].ShouldBe("Widget");
    }

    [Test]
    public async Task An_unavailable_toasted_column_materializes_when_excluded()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(
            ctx.Model, Exclusions(o => o.ExcludeProperty<Product>(p => p.Description)));

        // REPLICA IDENTITY DEFAULT: no old tuple; the unchanged TOASTed Description would otherwise poison
        // the change.
        var change = ProductInsert() with
        {
            Action = ChangeAction.Update,
            NewValues =
            [
                Col("Id", 42), Col("Name", "Widget v2"), Col("Price", 9.99m), Col("product_sku", "W-1"),
                Col("Status", "Active"), Col("Tags", "[\"a\",\"b\"]"), Toast("Description"), Col("CategoryId", 7),
            ],
            OldValues = null,
        };

        var ok = materializer.TryMaterialize(change, out var row);

        ok.ShouldBeTrue();
        ((Product)row!.Entity!).Name.ShouldBe("Widget v2");
        row.Record.ContainsKey(nameof(Product.Description)).ShouldBeFalse();
    }

    [Test]
    public async Task Capture_model_omits_excluded_columns()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model,
            new CaptureSpec { DeclaredEntities = [typeof(Product)] },
            Exclusions(o => o.ExcludeProperty<Product>(p => p.Description)));

        var product = model.FindByClrType(typeof(Product))!;
        product.Columns.ShouldNotContain(c => c.PropertyName == nameof(Product.Description));
        product.Columns.ShouldContain(c => c.PropertyName == nameof(Product.Name));
    }

    [Test]
    public async Task Excluding_a_primary_key_property_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(
            ctx.Model, Exclusions(o => o.ExcludeProperty<Product>(p => p.Id)));

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Product)] }));

        ex.Message.ShouldContain("primary-key");
    }

    [Test]
    public async Task Excluding_a_navigation_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(
            ctx.Model, Exclusions(o => o.ExcludeProperty<Product>(p => p.Category)));

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Product)] }));

        ex.Message.ShouldContain("not a mapped scalar property");
    }

    [Test]
    public async Task Excluding_an_entity_outside_the_model_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(
            ctx.Model, Exclusions(o => o.ExcludeProperty<NotMapped>(n => n.Payload)));

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Product)] }));

        ex.Message.ShouldContain("not part of the DbContext model");
    }

    [Test]
    public async Task Excluding_a_dependency_lookup_column_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        Expression<Func<Product, Category?>> navigation = p => p.Category;
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Product)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>
            {
                [typeof(Product)] = [navigation],
            },
        };

        var ex = Should.Throw<WallabyConfigurationException>(() => EfCoreCaptureModelBuilder.Build(
            ctx.Model, spec, Exclusions(o => o.ExcludeProperty<Product>(p => p.CategoryId))));

        ex.Message.ShouldContain("CategoryId");
        ex.Message.ShouldContain("dependency-lookup");
    }

    [Test]
    public void ExcludeProperty_requires_a_direct_property_access()
    {
        var options = new EfCoreProviderOptions();

        Should.Throw<WallabyConfigurationException>(
            () => options.ExcludeProperty<Product>(p => p.Name.Length));
    }

    private sealed class NotMapped
    {
        public int Id { get; set; }
        public string Payload { get; set; } = "";
    }
}
