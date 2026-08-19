using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Model;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Providers;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

public class ColumnConsumptionTests
{
    private static CaptureSpec Spec<TEntity>(ColumnSelectionMode mode, params string[] names) => new()
    {
        DeclaredEntities = [typeof(Product)],
        DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
        {
            [typeof(TEntity)] = [new ColumnSelection(mode, names)],
        },
    };

    private static EntityMapBuilder<Product> Map(out MappingRegistration registration)
    {
        registration = new MappingRegistration { EntityClrType = typeof(Product) };
        return new EntityMapBuilder<Product>(registration);
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
    public async Task Unselected_property_is_skipped_on_insert()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var spec = Spec<Product>(ColumnSelectionMode.Exclude, nameof(Product.Description));
        var materializer = new EntityMaterializer(ctx.Model, ColumnConsumptionResolver.Resolve(ctx.Model, spec));

        materializer.TryMaterialize(ProductInsert(), out var row);

        ((Product)row!.Entity!).Description.ShouldBe("");
        row.Record.ContainsKey(nameof(Product.Description)).ShouldBeFalse();
        row.Record[nameof(Product.Name)].ShouldBe("Widget");
    }

    [Test]
    public async Task An_unavailable_toasted_column_materializes_when_unselected()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var spec = Spec<Product>(ColumnSelectionMode.Exclude, nameof(Product.Description));
        var materializer = new EntityMaterializer(ctx.Model, ColumnConsumptionResolver.Resolve(ctx.Model, spec));

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
    public async Task Capture_model_omits_unselected_columns()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model, Spec<Product>(ColumnSelectionMode.Exclude, nameof(Product.Description)));

        var product = model.FindByClrType(typeof(Product))!;
        product.Columns.ShouldNotContain(c => c.PropertyName == nameof(Product.Description));
        product.Columns.ShouldContain(c => c.PropertyName == nameof(Product.Name));
    }

    [Test]
    public async Task Consumes_captures_only_named_properties_plus_the_primary_key()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model, Spec<Product>(ColumnSelectionMode.Include, nameof(Product.Name)));

        var product = model.FindByClrType(typeof(Product))!;
        product.Columns.Select(c => c.PropertyName)
            .ShouldBe([nameof(Product.Id), nameof(Product.Name)], ignoreOrder: true);
    }

    [Test]
    public async Task Selections_union_across_mappings()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Product)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Product)] =
                [
                    new ColumnSelection(ColumnSelectionMode.Include, [nameof(Product.Name)]),
                    new ColumnSelection(ColumnSelectionMode.Include, [nameof(Product.Price)]),
                ],
            },
        };

        var product = EfCoreCaptureModelBuilder.Build(ctx.Model, spec).FindByClrType(typeof(Product))!;

        product.Columns.Select(c => c.PropertyName)
            .ShouldBe([nameof(Product.Id), nameof(Product.Name), nameof(Product.Price)], ignoreOrder: true);
    }

    [Test]
    public async Task An_exclude_selection_unions_with_an_include_selection()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Product)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Product)] =
                [
                    new ColumnSelection(ColumnSelectionMode.Include, [nameof(Product.Name)]),
                    new ColumnSelection(ColumnSelectionMode.Exclude, [nameof(Product.Description)]),
                ],
            },
        };

        var product = EfCoreCaptureModelBuilder.Build(ctx.Model, spec).FindByClrType(typeof(Product))!;

        // The exclude-mode mapping consumes everything else, so only Description stays out.
        product.Columns.ShouldNotContain(c => c.PropertyName == nameof(Product.Description));
        product.Columns.ShouldContain(c => c.PropertyName == nameof(Product.Price));
    }

    [Test]
    public async Task Excluding_a_primary_key_property_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(ctx.Model);

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(Spec<Product>(ColumnSelectionMode.Exclude, nameof(Product.Id))));

        ex.Message.ShouldContain("primary-key");
    }

    [Test]
    public async Task Selecting_a_navigation_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(ctx.Model);

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(Spec<Product>(ColumnSelectionMode.Include, nameof(Product.Category))));

        ex.Message.ShouldContain("not a mapped scalar property");
    }

    [Test]
    public async Task A_selection_for_an_entity_outside_the_model_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(ctx.Model);

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(Spec<NotMapped>(ColumnSelectionMode.Exclude, nameof(NotMapped.Payload))));

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
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Product)] = [new ColumnSelection(ColumnSelectionMode.Exclude, [nameof(Product.CategoryId)])],
            },
        };

        var ex = Should.Throw<WallabyConfigurationException>(
            () => EfCoreCaptureModelBuilder.Build(ctx.Model, spec));

        ex.Message.ShouldContain("CategoryId");
        ex.Message.ShouldContain("dependency-lookup");
    }

    [Test]
    public async Task Dependency_lookup_columns_are_captured_automatically()
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
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Product)] = [new ColumnSelection(ColumnSelectionMode.Include, [nameof(Product.Name)])],
            },
        };

        var product = EfCoreCaptureModelBuilder.Build(ctx.Model, spec).FindByClrType(typeof(Product))!;

        // The fan-out lookup reads CategoryId from the captured row, so the selection cannot drop it.
        product.Columns.Select(c => c.PropertyName).ShouldBe(
            [nameof(Product.Id), nameof(Product.Name), nameof(Product.CategoryId)], ignoreOrder: true);
    }

    [Test]
    public async Task A_dependent_only_table_narrows_to_its_lookup_columns()
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

        var categories = EfCoreCaptureModelBuilder.Build(ctx.Model, spec).FindByClrType(typeof(Category))!;

        // Fan-out reads only the lookup key from a category change; no other column is consumed.
        categories.Columns.Select(c => c.PropertyName).ShouldBe([nameof(Category.Id)]);
    }

    [Test]
    public async Task A_dependent_that_is_also_declared_keeps_its_own_capture()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        Expression<Func<Product, Category?>> navigation = p => p.Category;
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Product), typeof(Category)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>
            {
                [typeof(Product)] = [navigation],
            },
        };

        var categories = EfCoreCaptureModelBuilder.Build(ctx.Model, spec).FindByClrType(typeof(Category))!;

        categories.Columns.ShouldContain(c => c.PropertyName == nameof(Category.Name));
    }

    [Test]
    public void Consumes_requires_a_direct_property_access()
    {
        Should.Throw<WallabyConfigurationException>(
            () => Map(out _).Consumes(p => p.Name.Length));
    }

    [Test]
    public void Mixing_selection_modes_on_one_mapping_fails()
    {
        var ex = Should.Throw<WallabyConfigurationException>(
            () => Map(out _).Consumes(p => p.Name).ConsumesAllExcept(p => p.Description));

        ex.Message.ShouldContain("cannot be combined");
    }

    [Test]
    public void Repeated_consumes_calls_accumulate()
    {
        Map(out var registration).Consumes(p => p.Name).Consumes(p => p.Price);

        registration.ColumnSelection!.Mode.ShouldBe(ColumnSelectionMode.Include);
        registration.ColumnSelection.PropertyNames.ShouldBe([nameof(Product.Name), nameof(Product.Price)]);
    }

    [Test]
    public void String_and_expression_selections_accumulate()
    {
        Map(out var registration).Consumes("Name").Consumes(p => p.Price);

        registration.ColumnSelection!.Mode.ShouldBe(ColumnSelectionMode.Include);
        registration.ColumnSelection.PropertyNames.ShouldBe([nameof(Product.Name), nameof(Product.Price)]);
    }

    [Test]
    public void ConsumesAllExcept_accepts_string_names()
    {
        Map(out var registration).ConsumesAllExcept("Description");

        registration.ColumnSelection!.Mode.ShouldBe(ColumnSelectionMode.Exclude);
        registration.ColumnSelection.PropertyNames.ShouldBe([nameof(Product.Description)]);
    }

    [Test]
    public void A_blank_string_property_name_fails()
    {
        Should.Throw<WallabyConfigurationException>(() => Map(out _).Consumes(" "));
    }

    [Test]
    public async Task String_names_select_private_and_shadow_properties()
    {
        await using var ctx = GadgetDbContext.CreateModelOnly();
        var spec = GadgetSpec("Name", "Region", "Tenant");

        var gadget = EfCoreCaptureModelBuilder.Build(ctx.Model, spec).FindByClrType(typeof(Gadget))!;

        gadget.Columns.Select(c => c.PropertyName)
            .ShouldBe(["Id", "Name", "Region", "Tenant"], ignoreOrder: true);
    }

    [Test]
    public async Task Private_and_shadow_properties_materialize_into_the_record()
    {
        await using var ctx = GadgetDbContext.CreateModelOnly();
        var materializer = new EntityMaterializer(
            ctx.Model,
            ColumnConsumptionResolver.Resolve(ctx.Model, GadgetSpec("Name", "Region", "Tenant")),
            capturedTypes: [typeof(Gadget)]);
        var change = new RawChange
        {
            RelationId = 1,
            Schema = "public",
            TableName = "gadgets",
            Action = ChangeAction.Insert,
            NewValues = [Col("Id", 1), Col("Name", "Gizmo"), Col("Region", "eu"), Col("Tenant", "t1")],
        };

        materializer.TryMaterialize(change, out var row).ShouldBeTrue();

        ((Gadget)row!.Entity!).GetRegion().ShouldBe("eu");
        row.Record["Region"].ShouldBe("eu");
        // A shadow property has no CLR member; the value is carried in the record only.
        row.Record["Tenant"].ShouldBe("t1");
    }

    private static CaptureSpec GadgetSpec(params string[] names) => new()
    {
        DeclaredEntities = [typeof(Gadget)],
        DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
        {
            [typeof(Gadget)] = [new ColumnSelection(ColumnSelectionMode.Include, names)],
        },
    };

    private sealed class NotMapped
    {
        public int Id { get; set; }
        public string Payload { get; set; } = "";
    }

    /// <summary>Entity with a private mapped property and a shadow property, both lambda-unreachable.</summary>
    public class Gadget
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        private string Region { get; set; } = "";

        public string GetRegion() => Region;
    }

    private sealed class GadgetDbContext(DbContextOptions<GadgetDbContext> options) : DbContext(options)
    {
        public DbSet<Gadget> Gadgets => Set<Gadget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Gadget>(b =>
            {
                b.ToTable("gadgets");
                b.HasKey(g => g.Id);
                b.Property<string>("Region");
                b.Property<string>("Tenant");
            });

        public static GadgetDbContext CreateModelOnly()
            => new(new DbContextOptionsBuilder<GadgetDbContext>()
                .UseNpgsql(TestModelFactory.ModelOnlyConnectionString)
                .Options);
    }
}
