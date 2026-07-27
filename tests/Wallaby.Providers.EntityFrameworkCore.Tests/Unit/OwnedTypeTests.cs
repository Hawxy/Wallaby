using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

/// <summary>
/// Same-table owned references and complex properties flatten into the capture model and materialize
/// with their owner; members whose data is not on the entity's rows (owned collections, separate-table
/// owned types, JSON-mapped members) warn at startup unless acknowledged, and fail loudly only when a
/// selection names them for inclusion or the type cannot be constructed.
/// </summary>
public class OwnedTypeTests
{
    private static CaptureSpec Declared(params Type[] types) => new() { DeclaredEntities = types };

    private static CaptureSpec SupplierSpec(ColumnSelectionMode mode, params string[] names) => new()
    {
        DeclaredEntities = [typeof(Supplier)],
        DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
        {
            [typeof(Supplier)] = [new ColumnSelection(mode, names)],
        },
    };

    private static RawColumn Col(string name, object? value) => new() { ColumnName = name, Value = value };

    private static RawChange SupplierInsert() => new()
    {
        RelationId = 9,
        Schema = "public",
        TableName = "suppliers",
        Action = ChangeAction.Insert,
        NewValues =
        [
            Col("Id", 7),
            Col("Name", "Acme"),
            Col("address_street", "1 Main St"),
            Col("address_city", "Springfield"),
            Col("address_lat", 1.5),
            Col("address_lon", 2.5),
            Col("billing_street", null),
            Col("billing_city", null),
            Col("billing_lat", null),
            Col("billing_lon", null),
            Col("contact_email", "acme@example.com"),
            Col("contact_phone", "555-0100"),
        ],
    };

    [Test]
    public async Task Capture_model_flattens_owned_and_complex_members()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Supplier)));
        var supplier = model.FindByClrType(typeof(Supplier))!;

        var byPath = supplier.Columns.ToDictionary(c => c.PropertyName, c => c.ColumnName);
        byPath["Address.Street"].ShouldBe("address_street");
        byPath["Address.Location.Lat"].ShouldBe("address_lat");
        byPath["BillingAddress.Location.Lon"].ShouldBe("billing_lon");
        byPath["Contact.Email"].ShouldBe("contact_email");
        supplier.Columns.Count.ShouldBe(12); // Id, Name + 2×(street, city, lat, lon) + email, phone

        // The owned types' shadow PKs map to the owner's PK column, which is captured exactly once.
        supplier.Columns.Count(c => c.IsPrimaryKey).ShouldBe(1);

        // Members whose data is not on suppliers rows are not captured.
        supplier.Columns.ShouldNotContain(c => c.PropertyName.StartsWith("Notes"));
        supplier.Columns.ShouldNotContain(c => c.PropertyName.StartsWith("Legal"));
        supplier.Columns.ShouldNotContain(c => c.PropertyName.StartsWith("Meta"));
    }

    [Test]
    public async Task Uncapturable_members_warn_at_startup()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, Declared(typeof(Supplier)));

        model.Warnings.Count.ShouldBe(3);
        model.Warnings.ShouldContain(w =>
            w.Contains("'Supplier.Notes'") && w.Contains("owned collection") && w.Contains("supplier_notes"));
        model.Warnings.ShouldContain(w =>
            w.Contains("'Supplier.Legal'") && w.Contains("its own table") && w.Contains("supplier_legal"));
        model.Warnings.ShouldContain(w =>
            w.Contains("'Supplier.Meta'") && w.Contains("JSON column 'meta'"));
    }

    [Test]
    public async Task A_DependsOn_declaration_acknowledges_a_side_table_member()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        Expression<Func<Supplier, LegalInfo?>> navigation = s => s.Legal;
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Supplier)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>
            {
                [typeof(Supplier)] = [navigation],
            },
        };

        var model = EfCoreCaptureModelBuilder.Build(ctx.Model, spec);

        model.Warnings.ShouldNotContain(w => w.Contains("'Supplier.Legal'"));
        model.Warnings.Count.ShouldBe(2); // Notes and Meta still warn
        model.DependentBindings.Count.ShouldBe(1);
    }

    [Test]
    public async Task An_exclude_selection_acknowledges_an_uncapturable_member()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model, SupplierSpec(ColumnSelectionMode.Exclude, "Notes", "Meta"));

        model.Warnings.Count.ShouldBe(1);
        model.Warnings[0].ShouldContain("'Supplier.Legal'");
    }

    [Test]
    public async Task Include_only_selections_drop_unnamed_members_without_warning()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model, SupplierSpec(ColumnSelectionMode.Include, nameof(Supplier.Name)));

        model.Warnings.ShouldBeEmpty();
        model.FindByClrType(typeof(Supplier))!.Columns.Select(c => c.PropertyName)
            .ShouldBe(["Id", "Name"], ignoreOrder: true);
    }

    [Test]
    public async Task Consuming_an_uncapturable_member_fails()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var provider = new EfCoreModelProvider(ctx.Model);

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(SupplierSpec(ColumnSelectionMode.Include, nameof(Supplier.Notes))));

        ex.Message.ShouldContain("Notes");
        ex.Message.ShouldContain("owned collection");
        ex.Message.ShouldContain("DependsOn");
    }

    [Test]
    public async Task Consumes_selects_an_owned_navigation_as_a_unit()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model, SupplierSpec(ColumnSelectionMode.Include, nameof(Supplier.Name), nameof(Supplier.Address)));

        model.FindByClrType(typeof(Supplier))!.Columns.Select(c => c.PropertyName).ShouldBe(
            ["Id", "Name", "Address.Street", "Address.City", "Address.Location.Lat", "Address.Location.Lon"],
            ignoreOrder: true);
    }

    [Test]
    public async Task ConsumesAllExcept_drops_an_owned_navigation_as_a_unit()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();

        var model = EfCoreCaptureModelBuilder.Build(
            ctx.Model, SupplierSpec(ColumnSelectionMode.Exclude, nameof(Supplier.BillingAddress)));

        var paths = model.FindByClrType(typeof(Supplier))!.Columns.Select(c => c.PropertyName).ToList();
        paths.ShouldNotContain(p => p.StartsWith("BillingAddress"));
        paths.ShouldContain("Address.Street");
        paths.ShouldContain("Contact.Phone");
        paths.Count.ShouldBe(8);
    }

    [Test]
    public async Task Materializes_owned_and_complex_members()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(ctx.Model);

        var ok = materializer.TryMaterialize(SupplierInsert(), out var row);

        ok.ShouldBeTrue();
        var supplier = (Supplier)row!.Entity!;
        supplier.Name.ShouldBe("Acme");
        supplier.Address.Street.ShouldBe("1 Main St");
        supplier.Address.City.ShouldBe("Springfield");
        supplier.Address.Location.ShouldBe(new GeoPoint(1.5, 2.5)); // nested ctor-bound record
        supplier.Contact.ShouldBe(new ContactCard("acme@example.com", "555-0100")); // complex record
        supplier.BillingAddress.ShouldBeNull(); // optional member with all-null columns

        row.Record["Address.Street"].ShouldBe("1 Main St");
        row.Record["Contact.Email"].ShouldBe("acme@example.com");
        row.Record["BillingAddress.City"].ShouldBeNull();
    }

    [Test]
    public async Task An_optional_owned_member_with_values_materializes()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(ctx.Model);
        var change = SupplierInsert();
        change = change with
        {
            NewValues =
            [
                .. change.NewValues!.Where(c => !c.ColumnName.StartsWith("billing")),
                Col("billing_street", "9 Side St"),
                Col("billing_city", "Shelbyville"),
                Col("billing_lat", 3.5),
                Col("billing_lon", 4.5),
            ],
        };

        materializer.TryMaterialize(change, out var row);

        var supplier = (Supplier)row!.Entity!;
        supplier.BillingAddress.ShouldNotBeNull();
        supplier.BillingAddress!.City.ShouldBe("Shelbyville");
        supplier.BillingAddress.Location.ShouldBe(new GeoPoint(3.5, 4.5));
    }

    [Test]
    public async Task An_optional_nested_member_with_null_columns_stays_null()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(ctx.Model);
        var change = SupplierInsert();
        change = change with
        {
            NewValues =
            [
                .. change.NewValues!.Where(c => c.ColumnName is not ("address_lat" or "address_lon")),
                Col("address_lat", null),
                Col("address_lon", null),
            ],
        };

        materializer.TryMaterialize(change, out var row);

        var supplier = (Supplier)row!.Entity!;
        supplier.Address.Street.ShouldBe("1 Main St");
        supplier.Address.Location.ShouldBeNull();
    }

    [Test]
    public async Task A_key_only_delete_materializes_with_owned_members_at_defaults()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(ctx.Model);
        var change = new RawChange
        {
            RelationId = 9, Schema = "public", TableName = "suppliers",
            Action = ChangeAction.Delete, NewValues = [], OldValues = [Col("Id", 7)],
        };

        var ok = materializer.TryMaterialize(change, out var row);

        ok.ShouldBeTrue();
        var supplier = (Supplier)row!.Entity!;
        supplier.Id.ShouldBe(7);
        supplier.Address.ShouldNotBeNull(); // required member constructs even without values
        supplier.BillingAddress.ShouldBeNull();
    }

    [Test]
    public async Task Changes_report_owned_members_under_their_dotted_path()
    {
        await using var ctx = TestModelFactory.CreateModelOnlyContext();
        var materializer = new EntityMaterializer(ctx.Model);
        var insert = SupplierInsert();
        var change = insert with
        {
            Action = ChangeAction.Update,
            OldValues =
            [
                .. insert.NewValues!.Where(c => c.ColumnName != "address_street"),
                Col("address_street", "2 Old Rd"),
            ],
        };

        materializer.TryMaterialize(change, out var row);

        row!.Changes.ShouldNotBeNull();
        row.Changes!["Address.Street"].ShouldBe("2 Old Rd");
        row.Changes.ContainsKey("Contact.Email").ShouldBeFalse(); // unchanged
    }

    [Test]
    public async Task An_unconstructible_owned_member_fails_startup_for_a_captured_entity()
    {
        await using var ctx = CreateDepotContext();
        var provider = new EfCoreModelProvider(ctx.Model);

        var ex = Should.Throw<WallabyConfigurationException>(
            () => provider.BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Depot)] }));

        ex.Message.ShouldContain("Depot.Spot");
        ex.Message.ShouldContain("not bound to a mapped property");
    }

    [Test]
    public async Task An_unconstructible_owned_member_on_an_uncaptured_table_degrades_silently()
    {
        await using var ctx = CreateDepotContext();
        // No captured types declared: the materializer plans every table best-effort.
        var materializer = new EntityMaterializer(ctx.Model);
        var change = new RawChange
        {
            RelationId = 3, Schema = "public", TableName = "depots",
            Action = ChangeAction.Insert,
            NewValues = [Col("Id", 1), Col("Spot_Code", "Z-9")],
        };

        var ok = materializer.TryMaterialize(change, out var row);

        ok.ShouldBeTrue();
        ((Depot)row!.Entity!).Spot.ShouldBeNull();
        row.Record.ContainsKey("Spot.Code").ShouldBeFalse();
    }

    private static DepotContext CreateDepotContext()
        => new(new DbContextOptionsBuilder<DepotContext>()
            .UseNpgsql(TestModelFactory.ModelOnlyConnectionString)
            .Options);

    private sealed class DepotContext(DbContextOptions<DepotContext> options) : DbContext(options)
    {
        public DbSet<Depot> Depots => Set<Depot>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Depot>(b =>
            {
                b.ToTable("depots");
                b.HasKey(d => d.Id);
                // The read-only Code needs explicit mapping for EF to bind the ctor parameter.
                b.OwnsOne(d => d.Spot, s => s.Property(x => x.Code));
            });
    }

    public sealed class Depot
    {
        public int Id { get; set; }
        public DepotSpot? Spot { get; set; }
    }

    /// <summary>Constructible by EF (it injects the context) but not from column values alone.</summary>
    public sealed class DepotSpot
    {
        public DepotSpot(string code, DbContext context)
        {
            Code = code;
            _ = context;
        }

        public string Code { get; }
    }
}
