using Wallaby.Abstractions;
using Wallaby.Providers.EntityFrameworkCore.Internal;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Unit;

public class MaterializationTests
{
    private static EntityMaterializer CreateMaterializer()
    {
        var ctx = TestModelFactory.CreateModelOnlyContext();
        return new EntityMaterializer(ctx.Model);
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
            Col("Status", "Active"),          // value-converted enum (stored as text)
            Col("Tags", "[\"a\",\"b\"]"),      // jsonb stored as text
            Col("Description", "a widget"),
            Col("CategoryId", 7),
        ],
    };

    [Test]
    public void Materializes_typed_entity_with_value_converters()
    {
        var materializer = CreateMaterializer();

        var ok = materializer.TryMaterialize(ProductInsert(), out var row);

        ok.ShouldBeTrue();
        var product = row.Entity as Product;
        product.ShouldNotBeNull();
        product!.Id.ShouldBe(42);
        product.Name.ShouldBe("Widget");
        product.Price.ShouldBe(9.99m);
        product.Sku.ShouldBe("W-1");
        product.Status.ShouldBe(ProductStatus.Active);   // string -> enum
        product.Tags.ShouldBe(new[] { "a", "b" }, ignoreOrder: true);  // jsonb text -> List<string>
        product.CategoryId.ShouldBe(7);
    }

    [Test]
    public void Record_and_primary_key_are_populated()
    {
        var materializer = CreateMaterializer();

        materializer.TryMaterialize(ProductInsert(), out var row);

        row.EntityClrType.ShouldBe(typeof(Product));
        row.Record[nameof(Product.Name)].ShouldBe("Widget");
        row.PrimaryKey.Count.ShouldBe(1);
        row.PrimaryKey[0].ShouldBe(42);
    }

    [Test]
    public void Update_with_full_old_values_computes_changed_fields()
    {
        var materializer = CreateMaterializer();

        // Simulate REPLICA IDENTITY FULL: full old row present, only Name changed.
        var change = ProductInsert() with
        {
            Action = ChangeAction.Update,
            NewValues =
            [
                Col("Id", 42), Col("Name", "Widget v2"), Col("Price", 9.99m), Col("product_sku", "W-1"),
                Col("Status", "Active"), Col("Tags", "[\"a\",\"b\"]"), Col("Description", "a widget"), Col("CategoryId", 7),
            ],
            OldValues =
            [
                Col("Id", 42), Col("Name", "Widget"), Col("Price", 9.99m), Col("product_sku", "W-1"),
                Col("Status", "Active"), Col("Tags", "[\"a\",\"b\"]"), Col("Description", "a widget"), Col("CategoryId", 7),
            ],
        };

        materializer.TryMaterialize(change, out var row);

        row.Changes.ShouldNotBeNull();
        row.Changes!.ContainsKey(nameof(Product.Name)).ShouldBeTrue();
        row.Changes[nameof(Product.Name)].ShouldBe("Widget");
        row.Changes.ContainsKey(nameof(Product.Price)).ShouldBeFalse(); // unchanged
    }

    [Test]
    public void Delete_materializes_primary_key_from_old_values()
    {
        var materializer = CreateMaterializer();

        var change = new RawChange
        {
            RelationId = 1,
            Schema = "public",
            TableName = "products",
            Action = ChangeAction.Delete,
            NewValues = [],
            OldValues = [Col("Id", 42)],
        };

        var ok = materializer.TryMaterialize(change, out var row);

        ok.ShouldBeTrue();
        row.PrimaryKey[0].ShouldBe(42);
        row.Changes.ShouldBeNull();
        ((Product)row.Entity!).Id.ShouldBe(42);
    }

    [Test]
    public void An_unchanged_toasted_column_falls_back_to_the_old_tuple()
    {
        var materializer = CreateMaterializer();

        // REPLICA IDENTITY FULL: the unchanged TOASTed Description is omitted from the new tuple but
        // carried in the old one.
        var change = ProductInsert() with
        {
            Action = ChangeAction.Update,
            NewValues =
            [
                Col("Id", 42), Col("Name", "Widget v2"), Col("Price", 9.99m), Col("product_sku", "W-1"),
                Col("Status", "Active"), Col("Tags", "[\"a\",\"b\"]"), Toast("Description"), Col("CategoryId", 7),
            ],
            OldValues =
            [
                Col("Id", 42), Col("Name", "Widget"), Col("Price", 9.99m), Col("product_sku", "W-1"),
                Col("Status", "Active"), Col("Tags", "[\"a\",\"b\"]"), Col("Description", "a widget"), Col("CategoryId", 7),
            ],
        };

        materializer.TryMaterialize(change, out var row);

        ((Product)row!.Entity!).Description.ShouldBe("a widget");
        row.Record[nameof(Product.Description)].ShouldBe("a widget");
    }

    [Test]
    public void An_unavailable_toasted_column_is_a_poison_change_with_replica_identity_guidance()
    {
        var materializer = CreateMaterializer();

        // REPLICA IDENTITY DEFAULT: no old tuple, so the unchanged TOASTed value is unrecoverable.
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

        var ex = Should.Throw<InvalidOperationException>(() => materializer.TryMaterialize(change, out _));

        ex.Message.ShouldContain("Description");
        ex.Message.ShouldContain("public.products");
        ex.Message.ShouldContain("REPLICA IDENTITY FULL");
        ex.Message.ShouldContain("https://wallabycdc.net/providers/entity-framework-core/");
    }

    [Test]
    public void Unmapped_table_returns_false()
    {
        var materializer = CreateMaterializer();

        var change = new RawChange
        {
            RelationId = 1, Schema = "public", TableName = "not_mapped",
            Action = ChangeAction.Insert, NewValues = [Col("Id", 1)],
        };

        materializer.TryMaterialize(change, out _).ShouldBeFalse();
    }

    [Test]
    public void ChangeEventFactory_builds_envelope_with_metadata()
    {
        var factory = new ChangeEventFactory(CreateMaterializer());

        var change = ProductInsert() with { CommitLsn = 123, CommitIdx = 2 };
        var ev = factory.Create(change);

        ev.ShouldNotBeNull();
        ev!.Action.ShouldBe(ChangeAction.Insert);
        ev.Metadata.TableName.ShouldBe("products");
        ev.Metadata.QualifiedTableName.ShouldBe("public.products");
        ev.Metadata.CommitLsn.ShouldBe(123UL);
        ev.Metadata.IsBackfill.ShouldBeFalse();
        ev.EntityClrType.ShouldBe(typeof(Product));
    }
}
