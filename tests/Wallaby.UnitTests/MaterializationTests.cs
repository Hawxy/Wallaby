using Wallaby.Abstractions;
using Wallaby.Internal.Materialization;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;
using Wallaby.TestModel;

namespace Wallaby.UnitTests;

public class MaterializationTests
{
    private static EntityMaterializer CreateMaterializer()
    {
        var ctx = TestModelFactory.CreateModelOnlyContext();
        return new EntityMaterializer(ctx.Model);
    }

    private static RawColumn Col(string name, object? value) => new() { ColumnName = name, Value = value };

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
