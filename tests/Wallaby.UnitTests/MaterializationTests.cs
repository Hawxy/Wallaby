using EFCore.CDC.TestModel;
using Wallaby.Abstractions;
using Wallaby.Internal.Materialization;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;

namespace EFCore.CDC.UnitTests;

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
    public async Task Materializes_typed_entity_with_value_converters()
    {
        var materializer = CreateMaterializer();

        var ok = materializer.TryMaterialize(ProductInsert(), out var row);

        await Assert.That(ok).IsTrue();
        var product = row.Entity as Product;
        await Assert.That(product).IsNotNull();
        await Assert.That(product!.Id).IsEqualTo(42);
        await Assert.That(product.Name).IsEqualTo("Widget");
        await Assert.That(product.Price).IsEqualTo(9.99m);
        await Assert.That(product.Sku).IsEqualTo("W-1");
        await Assert.That(product.Status).IsEqualTo(ProductStatus.Active);   // string -> enum
        await Assert.That(product.Tags).IsEquivalentTo(new[] { "a", "b" });  // jsonb text -> List<string>
        await Assert.That(product.CategoryId).IsEqualTo(7);
    }

    [Test]
    public async Task Record_and_primary_key_are_populated()
    {
        var materializer = CreateMaterializer();

        materializer.TryMaterialize(ProductInsert(), out var row);

        await Assert.That(row.EntityClrType).IsEqualTo(typeof(Product));
        await Assert.That(row.Record[nameof(Product.Name)]).IsEqualTo("Widget");
        await Assert.That(row.PrimaryKey.Count).IsEqualTo(1);
        await Assert.That(row.PrimaryKey[0]).IsEqualTo(42);
    }

    [Test]
    public async Task Update_with_full_old_values_computes_changed_fields()
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

        await Assert.That(row.Changes).IsNotNull();
        await Assert.That(row.Changes!.ContainsKey(nameof(Product.Name))).IsTrue();
        await Assert.That(row.Changes[nameof(Product.Name)]).IsEqualTo("Widget");
        await Assert.That(row.Changes.ContainsKey(nameof(Product.Price))).IsFalse(); // unchanged
    }

    [Test]
    public async Task Delete_materializes_primary_key_from_old_values()
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

        await Assert.That(ok).IsTrue();
        await Assert.That(row.PrimaryKey[0]).IsEqualTo(42);
        await Assert.That(row.Changes).IsNull();
        await Assert.That(((Product)row.Entity!).Id).IsEqualTo(42);
    }

    [Test]
    public async Task Unmapped_table_returns_false()
    {
        var materializer = CreateMaterializer();

        var change = new RawChange
        {
            RelationId = 1, Schema = "public", TableName = "not_mapped",
            Action = ChangeAction.Insert, NewValues = [Col("Id", 1)],
        };

        await Assert.That(materializer.TryMaterialize(change, out _)).IsFalse();
    }

    [Test]
    public async Task ChangeEventFactory_builds_envelope_with_metadata()
    {
        var factory = new ChangeEventFactory(CreateMaterializer());

        var change = ProductInsert() with { CommitLsn = 123, CommitIdx = 2 };
        var ev = factory.Create(change);

        await Assert.That(ev).IsNotNull();
        await Assert.That(ev!.Action).IsEqualTo(ChangeAction.Insert);
        await Assert.That(ev.Metadata.TableName).IsEqualTo("products");
        await Assert.That(ev.Metadata.QualifiedTableName).IsEqualTo("public.products");
        await Assert.That(ev.Metadata.CommitLsn).IsEqualTo(123UL);
        await Assert.That(ev.Metadata.IsBackfill).IsFalse();
        await Assert.That(ev.EntityClrType).IsEqualTo(typeof(Product));
    }
}
