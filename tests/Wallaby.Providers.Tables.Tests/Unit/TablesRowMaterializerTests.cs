using Wallaby.Abstractions;
using Wallaby.Model;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables.Tests.Unit;

/// <summary>
/// The materializer over decoded changes: records and classes, coercion, the update diff, the
/// unchanged-TOAST fallback, partial entities on delete, and narrowed columns.
/// </summary>
public class TablesRowMaterializerTests
{
    private static readonly Guid WidgetId = Guid.Parse("5e6f8f4e-1111-2222-3333-444455556666");

    private static IRowMaterializer Materializer(IReadOnlyList<ColumnSelection>? orderSelections = null)
    {
        var builder = new TablesModelBuilder();
        builder.Add<Order>();
        builder.Add<Widget>();
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(Order), typeof(Widget)],
            DeclaredColumnSelections = orderSelections is null
                ? new Dictionary<Type, IReadOnlyList<ColumnSelection>>()
                : new Dictionary<Type, IReadOnlyList<ColumnSelection>> { [typeof(Order)] = orderSelections },
        };
        return new TablesModelProvider(builder.Build()).BuildCapturePlan(spec).Materializer;
    }

    private static RawColumn Col(string name, object? value) => new() { ColumnName = name, Value = value };

    private static RawColumn Toast(string name) => new() { ColumnName = name, Value = null, IsUnchangedToast = true };

    private static RawChange Change(
        string schema, string table, ChangeAction action, RawColumn[]? newValues = null, RawColumn[]? oldValues = null) => new()
    {
        RelationId = 1,
        Schema = schema,
        TableName = table,
        Action = action,
        NewValues = newValues ?? [],
        OldValues = oldValues,
    };

    private static RawChange OrderChange(ChangeAction action, RawColumn[]? newValues = null, RawColumn[]? oldValues = null)
        => Change("sales", "orders", action, newValues, oldValues);

    private static RawColumn[] OrderRow(string customer = "acme", decimal total = 12.5m, object? status = null) =>
    [
        Col("Id", 7),
        Col("customer_ref", customer),
        Col("Total", total),
        Col("Status", status ?? "Paid"),
        Col("PaidAt", new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)),
        Col("unmodeled", "ignored"),
    ];

    [Test]
    public void An_insert_constructs_a_positional_record()
    {
        Materializer().TryMaterialize(OrderChange(ChangeAction.Insert, OrderRow()), out var row).ShouldBeTrue();

        var order = row!.Entity.ShouldBeOfType<Order>();
        order.ShouldBe(new Order(7, "acme", 12.5m, Status.Paid, new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero)));
        row.Action.ShouldBe(ChangeAction.Insert);
        row.Record["CustomerRef"].ShouldBe("acme");
        row.Record.ShouldNotContainKey("unmodeled");
        row.PrimaryKey.ShouldBe([7]);
        row.EntityClrType.ShouldBe(typeof(Order));
        row.Changes.ShouldBeNull();
    }

    [Test]
    public void Values_are_coerced_to_the_property_types()
    {
        // Text ids, enums as numbers, and totals arriving as doubles all coerce.
        var change = OrderChange(ChangeAction.Insert,
        [
            Col("Id", "7"), Col("customer_ref", "acme"), Col("Total", 3.25d), Col("Status", 1), Col("PaidAt", null),
        ]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        var order = row!.Entity.ShouldBeOfType<Order>();
        order.Id.ShouldBe(7);
        order.Total.ShouldBe(3.25m);
        order.Status.ShouldBe(Status.Paid);
        order.PaidAt.ShouldBeNull();
    }

    [Test]
    public void An_insert_assigns_a_class_through_its_setters()
    {
        var change = Change("public", "Widget", ChangeAction.Insert,
        [
            Col("Id", WidgetId), Col("DisplayName", "kanga"), Col("Qty", 3), Col("Tags", new[] { 1, 2 }), Col("Derived", "KANGA"),
        ]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        var widget = row!.Entity.ShouldBeOfType<Widget>();
        widget.Id.ShouldBe(WidgetId);
        widget.DisplayName.ShouldBe("kanga");
        widget.Qty.ShouldBe(3);
        widget.Tags.ShouldBe([1, 2]);
        // Getter-only: computed by the entity, still present in the record from the wire.
        widget.Derived.ShouldBe("KANGA");
        row.Record["Derived"].ShouldBe("KANGA");
    }

    [Test]
    public void An_update_reports_the_previous_values_of_changed_columns()
    {
        var change = OrderChange(ChangeAction.Update, OrderRow(customer: "acme", total: 20m),
            oldValues: OrderRow(customer: "acme", total: 12.5m));

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        row!.Changes.ShouldNotBeNull();
        row.Changes.Keys.ShouldBe(["Total"]);
        row.Changes["Total"].ShouldBe(12.5m);
    }

    [Test]
    public void A_key_only_old_tuple_yields_no_changes_beyond_the_key()
    {
        var change = OrderChange(ChangeAction.Update, OrderRow(), oldValues: [Col("Id", 7)]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        row!.Changes.ShouldNotBeNull().ShouldBeEmpty();
    }

    [Test]
    public void An_unchanged_toasted_value_falls_back_to_the_old_tuple()
    {
        var change = OrderChange(ChangeAction.Update,
            [Col("Id", 7), Toast("customer_ref"), Col("Total", 1m), Col("Status", "Paid"), Col("PaidAt", null)],
            oldValues: OrderRow(customer: "from-old-tuple"));

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        row!.Entity.ShouldBeOfType<Order>().CustomerRef.ShouldBe("from-old-tuple");
    }

    [Test]
    public void An_unchanged_toasted_value_without_an_old_tuple_is_unavailable()
    {
        var change = OrderChange(ChangeAction.Update,
            [Col("Id", 7), Toast("customer_ref"), Col("Total", 1m), Col("Status", "Paid"), Col("PaidAt", null)]);

        var ex = Should.Throw<UnavailableValueException>(() => Materializer().TryMaterialize(change, out _));
        ex.ColumnName.ShouldBe("customer_ref");
        ex.TableName.ShouldBe("orders");
        ex.Message.ShouldContain("REPLICA IDENTITY FULL");
    }

    [Test]
    public void A_delete_materializes_a_partial_entity_from_the_old_tuple()
    {
        var change = OrderChange(ChangeAction.Delete, oldValues: [Col("Id", 7)]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        row!.Action.ShouldBe(ChangeAction.Delete);
        row.PrimaryKey.ShouldBe([7]);
        // Absent constructor parameters take their defaults.
        var order = row.Entity.ShouldBeOfType<Order>();
        order.Id.ShouldBe(7);
        order.CustomerRef.ShouldBeNull();
        order.Total.ShouldBe(0m);
        row.Record.Keys.ShouldBe(["Id"]);
    }

    [Test]
    public void A_delete_without_old_values_is_an_error()
    {
        Should.Throw<InvalidOperationException>(() => Materializer().TryMaterialize(OrderChange(ChangeAction.Delete), out _))
            .Message.ShouldContain("no old values");
    }

    [Test]
    public void A_null_key_is_an_error()
    {
        var change = OrderChange(ChangeAction.Insert, [Col("Id", null), Col("customer_ref", "x"), Col("Total", 1m), Col("Status", "Paid")]);

        Should.Throw<InvalidOperationException>(() => Materializer().TryMaterialize(change, out _))
            .Message.ShouldContain("Key column 'Id'");
    }

    [Test]
    public void An_unknown_table_is_skipped()
    {
        Materializer().TryMaterialize(Change("sales", "other", ChangeAction.Insert, [Col("Id", 1)]), out var row).ShouldBeFalse();
        row.ShouldBeNull();
    }

    [Test]
    public void A_backfill_read_materializes_like_an_insert()
    {
        Materializer().TryMaterialize(OrderChange(ChangeAction.Read, OrderRow()), out var row).ShouldBeTrue();

        row!.Action.ShouldBe(ChangeAction.Read);
        row.Entity.ShouldBeOfType<Order>().CustomerRef.ShouldBe("acme");
    }

    [Test]
    public void Narrowed_columns_leave_the_rest_at_their_defaults()
    {
        var materializer = Materializer([new ColumnSelection(ColumnSelectionMode.Include, ["CustomerRef"])]);

        materializer.TryMaterialize(OrderChange(ChangeAction.Insert, OrderRow()), out var row).ShouldBeTrue();

        var order = row!.Entity.ShouldBeOfType<Order>();
        order.CustomerRef.ShouldBe("acme");
        order.Total.ShouldBe(0m);
        order.Status.ShouldBe(Status.Pending);
        row.Record.Keys.ShouldBe(["Id", "CustomerRef"]);
    }

    [Test]
    public void A_null_for_a_non_nullable_member_keeps_the_default()
    {
        var change = Change("public", "Widget", ChangeAction.Delete, oldValues:
            [Col("Id", WidgetId), Col("DisplayName", null), Col("Qty", null)]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();

        var widget = row!.Entity.ShouldBeOfType<Widget>();
        widget.DisplayName.ShouldBeNull();
        widget.Qty.ShouldBeNull();
        widget.Tags.ShouldBeEmpty();
    }
}
