using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables.Tests.Unit;

/// <summary>
/// Registration rules: table and column naming (attribute, fluent, convention, snake_case), key discovery
/// order, member filtering, the constructor pick, and the startup rejections. No database involved.
/// </summary>
public class TablesModelBuilderTests
{
    private static IReadOnlyDictionary<Type, TableRegistration> Build(Action<TablesModelBuilder> configure)
    {
        var builder = new TablesModelBuilder();
        configure(builder);
        return builder.Build();
    }

    private static TableRegistration Single<T>(Action<TablesModelBuilder> configure) => Build(configure)[typeof(T)];

    [Test]
    public void Attributes_name_the_table_schema_and_columns()
    {
        var order = Single<Order>(t => t.Add<Order>());

        order.Schema.ShouldBe("sales");
        order.Table.ShouldBe("orders");
        order.Members.Select(m => m.ColumnName).ShouldBe(["Id", "customer_ref", "Total", "Status", "PaidAt"]);
        order.Key.Single().PropertyName.ShouldBe("Id");
    }

    [Test]
    public void Conventions_use_the_clr_names_verbatim_in_the_default_schema()
    {
        var widget = Single<Widget>(t => t.Add<Widget>());

        widget.Schema.ShouldBe("public");
        widget.Table.ShouldBe("Widget");
        widget.Members.Select(m => m.ColumnName).ShouldBe(["Id", "DisplayName", "Qty", "Tags", "Derived"]);
    }

    [Test]
    public void Snake_case_and_default_schema_apply_to_unannotated_names_only()
    {
        var registrations = Build(t =>
        {
            t.UseSnakeCase().DefaultSchema("app");
            t.Add<Widget>();
            t.Add<Order>();
        });

        var widget = registrations[typeof(Widget)];
        widget.Schema.ShouldBe("app");
        widget.Table.ShouldBe("widget");
        widget.Members.Select(m => m.ColumnName).ShouldBe(["id", "display_name", "qty", "tags", "derived"]);

        var order = registrations[typeof(Order)];
        order.Table.ShouldBe("orders");
        order.Schema.ShouldBe("sales");
        // [Column] wins over the convention; unannotated members still snake_case.
        order.Members.Select(m => m.ColumnName).ShouldBe(["id", "customer_ref", "total", "status", "paid_at"]);
    }

    [Test]
    [Arguments("OrderId", "order_id")]
    [Arguments("HTTPStatus", "http_status")]
    [Arguments("Line2Number", "line2_number")]
    [Arguments("Id", "id")]
    [Arguments("already_snake", "already_snake")]
    public void Snake_case_handles_acronyms_and_digits(string input, string expected)
        => NameConventions.ToSnakeCase(input).ShouldBe(expected);

    [Test]
    public void Fluent_overrides_win_over_attributes()
    {
        var order = Single<Order>(t => t.Add<Order>()
            .ToTable("orders_v2", "archive")
            .Column(o => o.CustomerRef, "cust")
            .Ignore(o => o.PaidAt));

        order.Schema.ShouldBe("archive");
        order.Table.ShouldBe("orders_v2");
        order.Members.Select(m => m.ColumnName).ShouldBe(["Id", "cust", "Total", "Status"]);
    }

    [Test]
    public void Not_mapped_and_getter_only_members_are_handled()
    {
        var widget = Single<Widget>(t => t.Add<Widget>());

        widget.Members.ShouldNotContain(m => m.PropertyName == "Scratch");
        // Getter-only members land in the record but are never assigned.
        widget.Members.Single(m => m.PropertyName == "Derived").CanSet.ShouldBeFalse();
        widget.Members.Single(m => m.PropertyName == "DisplayName").CanSet.ShouldBeTrue();
    }

    [Test]
    public void HasKey_sets_a_composite_key_in_argument_order()
    {
        var line = Single<OrderLine>(t => t.Add<OrderLine>().HasKey(l => l.LineNo, l => l.OrderId));

        line.Key.Select(k => k.PropertyName).ShouldBe(["LineNo", "OrderId"]);
        line.Members.Where(m => m.IsKey).Select(m => m.PropertyName).ShouldBe(["OrderId", "LineNo"]);
    }

    [Test]
    public void Key_attributes_order_by_column_order_then_declaration()
    {
        var keyed = Single<KeyedByAttribute>(t => t.Add<KeyedByAttribute>());

        keyed.Key.Select(k => k.PropertyName).ShouldBe(["Code", "Region"]);
    }

    [Test]
    public void The_type_name_id_convention_applies()
    {
        Single<Invoice>(t => t.Add<Invoice>()).Key.Single().PropertyName.ShouldBe("InvoiceId");
    }

    [Test]
    public void A_type_without_a_key_is_rejected()
    {
        var ex = Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<NoKey>()));
        ex.Message.ShouldContain("[Key]");
        ex.Message.ShouldContain("HasKey");
    }

    [Test]
    public void HasKey_must_name_a_mapped_property()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<Widget>().HasKey(w => w.Scratch)))
            .Message.ShouldContain("Scratch");
    }

    [Test]
    public void A_key_outside_the_cursor_types_is_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<UnsupportedKey>()))
            .Message.ShouldContain("backfill cursor");
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<EnumKey>()))
            .Message.ShouldContain("backfill cursor");
    }

    [Test]
    public void Nested_objects_and_collections_are_rejected_with_the_remedy()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<NestedProperty>()))
            .Message.ShouldContain("[NotMapped]");
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<ListProperty>()))
            .Message.ShouldContain("JSON columns are not supported");
    }

    [Test]
    public void Positional_records_bind_their_primary_constructor()
    {
        var order = Single<Order>(t => t.Add<Order>());

        order.Constructor.ParameterMembers.Length.ShouldBe(5);
        order.Constructor.ParameterMembers.Select(i => order.Members[i].PropertyName)
            .ShouldBe(["Id", "CustomerRef", "Total", "Status", "PaidAt"]);
    }

    [Test]
    public void An_ignored_positional_parameter_is_left_unbound()
    {
        var order = Single<Order>(t => t.Add<Order>().Ignore(o => o.PaidAt));

        order.Constructor.ParameterMembers.ShouldBe([0, 1, 2, 3, -1]);
    }

    [Test]
    public void Classes_with_a_parameterless_constructor_use_it()
    {
        Single<Widget>(t => t.Add<Widget>()).Constructor.ParameterMembers.ShouldBeEmpty();
    }

    [Test]
    public void Ambiguous_or_unmatched_constructors_are_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<TwoCtors>()))
            .Message.ShouldContain("2 match");
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<UnmatchedCtor>()))
            .Message.ShouldContain("0 match");
    }

    [Test]
    public void Registering_a_type_twice_is_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t => { t.Add<Widget>(); t.Add<Widget>(); }))
            .Message.ShouldContain("already registered");
    }

    [Test]
    public void Two_types_on_one_table_are_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t =>
        {
            t.Add<Widget>();
            t.Add<Invoice>().ToTable("Widget");
        })).Message.ShouldContain("both map to table");
    }

    [Test]
    public void Fluent_selectors_must_name_a_property_directly()
    {
        Should.Throw<WallabyConfigurationException>(() => Build(t => t.Add<Widget>().Ignore(w => w.DisplayName.Length)))
            .Message.ShouldContain("directly on the entity");
    }
}
