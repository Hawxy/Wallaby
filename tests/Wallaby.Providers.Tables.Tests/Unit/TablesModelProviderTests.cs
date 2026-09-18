using System.Linq.Expressions;
using Wallaby.Providers.Tables.Internal;

namespace Wallaby.Providers.Tables.Tests.Unit;

/// <summary>
/// Capture-plan derivation from the registrations: declared entities only, spec flags, column selection
/// semantics, and the rejections (DependsOn, tenant scoping, unregistered types).
/// </summary>
public class TablesModelProviderTests
{
    private static TablesModelProvider Provider()
    {
        var builder = new TablesModelBuilder();
        builder.Add<Order>();
        builder.Add<Widget>();
        builder.Add<OrderLine>().HasKey(l => l.OrderId, l => l.LineNo);
        return new TablesModelProvider(builder.Build());
    }

    [Test]
    public void Captures_only_the_declared_entities()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Order)] });

        var table = plan.Model.Tables.ShouldHaveSingleItem();
        table.EntityClrType.ShouldBe(typeof(Order));
        table.QualifiedName.ShouldBe("sales.orders");
        table.Columns.Select(c => c.ColumnName).ShouldBe(["Id", "customer_ref", "Total", "Status", "PaidAt"]);
        table.PrimaryKey.Single().ColumnName.ShouldBe("Id");
        table.PrimaryKey.Single().ClrType.ShouldBe(typeof(int));
        table.ColumnsNarrowed.ShouldBeFalse();
        plan.Model.DependentBindings.ShouldBeEmpty();
    }

    [Test]
    public void Composite_keys_keep_their_declared_order()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(OrderLine)] });

        plan.Model.Tables.Single().PrimaryKey.Select(c => c.ColumnName).ShouldBe(["OrderId", "LineNo"]);
    }

    [Test]
    public void Spec_flags_flow_to_the_table()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            RequiresFullReplicaIdentity = new HashSet<Type> { typeof(Order) },
            RequiresMaterializedEntity = new HashSet<Type> { typeof(Order) },
        });

        var table = plan.Model.Tables.Single();
        table.RequiresFullReplicaIdentity.ShouldBeTrue();
        table.RequiresMaterializedEntity.ShouldBeTrue();
    }

    [Test]
    public void Consumes_keeps_the_key_and_narrows_the_columns()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Order)] = [new ColumnSelection(ColumnSelectionMode.Include, ["Total"])],
            },
        });

        var table = plan.Model.Tables.Single();
        table.Columns.Select(c => c.ColumnName).ShouldBe(["Id", "Total"]);
        table.ColumnsNarrowed.ShouldBeTrue();
    }

    [Test]
    public void Selections_union_across_mappings()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Order)] =
                [
                    new ColumnSelection(ColumnSelectionMode.Include, ["Total"]),
                    new ColumnSelection(ColumnSelectionMode.Exclude, ["PaidAt", "Status"]),
                ],
            },
        });

        plan.Model.Tables.Single().Columns.Select(c => c.ColumnName).ShouldBe(["Id", "customer_ref", "Total"]);
    }

    [Test]
    public void Excluding_a_key_property_is_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Order)] = [new ColumnSelection(ColumnSelectionMode.Exclude, ["Id"])],
            },
        })).Message.ShouldContain("cannot exclude key property 'Id'");
    }

    [Test]
    public void An_unknown_property_in_a_selection_is_rejected()
    {
        Should.Throw<WallabyConfigurationException>(() => Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            DeclaredColumnSelections = new Dictionary<Type, IReadOnlyList<ColumnSelection>>
            {
                [typeof(Order)] = [new ColumnSelection(ColumnSelectionMode.Include, ["Nope"])],
            },
        })).Message.ShouldContain("'Nope'");
    }

    [Test]
    public void DependsOn_is_rejected()
    {
        Expression<Func<Order, Widget>> navigation = _ => null!;
        Should.Throw<WallabyConfigurationException>(() => Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>> { [typeof(Order)] = [navigation] },
        })).Message.ShouldContain("DependsOn");
    }

    [Test]
    public void Tenant_scoping_is_rejected_with_the_alternative()
    {
        Should.Throw<WallabyConfigurationException>(() => Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(Order)],
            RequiresTenantColumn = new HashSet<Type> { typeof(Order) },
        })).Message.ShouldContain("ScopedBy(e => e.TenantId)");
    }

    [Test]
    public void Unregistered_types_fail_everywhere()
    {
        var provider = Provider();

        provider.Handles(typeof(Unregistered)).ShouldBeFalse();
        Should.Throw<WallabyConfigurationException>(() => provider.ResolveTable(typeof(Unregistered)))
            .Message.ShouldContain("Add<Unregistered>()");
        Should.Throw<WallabyConfigurationException>(() =>
            provider.BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Unregistered)] }));
    }

    [Test]
    public void Registered_types_resolve_their_tables()
    {
        var provider = Provider();

        provider.Handles(typeof(Widget)).ShouldBeTrue();
        provider.ResolveTable(typeof(Order)).ShouldBe(new QualifiedTable("sales", "orders"));
        provider.ResolveAllTables().ShouldBe(
        [
            new QualifiedTable("sales", "orders"),
            new QualifiedTable("public", "Widget"),
            new QualifiedTable("public", "OrderLine"),
        ], ignoreOrder: true);
    }
}
