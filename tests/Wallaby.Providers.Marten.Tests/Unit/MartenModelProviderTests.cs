using System.Linq.Expressions;
using Marten;
using Wallaby.Providers.Marten.Internal;
using Wallaby.Providers;

namespace Wallaby.Providers.Marten.Tests.Unit;

/// <summary>
/// Capture-model derivation from a Marten store's registered documents: table names/schema, the minimal
/// column set, conjoined-tenancy key order, soft-delete replica-identity flags, and the v1 rejections
/// (DependsOn, unregistered types, hierarchies). Model building never touches a database.
/// </summary>
public class MartenModelProviderTests
{
    private static MartenModelProvider Provider(Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions();
        options.Connection("Host=localhost;Database=db;Username=u;Password=p");
        options.DatabaseSchemaName = "docs";
        options.RegisterDocumentType<PlainDoc>();
        options.Schema.For<SoftDoc>().SoftDeleted();
        options.Schema.For<TenantDoc>().MultiTenanted();
        configure?.Invoke(options);
        return new MartenModelProvider(options);
    }

    private static CaptureSpec All => new() { DeclaredEntities = [typeof(PlainDoc), typeof(SoftDoc), typeof(TenantDoc)] };

    [Test]
    public void Captures_the_document_table_with_the_minimal_column_set()
    {
        var plan = Provider().BuildCapturePlan(All);

        var table = plan.Model.FindByClrType(typeof(PlainDoc)).ShouldNotBeNull();
        table.Schema.ShouldBe("docs");
        table.TableName.ShouldBe("mt_doc_plaindoc");
        table.Columns.Select(c => c.ColumnName).ShouldBe(new[] { "id", "data" });
        table.PrimaryKey.Single().ColumnName.ShouldBe("id");
        table.PrimaryKey.Single().ClrType.ShouldBe(typeof(Guid));
        table.PrimaryKey.Single().PropertyName.ShouldBe("Id");
        table.RequiresFullReplicaIdentity.ShouldBeFalse();
    }

    [Test]
    public void Soft_deleted_documents_model_the_delete_flags_and_need_full_replica_identity()
    {
        var plan = Provider().BuildCapturePlan(All);

        var table = plan.Model.FindByClrType(typeof(SoftDoc)).ShouldNotBeNull();
        table.Columns.Select(c => c.ColumnName).ShouldBe(new[] { "id", "data", "mt_deleted", "mt_deleted_at" });
        table.RequiresFullReplicaIdentity.ShouldBeTrue();
    }

    [Test]
    public void Conjoined_tenancy_puts_the_tenant_in_the_key_in_martens_column_order()
    {
        var plan = Provider().BuildCapturePlan(All);

        var table = plan.Model.FindByClrType(typeof(TenantDoc)).ShouldNotBeNull();
        table.PrimaryKey.Select(c => c.ColumnName).ShouldBe(new[] { "tenant_id", "id" });
        var tenant = table.PrimaryKey[0];
        tenant.PropertyName.ShouldBe("TenantId");
        tenant.ClrType.ShouldBe(typeof(string));
        var id = table.PrimaryKey[1];
        id.PropertyName.ShouldBe("Id");
        id.ClrType.ShouldBe(typeof(string));
    }

    [Test]
    public void The_data_column_is_flagged_to_read_as_utf8_json()
    {
        var plan = Provider().BuildCapturePlan(All);

        var table = plan.Model.FindByClrType(typeof(PlainDoc)).ShouldNotBeNull();
        table.Columns.Single(c => c.ColumnName == "data").ReadAsUtf8Json.ShouldBeTrue();
        table.Columns.Where(c => c.ColumnName != "data").ShouldAllBe(c => !c.ReadAsUtf8Json);
    }

    [Test]
    public void Declared_entities_capture_only_their_tables()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(PlainDoc)] });

        plan.Model.Tables.Select(t => t.EntityClrType).ShouldBe(new[] { typeof(PlainDoc) });
    }

    [Test]
    public void Spec_replica_identity_flags_apply_to_plain_documents()
    {
        var plan = Provider().BuildCapturePlan(new CaptureSpec
        {
            DeclaredEntities = [typeof(PlainDoc)],
            RequiresFullReplicaIdentity = new HashSet<Type> { typeof(PlainDoc) },
        });

        plan.Model.FindByClrType(typeof(PlainDoc))!.RequiresFullReplicaIdentity.ShouldBeTrue();
    }

    [Test]
    public void An_unregistered_declared_entity_fails_with_registration_guidance()
    {
        var ex = Should.Throw<WallabyConfigurationException>(
            () => Provider().BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Unregistered)] }));

        ex.Message.ShouldContain(nameof(Unregistered));
        ex.Message.ShouldContain("RegisterDocumentType");
    }

    [Test]
    public void DependsOn_is_rejected_for_documents()
    {
        Expression<Func<PlainDoc, string>> navigation = d => d.Name;
        var spec = new CaptureSpec
        {
            DeclaredEntities = [typeof(PlainDoc)],
            DeclaredDependencies = new Dictionary<Type, IReadOnlyList<LambdaExpression>>
            {
                [typeof(PlainDoc)] = [navigation],
            },
        };

        Should.Throw<WallabyConfigurationException>(() => Provider().BuildCapturePlan(spec))
            .Message.ShouldContain("DependsOn");
    }

    [Test]
    public void Document_hierarchies_are_rejected()
    {
        var provider = Provider(o => o.Schema.For<BaseDoc>().AddSubClass<SubDoc>());

        Should.Throw<WallabyConfigurationException>(
                () => provider.BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(BaseDoc)] }))
            .Message.ShouldContain("hierarchy");
    }

    [Test]
    public void Handles_claims_registered_documents_only()
    {
        var provider = Provider();

        provider.Handles(typeof(PlainDoc)).ShouldBeTrue();
        provider.Handles(typeof(TenantDoc)).ShouldBeTrue();
        provider.Handles(typeof(Unregistered)).ShouldBeFalse();

        // Probing must not register the type as a side effect (multi-provider affinity probes every mapping).
        provider.Handles(typeof(Unregistered)).ShouldBeFalse();
        Should.Throw<WallabyConfigurationException>(() => provider.ResolveTable(typeof(Unregistered)));
    }

    [Test]
    public void ResolveTable_returns_the_qualified_document_table()
    {
        var table = Provider().ResolveTable(typeof(TenantDoc));

        table.Schema.ShouldBe("docs");
        table.Table.ShouldBe("mt_doc_tenantdoc");
    }
}
