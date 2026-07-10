using Marten;
using NSubstitute;
using Wallaby.Abstractions;
using Wallaby.Providers.Marten.Internal;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Providers.Marten.Tests.Unit;

/// <summary>
/// The materializer's decision table over decoded changes: document rehydration through the store's
/// serializer, soft-delete translation to Delete events, the backfill skip of soft-deleted rows, the
/// unchanged-TOAST fallback to the old tuple, and key-only hard deletes.
/// </summary>
public class MartenRowMaterializerTests
{
    private static readonly Guid DocId = Guid.Parse("5e6f8f4e-1111-2222-3333-444455556666");

    private static IRowMaterializer Materializer()
    {
        var options = new StoreOptions();
        options.Connection("Host=localhost;Database=db;Username=u;Password=p");
        options.DatabaseSchemaName = "docs";
        options.Schema.For<SoftDoc>().SoftDeleted();
        options.Schema.For<TenantDoc>().MultiTenanted();
        return new MartenModelProvider(options)
            .BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(SoftDoc), typeof(TenantDoc)] })
            .Materializer;
    }

    private static RawColumn Col(string name, object? value) => new() { ColumnName = name, Value = value };

    private static RawColumn Toast(string name) => new() { ColumnName = name, Value = null, IsUnchangedToast = true };

    private static RawChange Change(
        string table, ChangeAction action, RawColumn[]? newValues = null, RawColumn[]? oldValues = null) => new()
    {
        RelationId = 1,
        Schema = "docs",
        TableName = table,
        Action = action,
        NewValues = newValues ?? [],
        OldValues = oldValues,
    };

    [Test]
    public void An_insert_rehydrates_the_document_through_the_serializer()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Insert, newValues:
        [
            Col("id", DocId),
            Col("data", $$"""{"Id":"{{DocId}}","Name":"kanga"}"""),
            Col("mt_deleted", false),
            Col("mt_version", Guid.NewGuid()), // unmodeled columns on the wire are ignored
        ]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();
        row!.Action.ShouldBe(ChangeAction.Insert);
        row.Entity.ShouldBeOfType<SoftDoc>().Name.ShouldBe("kanga");
        row.Record["Id"].ShouldBe(DocId);
        row.Record["Deleted"].ShouldBe(false);
        row.PrimaryKey.ShouldBe(new object[] { DocId });
        row.EntityClrType.ShouldBe(typeof(SoftDoc));
    }

    [Test]
    public void A_string_id_arriving_as_text_is_coerced_to_the_id_type()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Insert, newValues:
        [
            Col("id", DocId.ToString()), // e.g. a spilled change round-tripped as text
            Col("data", $$"""{"Id":"{{DocId}}","Name":"kanga"}"""),
            Col("mt_deleted", false),
        ]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();
        row!.PrimaryKey.ShouldBe(new object[] { DocId });
    }

    [Test]
    public void An_update_flipping_the_soft_delete_flag_becomes_a_key_only_delete()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Update, newValues:
        [
            Col("id", DocId),
            Col("data", """{"unused":true}"""),
            Col("mt_deleted", true),
        ]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();
        row!.Action.ShouldBe(ChangeAction.Delete);
        row.Entity.ShouldBeNull();
        row.Record["Deleted"].ShouldBe(true);
        row.PrimaryKey.ShouldBe(new object[] { DocId });
    }

    [Test]
    public void A_backfill_read_of_a_soft_deleted_row_is_skipped()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Read, newValues:
        [
            Col("id", DocId),
            Col("data", """{"unused":true}"""),
            Col("mt_deleted", true),
        ]);

        Materializer().TryMaterialize(change, out _).ShouldBeFalse();
    }

    [Test]
    public void A_hard_delete_materializes_the_key_from_the_old_tuple()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Delete, oldValues: [Col("id", DocId)]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();
        row!.Action.ShouldBe(ChangeAction.Delete);
        row.Entity.ShouldBeNull();
        row.PrimaryKey.ShouldBe(new object[] { DocId });
    }

    [Test]
    public void An_unchanged_toasted_body_falls_back_to_the_old_tuple()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Update,
            newValues: [Col("id", DocId), Toast("data"), Col("mt_deleted", false)],
            oldValues: [Col("id", DocId), Col("data", $$"""{"Id":"{{DocId}}","Name":"restored"}"""), Col("mt_deleted", true)]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();
        row!.Action.ShouldBe(ChangeAction.Update);
        row.Entity.ShouldBeOfType<SoftDoc>().Name.ShouldBe("restored");
    }

    [Test]
    public void An_unavailable_body_is_a_poison_change_with_replica_identity_guidance()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Update,
            newValues: [Col("id", DocId), Toast("data"), Col("mt_deleted", false)]);

        var ex = Should.Throw<InvalidOperationException>(() => Materializer().TryMaterialize(change, out _));

        ex.Message.ShouldContain("REPLICA IDENTITY FULL");
        ex.Message.ShouldContain("https://wallabycdc.net/providers/marten/");
    }

    [Test]
    public void Malformed_json_is_a_poison_change()
    {
        var change = Change("mt_doc_softdoc", ChangeAction.Insert,
            newValues: [Col("id", DocId), Col("data", "not json"), Col("mt_deleted", false)]);

        Should.Throw<InvalidOperationException>(() => Materializer().TryMaterialize(change, out _))
            .Message.ShouldContain("deserialize");
    }

    [Test]
    public void A_conjoined_document_keys_by_tenant_then_id()
    {
        var change = Change("mt_doc_tenantdoc", ChangeAction.Insert, newValues:
        [
            Col("tenant_id", "kanga"),
            Col("id", "doc-1"),
            Col("data", """{"Id":"doc-1","Name":"pouch"}"""),
        ]);

        Materializer().TryMaterialize(change, out var row).ShouldBeTrue();
        row!.Record["TenantId"].ShouldBe("kanga");
        row.PrimaryKey.ShouldBe(new object[] { "kanga", "doc-1" });
    }

    [Test]
    public void An_unknown_table_is_a_benign_skip()
    {
        var change = Change("mt_doc_other", ChangeAction.Insert, newValues: [Col("id", DocId)]);

        Materializer().TryMaterialize(change, out _).ShouldBeFalse();
    }

    [Test]
    public async Task The_transform_invoker_hands_the_session_through_as_IQuerySession()
    {
        IQuerySession? received = null;
        var transform = new DelegateTransform<SoftDoc>((session, changes, _) =>
        {
            received = session;
            return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
                changes.ToDictionary(c => c.Key, _ => (WallabyDocument?)null));
        });
        var invoker = new MartenTransformInvoker<SoftDoc>(transform);
        var session = Substitute.For<IQuerySession>();

        await invoker.InvokeAsync(session, [], CancellationToken.None);

        received.ShouldBeSameAs(session);
    }
}
