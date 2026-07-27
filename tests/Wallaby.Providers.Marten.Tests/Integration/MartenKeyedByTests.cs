using Marten;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.Marten;

namespace Wallaby.Providers.Marten.Tests.Integration;

/// <summary>
/// <c>KeyedBy(...)</c> on Marten documents: deletes rehydrate the document from the old tuple's
/// <c>data</c> (REPLICA IDENTITY FULL), so both sides of the lifecycle use the custom id and the delete
/// removes the document the upsert created.
/// </summary>
[NotInParallel]
[ClassDataSource<MartenStoreFixture>(Shared = SharedType.PerTestSession)]
public class MartenKeyedByTests(MartenStoreFixture pg)
{
    [Test]
    public async Task Hard_delete_of_a_keyed_by_document_removes_the_custom_keyed_document()
    {
        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Widget>("capture", "widgets",
            w => new WallabyDocument { ["name"] = w.Name }, keyedBy: w => w.Name);

        // KeyedBy needs the deleted document's body on the wire to compute the custom id.
        await SetReplicaIdentityAsync("mt_doc_widget", "FULL");
        try
        {
            await harness.SelfConfigureAsync();
            await harness.StartAsync();

            var id = Guid.NewGuid();
            await using (var session = pg.Store.LightweightSession())
            {
                session.Store(new Widget { Id = id, Name = "keyed_widget", Qty = 1 });
                await session.SaveChangesAsync();
            }
            await harness.WaitUntilAsync(() => capture.For("mt_doc_widget").Any(r => !r.IsDeletion));

            await using (var session = pg.Store.LightweightSession())
            {
                session.Delete<Widget>(id);
                await session.SaveChangesAsync();
            }
            await harness.WaitUntilAsync(() => capture.For("mt_doc_widget").Any(r => r.IsDeletion));
            await harness.StopAsync();

            capture.For("mt_doc_widget").Single(r => !r.IsDeletion).DocumentId.ShouldBe("keyed_widget");
            capture.For("mt_doc_widget").Single(r => r.IsDeletion).DocumentId.ShouldBe("keyed_widget");
        }
        finally
        {
            // The session database is shared; later tests expect the default identity.
            await SetReplicaIdentityAsync("mt_doc_widget", "DEFAULT");
        }
    }

    [Test]
    public async Task Soft_delete_of_a_keyed_by_document_removes_the_custom_keyed_document()
    {
        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<SoftWidget>("capture", "softwidgets",
            w => new WallabyDocument { ["name"] = w.Name }, keyedBy: w => w.Name);
        await harness.SelfConfigureAsync(); // mt_doc_softwidget is FULL via the fixture

        // A payload large enough to TOAST: the soft-delete UPDATE doesn't assign data, so the new tuple
        // omits the body and the delete's entity must come from the old tuple.
        var id = Guid.NewGuid();
        var name = $"soft_keyed_{Guid.NewGuid():N}";
        await using (var session = pg.Store.LightweightSession())
        {
            session.Store(new SoftWidget { Id = id, Name = name, Payload = new string('x', 16_000) });
            await session.SaveChangesAsync();
        }

        await harness.StartAsync();
        await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => !r.IsDeletion));

        await using (var session = pg.Store.LightweightSession())
        {
            session.Delete<SoftWidget>(id);
            await session.SaveChangesAsync();
        }
        await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => r.IsDeletion));
        await harness.StopAsync();

        capture.For("mt_doc_softwidget").Single(r => !r.IsDeletion).DocumentId.ShouldBe(name);
        capture.For("mt_doc_softwidget").Single(r => r.IsDeletion).DocumentId.ShouldBe(name);
    }

    private async Task SetReplicaIdentityAsync(string table, string identity)
    {
        await using var connection = new NpgsqlConnection(pg.ConnectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"ALTER TABLE {MartenTestStore.Schema}.{table} REPLICA IDENTITY {identity}", connection);
        await cmd.ExecuteNonQueryAsync();
    }
}
