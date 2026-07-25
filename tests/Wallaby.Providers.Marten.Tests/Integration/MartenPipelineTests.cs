using Marten;
using Npgsql;
using Wallaby.Abstractions;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.Marten;

namespace Wallaby.Providers.Marten.Tests.Integration;

/// <summary>
/// End-to-end capture of Marten documents: JSONB rehydration through the store's serializer, hard and
/// soft deletes (both surface as sink deletions), the undelete TOAST fallback, and backfill semantics.
/// </summary>
[NotInParallel]
[ClassDataSource<MartenStoreFixture>(Shared = SharedType.PerTestSession)]
public class MartenPipelineTests(MartenStoreFixture pg)
{
    private static string? NameOf(SinkRecord r) => r.Document?.GetValueOrDefault("name") as string;

    [Test]
    public async Task Insert_update_and_hard_delete_flow_to_the_sink()
    {
        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<Widget>("capture", "widgets", w => new WallabyDocument { ["name"] = w.Name, ["qty"] = w.Qty });
        await harness.SelfConfigureAsync();

        var id = Guid.NewGuid();
        await using (var session = pg.Store.LightweightSession())
        {
            session.Store(new Widget { Id = id, Name = "kanga", Qty = 1 });
            await session.SaveChangesAsync();
        }

        await harness.StartAsync();
        await harness.WaitUntilAsync(() => capture.For("mt_doc_widget").Any(r => NameOf(r) == "kanga"));

        await using (var session = pg.Store.LightweightSession())
        {
            session.Store(new Widget { Id = id, Name = "roo", Qty = 2 });
            await session.SaveChangesAsync();
        }
        await harness.WaitUntilAsync(() => capture.For("mt_doc_widget").Any(r => NameOf(r) == "roo"));

        await using (var session = pg.Store.LightweightSession())
        {
            session.Delete<Widget>(id);
            await session.SaveChangesAsync();
        }
        await harness.WaitUntilAsync(() => capture.For("mt_doc_widget").Any(r => r.IsDeletion));
        await harness.StopAsync();

        // The document's history resolves to a deletion, and the update carried the re-serialized doc.
        var latest = capture.LatestByDocumentId()[id.ToString()];
        latest.IsDeletion.ShouldBeTrue();
        capture.For("mt_doc_widget").Count(r => NameOf(r) == "roo" && r.Document?["qty"] is 2).ShouldBe(1);
    }

    [Test]
    public async Task Soft_delete_removes_the_sink_document_and_undelete_restores_it()
    {
        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<SoftWidget>(
            "capture", "softwidgets", w => new WallabyDocument { ["name"] = w.Name, ["payload"] = w.Payload });
        await harness.SelfConfigureAsync();

        // A payload large enough to TOAST, so the undelete UPDATE (which doesn't assign data) omits the
        // body from the new tuple and the materializer must fall back to the old tuple (RI FULL).
        var id = Guid.NewGuid();
        var payload = new string('x', 16_000);
        await using (var session = pg.Store.LightweightSession())
        {
            session.Store(new SoftWidget { Id = id, Name = "soft", Payload = payload });
            await session.SaveChangesAsync();
        }

        await harness.StartAsync();
        await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => NameOf(r) == "soft"));

        await using (var session = pg.Store.LightweightSession())
        {
            session.Delete<SoftWidget>(id);
            await session.SaveChangesAsync();
        }
        await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => r.IsDeletion));

        await using (var session = pg.Store.LightweightSession())
        {
            session.UndoDeleteWhere<SoftWidget>(w => w.Id == id);
            await session.SaveChangesAsync();
        }
        await harness.WaitUntilAsync(() =>
            capture.For("mt_doc_softwidget").Count(r => NameOf(r) == "soft" && !r.IsDeletion) >= 2);
        await harness.StopAsync();

        // The restored document round-tripped the full TOASTed body from the old tuple.
        var restored = capture.For("mt_doc_softwidget").Last(r => !r.IsDeletion);
        restored.Document!["payload"].ShouldBe(payload);
        capture.LatestByDocumentId()[id.ToString()].IsDeletion.ShouldBeFalse();
    }

    [Test]
    public async Task Backfill_reads_existing_documents_but_skips_soft_deleted_ones()
    {
        // Seeded BEFORE the slot exists — only a backfill can deliver these.
        var aliveId = Guid.NewGuid();
        await using (var session = pg.Store.LightweightSession())
        {
            session.Store(new SoftWidget { Id = aliveId, Name = "alive", Payload = "p" });
            session.Store(new SoftWidget { Id = Guid.NewGuid(), Name = "buried", Payload = "p" });
            await session.SaveChangesAsync();
        }
        await using (var session = pg.Store.LightweightSession())
        {
            session.DeleteWhere<SoftWidget>(w => w.Name == "buried");
            await session.SaveChangesAsync();
        }

        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        var capture = harness.AddCaptureSink();
        harness.Project<SoftWidget>(
            "capture", "softwidgets", w => new WallabyDocument { ["name"] = w.Name }, backfill: true);
        await harness.SelfConfigureAsync();

        await harness.StartAsync();
        await harness.RunBackfillAsync();
        await harness.WaitUntilAsync(() => capture.For("mt_doc_softwidget").Any(r => r.Metadata.IsBackfill));
        await harness.StopAsync();

        // Other tests may have left live documents behind (shared container); the invariants are that the
        // alive document backfills and the soft-deleted one never does.
        var backfilled = capture.For("mt_doc_softwidget").Where(r => r.Metadata.IsBackfill).ToList();
        backfilled.ShouldContain(r => r.DocumentId == aliveId.ToString() && !r.IsDeletion);
        backfilled.ShouldAllBe(r => NameOf(r) != "buried");
    }

    [Test]
    public async Task Marten_documents_publish_whole_rows()
    {
        await using var harness = WallabyTestHarness.ForMartenStore(pg.Store, pg.ConnectionString);
        harness.AddCaptureSink();
        harness.Project<Widget>("capture", "widgets", w => new WallabyDocument());
        harness.Project<SoftWidget>("capture", "softwidgets", w => new WallabyDocument());
        await harness.SelfConfigureAsync();

        var columns = new Dictionary<string, HashSet<string>?>();
        await using (var conn = new NpgsqlConnection(pg.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                """
                SELECT c.relname,
                       CASE WHEN pr.prattrs IS NULL THEN NULL
                            ELSE (SELECT array_agg(a.attname::text)
                                  FROM pg_attribute a
                                  WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs))
                       END
                FROM pg_publication p
                JOIN pg_publication_rel pr ON pr.prpubid = p.oid
                JOIN pg_class c ON c.oid = pr.prrelid
                WHERE p.pubname = @p
                """,
                conn);
            cmd.Parameters.AddWithValue("p", harness.Names.Publication);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns[reader.GetString(0)] = reader.IsDBNull(1)
                    ? null
                    : new HashSet<string>(reader.GetFieldValue<string[]>(1), StringComparer.Ordinal);
            }
        }

        // Column lists are opt-in through a column selection, and Marten rejects Consumes/
        // ConsumesAllExcept (transforms receive the whole document body), so no Marten table is ever
        // narrowed and none is listed. Unmodeled mt_* metadata reaches the wire and is dropped at
        // materialization instead, which leaves Marten's own schema management free to change those
        // columns.
        columns["mt_doc_widget"].ShouldBeNull();

        // Soft-delete documents additionally require REPLICA IDENTITY FULL (undelete TOAST fallback).
        columns["mt_doc_softwidget"].ShouldBeNull();
    }
}
