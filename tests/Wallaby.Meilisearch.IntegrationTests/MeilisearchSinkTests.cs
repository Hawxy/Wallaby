using EFCore.CDC.TestInfrastructure;
using EFCore.CDC.TestModel;
using Meilisearch;
using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Meilisearch.IntegrationTests.Infrastructure;
using Wallaby.Sinks.Meilisearch;

namespace Wallaby.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class MeilisearchSinkTests(PostgresFixture pg, MeilisearchFixture meili)
{
    private sealed record ProductRow(int Id, string Name);

    private MeilisearchSink Sink() => new("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey });

    [Test]
    public async Task Product_projection_syncs_insert_update_and_delete()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products");
        harness.AddSink(Sink())
            .Project<Product>("meili", index, p => new CdcDocument { ["name"] = p.Name });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var categoryId = await harness.Db.AddCategoryAsync();
        var id = await harness.Db.AddProductAsync(categoryId, "alpha");

        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha");

        await harness.Db.UpdateProductNameAsync(id, "alpha-v2");
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha-v2");

        await harness.Db.DeleteProductAsync(id);
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) is null);

        await Assert.That(await probe.NameAsync(index, id)).IsNull();
    }

    [Test]
    public async Task Order_aggregate_is_flattened_via_ef_joins()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("orders");
        harness.AddSink(Sink())
            .Map<Order>("meili", index, async (db, changes, ct) =>
            {
                var ids = changes.Select(c => (int)c.PrimaryKey[0]!).ToList();
                var orders = await db.Set<Order>()
                    .Where(o => ids.Contains(o.Id))
                    .Include(o => o.Customer)
                    .Include(o => o.Lines)
                    .ToListAsync(ct);

                var docs = new Dictionary<DocumentKey, CdcDocument?>();
                foreach (var o in orders)
                {
                    docs[new DocumentKey(new object?[] { o.Id })] =
                        new CdcDocument { ["customer"] = o.Customer?.Name, ["lineCount"] = o.Lines.Count };
                }
                return docs;
            });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var orderId = await harness.Db.AddOrderWithLinesAsync("Ada", lineCount: 3);

        await harness.RunUntilAsync(async () =>
        {
            var doc = await probe.GetAsync(index, orderId.ToString());
            return doc?["customer"]?.GetValue<string>() == "Ada" && doc["lineCount"]?.GetValue<int>() == 3;
        });

        var indexed = await probe.GetAsync(index, orderId.ToString());
        await Assert.That(indexed!["customer"]!.GetValue<string>()).IsEqualTo("Ada");
        await Assert.That(indexed["lineCount"]!.GetValue<int>()).IsEqualTo(3);
    }

    [Test]
    public async Task RawSql_transform_indexes_and_null_document_deletes()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products");
        harness.AddSink(Sink())
            .Map<Product>("meili", index, async (db, changes, ct) =>
            {
                var ids = changes.Select(c => (int)c.PrimaryKey[0]!).ToArray();
                var rows = await db.Database
                    .SqlQuery<ProductRow>($"SELECT \"Id\" AS \"Id\", \"Name\" AS \"Name\" FROM products WHERE \"Id\" = ANY({ids})")
                    .ToListAsync(ct);

                var docs = new Dictionary<DocumentKey, CdcDocument?>();
                foreach (var row in rows)
                {
                    docs[new DocumentKey([row.Id])] =
                        row.Name == "skip" ? null : new CdcDocument { ["name"] = row.Name };
                }
                return docs;
            });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var categoryId = await harness.Db.AddCategoryAsync();
        var keepId = await harness.Db.AddProductAsync(categoryId, "keep");
        var skipId = await harness.Db.AddProductAsync(categoryId, "skip");

        await harness.RunUntilAsync(async () => await probe.NameAsync(index, keepId) == "keep");

        await Assert.That(await probe.NameAsync(index, keepId)).IsEqualTo("keep");
        await Assert.That(await probe.NameAsync(index, skipId)).IsNull(); // null document => not indexed
    }

    [Test]
    public async Task Declared_index_is_created_and_configured_on_start()
    {
        await using var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products_cfg");

        var options = new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey };
        options.ConfigureIndex(index,s =>
        {
            s.SearchableAttributes = ["name"];
            s.FilterableAttributes = ["category"];
            s.SortableAttributes = ["price"];
        });
        harness.AddSink(new MeilisearchSink("meili", options))
            .Project<Product>("meili", index, p => new CdcDocument { ["name"] = p.Name });

        await harness.SelfConfigureAsync();
        // ISinkInitializer runs during StartAsync, before any change is streamed — so the index is
        // created and configured without a single document being delivered.
        await harness.StartAsync();
        try
        {
            var probe = new MeiliProbe(meili);
            await Assert.That(await probe.IndexExistsAsync(index)).IsTrue();
            await Assert.That(await probe.PrimaryKeyAsync(index)).IsEqualTo("id");

            var settings = await probe.SettingsAsync(index);
            await Assert.That(settings.FilterableAttributes).Contains("category");
            await Assert.That(settings.SortableAttributes).Contains("price");
        }
        finally
        {
            await harness.StopAsync();
        }
    }
}
