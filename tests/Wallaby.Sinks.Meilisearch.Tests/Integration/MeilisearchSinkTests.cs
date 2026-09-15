using Meilisearch;
using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;
using Wallaby.Sinks.Meilisearch;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Sinks.Meilisearch.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class MeilisearchSinkTests(TestModelPostgresFixture pg, MeilisearchFixture meili)
{
    private sealed record ProductRow(int Id, string Name);

    private MeilisearchSink Sink() => TestMeilisearchSink.Create("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey });

    [Test]
    public async Task Product_projection_syncs_insert_update_and_delete()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products");
        harness.AddSink(Sink())
            .Project<Product>("meili", index, p => new WallabyDocument { ["name"] = p.Name });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var categoryId = await harness.Db.AddCategoryAsync();
        var id = await harness.Db.AddProductAsync(categoryId, "alpha");

        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha");

        await harness.Db.UpdateProductNameAsync(id, "alpha-v2");
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha-v2");

        await harness.Db.DeleteProductAsync(id);
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) is null);

        (await probe.NameAsync(index, id)).ShouldBeNull();
    }

    [Test]
    public async Task Order_aggregate_is_flattened_via_ef_joins()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
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

                var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var o in orders)
                {
                    docs[new DocumentKey(new object?[] { o.Id })] =
                        new WallabyDocument { ["customer"] = o.Customer?.Name, ["lineCount"] = o.Lines.Count };
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
        indexed!["customer"]!.GetValue<string>().ShouldBe("Ada");
        indexed["lineCount"]!.GetValue<int>().ShouldBe(3);
    }

    [Test]
    public async Task RawSql_transform_indexes_and_null_document_deletes()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products");
        harness.AddSink(Sink())
            .Map<Product>("meili", index, async (db, changes, ct) =>
            {
                var ids = changes.Select(c => (int)c.PrimaryKey[0]!).ToArray();
                var rows = await db.Database
                    .SqlQuery<ProductRow>($"SELECT \"Id\" AS \"Id\", \"Name\" AS \"Name\" FROM products WHERE \"Id\" = ANY({ids})")
                    .ToListAsync(ct);

                var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var row in rows)
                {
                    docs[new DocumentKey([row.Id])] =
                        row.Name == "skip" ? null : new WallabyDocument { ["name"] = row.Name };
                }
                return docs;
            });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var categoryId = await harness.Db.AddCategoryAsync();
        var keepId = await harness.Db.AddProductAsync(categoryId, "keep");
        var skipId = await harness.Db.AddProductAsync(categoryId, "skip");

        await harness.RunUntilAsync(async () => await probe.NameAsync(index, keepId) == "keep");

        (await probe.NameAsync(index, keepId)).ShouldBe("keep");
        (await probe.NameAsync(index, skipId)).ShouldBeNull(); // null document => not indexed
    }

    [Test]
    public async Task Document_missing_a_configured_attribute_fails_permanently()
    {
        // Validation is on by default.
        var options = new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey };
        options.ConfigureIndex("products_validated", s =>
        {
            s.SearchableAttributes = ["name"];
            s.FilterableAttributes = ["category"];
        });
        var sink = TestMeilisearchSink.Create("meili", options);

        var meta = new ChangeMetadata("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, false);
        // The document carries "name" but not the configured filterable "category".
        var record = new SinkRecord("products_validated", "1", new WallabyDocument { ["name"] = "alpha" }, IsDeletion: false, meta);

        var result = await sink.DeliverAsync(new SinkBatch("meili", [record]), CancellationToken.None);

        // A configured-but-absent attribute is not retryable — it must fail permanently (before any network call).
        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("category");
    }

    [Test]
    public async Task Granular_filterable_attribute_pattern_is_validated_like_a_plain_name()
    {
        // The Meilisearch 0.20 granular form (opting comparison/facet-search out) uses AttributePatterns
        // instead of a plain string. A wildcard-free pattern is a concrete field name, so it must still be
        // validated like the legacy string form.
        var options = new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey };
        options.ConfigureIndex("products_granular", s =>
        {
            s.SearchableAttributes = ["name"];
            s.FilterableAttributes =
            [
                new FilterableAttribute
                {
                    AttributePatterns = ["category"],
                    Features = new FilterableAttributeFeatures
                    {
                        FacetSearch = false,
                        Filter = new FilterableAttributeFilterFeatures { Equality = true, Comparison = false },
                    },
                },
            ];
        });
        var sink = TestMeilisearchSink.Create("meili", options);

        var meta = new ChangeMetadata("public", "products", ChangeAction.Insert, DateTimeOffset.UtcNow, 1, 0, false);
        // The document carries "name" but not the granular filterable "category".
        var record = new SinkRecord("products_granular", "1", new WallabyDocument { ["name"] = "alpha" }, IsDeletion: false, meta);

        var result = await sink.DeliverAsync(new SinkBatch("meili", [record]), CancellationToken.None);

        result.Status.ShouldBe(DeliveryStatus.PermanentFailure);
        result.Error!.ShouldContain("category");
    }

    [Test]
    public async Task Document_with_all_configured_attributes_passes_validation_and_indexes()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products_validated_ok");

        // Validation is on by default; the projection emits every configured attribute.
        var options = new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey };
        options.ConfigureIndex(index, s => s.FilterableAttributes = ["category"]);
        harness.AddSink(TestMeilisearchSink.Create("meili", options))
            .Project<Product>("meili", index, p => new WallabyDocument { ["name"] = p.Name, ["category"] = p.CategoryId });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var categoryId = await harness.Db.AddCategoryAsync();
        var id = await harness.Db.AddProductAsync(categoryId, "alpha");

        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha");

        (await probe.NameAsync(index, id)).ShouldBe("alpha");
    }

    [Test]
    public async Task Configured_embedder_applies_and_user_provided_vectors_index()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products_vec");

        // Embedders ride the same settings update the initializer already applies.
        var options = new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey };
        options.ConfigureIndex(index, s => s.Embedders = new Dictionary<string, Embedder>
        {
            ["default"] = new Embedder { Source = EmbedderSource.UserProvided, Dimensions = 3 },
        });
        harness.AddSink(TestMeilisearchSink.Create("meili", options))
            .Project<Product>("meili", index, p => new WallabyDocument
            {
                ["name"] = p.Name,
                ["_vectors"] = new Dictionary<string, object?> { ["default"] = new[] { 0.1f, 0.2f, 0.3f } },
            });
        await harness.SelfConfigureAsync();

        var probe = new MeiliProbe(meili);
        var categoryId = await harness.Db.AddCategoryAsync();
        var id = await harness.Db.AddProductAsync(categoryId, "alpha");

        // A rejected _vectors payload would fail the enqueue task and nothing would index.
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha");

        var embedders = await probe.EmbeddersAsync(index);
        embedders.ShouldContainKey("default");
        embedders["default"].Source.ShouldBe(EmbedderSource.UserProvided);
    }

    [Test]
    public async Task Declared_index_is_created_and_configured_on_start()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products_cfg");

        var options = new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey };
        options.ConfigureIndex(index,s =>
        {
            s.SearchableAttributes = ["name"];
            s.FilterableAttributes = ["category"];
            s.SortableAttributes = ["price"];
        });
        harness.AddSink(TestMeilisearchSink.Create("meili", options))
            .Project<Product>("meili", index, p => new WallabyDocument { ["name"] = p.Name });

        await harness.SelfConfigureAsync();
        // ISinkInitializer runs during StartAsync, before any change is streamed — so the index is
        // created and configured without a single document being delivered.
        await harness.StartAsync();
        try
        {
            var probe = new MeiliProbe(meili);
            (await probe.IndexExistsAsync(index)).ShouldBeTrue();
            (await probe.PrimaryKeyAsync(index)).ShouldBe("id");

            var settings = await probe.SettingsAsync(index);
            // Meilisearch 0.20: FilterableAttributes are FilterableAttribute objects, compare on the name.
            settings.FilterableAttributes.Select(f => f.Attribute).ShouldContain("category");
            settings.SortableAttributes.ShouldContain("price");
        }
        finally
        {
            await harness.StopAsync();
        }
    }
}
