using EFCore.CDC.TestInfrastructure;
using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Wallaby.Abstractions;
using Wallaby.Meilisearch.IntegrationTests.Infrastructure;
using Wallaby.Sinks.Meilisearch;

namespace Wallaby.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class DependentFanoutTests(PostgresFixture pg, MeilisearchFixture meili)
{
    private CdcTestHarness NewHarness(out string index)
    {
        var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        index = harness.Names.Named("products_fanout");

        harness.AddSink(new MeilisearchSink("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey }));
        harness.Map<Product>("meili", index, async (db, changes, ct) =>
        {
            var ids = changes.Where(c => c.Entity is not null).Select(c => (int)c.PrimaryKey[0]!).ToList();
            var products = await db.Set<Product>()
                .Include(p => p.Category)
                .Include(p => p.Labels)
                .Where(p => ids.Contains(p.Id))
                .ToListAsync(ct);

            var documents = new Dictionary<DocumentKey, CdcDocument?>();
            foreach (var product in products)
            {
                documents[new DocumentKey(product.Id)] = new CdcDocument
                {
                    ["name"] = product.Name,
                    ["category"] = product.Category?.Name,
                    ["labels"] = product.Labels.OrderBy(l => l.Id).Select(l => l.Name).ToList(),
                };
            }
            return documents;
        });

        harness.DependsOn<Product, Category?>(p => p.Category);
        harness.DependsOn<Product, List<Label>>(p => p.Labels);
        return harness;
    }

    [Test]
    public async Task Updating_a_referenced_principal_fans_out_to_its_dependents()
    {
        await using var harness = NewHarness(out var index);

        var categoryId = await harness.Db.AddCategoryAsync("OriginalCat");
        var product1 = await harness.Db.AddProductAsync(categoryId, "P1");
        var product2 = await harness.Db.AddProductAsync(categoryId, "P2");

        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        // Touch the products once so they land in the index (no backfill in this test).
        await harness.Db.UpdateProductNameAsync(product1, "P3");
        await harness.Db.UpdateProductNameAsync(product2, "P4");

        var probe = new MeiliProbe(meili);
        await harness.WaitUntilAsync(async () =>
                (await probe.GetAsync(index, product1.ToString()))?["category"]?.GetValue<string>() == "OriginalCat"
             && (await probe.GetAsync(index, product2.ToString()))?["category"]?.GetValue<string>() == "OriginalCat",
            TimeSpan.FromSeconds(60));

        // Update the Category alone — the dependent products must re-emit with the new name.
        await harness.Db.SetCategoryNameAsync(categoryId, "RenamedCat");

        await harness.WaitUntilAsync(async () =>
                (await probe.GetAsync(index, product1.ToString()))?["category"]?.GetValue<string>() == "RenamedCat"
             && (await probe.GetAsync(index, product2.ToString()))?["category"]?.GetValue<string>() == "RenamedCat",
            TimeSpan.FromSeconds(60));
    }

    [Test]
    public async Task Adding_and_removing_a_skip_navigation_link_re_emits_the_primary()
    {
        await using var harness = NewHarness(out var index);

        var categoryId = await harness.Db.AddCategoryAsync();
        var productId = await harness.Db.AddProductAsync(categoryId, "P-labels");
        var labelA = await harness.Db.AddLabelAsync("alpha");
        var labelB = await harness.Db.AddLabelAsync("beta");

        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        // Seed: the product starts with no labels. Touch it once so the document exists in the sink.
        await harness.Db.UpdateProductNameAsync(productId, "P-labels-2");
        var probe = new MeiliProbe(meili);
        await harness.WaitUntilAsync(async () =>
                (await probe.GetAsync(index, productId.ToString()))?["labels"] is { } labels && labels.AsArray().Count == 0,
            TimeSpan.FromSeconds(60));

        // Attach label A — change is to the join table only; fan-out must re-emit the product.
        await harness.Db.AttachLabelAsync(productId, labelA);
        await harness.WaitUntilAsync(async () =>
                IndexedLabels(await probe.GetAsync(index, productId.ToString())) is { } current
                && current.SequenceEqual(new[] { "alpha" }),
            TimeSpan.FromSeconds(60));

        // Attach label B.
        await harness.Db.AttachLabelAsync(productId, labelB);
        await harness.WaitUntilAsync(async () =>
                IndexedLabels(await probe.GetAsync(index, productId.ToString())) is { } current
                && current.SequenceEqual(new[] { "alpha", "beta" }),
            TimeSpan.FromSeconds(60));

        // Detach label A.
        await harness.Db.DetachLabelAsync(productId, labelA);
        await harness.WaitUntilAsync(async () =>
                IndexedLabels(await probe.GetAsync(index, productId.ToString())) is { } current
                && current.SequenceEqual(new[] { "beta" }),
            TimeSpan.FromSeconds(60));
    }

    [Test]
    public async Task Wide_fanout_indexes_the_first_page_inline_then_offloads_the_tail()
    {
        await using var harness = NewHarness(out var index);
        harness.MaxBatchSize = 5;
        harness.ChunkSize = 5;

        var categoryId = await harness.Db.AddCategoryAsync("OriginalCat");
        // Seed before self-config so the inserts are not streamed; only the rename below is captured.
        var products = await harness.Db.AddProductsAsync(categoryId, Enumerable.Range(0, 12).Select(i => $"wp{i}").ToArray());
        var first = products[0].Id;
        var last = products[^1].Id;

        await harness.SelfConfigureAsync();
        await harness.StartAsync();

        var probe = new MeiliProbe(meili);

        // Rename the category alone. The first page (5 lowest-id products) re-indexes inline; the rest is offloaded.
        await harness.Db.SetCategoryNameAsync(categoryId, "RenamedCat");
        await harness.WaitUntilAsync(async () =>
                (await probe.GetAsync(index, first.ToString()))?["category"]?.GetValue<string>() == "RenamedCat",
            TimeSpan.FromSeconds(60));

        // The trigger transaction is acknowledged (slot advanced) while the tail is still queued, not indexed.
        await harness.WaitUntilAsync(() => harness.LastAcknowledgedLsn > 0, TimeSpan.FromSeconds(10));
        await Assert.That(await probe.GetAsync(index, last.ToString())).IsNull();
        await Assert.That(await harness.PendingFanoutJobCountAsync()).IsEqualTo(1);

        // Draining the offloaded job re-indexes every remaining product with the new category.
        await harness.DrainFanoutAsync();
        await harness.WaitUntilAsync(async () =>
                (await probe.GetAsync(index, last.ToString()))?["category"]?.GetValue<string>() == "RenamedCat",
            TimeSpan.FromSeconds(60));

        foreach (var (id, _) in products)
        {
            await Assert.That((await probe.GetAsync(index, id.ToString()))?["category"]?.GetValue<string>())
                .IsEqualTo("RenamedCat");
        }
    }

    private static IReadOnlyList<string>? IndexedLabels(System.Text.Json.Nodes.JsonObject? document)
        => document?["labels"]?.AsArray().Select(n => n!.GetValue<string>()).ToList();
}
