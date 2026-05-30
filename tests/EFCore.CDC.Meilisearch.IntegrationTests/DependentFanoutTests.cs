using EFCore.CDC.Abstractions;
using EFCore.CDC.Meilisearch;
using EFCore.CDC.Meilisearch.IntegrationTests.Infrastructure;
using EFCore.CDC.TestInfrastructure;
using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using TUnit.Core.Interfaces;

namespace EFCore.CDC.Meilisearch.IntegrationTests;

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

            var documents = new Dictionary<DocumentKey, object?>();
            foreach (var product in products)
            {
                documents[new DocumentKey(product.Id)] = new Dictionary<string, object?>
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

    private static IReadOnlyList<string>? IndexedLabels(System.Text.Json.Nodes.JsonObject? document)
        => document?["labels"]?.AsArray().Select(n => n!.GetValue<string>()).ToList();
}
