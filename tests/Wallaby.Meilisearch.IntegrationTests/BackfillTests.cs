using EFCore.CDC.TestModel;
using Wallaby.Abstractions;
using Wallaby.Meilisearch.IntegrationTests.Infrastructure;
using Wallaby.Sinks.Meilisearch;
using Wallaby.TestInfrastructure;

namespace Wallaby.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class BackfillTests(PostgresFixture pg, MeilisearchFixture meili)
{
    private CdcTestHarness NewHarness(out string index, int chunkSize = 3)
    {
        var harness = CdcTestHarness.ForTestModel(pg.ConnectionString);
        harness.ChunkSize = chunkSize;
        index = harness.Names.Named("products");
        // A unique backfill version isolates this test from the shared wallaby.backfill_state for public.products
        // (otherwise the scheduler would skip an already-"Completed" table from a prior test).
        harness.AddSink(new MeilisearchSink("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey }))
            .Project<Product>("meili", index, p => new CdcDocument { ["name"] = p.Name },
                backfill: true, backfillVersion: harness.Names.Suffix);
        return harness;
    }

    // The plain "seed before the slot exists, backfill indexes everything" path is covered by the
    // first pass of BackfillSchedulerIntegrationTests.Scheduler_re_backfills_on_version_change_and_on_manual_request.

    [Test]
    public async Task Backfill_with_concurrent_writes_converges_to_final_table_state()
    {
        await using var harness = NewHarness(out var index);

        var categoryId = await harness.Db.AddCategoryAsync();
        var seeded = await harness.Db.AddProductsAsync(categoryId, Enumerable.Range(0, 20).Select(i => $"v0_{i}").ToArray());

        await harness.SelfConfigureAsync();

        // Final expected state after the concurrent writer runs.
        var expected = new Dictionary<int, string?>();
        for (var i = 0; i < 20; i++)
        {
            expected[seeded[i].Id] = i < 8 ? $"upd_{i}" : i >= 15 ? null : seeded[i].Name;
        }

        await harness.StartAsync();
        var backfill = harness.RunBackfillAsync(); // run concurrently with the writer below

        for (var i = 0; i < 8; i++)
        {
            await harness.Db.UpdateProductNameAsync(seeded[i].Id, $"upd_{i}");
        }
        for (var i = 15; i < 20; i++)
        {
            await harness.Db.DeleteProductAsync(seeded[i].Id);
        }

        var probe = new MeiliProbe(meili);
        await harness.WaitUntilAsync(async () =>
        {
            foreach (var (id, name) in expected)
            {
                if (await probe.NameAsync(index, id) != name) return false;
            }
            return true;
        }, TimeSpan.FromSeconds(90));
        await backfill;

        foreach (var (id, name) in expected)
        {
            await Assert.That(await probe.NameAsync(index, id)).IsEqualTo(name);
        }
    }
}
