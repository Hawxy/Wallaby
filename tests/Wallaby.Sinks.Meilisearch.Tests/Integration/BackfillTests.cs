using Wallaby.Abstractions;
using Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;
using Wallaby.Sinks.Meilisearch;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Sinks.Meilisearch.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class BackfillTests(TestModelPostgresFixture pg, MeilisearchFixture meili)
{
    private WallabyTestHarness NewHarness(out string index, int chunkSize = 3)
    {
        var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.ChunkSize = chunkSize;
        index = harness.Names.Named("products");
        // A unique backfill version isolates this test from the shared wallaby.backfill_state for public.products
        // (otherwise the scheduler would skip an already-"Completed" table from a prior test).
        harness.AddSink(TestMeilisearchSink.Create("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey }))
            .Project<Product>("meili", index, p => new WallabyDocument { ["name"] = p.Name },
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
            (await probe.NameAsync(index, id)).ShouldBe(name);
        }
    }
}
