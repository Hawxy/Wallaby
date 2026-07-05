using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Wallaby.Abstractions;
using Wallaby.Meilisearch.IntegrationTests.Infrastructure;
using Wallaby.Sinks.Meilisearch;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class BackfillSchedulerIntegrationTests(TestModelPostgresFixture pg, MeilisearchFixture meili)
{
    [Test]
    public async Task Scheduler_re_backfills_on_version_change_and_on_manual_request()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        harness.ChunkSize = 2;
        var index = harness.Names.Named("products");
        harness.AddSink(new MeilisearchSink("meili", new MeilisearchSinkOptions { Host = meili.Host, ApiKey = meili.ApiKey }))
            .Project<Product>("meili", index, p => new WallabyDocument { ["name"] = p.Name }, backfill: true);

        var categoryId = await harness.Db.AddCategoryAsync();
        var ids = await harness.Db.AddProductsAsync(categoryId, Enumerable.Range(0, 6).Select(i => $"s{i}").ToArray());

        await harness.SelfConfigureAsync();

        using var rows = new MetricCollector<long>(harness.Instrumentation.Meter, "wallaby.backfill.rows");
        using var active = new MetricCollector<int>(harness.Instrumentation.Meter, "wallaby.backfill.active");

        await harness.StartAsync();

        var probe = new MeiliProbe(meili);
        Task AllIndexed() => harness.WaitUntilAsync(async () =>
        {
            foreach (var (id, name) in ids)
            {
                if (await probe.NameAsync(index, id) != name) return false;
            }
            return true;
        });
        Task IndexEmpty() => harness.WaitUntilAsync(async () => await probe.NameAsync(index, ids[0].Id) is null);

        // Pass 1 (v1): a new table auto-backfills.
        await harness.RunBackfillAsync("v1");
        await AllIndexed();

        // The pass recorded the copied rows and moved the active-backfill gauge.
        rows.GetMeasurementSnapshot().Sum(m => m.Value).ShouldBeGreaterThanOrEqualTo(6L);
        active.GetMeasurementSnapshot().Any(m => m.Value == 1).ShouldBeTrue();

        // Version change (v2) re-backfills.
        await probe.DropAsync(index);
        await IndexEmpty();
        await harness.RunBackfillAsync("v2");
        await AllIndexed();

        // Manual request re-backfills (same version).
        await probe.DropAsync(index);
        await IndexEmpty();
        await harness.BackfillManager.RequestBackfillAsync<Product>();
        await harness.RunBackfillAsync("v2");
        await AllIndexed();
    }
}
