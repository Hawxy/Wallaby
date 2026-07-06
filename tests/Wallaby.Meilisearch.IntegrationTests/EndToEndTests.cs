using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Meilisearch.IntegrationTests.Infrastructure;
using Wallaby.Sinks.Meilisearch;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class EndToEndTests(TestModelPostgresFixture pg, MeilisearchFixture meili)
{
    private TestDatabase Db => new(pg.ConnectionString);

    private ServiceProvider BuildNode(WallabyNames names, string index)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // Scoped AddDbContext (no factory) — exercises Wallaby's scope fallback end-to-end: the model read and
        // per-batch enrichment contexts are resolved from a DI scope, with no IDbContextFactory registered.
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .ConfigureOptions(o =>
               {
                   o.SlotName = names.Slot;
                   o.PublicationName = names.Publication;
                   o.ChunkSize = 50;
                   o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
               })
               .AddMeilisearchSink("meili", m => { m.Host = meili.Host; m.ApiKey = meili.ApiKey; })
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination(index)
                   .WithBackfillVersion(Guid.NewGuid().ToString("N")) // unique => isolates this test's backfill state
                   .UsingTransform((_, changes, _) =>
                   {
                       var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                       foreach (var c in changes)
                       {
                           docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
                       }
                       return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
                   }));
        });
        return services.BuildServiceProvider();
    }

    private static async Task StartAsync(ServiceProvider provider)
    {
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(CancellationToken.None);
        }
    }

    private static async Task StopAsync(ServiceProvider provider)
    {
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StopAsync(CancellationToken.None);
        }
        await provider.DisposeAsync();
    }

    [Test]
    public async Task AddWallaby_indexes_changes_end_to_end()
    {
        var names = WallabyNames.Unique();
        var index = names.Named("products");
        var probe = new MeiliProbe(meili);

        var provider = BuildNode(names, index);
        await StartAsync(provider);
        try
        {
            var categoryId = await Db.AddCategoryAsync();
            var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");

            await Polling.UntilAsync(async () => await probe.NameAsync(index, id) == $"e2e_{names.Suffix}");
            (await probe.NameAsync(index, id)).ShouldBe($"e2e_{names.Suffix}");
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    [Test]
    public async Task Cluster_keeps_serving_when_a_node_stops()
    {
        var names = WallabyNames.Unique();
        var index = names.Named("products");
        var probe = new MeiliProbe(meili);

        var node1 = BuildNode(names, index);
        var node2 = BuildNode(names, index);
        await StartAsync(node1);
        await StartAsync(node2);
        try
        {
            var categoryId = await Db.AddCategoryAsync();

            // First change is served by whichever node is the leader.
            var id1 = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
            await Polling.UntilAsync(async () => await probe.NameAsync(index, id1) == $"before_{names.Suffix}");

            // Stop node1; if it was the leader, node2 must take over (single-owner slot + advisory lock).
            await StopAsync(node1);

            var id2 = await Db.AddProductAsync(categoryId, $"after_{names.Suffix}");
            await Polling.UntilAsync(async () => await probe.NameAsync(index, id2) == $"after_{names.Suffix}");
            (await probe.NameAsync(index, id2)).ShouldBe($"after_{names.Suffix}");
        }
        finally
        {
            await StopAsync(node2);
        }
    }
}
