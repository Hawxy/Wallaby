using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Sinks.Meilisearch.Tests.Integration.Infrastructure;
using Wallaby.Sinks.Meilisearch;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Sinks.Meilisearch.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class EndToEndTests(TestModelPostgresFixture pg, MeilisearchFixture meili)
{
    private TestDatabase Db => new(pg.ConnectionString);

    private ServiceCollection BuildServices(WallabyNames names, string index)
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
                   .UsingTransform(TestTransforms.ProductNames));
        });
        return services;
    }

    [Test]
    public async Task AddWallaby_indexes_changes_end_to_end()
    {
        var names = WallabyNames.Unique();
        var index = names.Named("products");
        var probe = new MeiliProbe(meili);

        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, index));

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");

        await Polling.UntilAsync(async () => await probe.NameAsync(index, id) == $"e2e_{names.Suffix}");
        (await probe.NameAsync(index, id)).ShouldBe($"e2e_{names.Suffix}");
    }

    [Test]
    public async Task Cluster_keeps_serving_when_a_node_stops()
    {
        var names = WallabyNames.Unique();
        var index = names.Named("products");
        var probe = new MeiliProbe(meili);

        await using var node1 = await WallabyTestNode.StartAsync(BuildServices(names, index));
        await using var node2 = await WallabyTestNode.StartAsync(BuildServices(names, index));

        var categoryId = await Db.AddCategoryAsync();

        // First change is served by whichever node is the leader.
        var id1 = await Db.AddProductAsync(categoryId, $"before_{names.Suffix}");
        await Polling.UntilAsync(async () => await probe.NameAsync(index, id1) == $"before_{names.Suffix}");

        // Stop node1; if it was the leader, node2 must take over (single-owner slot + advisory lock).
        await node1.DisposeAsync();

        var id2 = await Db.AddProductAsync(categoryId, $"after_{names.Suffix}");
        await Polling.UntilAsync(async () => await probe.NameAsync(index, id2) == $"after_{names.Suffix}");
        (await probe.NameAsync(index, id2)).ShouldBe($"after_{names.Suffix}");
    }
}
