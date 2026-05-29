using EFCore.CDC.Abstractions;
using EFCore.CDC.DependencyInjection;
using EFCore.CDC.Meilisearch;
using EFCore.CDC.Testing;
using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TUnit.Core.Interfaces;

namespace EFCore.CDC.Meilisearch.IntegrationTests;

[NotInParallel]
[ClassDataSource<PostgresFixture, MeilisearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class EndToEndTests(PostgresFixture pg, MeilisearchFixture meili)
{
    private TestDatabase Db => new(pg.ConnectionString);

    private ServiceProvider BuildNode(CdcNames names, string index)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContextFactory<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddCdc<AppDbContext>(cdc =>
        {
            cdc.UseConnectionString(pg.ConnectionString)
               .ConfigureOptions(o =>
               {
                   o.SlotName = names.Slot;
                   o.PublicationName = names.Publication;
                   o.ChunkSize = 50;
                   o.StandbyRetryInterval = TimeSpan.FromSeconds(1);
               })
               .AddMeilisearchSink("meili", m => { m.Host = meili.Host; m.ApiKey = meili.ApiKey; })
               .Map<Product>()
                   .ToSink("meili", destination: index)
                   .WithBackfillVersion(Guid.NewGuid().ToString("N")) // unique => isolates this test's backfill state
                   .UsingTransform<Dictionary<string, object?>>((_, changes, _) =>
                   {
                       var docs = new Dictionary<DocumentKey, Dictionary<string, object?>?>();
                       foreach (var c in changes)
                       {
                           docs[c.Key] = new Dictionary<string, object?> { ["name"] = c.Entity!.Name };
                       }
                       return Task.FromResult<IReadOnlyDictionary<DocumentKey, Dictionary<string, object?>?>>(docs);
                   });
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
    public async Task AddCdc_indexes_changes_end_to_end()
    {
        var names = CdcNames.Unique();
        var index = names.Named("products");
        var probe = new MeiliProbe(meili);

        var provider = BuildNode(names, index);
        await StartAsync(provider);
        try
        {
            var categoryId = await Db.AddCategoryAsync();
            var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");

            await Polling.UntilAsync(async () => await probe.NameAsync(index, id) == $"e2e_{names.Suffix}");
            await Assert.That(await probe.NameAsync(index, id)).IsEqualTo($"e2e_{names.Suffix}");
        }
        finally
        {
            await StopAsync(provider);
        }
    }

    [Test]
    public async Task Cluster_keeps_serving_when_a_node_stops()
    {
        var names = CdcNames.Unique();
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
            await Assert.That(await probe.NameAsync(index, id2)).IsEqualTo($"after_{names.Suffix}");
        }
        finally
        {
            await StopAsync(node2);
        }
    }
}
