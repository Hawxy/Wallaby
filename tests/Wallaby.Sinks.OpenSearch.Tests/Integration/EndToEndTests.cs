using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Sinks.OpenSearch.Tests.Integration.Infrastructure;
using Wallaby.Sinks.OpenSearch;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestModel;

namespace Wallaby.Sinks.OpenSearch.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, OpenSearchFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class EndToEndTests(TestModelPostgresFixture pg, OpenSearchFixture opensearch)
{
    private TestDatabase Db => new(pg.ConnectionString);

    private ServiceCollection BuildServices(WallabyNames names, string index)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .ConfigureOptions(o =>
               {
                   o.SlotName = names.Slot;
                   o.PublicationName = names.Publication;
                   o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
               })
               .AddOpenSearchSink("search", s => s.Endpoint = opensearch.Endpoint)
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
        using var probe = new OpenSearchProbe(opensearch);

        // The index is never created explicitly — it auto-creates on the first bulk write.
        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, index));

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");

        await Polling.UntilAsync(async () => await probe.NameAsync(index, id) == $"e2e_{names.Suffix}");
        (await probe.NameAsync(index, id)).ShouldBe($"e2e_{names.Suffix}");
    }

    [Test]
    public async Task Product_projection_syncs_insert_update_and_delete()
    {
        await using var harness = WallabyTestHarness.ForTestModel(pg.ConnectionString);
        var index = harness.Names.Named("products");
        harness.AddSink(new OpenSearchSink("search", new OpenSearchSinkOptions { Endpoint = opensearch.Endpoint }))
            .Project<Product>("search", index, p => new WallabyDocument { ["name"] = p.Name });
        await harness.SelfConfigureAsync();

        using var probe = new OpenSearchProbe(opensearch);
        var categoryId = await harness.Db.AddCategoryAsync();
        var id = await harness.Db.AddProductAsync(categoryId, "alpha");

        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha");

        await harness.Db.UpdateProductNameAsync(id, "alpha-v2");
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) == "alpha-v2");

        await harness.Db.DeleteProductAsync(id);
        await harness.RunUntilAsync(async () => await probe.NameAsync(index, id) is null);

        (await probe.NameAsync(index, id)).ShouldBeNull();
    }
}
