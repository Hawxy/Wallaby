using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Client;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// Manual backfill driven remotely through Wallaby.Client: a request addressed by schema-qualified
/// table name is served by the running leader and re-snapshots the table through the normal sink path.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class RemoteBackfillTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task A_remote_request_rebackfills_the_table_through_the_running_leader()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var client = new WallabyControlClient(pg.ConnectionString);

        var capture = new CaptureSink();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString(pg.ConnectionString)
            .AddDelegateSink("capture", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
            .WithMappings(sink => sink
                .Map<Product>()
                .ToDestination("products")
                .UsingTransform(TestTransforms.ProductNames)));
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
        });
        services.ReplaceWallabySink("capture", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var categoryId = await Db.AddCategoryAsync();
        var productId = await Db.AddProductAsync(categoryId, $"live_{names.Suffix}");
        await capture.WaitForDocumentsAsync([productId.ToString()]);
        capture.Clear();

        await client.RequestBackfillAsync("public.products");

        // The re-snapshot delivers the product again with no new source change.
        await capture.WaitForDocumentsAsync([productId.ToString()]);

        // Delivery happens mid-snapshot, so poll for the terminal status rather than asserting it.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while ((await client.GetBackfillStatusAsync()).Single(s => s.Table == "public.products").Status
               != WallabyBackfillStatus.Completed)
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("The remote backfill never reported Completed.");
            }
            await Task.Delay(100);
        }
    }
}
