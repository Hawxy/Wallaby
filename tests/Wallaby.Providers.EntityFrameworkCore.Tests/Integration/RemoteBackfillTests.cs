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
        await WaitForCompletedAsync(client);
    }

    [Test]
    public async Task Separate_backfill_runs_carry_distinct_run_tokens()
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
            o.ChunkSize = 2; // several chunks per run, so within-run token stability is actually exercised
        });
        services.ReplaceWallabySink("capture", capture);

        await using var node = await WallabyTestNode.StartAsync(services);
        await WallabyReadiness.WaitForStreamingAsync(node.Services);

        var categoryId = await Db.AddCategoryAsync();
        var ids = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            ids.Add((await Db.AddProductAsync(categoryId, $"tok{i}_{names.Suffix}")).ToString());
        }
        await capture.WaitForDocumentsAsync(ids);
        await WaitForCompletedAsync(client); // the startup auto-backfill, if one ran
        capture.Clear();

        await client.RequestBackfillAsync("public.products");
        await capture.WaitForDocumentsAsync(ids);
        await WaitForCompletedAsync(client);
        var firstRun = capture.Records.Where(r => r.Metadata.IsBackfill).ToList();
        var firstRunId = firstRun.Select(r => r.Metadata.BackfillRunId).Distinct()
            .ShouldHaveSingleItem().ShouldNotBeNull();
        var sample = firstRun.First(r => r.DocumentId == ids[0]);
        Wallaby.Sinks.SinkEnvelopeJson.IdempotencyKey(sample)
            .ShouldBe($"backfill:{firstRunId}:products:{ids[0]}");

        capture.Clear();
        await client.RequestBackfillAsync("public.products");
        await capture.WaitForDocumentsAsync(ids);
        var secondRunId = capture.Records.Where(r => r.Metadata.IsBackfill)
            .Select(r => r.Metadata.BackfillRunId).Distinct()
            .ShouldHaveSingleItem().ShouldNotBeNull();

        // A re-run over the same rows must produce new keys, or consumer-side dedupe swallows it.
        secondRunId.ShouldNotBe(firstRunId);
    }

    private static async Task WaitForCompletedAsync(WallabyControlClient client)
    {
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
