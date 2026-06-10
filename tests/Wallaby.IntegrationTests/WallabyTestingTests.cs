using EFCore.CDC.TestModel;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;

namespace Wallaby.IntegrationTests;

/// <summary>
/// End-to-end coverage of the consumer-facing <c>Wallaby.Testing</c> package: a production-style
/// <c>AddWallaby</c> registration has its sink swapped for a <see cref="CaptureSink"/> and its slot/publication
/// renamed AFTER the fact (mirroring <c>WebApplicationFactory.ConfigureTestServices</c> ordering), then real
/// changes stream through logical replication into the capture sink.
/// </summary>
[NotInParallel]
[ClassDataSource<PostgresFixture>(Shared = SharedType.PerTestSession)]
public class WallabyTestingTests(PostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task Replaced_sink_captures_changes_end_to_end()
    {
        var names = CdcNames.Unique();
        var capture = new CaptureSink();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseContext<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               // Production-style sink the test will replace; throws if a batch ever reaches it.
               .AddDelegateSink("meili", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .Map<Product>()
                   .ToSink("meili", destination: "products")
                   .UsingTransform((_, changes, _) =>
                   {
                       var docs = new Dictionary<DocumentKey, CdcDocument?>();
                       foreach (var c in changes)
                       {
                           docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name };
                       }
                       return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(docs);
                   });
        });

        // Post-AddWallaby overrides — the WebApplicationFactory.ConfigureTestServices ordering.
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.StandbyRetryInterval = TimeSpan.FromSeconds(1);
        });
        services.ReplaceWallabySink("meili", capture);

        var provider = services.BuildServiceProvider();
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(CancellationToken.None);
        }

        try
        {
            await WallabyReadiness.WaitForStreamingAsync(provider);

            var categoryId = await Db.AddCategoryAsync();
            var id = await Db.AddProductAsync(categoryId, $"testing_{names.Suffix}");

            await capture.WaitForDocumentsAsync([id.ToString()]);

            var latest = capture.LatestByDocumentId(destination: "products");
            var record = latest[id.ToString()];
            await Assert.That(record.IsDeletion).IsFalse();
            await Assert.That(record.Destination).IsEqualTo("products");
            await Assert.That(record.Document!["name"]).IsEqualTo($"testing_{names.Suffix}");
        }
        finally
        {
            foreach (var hosted in provider.GetServices<IHostedService>())
            {
                await hosted.StopAsync(CancellationToken.None);
            }
            await provider.DisposeAsync();
        }
    }
}

/// <summary>Failure-mode coverage for the post-registration override extensions (no database needed).</summary>
public class WallabyTestingExtensionTests
{
    [Test]
    public async Task ReplaceWallabySink_throws_without_AddWallaby()
    {
        var services = new ServiceCollection();
        await Assert.That(() => services.ReplaceWallabySink("meili", new CaptureSink()))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReplaceWallabySink_throws_for_unknown_sink_name()
    {
        var services = BuildRegisteredServices();
        await Assert.That(() => services.ReplaceWallabySink("wrong", new CaptureSink()))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ConfigureWallabyOptions_mutates_the_registered_instance()
    {
        var services = BuildRegisteredServices();

        services.ConfigureWallabyOptions(o => o.SlotName = "overridden_slot");

        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetRequiredService<CdcOptions>().SlotName).IsEqualTo("overridden_slot");
    }

    private static ServiceCollection BuildRegisteredServices()
    {
        var services = new ServiceCollection();
        services.AddWallaby(cdc =>
        {
            cdc.UseContext<AppDbContext>()
               .UseConnectionString("Host=localhost;Database=unused")
               .AddDelegateSink("real", (_, _) => Task.FromResult(DeliveryResult.Success))
               .Map<Product>()
                   .ToSink("real")
                   .UsingTransform((_, changes, _) =>
                   {
                       var docs = new Dictionary<DocumentKey, CdcDocument?>();
                       foreach (var c in changes)
                       {
                           docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name };
                       }
                       return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(docs);
                   });
        });
        return services;
    }
}
