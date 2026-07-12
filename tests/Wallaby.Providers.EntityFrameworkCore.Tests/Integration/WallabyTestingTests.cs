using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;

namespace Wallaby.Providers.EntityFrameworkCore.Tests.Integration;

/// <summary>
/// End-to-end coverage of the consumer-facing <c>Wallaby.Testing</c> package: a production-style
/// <c>AddWallaby</c> registration has its sink swapped for a <see cref="CaptureSink"/> and its slot/publication
/// renamed AFTER the fact (mirroring <c>WebApplicationFactory.ConfigureTestServices</c> ordering), then real
/// changes stream through logical replication into the capture sink.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class WallabyTestingTests(TestModelPostgresFixture pg)
{
    private TestDatabase Db => new(pg.ConnectionString);

    [Test]
    public async Task Replaced_sink_captures_changes_end_to_end()
    {
        var names = WallabyNames.Unique();
        var capture = new CaptureSink();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               // Production-style sink the test will replace; throws if a batch ever reaches it.
               .AddDelegateSink("meili", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination("products")
                   .UsingTransform(TestTransforms.ProductNames));
        });

        // Post-AddWallaby overrides — the WebApplicationFactory.ConfigureTestServices ordering.
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
        });
        services.ReplaceWallabySink("meili", capture);

        try
        {
            await using var node = await WallabyTestNode.StartAsync(services);
            await WallabyReadiness.WaitForStreamingAsync(node.Services);

            var categoryId = await Db.AddCategoryAsync();
            var id = await Db.AddProductAsync(categoryId, $"testing_{names.Suffix}");

            await capture.WaitForDocumentsAsync([id.ToString()]);

            var latest = capture.LatestByDocumentId(destination: "products");
            var record = latest[id.ToString()];
            record.IsDeletion.ShouldBeFalse();
            record.Destination.ShouldBe("products");
            record.Document!["name"].ShouldBe($"testing_{names.Suffix}");
        }
        finally
        {
            await PostgresReplicationCleanup.DropAsync(pg.ConnectionString, names);
        }
    }

    [Test]
    public async Task Provider_aware_connection_string_streams_to_the_replaced_sink()
    {
        var names = WallabyNames.Unique();
        var capture = new CaptureSink();

        // The connection string travels through IConfiguration and is only read when the provider exists —
        // the pattern that lets a test host redirect a real app via plain configuration overrides.
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:App"] = pg.ConnectionString })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(sp => sp.GetRequiredService<IConfiguration>().GetConnectionString("App")!)
               .AddDelegateSink("meili", (_, _) => throw new InvalidOperationException("The replaced sink must never be invoked."))
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination("products")
                   .UsingTransform(TestTransforms.ProductNames));
        });

        // ConfigureTestServices ordering: overrides registered after AddWallaby.
        services.ConfigureWallabyOptions(o =>
        {
            o.SlotName = names.Slot;
            o.PublicationName = names.Publication;
            o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
        });
        services.ReplaceWallabySink("meili", capture);

        try
        {
            await using var node = await WallabyTestNode.StartAsync(services);
            await WallabyReadiness.WaitForStreamingAsync(node.Services);

            var categoryId = await Db.AddCategoryAsync();
            var id = await Db.AddProductAsync(categoryId, $"late_bound_{names.Suffix}");

            await capture.WaitForDocumentsAsync([id.ToString()]);

            var record = capture.LatestByDocumentId(destination: "products")[id.ToString()];
            record.IsDeletion.ShouldBeFalse();
            record.Document!["name"].ShouldBe($"late_bound_{names.Suffix}");
        }
        finally
        {
            await PostgresReplicationCleanup.DropAsync(pg.ConnectionString, names);
        }
    }
}

/// <summary>Failure-mode coverage for the post-registration override extensions (no database needed).</summary>
public class WallabyTestingExtensionTests
{
    [Test]
    public void ReplaceWallabySink_throws_without_AddWallaby()
    {
        var services = new ServiceCollection();
        Should.Throw<InvalidOperationException>(() => services.ReplaceWallabySink("meili", new CaptureSink()));
    }

    [Test]
    public void ReplaceWallabySink_throws_for_unknown_sink_name()
    {
        var services = BuildRegisteredServices();
        Should.Throw<InvalidOperationException>(() => services.ReplaceWallabySink("wrong", new CaptureSink()));
    }

    [Test]
    public async Task ConfigureWallabyOptions_mutates_the_registered_instance()
    {
        var services = BuildRegisteredServices();

        services.ConfigureWallabyOptions(o => o.SlotName = "overridden_slot");

        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<WallabyOptions>().SlotName.ShouldBe("overridden_slot");
    }

    [Test]
    public async Task Overrides_compose_in_call_order()
    {
        var capture = new CaptureSink();
        var services = BuildRegisteredServices();

        services.ConfigureWallabyOptions(o => o.SlotName = "first");
        services.ConfigureWallabyOptions(o => o.SlotName = "second");
        services.ReplaceWallabySink("real", capture);

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<WallabyOptions>().SlotName.ShouldBe("second");
        var sinks = provider.GetRequiredService<WallabyConfiguration>().Sinks;
        sinks.Count.ShouldBe(1);
        ReferenceEquals(sinks[0].Factory(provider), capture).ShouldBeTrue();
    }

    private static ServiceCollection BuildRegisteredServices()
    {
        var services = new ServiceCollection();
        services.AddWallaby(cdc => cdc
            .UseEntityFrameworkCore<AppDbContext>()
            .UseConnectionString("Host=localhost;Database=unused")
            .AddDelegateSink("real", (_, _) => Task.FromResult(DeliveryResult.Success))
            .WithMappings(sink => sink
                .Map<Product>()
                .UsingTransform(TestTransforms.ProductNames)));
        return services;
    }
}
