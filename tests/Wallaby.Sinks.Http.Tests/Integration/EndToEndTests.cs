using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestModel;

namespace Wallaby.Sinks.Http.Tests.Integration;

/// <summary>
/// <c>AddHttpSink</c> end to end: changes stream from Postgres through a transform and arrive at an
/// in-process webhook receiver as signed envelope records.
/// </summary>
[NotInParallel]
[ClassDataSource<TestModelPostgresFixture>(Shared = SharedType.PerTestSession)]
public class EndToEndTests(TestModelPostgresFixture pg)
{
    private const string SigningSecret = "e2e-signing-secret";

    private TestDatabase Db => new(pg.ConnectionString);

    private ServiceCollection BuildServices(WallabyNames names, string endpoint)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHttpClient();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .ConfigureOptions(o =>
               {
                   o.SlotName = names.Slot;
                   o.PublicationName = names.Publication;
               })
               .AddHttpSink("webhook", o =>
               {
                   o.Endpoint = endpoint;
                   o.SigningSecret = SigningSecret;
               })
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination("products")
                   .WithBackfillVersion(Guid.NewGuid().ToString("N")) // unique => isolates this test's backfill state
                   .UsingTransform(TestTransforms.ProductNames));
        });
        return services;
    }

    [Test]
    public async Task AddWallaby_posts_signed_upserts_and_deletes_end_to_end()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        await using var receiver = new WebhookReceiver(SigningSecret);
        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, receiver.Endpoint));

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");

        // The backfill also delivers the row (action "read"), so match the live insert specifically.
        await Polling.UntilAsync(() => receiver.Latest(id.ToString(), "upsert", action: "insert") is not null);
        var upsert = receiver.Latest(id.ToString(), "upsert", action: "insert")!.Value;
        upsert.GetProperty("destination").GetString().ShouldBe("products");
        upsert.GetProperty("document").GetProperty("name").GetString().ShouldBe($"e2e_{names.Suffix}");
        upsert.GetProperty("idempotencyKey").GetString().ShouldNotBeNullOrEmpty();
        upsert.GetProperty("metadata").GetProperty("schema").GetString().ShouldBe("public");
        upsert.GetProperty("metadata").GetProperty("table").GetString().ShouldBe("products");

        await Db.DeleteProductAsync(id);
        await Polling.UntilAsync(() => receiver.Latest(id.ToString(), "delete") is not null);

        receiver.SawInvalidSignature.ShouldBeFalse();
    }
}
