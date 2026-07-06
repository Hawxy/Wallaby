using Wallaby.Sinks.Meilisearch;
using Microsoft.EntityFrameworkCore;
using Sample.WorkerApp;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

// Defaults target samples/docker-compose.yml; override via configuration/env.
var postgres = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Port=5432;Username=cdc;Password=cdc;Database=app";
var meiliHost = builder.Configuration["Meilisearch:Host"] ?? "http://localhost:7700";
var meiliKey = builder.Configuration["Meilisearch:ApiKey"] ?? "masterKey";

builder.Services.AddDbContextFactory<SampleDbContext>(o => o.UseNpgsql(postgres));

// Create the sample schema on startup (demo convenience).
builder.Services.AddHostedService<SchemaInitializer>();

builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<SampleDbContext>()
       .UseConnectionString(postgres)
       .ConfigureOptions(o =>
       {
           o.SlotName = "sample_cdc_slot";
           o.PublicationName = "sample_cdc_pub";
       })
       .AddMeilisearchSink("meili", m =>
       {
           m.Host = meiliHost;
           m.ApiKey = meiliKey;
       })
       // Project each product into a flat search document; bump the version to force a reindex.
       .WithMappings(sink => sink
            .Map<Product>()
            .ToDestination("products")
            .WithBackfillVersion("v1")
            .UsingTransform((_, changes, _) =>
            {
                var documents = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var change in changes)
                {
                    var product = change.Entity!;
                    documents[change.Key] = new WallabyDocument
                    {
                        ["name"] = product.Name,
                        ["price"] = product.Price,
                        ["category"] = product.Category,
                    };
                }
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(documents);
            }));
});

using var host = builder.Build();
await host.RunAsync();

namespace Sample.WorkerApp
{
    /// <summary>Ensures the sample tables exist before CDC starts (demo convenience only).</summary>
    internal sealed class SchemaInitializer(IDbContextFactory<SampleDbContext> factory) : IHostedService
    {
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await using var context = await factory.CreateDbContextAsync(cancellationToken);
            await context.Database.EnsureCreatedAsync(cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
