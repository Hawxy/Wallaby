using Wallaby.Sinks.Meilisearch;
using Microsoft.EntityFrameworkCore;
using Sample.WorkerApp;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;

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
       // Class-based transform, resolved from the container; bump the version to force a reindex.
       .WithMappings(sink => sink
            .Map<Product>()
            .ToDestination("products")
            .WithBackfillVersion("v1")
            .UsingTransform<Product, ProductSearchTransform>());
});

using var host = builder.Build();
await host.RunAsync();
