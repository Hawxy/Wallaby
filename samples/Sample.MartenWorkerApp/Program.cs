using Marten;
using Sample.MartenWorkerApp;
using Wallaby.DependencyInjection;
using Wallaby.Providers.Marten;
using Wallaby.Sinks.Meilisearch;

var builder = Host.CreateApplicationBuilder(args);

// Defaults target samples/docker-compose.yml; override via configuration/env.
var postgres = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Port=5432;Username=cdc;Password=cdc;Database=app";
var meiliHost = builder.Configuration["Meilisearch:Host"] ?? "http://localhost:7700";
var meiliKey = builder.Configuration["Meilisearch:ApiKey"] ?? "masterKey";

builder.Services.AddMarten(options =>
{
    options.Connection(postgres);
    options.DatabaseSchemaName = "marten_sample";
    options.RegisterDocumentType<Customer>();
    options.Schema.For<Order>().SoftDeleted();
})
// Marten creates the document tables on startup, before capture begins.
.ApplyAllDatabaseChangesOnStartup()
// Soft-deleted orders need REPLICA IDENTITY FULL; Marten's migrations apply the DDL.
.ManageWallabyReplicaIdentity();

builder.Services.AddWallaby(cdc =>
{
    cdc.UseMarten()
       .UseConnectionString(postgres)
       .ConfigureOptions(o =>
       {
           o.SlotName = "sample_marten_slot";
           o.PublicationName = "sample_marten_pub";
       })
       .AddMeilisearchSink("meili", m =>
       {
           m.Host = meiliHost;
           m.ApiKey = meiliKey;
       })
       // Class-based transform, resolved from the container; bump the version to force a reindex.
       .WithMappings(sink => sink
            .Map<Order>()
            .ToDestination("orders")
            .WithBackfillVersion("v1")
            .UsingTransform<Order, OrderSearchTransform>());
});

using var host = builder.Build();
await host.RunAsync();
