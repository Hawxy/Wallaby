using Wallaby.Sinks.Meilisearch;
using Microsoft.EntityFrameworkCore;
using Sample.WorkerApp;
using Wallaby.DependencyInjection;
using Wallaby.Diagnostics;
using Wallaby.Providers.EntityFrameworkCore;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = Host.CreateApplicationBuilder(args);

// Defaults target samples/docker-compose.yml; override via configuration/env.
var postgres = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Port=5432;Username=cdc;Password=cdc;Database=app";
var meiliHost = builder.Configuration["Meilisearch:Host"] ?? "http://localhost:7700";
var meiliKey = builder.Configuration["Meilisearch:ApiKey"] ?? "masterKey";

builder.Services.AddDbContextFactory<SampleDbContext>(o => o.UseNpgsql(postgres));

// Export Wallaby's traces and metrics when an OTLP endpoint is configured (e.g. by the Aspire AppHost).
if (!string.IsNullOrEmpty(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
{
    builder.Services.AddOpenTelemetry()
        .WithTracing(t => t.AddSource(WallabyInstrumentation.ActivitySourceName, "Npgsql"))
        .WithMetrics(m => m.AddMeter(WallabyInstrumentation.MeterName))
        .UseOtlpExporter();
}

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
           m.Endpoint = meiliHost;
           m.ApiKey = meiliKey;
       })
       // Use Apply<T> to consolidate your mapping configuration (see ProductSearchMapping).
       .WithMappings(sink => sink.Apply<ProductSearchMapping>());
});

using var host = builder.Build();
await host.RunAsync();
