# EF Core

The `Wallaby.EntityFrameworkCore` package drives capture from your EF Core model: Wallaby watches the
tables behind your mapped entities, materializes each row change back into your **entity types**, lets
you transform/enrich them, and routes the resulting documents through the usual transform → sink
pipeline.

## Install

```bash
dotnet add package Wallaby.EntityFrameworkCore
dotnet add package Wallaby.Sinks.Meilisearch #optionally add a sink
```

## Register

Call `AddWallaby` to start and chain in your `DbContext` via `UseEntityFrameworkCore<TContext>()`. Wallaby will resolve your context regardless of if it's registered with either `AddDbContext<TContext>()` or `AddDbContextFactory<TContext>()`.

You must also supply a connection string — via `UseConnectionString(...)`, or any other [options-pattern mechanism](/configuration#the-options-pattern) such as configuration binding — so Wallaby can manage additional connections itself. Multi-host connection strings are supported, but Wallaby will only connect to your primary node.

```csharp
using Wallaby.Abstractions;
using Wallaby.EntityFrameworkCore;
using Wallaby.DependencyInjection;
using Wallaby.Sinks.Meilisearch;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .ConfigureOptions(o =>
       {
           o.SlotName = "app_cdc";
           o.PublicationName = "app_cdc_pub";
       })
       .AddMeilisearchSink("meili", m => { m.Host = "http://localhost:7700"; m.ApiKey = key; })

       // specify mapping and then configure the transform and destination
       .Map<Product>()
            .ToSink("meili", destination: "products")
            .WithBackfillVersion("v1")
            .UsingTransform((_, changes, _) =>
            {
                var docs = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
                foreach (var c in changes)
                    docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name, ["price"] = c.Entity!.Price };
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
            });
});

await builder.Build().RunAsync();
```

On startup Wallaby validates the server, creates the `wallaby` state schema, the publication, and the
replication slot, backfills the mapped tables, then streams live changes. Wallaby holds a distributed lock so these operations will only run on a single node within a HA environment.

Transforms receive a leased `DbContext` for enrichment lookups — see [Transforms](/transforms).

### Reading configuration at startup

When the builder needs services use the provider-included overload of `AddWallaby`:

```csharp
builder.Services.AddWallaby((sp, cdc) =>
{
    var config = sp.GetRequiredService<IConfiguration>();

    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(config.GetConnectionString("App")!)
       // ... sinks and mappings as usual ...
});
```

The callback runs once, when the host first resolves Wallaby's services. Two consequences of the deferred timing: the
callback receives the **root** provider (scoped services are unavailable), and configuration errors surface
at host start instead of at registration.

::: tip
`WallabyOptions` also participates in the standard options pattern, see
[Configuration](/configuration#the-options-pattern).
:::

## What gets tracked

Only entities you **declare** are captured and added to the publication: `Map<T>()` declares a table
*and* routes it to a sink. Tables a mapping [`DependsOn`](/transforms#dependent-tables) are captured
automatically. Captured tables must have a primary key.

## Combining providers

EF Core can be combined with another storage provider in the same Wallaby instance, sharing a single
replication slot — see the [providers overview](/providers/overview#combining-providers).

## Next steps

- [Configuration](/configuration) - All configuration options
- [Transforms](/transforms) - shaping and enriching documents.
- [Meilisearch sink](/sinks/meilisearch) and [custom sinks](/sinks/custom).
- [Backfill](/backfill) - initial snapshots and version-triggered reindex.
- [Multi-tenancy](/multi-tenancy) - per-row scoped contexts and destinations.
- [Observability](/operations/observability) - OpenTelemetry metrics and traces.
