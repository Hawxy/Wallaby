---
outline: deep
---

# Getting Started

Wallaby streams row changes from Postgres logical replication, materializes them into your mapped
**EF Core entities**, lets you transform/enrich them, and routes the resulting documents to pluggable
**sinks** (destinations).

## Install

```bash
dotnet add package Wallaby
dotnet add package Wallaby.Sinks.Meilisearch   # optionally add a pre-built sink
```

Wallaby requires .NET 10+.

## Server prerequisites

Your Postgres server must already have:

- **`wal_level = logical`** set in `postgresql.conf`  required for logical replication.
- A role with the **`REPLICATION`** attribute (or superuser) for the connection string you give Wallaby.
- Headroom in `max_replication_slots` and `max_wal_senders` (at least one slot/sender per Wallaby cluster).

Wallaby validates these on startup and fails fast with an actionable error if something is missing.

## Register

Call `AddWallaby` to started and chain in your `DbContext` via `UseContext<TContext>()`. Wallably will resolve your context regardless of if it's registered with either `AddDbContext<TContext>()` or `AddDbContextFactory<TContext>()`.

You must also supply a connection string — via `UseConnectionString(...)`, or any other [options-pattern mechanism](/configuration#the-options-pattern) such as configuration binding — so Wallaby can manage additional connections itself. Multi-host connection strings are supported, but Wallaby will only connect to your primary node.

::: tip
Wallaby also supports running in provision-only mode, where slots are provisioned but not consumed and EF Core can be omitted.
See [External slots](/external-slots).
:::

```csharp
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Sinks.Meilisearch;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddWallaby(cdc =>
{
    cdc.UseContext<AppDbContext>()
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
replication slot, backfills the mapped tables, then streams live changes. Wallably holds a distributed lock so these operations will only run on a single node within a HA environment.

### Reading configuration at startup

When the builder needs services use the provider-included overload of `AddWallaby`:

```csharp
builder.Services.AddWallaby((sp, cdc) =>
{
    var config = sp.GetRequiredService<IConfiguration>();

    cdc.UseContext<AppDbContext>()
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

Only entities you **declare** are captured and added to the publication:

- `Map<T>()` declares a table *and* routes it to a sink.
- `CaptureAllMappedTables()` opts every mapped entity in (not recommended).

Tables a mapping [`DependsOn`](/transforms#dependent-tables) are captured automatically. Captured tables must have a primary key.

## Deployment

It's highly recommended to deploy Wallaby as a seperate service, not as an integrated part of your main application. This allows you to scale CDC operations independently as the need arises. 

## Next steps

- [Configuration](/configuration) - All configuration options
- [Transforms](/transforms) - shaping and enriching documents.
- [Meilisearch sink](/sinks/meilisearch) and [custom sinks](/sinks/custom).
- [Backfill](/backfill) - initial snapshots and version-triggered reindex.
- [Multi-tenancy](/multi-tenancy) - per-row scoped contexts and destinations.
- [External slots](/external-slots) - provision extra publications/slots for an ELT or other CDC consumer.
- [Observability](/operations/observability) - OpenTelemetry metrics and traces.
