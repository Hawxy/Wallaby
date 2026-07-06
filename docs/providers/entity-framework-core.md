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

## Transforms

### Enrichment via the DbContext

The `db` argument is a scoped `DbContext` you can query to flatten or enrichment an aggregate:
```csharp
cdc.Map<Order>()
   .ToSink("meili", "orders")
   .UsingTransform(async (db, changes, ct) =>
   {
       var ids = changes.Select(c => c.GetPrimaryKey<int>()).ToList();
       var orders = await db.Set<Order>()
           .Where(o => ids.Contains(o.Id))
           .Include(o => o.Customer)
           .Include(o => o.Lines)
           .ToListAsync(ct);

       var docs = new Dictionary<DocumentKey, WallabyDocument?>(orders.Count);
       foreach (var o in orders)
           docs[new DocumentKey(o.Id)] = new WallabyDocument
           {
               ["customer"] = o.Customer?.Name,
               ["lineCount"] = o.Lines.Count,
           };
       return docs;
   });
```

::: tip
A transform that queries `db` reads *current* database state, which may be newer than the change's LSN. If you must observe the exact post-change snapshot, 
project from the `ChangeEvent` instead and use `REPLICA IDENTITY FULL`.
:::

## Dependent tables

When a transform reads from a *related* table, changes to that table won't trigger a re-emit on their
own. Declare the relationship with `DependsOn(...)` — an [EF Core provider](/providers/entity-framework-core)
mapping extension — so Wallaby captures the related table and fans its changes out to synthetic updates
of your entity:

```csharp
cdc.Map<Product>()
   .ToSink("meili", "products")
   .DependsOn(p => p.Category)   // a referenced principal
   .DependsOn(p => p.Labels)     // a many-to-many / skip-navigation join table
   .UsingTransform(/* reads Category + Labels */);
```

The navigation is resolved against the EF model at startup; it must be a single one-hop navigation.
A change to `categories` or the `product_labels` join table then re-emits the affected products through
the same transform.

### Scaling fan-out

A single change to a principal row can affect a large number of dependents (e.g. renaming a category with
a million products). Wallaby keeps this bounded:

- **Consolidated lookups.** All distinct keys changed for a dependent table in one transaction are resolved
  with a single `IN (…)` query per relationship.
- **Inline first page, offloaded tail.** The first [`MaxBatchSize`](/configuration#options) affected rows
  are re-emitted inline; if more remain, the rest is handed to a *scoped backfill job* that re-snapshots
  them asynchronously. This lets the trigger
  transaction be acknowledged immediately, so a huge fan-out never stalls replication.
- **On-demand processing.** The offloaded queue is drained by a worker woken via Postgres `LISTEN`/`NOTIFY`
  the instant a job is enqueued so the tail is picked up promptly. A periodic
  [`FanoutPollInterval`](/configuration#options) (default 30s) is only a safety-net fallback.
- **Coalescing.** Repeated changes to the same principal collapse into a single pending re-snapshot.
- **Same-transaction de-duplication.** If a primary row is changed *and* one of its dependents changes in
  the same transaction, the row is emitted once (its own change wins — the transform already re-reads the
  dependent from current state).

The offloaded tail is therefore **eventually consistent**: for a wide fan-out, the bulk of the re-index
lands shortly *after* the trigger commits rather than in commit order with it. Sinks must be idempotent
(upsert by id) and support at-least-once delivery.

## Next steps

- [Configuration](/configuration) - All configuration options
- [Transforms](/transforms) - shaping and enriching documents.
- [Meilisearch sink](/sinks/meilisearch) and [custom sinks](/sinks/custom).
- [Backfill](/backfill) - initial snapshots and version-triggered reindex.
- [Multi-tenancy](/multi-tenancy) - per-row scoped contexts and destinations.
- [Observability](/operations/observability) - OpenTelemetry metrics and traces.
