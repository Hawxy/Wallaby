---
description: "Capturing changes through your EF Core model: registration, enrichment transforms, dependent tables, and replica identity migrations."
---

# EF Core

The `Wallaby.Providers.EntityFrameworkCore` package drives capture from your EF Core model: Wallaby watches the
tables behind your mapped entities, materializes each row change back into your **entity types**, lets
you transform/enrich them, and routes the resulting documents through the usual transform → sink
pipeline.

## Install

```bash
dotnet add package Wallaby.Providers.EntityFrameworkCore
```

## Register

Call `AddWallaby` to start and chain in your `DbContext` via `UseEntityFrameworkCore<TContext>()`. Wallaby will resolve your context regardless of if it's registered with `AddDbContext<TContext>()` or `AddDbContextFactory<TContext>()`.

You must also supply a connection string via `UseConnectionString(...)`, or any other [options-pattern mechanism](/configuration#options-pattern) such as configuration binding. This is so Wallaby can manage additional connections itself. Multi-host connection strings are supported, but Wallaby will only connect to your primary node.

```csharp
using Wallaby.Abstractions;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.DependencyInjection;
using Wallaby.Sinks.Meilisearch;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       
       // Sink configuration below - example
       .AddMeilisearchSink("meili", m => {/* ... */})
       .WithMappings(sink => sink
            .Map<Product>()
            .ToDestination("products")
            .WithBackfillVersion("v1")
            .UsingTransform(/** **/);
});

await builder.Build().RunAsync();
```

Transforms receive a leased `DbContext` for enrichment lookups - see [Mappings](/mappings).

::: tip
If the builder needs services at registration time (e.g. `IConfiguration`), use the
`AddWallaby((sp, cdc) => ...)` overload - see
[Reading configuration at startup](/configuration#reading-configuration-at-startup).
:::

## What gets tracked

Only entities you **declare** are captured and added to the publication: `Map<T>()` inside a sink's
`WithMappings(...)` declares a table *and* routes it to that sink. Tables a mapping
[`DependsOn`](#dependent-tables) are captured automatically. Captured tables must have a
primary key. The same entity may be mapped under several sinks - it is captured once and each sink's
mapping runs its own transform.

## Transforms

### Enrichment via the DbContext

The `db` argument is a scoped `DbContext` you can query to flatten or enrichment an aggregate:
```csharp
sink.Map<Order>()
    .ToDestination("orders")
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

### Class-based transforms

For more complex transforms, or anything with dependencies, implement `IWallabyEfTransform<TEntity>`
as a class - it is resolved from the container:

```csharp
public sealed class ProductSearchTransform(IPricingService pricing) : IWallabyEfTransform<Product>
{
    public async Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<Product>> changes, CancellationToken ct)
    {
        var docs = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
        foreach (var c in changes)
            docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name, ["rrp"] = await pricing.RrpAsync(c.Entity!.Id, ct) };
        return docs;
    }
}

// register:
sink.Map<Product>()
    .ToDestination("products")
    .UsingTransform<Product, ProductSearchTransform>();
```

::: tip
The registration itself can also live outside `AddWallaby` as a
[mapping class](/mappings#mapping-classes): `sink.Apply<ProductSearchMapping>()`.
:::

## Dependent tables

When a transform reads from a *related* table, changes to that table won't trigger a re-emit on their
own. Declare the relationship with `DependsOn(...)` so Wallaby captures the related table and fans its changes out to synthetic updates
of your entity:

```csharp
sink.Map<Product>()
    .ToDestination("products")
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

- **Consolidated lookups**: All distinct keys changed for a dependent table in one transaction are resolved
  with a single `IN (…)` query per relationship.
- **Inline first page, offloaded tail**: The first [`MaxBatchSize`](/configuration#general-options) affected rows
  are re-emitted inline. If more remain, the rest is handed to a *scoped backfill job* that re-snapshots
  them asynchronously. This lets the trigger
  transaction be acknowledged immediately, so a huge fan-out never stalls replication.
- **On-demand processing**: The offloaded queue is drained by a worker woken via Postgres `LISTEN`/`NOTIFY`
  the instant a job is enqueued so the tail is picked up promptly. A periodic
  [`FanoutPollInterval`](/configuration#advanced-options) (default 30s) is only a safety-net fallback.
- **Coalescing**: Repeated changes to the same principal collapse into a single pending re-snapshot.
- **Same-transaction de-duplication**: If a primary row is changed *and* one of its dependents changes in
  the same transaction, the row is emitted once (its own change wins - the transform already re-reads the
  dependent from current state).

The offloaded tail is therefore **eventually consistent**: for a wide fan-out, the bulk of the re-index
lands shortly *after* the trigger commits rather than in commit order with it. Sinks must be idempotent
(upsert by id) and support at-least-once delivery.

## Replica identity in migrations

Some captured tables need `REPLICA IDENTITY FULL` so previous rows appear in the changeset. This is required for
mappings with a [`ScopedDestination`](/providers/entity-framework-core/multi-tenancy) (deletes must
carry the scope key) and transforms that project from the change's old values. Self-config detects a
missing replica identity at startup and logs the exact DDL, apply it through your EF migrations with
the `MigrationBuilder` helpers:

```csharp
public partial class OrdersReplicaIdentity : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
        => migrationBuilder.SetReplicaIdentityFull("orders", schema: "sales");

    protected override void Down(MigrationBuilder migrationBuilder)
        => migrationBuilder.SetReplicaIdentityDefault("orders", schema: "sales");
}
```

## Next steps

- [Configuration](/configuration): all configuration options.
- [Mappings](/mappings): routing entities to destinations, shaping and enriching documents.
- [Meilisearch](/sinks/meilisearch), [HTTP](/sinks/http), and [custom](/sinks/custom) sinks.
- [Backfill](/backfill): initial snapshots and version-triggered reindex.
- [Multi-tenancy](/providers/entity-framework-core/multi-tenancy): per-row scoped contexts and destinations.
- [Observability](/operations/observability): OpenTelemetry metrics and traces.
