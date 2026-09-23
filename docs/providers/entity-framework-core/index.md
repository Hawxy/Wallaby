---
title: "Postgres CDC for EF Core"
description: "Postgres change data capture driven by your EF Core model in .NET: registration, enrichment transforms, dependent tables, and replica identity migrations."
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

Call `AddWallaby` to start and chain in your `DbContext` via `UseEntityFrameworkCore<TContext>()`. Wallaby will resolve your context whether it's registered with `AddDbContext<TContext>()` or `AddDbContextFactory<TContext>()`.

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
            .UsingTransform(/** **/));
});

await builder.Build().RunAsync();
```

Transforms receive a leased `DbContext` for enrichment lookups - see [Mappings](/mappings).

::: tip
If option values need services (e.g. `IConfiguration`), use the provider-aware overloads of
`UseConnectionString`/`ConfigureOptions` - see
[Reading configuration at startup](/configuration#reading-configuration-at-startup).
:::

## Mappings

### What gets tracked

Only entities you **declare** are captured and added to the publication. Calling `Map<T>()` inside a
sink's `WithMappings(...)` both declares the table *and* routes it to that sink. Captured tables must
have a primary key.

The same entity can be mapped under several sinks. It's captured once, and each sink's mapping runs
its own transform.

Entities in a **TPH hierarchy** can't be captured. Every member of the hierarchy shares one table, so
rows would materialize as one arbitrary type and lose subclass data. Map hierarchies with TPT or TPC
instead.

### Dependent tables

When a transform reads from a *related* table, changes to that table don't re-emit your entity on
their own. Declare the relationship with `DependsOn(...)` and Wallaby captures the related table too,
fanning its changes out as synthetic updates of your entity:

```csharp
sink.Map<Product>()
    .ToDestination("products")
    .DependsOn(p => p.Category)   // a referenced principal
    .DependsOn(p => p.Labels)     // a many-to-many / skip-navigation join table
    .UsingTransform(/* reads Category + Labels */);
```

The navigation is resolved against the EF model at startup and must be a single one-hop navigation.
With the mapping above, a change to `categories` or the `product_labels` join table re-emits the
affected products through the same transform.

If a dependent table isn't also mapped and consumed in its own right, Wallaby only captures (and
[publishes](/configuration#publication-column-lists)) its primary-key and lookup columns, which keeps
the amount of data sent over the wire down.

### Declaring consumed columns

Each mapping can optionally declare which properties its transform consumes, either as the columns
to keep or the columns to leave out. Anything no mapping consumes isn't sent over the wire:

```csharp
sink.WithMappings(m =>
{
    m.Map<Customer>()
        .Consumes(c => c.Id, c => c.Email)        // only these (plus the primary key)
        .UsingTransform(...);

    m.Map<Product>()
        .ConsumesAllExcept(p => p.Description)     // everything but these
        .UsingTransform(...);
});
```

An entity's captured columns are the **union across all its mappings**. If you map `Product` to a
second sink whose transform reads `Description` (or declares no selection at all), that column is
captured again automatically. Primary-key properties and the columns `DependsOn(...)` resolves through
are always captured, and a mapping without a selection keeps the entity at consume-all.

An unselected property is dropped from capture entirely: its column is left out of the
[publication column list](/configuration#publication-column-lists) (so the value never leaves the
server), it's skipped during materialization, and it's never read during backfill.

Both methods also accept **EF model property names as strings**, for members a lambda can't reach:
properties that aren't visible from the assembly doing the configuration, shadow properties, or a
single owned/complex leaf via its dotted path:

```csharp
m.Map<Invoice>()
    .Consumes("Number", "TenantId", "Address.City")  // "TenantId" can be internal, private, or shadow
    .UsingTransform(...);
```

Names are validated against the EF model at startup, and you can mix string and lambda calls freely.
A selected shadow property has no CLR member to materialize into, so read it from
`ChangeEvent.Record` in the transform.

::: warning
Excluding a property your transform actually reads doesn't raise an error. The property just stays at
its CLR default. A selection is an optimization for columns that *no* transform consumes. It isn't the
fix for a large (TOASTed) column a transform *does* read: that table needs
[`REPLICA IDENTITY FULL`](#replica-identity-in-migrations).
:::

### Owned and complex types

Whether a value-object member is captured depends on where its data physically lives:

| Member shape | Behavior |
| --- | --- |
| Same-table `OwnsOne` reference (including nested) | Captured and materialized with the owner |
| Complex property (`ComplexProperty`, column-mapped) | Captured and materialized with the owner |
| Owned collection (`OwnsMany`) | Not captured (startup warning; member stays at its default) |
| `OwnsOne` mapped to its own table (`ToTable`) | Not captured (startup warning; member stays at its default) |
| Owned or complex member mapped to JSON (`ToJson`) | Not captured (startup warning; member stays at its default) |

Captured members behave like ordinary properties: their columns join the
[publication column list](/configuration#publication-column-lists), backfills read them, and the
materialized entity carries the constructed instances. In `ChangeEvent.Record` and `Changes`, their
keys use the dotted member path, e.g. `"Address.Street"`. An optional member whose columns are all
null stays null, mirroring EF. If a captured entity has an owned or complex type that can't be
constructed from column values (for example, one whose constructor injects the `DbContext`), startup
fails.

For the shapes that can't be captured, the data lives outside the entity's rows, so the materialized
member stays at its default and Wallaby logs one warning per member at startup. To silence the
warning, state your intent either way:

- `DependsOn(e => e.Lines)`: changes to the member's side table re-emit the entity. The member itself
  still isn't populated, so read it in the transform via the `DbContext`.
- `ConsumesAllExcept(e => e.Lines)`: acknowledges that the member isn't consumed.

### Computed columns

A property mapped with `HasComputedColumnSql(..., stored: true)` is a Postgres stored generated
column. Logical replication only publishes these from **PostgreSQL 18**; older servers never put them
on the wire. Virtual generated columns (the PostgreSQL 18 default for `GENERATED ALWAYS AS`) aren't
published on any version.

Since a backfill would read the real value while a live change would carry the property's default,
Wallaby refuses to start when a captured column falls into either category, and names the offending
columns. Either exclude the property from every mapping of the entity with
`ConsumesAllExcept(e => e.Computed)`, or (for stored columns) upgrade to PostgreSQL 18+.

On PostgreSQL 18+, a managed publication is created with `publish_generated_columns = stored`, so
stored computed columns arrive on live changes like any other column. An unmanaged publication
(`ManagePublicationTables = false`) has to set that option itself, and Wallaby warns at startup when
it doesn't.

## Transforms

### Enrichment via the DbContext

The `db` argument is a scoped `DbContext` you can query to flatten or enrich an aggregate:
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

::: warning Large (TOASTed) columns
Postgres omits an *unchanged* TOASTed value (long text, big jsonb, bytea over ~2KB) from an update's
new tuple, so under the default identity the value isn't carried in the change at all. Rather than
deliver a document with the field silently nulled, Wallaby
[heals the change by re-reading the row](/how-it-works#unavailable-value-self-healing-reselect) (a
warning is logged per healed change, naming the DDL above); with
[`ReselectUnavailableValues`](/configuration) disabled it fails the change instead.

Pick the permanent fix by whether any transform reads the column:

- **A transform reads it** → `REPLICA IDENTITY FULL` is the fix. A column selection is *not* an
  alternative here: excluding a consumed column leaves the transform reading a silently defaulted
  property. Full identity puts the value on the wire and removes the per-change re-read.
- **No transform reads it** →
  [drop it from the mapping's column selection](#declaring-consumed-columns). The value then never
  leaves the server, and the table avoids full identity's whole-old-row WAL cost.
:::

## Internals

### How fan-out scales

A single change to a principal row can affect a large number of dependents (e.g. renaming a category with
a million products). Wallaby keeps this bounded:

- **Consolidated lookups**: All distinct keys changed for a dependent table in one transaction are resolved
  with a single `IN (…)` query per relationship.
- **Inline first page, offloaded tail**: The first [`MaxBatchSize`](/configuration#general-options) affected rows
  are re-emitted inline. If more remain, the rest is handed to *scoped backfill jobs* that re-snapshot
  them asynchronously. This lets the trigger
  transaction be acknowledged immediately, so a huge fan-out never stalls replication.
- **Bounded memory**: A very wide fan-out (tens of thousands of distinct keys in one transaction) is
  offloaded in chunk jobs *as the keys accumulate*, so memory stays flat no matter how many keys the
  transaction touches. Past [`MaxFanoutKeysPerTransaction`](/configuration#advanced-options) the
  transaction has effectively rewritten the dependent table, and the whole primary table is
  re-snapshotted instead.
- **On-demand processing**: The offloaded queue is drained by a worker woken via Postgres `LISTEN`/`NOTIFY`
  the instant a job is enqueued so the tail is picked up promptly. A periodic
  [`FanoutPollInterval`](/configuration#advanced-options) (default 30s) is only a fallback for a missed notification.
- **Coalescing**: Repeated changes to the same principal collapse into a single pending re-snapshot.
- **Same-transaction de-duplication**: If a primary row is changed *and* one of its dependents changes in
  the same transaction, the row is emitted once (its own change wins - the transform already re-reads the
  dependent from current state).

The offloaded tail is therefore **eventually consistent**: for a wide fan-out, the bulk of the re-index
lands shortly *after* the trigger commits rather than in commit order with it. Sinks must be idempotent
(upsert by id) and support at-least-once delivery.

## Next steps

- [Configuration](/configuration): all configuration options.
- [Mappings](/mappings): routing entities to destinations, shaping and enriching documents.
- [Meilisearch](/sinks/meilisearch), [HTTP](/sinks/http), [Elasticsearch](/sinks/elasticsearch), [OpenSearch](/sinks/opensearch), and [custom](/sinks/custom) sinks.
- [Backfill](/backfill): initial snapshots and version-triggered reindex.
- [Multi-tenancy](/providers/entity-framework-core/multi-tenancy): per-row scoped contexts and destinations.
- [Observability](/operations/observability): OpenTelemetry metrics and traces.
