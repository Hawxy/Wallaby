---
outline: deep
---

# Transforms

A **transform** turns the changes for one entity type into the documents you want in a destination.
It is the single place all enrichment/shaping happens.

## Configuration

For trivial shaping, pass a lambda:

```csharp
cdc.Map<Product>()
   .ToSink("meili", "products")
   .UsingTransform((_, changes, _) =>
   {
       var docs = new Dictionary<DocumentKey, CdcDocument?>(changes.Count);
       foreach (var c in changes)
           docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name };
       return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(docs);
   });
```

For more complex transforms, or anything with dependencies, implement `ICdcTransform<TEntity>` as a class:

```csharp
public sealed class ProductSearchTransform(IPricingService pricing) : ICdcTransform<Product>
{
    public async Task<IReadOnlyDictionary<DocumentKey, CdcDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<Product>> changes, CancellationToken ct)
    {
        var docs = new Dictionary<DocumentKey, CdcDocument?>(changes.Count);
        foreach (var c in changes)
            docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name, ["rrp"] = await pricing.RrpAsync(c.Entity!.Id, ct) };
        return docs;
    }
}

// register:
cdc.Map<Product>()
    .ToSink("meili", "products")
    .UsingTransform<ProductSearchTransform>();
```

## Internals

Transforms are **batch-invoked**: you receive all the insert/update/read changes for the entity in a commit (or a
backfill chunk) and return one document per source key. This lets you resolve many keys in a single
round-trip. Return a `null` document (or simply omit a key) to **delete** that key from the sink.

::: tip
Deletes never reach a transform as the row is already gone. The engine deletes by key directly, using
the mapping's id rule. Your transform only sees inserts, updates, and backfill reads.
:::

## Documents

A document is a `CdcDocument` - a field bag keyed by destination field name. It derives from
`Dictionary<string, object?>`, so it supports the usual initializer syntax:

```csharp
var doc = new CdcDocument { ["name"] = product.Name, ["price"] = product.Price };
// or fluent:
var doc2 = new CdcDocument().Set("name", product.Name).Set("price", product.Price);
```

Sinks consume the document as an `IReadOnlyDictionary<string, object?>`.

## The change event

Each `ChangeEvent<TEntity>` exposes:

| Member | Description |
| --- | --- |
| `Entity` | The current row materialized as `TEntity` (non-null for insert/update/read). |
| `Record` | Current column values keyed by EF property name. |
| `Changes` | Previous values of changed columns (updates), subject to `REPLICA IDENTITY`. |
| `PrimaryKey` / `Key` | The source primary key, and its `DocumentKey`. |
| `GetPrimaryKey<TKey>()` | The single-column key cast to `TKey`. |
| `Metadata` | `Action`, `IsBackfill`, `CommitTimestamp`, `CommitLsn`, table name. |

## Enrichment via the DbContext

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

       var docs = new Dictionary<DocumentKey, CdcDocument?>(orders.Count);
       foreach (var o in orders)
           docs[new DocumentKey(o.Id)] = new CdcDocument
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
own. Declare the relationship with `DependsOn(...)` so Wallaby captures the related table and fans its
changes out to synthetic updates of your entity:

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
- **Inline first page, offloaded tail.** The first [`MaxBatchSize`](/getting-started#options) affected rows
  are re-emitted inline; if more remain, the rest is handed to a *scoped backfill job* that re-snapshots
  them asynchronously. This lets the trigger
  transaction be acknowledged immediately, so a huge fan-out never stalls replication.
- **Coalescing.** Repeated changes to the same principal collapse into a single pending re-snapshot.
- **Same-transaction de-duplication.** If a primary row is changed *and* one of its dependents changes in
  the same transaction, the row is emitted once (its own change wins — the transform already re-reads the
  dependent from current state).

The offloaded tail is therefore **eventually consistent**: for a wide fan-out, the bulk of the re-index
lands shortly *after* the trigger commits rather than in commit order with it. Sinks must be idempotent
(upsert by id) and support at-least-once delivery.

## Document id and backfill version

- `KeyedBy(p => p.Code)` overrides the document id (defaults to the source primary key).
- `WithBackfillVersion("v2")` triggers an automatic re-backfill of just that entity when the string
  changes, bump it whenever you change a transform's output shape. See [Backfill](/backfill).

## Per-row scoping

When the enrichment context or the destination depends on the row's own data (e.g. a `TenantId`), see
[Multi-tenancy](/multi-tenancy) for `ScopedBy` / `UseScopedContext` / `ScopedDestination`.
