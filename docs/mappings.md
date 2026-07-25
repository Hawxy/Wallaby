---
description: "Entity mappings: routing an entity to a sink destination - transforms, mapping classes, document ids, backfill versions, and batch semantics."
---

# Mappings

An **entity mapping** is Wallaby's unit of routing: it declares how one entity type is captured,
transformed and delivered to the destination in a sink. Mappings are
declared per sink inside `WithMappings(...)`:

```csharp
sink.Map<Product>()
    .ToDestination("products") 
    .WithBackfillVersion("v1") 
    .UsingTransform(/* ... */);
```

A mapping's core components:

- **Entity**: `Map<T>()` declares the backing table for capture *and* routes its changes to the
  enclosing sink. The same entity may be mapped under several sinks as each
  mapping runs its own transform.
- **Destination**: `ToDestination(...)` names where documents land within the sink: a search index,
  a table, an endpoint route - whatever the sink maps it to.
- **Transform**: `UsingTransform(...)` specifies how a batch of entity changes into the destination documents.
  See [below](#transforms).
- **Backfill version**: `WithBackfillVersion(...)` re-snapshots the table when the version changes,
  so destinations are rebuilt whenever the output shape changes. See [Backfill](/backfill).
- **Provider extensions**: [`DependsOn(...)`](/providers/entity-framework-core/#dependent-tables)
  re-emits an entity when a related table changes, and [per-row scoping](#per-row-scoping) routes
  each row through tenant-specific contexts and destinations.

## Transforms

A **transform** turns the changes for one entity type into the documents you want in a destination.
For trivial shaping, pass a lambda:

```csharp
sink.Map<Product>()
    .ToDestination("products")
    .UsingTransform((_, changes, _) =>
    {
        var docs = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
        foreach (var c in changes)
            docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
        return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
    });
```

For more complex transforms, or anything with dependencies, implement your provider's transform
interface as a class - [`IWallabyEfTransform<T>`](/providers/entity-framework-core/#class-based-transforms)
(EF Core) or [`IWallabyMartenTransform<T>`](/providers/marten/#class-based-transforms) (Marten) - and
register it with `UsingTransform<TEntity, TTransform>()`. This class is registered & resolved from the container.

## Mapping classes

Inline mappings grow the `AddWallaby` callback and can make your `Program.cs` unwieldy. Move each mapping into a
class implementing `IWallabyEntityMapping<TEntity>`. This is also convenient if you want to store your mappings alongside the transform it wires up:

```csharp
public sealed class ProductSearchMapping : IWallabyEntityMapping<Product>
{
    public void Configure(EntityMapBuilder<Product> map) => map
        .ToDestination("products")
        .WithBackfillVersion("v1")
        .UsingTransform<Product, ProductSearchTransform>();
}
```
Apply it by type:

```csharp
.WithMappings(sink => sink
    .Apply<ProductSearchMapping>()
    .Apply<CategorySearchMapping>());
```

For a mapping that needs constructor arguments, pass an instance directly:
`sink.Apply(new ProductSearchMapping(indexName))`.

## Internals

Transforms are **batch-invoked**: you receive all the insert/update/read changes for the entity in a commit (or a
backfill chunk) and return one document per source key. This lets you resolve many keys in a single
round-trip. Return a `null` document (or simply omit a key) to **delete** that key from the sink.

::: tip
Deletes never reach a transform as the row is already gone. The engine deletes by key directly, using
the mapping's id rule. Your transform only sees inserts, updates, and backfill reads.
:::

## Document ids

Documents are keyed by the source primary key by default; `KeyedBy(...)` derives the id from the
entity instead:

```csharp
sink.Map<Product>()
    .ToDestination("products")
    .KeyedBy(p => p.Sku);
```

Because the engine deletes by key, the custom id must also be computable when the row is gone. A
`KeyedBy` mapping therefore requires `REPLICA IDENTITY FULL` on its table, and self-configuration
**fails at startup** when it is missing (with the DDL to run) - with a partial old row the delete
would silently target a wrong or PK-named document that was never written. With full identity, EF Core
materializes the deleted entity from the old row's values, and Marten rehydrates the deleted document
from the old tuple's `data`; if a delete still arrives without an entity, it fails loudly rather than
falling back to the primary key. The same applies to an entity-derived `ScopedBy` paired with
`ScopedDestination` (deletes must resolve their destination); the `ChangeEvent` overload of `ScopedBy`
reads captured columns instead and carries no such requirement.

## Documents

A document is a `WallabyDocument`, simply a field bag keyed by destination field name. It derives from
`Dictionary<string, object?>`, so it supports the usual initializer syntax alongside a slightly more fluent one:

```csharp
var doc = new WallabyDocument { ["name"] = product.Name, ["price"] = product.Price };
// or fluent:
var doc2 = new WallabyDocument().Set("name", product.Name).Set("price", product.Price);
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

## Per-row scoping

When the enrichment context or the destination depends on the row's own data (e.g. a `TenantId`), see
multi-tenancy for [EF Core](/providers/entity-framework-core/multi-tenancy) (`ScopedBy` /
`UseScopedDbContext` / `ScopedDestination`) or [Marten](/providers/marten/multi-tenancy)
(`ScopedByTenant` / `UseTenantSessions`).
