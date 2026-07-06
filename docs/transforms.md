# Transforms

A **transform** turns the changes for one entity type into the documents you want in a destination.
It is the single place all enrichment/shaping happens.

## Configuration

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
interface as a class — [`IWallabyEfTransform<T>`](/providers/entity-framework-core/#class-based-transforms)
(EF Core) or [`IWallabyMartenTransform<T>`](/providers/marten/#class-based-transforms) (Marten) — and
register it with `UsingTransform<TEntity, TTransform>()`; the class is resolved from the container.

## Mapping classes

Inline mappings grow the `AddWallaby` callback by a block per entity per sink and can make your `Program.cs` unwieldy. Move each mapping into a
class implementing `IWallabyEntityMapping<TEntity>` - typically alongside the transform it wires up -
and apply it by type:

```csharp
public sealed class ProductSearchMapping : IWallabyEntityMapping<Product>
{
    public void Configure(EntityMapBuilder<Product> map) => map
        .ToDestination("products")
        .WithBackfillVersion("v1")
        .UsingTransform<Product, ProductSearchTransform>();
}
```

```csharp
.WithMappings(sink => sink
    .Apply<ProductSearchMapping>()
    .Apply<CategorySearchMapping>());
```

For a mapping that needs constructor arguments, pass a configured instance:
`sink.Apply(new ProductSearchMapping(indexName))`.

## Internals

Transforms are **batch-invoked**: you receive all the insert/update/read changes for the entity in a commit (or a
backfill chunk) and return one document per source key. This lets you resolve many keys in a single
round-trip. Return a `null` document (or simply omit a key) to **delete** that key from the sink.

::: tip
Deletes never reach a transform as the row is already gone. The engine deletes by key directly, using
the mapping's id rule. Your transform only sees inserts, updates, and backfill reads.
:::

## Documents

A document is a `WallabyDocument` - a field bag keyed by destination field name. It derives from
`Dictionary<string, object?>`, so it supports the usual initializer syntax:

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

## Document id and backfill version

- `KeyedBy(p => p.Code)` overrides the document id (defaults to the source primary key).

## Per-row scoping

When the enrichment context or the destination depends on the row's own data (e.g. a `TenantId`), see
multi-tenancy for [EF Core](/providers/entity-framework-core/multi-tenancy) (`ScopedBy` /
`UseScopedDbContext` / `ScopedDestination`) or [Marten](/providers/marten/multi-tenancy)
(`ScopedByTenant` / `UseTenantSessions`).
