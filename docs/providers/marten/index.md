---
description: "Capturing Marten document changes: JSONB rehydration through the store's serializer, soft deletes, multi-tenancy, and backfills."
---

# Marten

The `Wallaby.Providers.Marten` package drives capture from a [Marten](https://martendb.io) document store:
Wallaby watches Marten's document tables (`mt_doc_*`), rehydrates each change's JSONB body back into
your document type through the store's own serializer, and routes the documents through the usual
transform → sink pipeline. 

## Install

```bash
dotnet add package Wallaby.Providers.Marten
```

## Register

Chain `UseMarten()` after your usual `AddMarten` registration. Wallaby resolves the `IDocumentStore`
from the container. Optionally use the `UseMarten(sp => ...)` overload for multiple stores.

You must also supply a connection string via `UseConnectionString(...)`, or any other [options-pattern mechanism](/configuration#options-pattern) such as configuration binding. This is so Wallaby can manage additional connections itself. Multi-host connection strings are supported, but Wallaby will only connect to your primary node.

```csharp
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.Marten;

builder.Services.AddMarten(options =>
{
    options.Connection(conn);
    options.RegisterDocumentType<Order>();          // documents must be registered up front
    options.Schema.For<Invoice>().SoftDeleted();
});

builder.Services.AddWallaby(cdc =>
{
    cdc.UseMarten()
       .UseConnectionString(conn)

        // Sink configuration below - example
       .AddMeilisearchSink("meili", m => { /* ... */ })
       .WithMappings(sink => sink
            .Map<Order>()
            .ToDestination("orders")
            .UsingTransform(/** **/);
});
```


::: warning
Wallaby builds its capture model at startup from the store's registered documents
(`RegisterDocumentType<T>()`, `Schema.For<T>()`, …). Marten's lazy discovery of registering a
document type the first time a session touches it happens too late for capture, so a `Map<T>()` for
an unregistered document fails fast with guidance.
:::

## Transforms

Transforms receive a Marten `IQuerySession` for enrichment lookups. Three `UsingTransform`
overloads are available: one taking a standalone `IWallabyMartenTransform<T>` instance, a container-resolved
`UsingTransform<TEntity, TTransform>()`, or an inline lambda.

### Class-based transforms

For more complex transforms, or anything with dependencies, implement `IWallabyMartenTransform<TEntity>`
as a class. It is resolved from the container. The leased `IQuerySession` supports enrichment lookups
against other documents:

```csharp
public sealed class OrderSearchTransform : IWallabyMartenTransform<Order>
{
    public async Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        IQuerySession session, IReadOnlyList<ChangeEvent<Order>> changes, CancellationToken ct)
    {
        var customerIds = changes.Select(c => c.Entity!.CustomerId).Distinct().ToList();
        var customers = await session.Query<Customer>()
            .Where(c => customerIds.Contains(c.Id))
            .ToListAsync(ct);
        var names = customers.ToDictionary(c => c.Id, c => c.Name);

        var docs = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
        foreach (var c in changes)
            docs[c.Key] = new WallabyDocument
            {
                ["number"] = c.Entity!.Number,
                ["customer"] = names.GetValueOrDefault(c.Entity!.CustomerId),
            };
        return docs;
    }
}

// register:
sink.Map<Order>()
    .ToDestination("orders")
    .UsingTransform<Order, OrderSearchTransform>();
```

::: tip
The registration itself can also live outside `AddWallaby` as a
[mapping class](/mappings#mapping-classes): `sink.Apply<ProductSearchMapping>()`.
:::

## What gets captured

For each mapped document Wallaby captures the minimal column set: `id`, the `data` JSONB body,
`tenant_id` for conjoined tenancy, and the `mt_deleted`/`mt_deleted_at` flags for soft-deleted
documents. Marten's other metadata columns (`mt_version`, `mt_last_modified`, …) are ignored, and `ChangeEvent.Changes` is always `null` as change granularity is the whole document.

The change's `Record` carries the id under your document's id member name, plus `TenantId` and
`Deleted` when applicable.

## Deletes and document identity

A delete's `ChangeEvent.Entity` carries the deleted document, rehydrated from the old tuple's `data`,
whenever the table has `REPLICA IDENTITY FULL` (without it, only the key columns are on the wire and
`Entity` is null). This is what makes [`KeyedBy(...)`](/mappings#document-ids) and entity-derived
[`ScopedDestination`](/providers/marten/multi-tenancy) routing work on deletes as the custom id or scope
key comes from the document itself.

## Soft deletes

A soft delete (`session.Delete<T>(...)` on a `SoftDeleted()` document) flips `mt_deleted` in place, but consumers of a search index or read model almost never want "deleted but still there" documents.
Wallaby surfaces it as a **Delete event**: the sink document is removed, exactly like a hard delete.
Correspondingly, backfills skip rows where `mt_deleted = true`, and an **un-delete**
(`session.UndoDeleteWhere<T>(...)`) re-emits the full document as an upsert.

Soft-deleted document tables need `REPLICA IDENTITY FULL`: an un-delete's UPDATE doesn't touch `data`, so for a TOASTed (large) body Postgres omits it from the new tuple. 
If you're using soft deletes you should opt into the below.

## Managed replica identity

Since Marten already manages its own schema, Wallaby can hand it the replica-identity DDL too.
Chain the schema feature onto your `AddMarten` registration:

```csharp
builder.Services.AddMarten(options => { /* ... */ })
    .ApplyAllDatabaseChangesOnStartup()
    .ManageWallabyReplicaIdentity();
```

Every captured table that needs `REPLICA IDENTITY FULL`, such as soft-deleted documents and mappings with a
[`ScopedDestination`](/providers/marten/multi-tenancy) are derived from the capture model and joins
Marten's normal migration flow: applied by `ApplyAllDatabaseChangesOnStartup()` /
`ApplyAllConfiguredChangesToDatabaseAsync()`, and included in exported patches. 
The same call chains onto `AddMartenStore<TStore>(...)` for a separately
registered document store.

::: tip
With [`RequireFullReplicaIdentity`](/configuration#general-options) set, pair this with
`ApplyAllDatabaseChangesOnStartup()` and register `AddMarten` **before** `AddWallaby` so the DDL is
applied before Wallaby validates the tables.
:::

## NativeAOT

The core `Wallaby` and `Wallaby.Providers.Marten` packages are trim- and NativeAOT-compatible
(`IsAotCompatible`). Running under `PublishAot` additionally needs the Marten store itself configured for AOT. 
Namely, you'll need to configure Marten's `System.Text.Json` serializer with a
source-generated `JsonSerializerContext` covering your document types, so Wallaby's rehydration through the store's serializer needs no reflection.

One core caveat: when a very large transaction spills to disk/database, column values of common scalar and scalar-array types are encoded natively, 
but exotic column types fall back to reflection-based JSON - under NativeAOT that fallback fails the spill with a descriptive error. 
Marten's captured columns (`id`, `data`, `tenant_id`, `mt_deleted`, `mt_deleted_at`) are all natively encoded.

## Limitations (v1)

- **`DependsOn(...)` is not supported**: documents are self-contained; there are no related tables to fan out from.
- **`ChangeEvent.Changes` is always `null`** (no JSON diffing).
- **Document hierarchies** (subclass documents sharing the root table) are not capturable and fail fast when mapped.
- Strong-typed identifiers and `Duplicate(...)`d fields are not mapped into `Record`.
- Separate-database tenancy is not supported (conjoined and single tenancy are).
