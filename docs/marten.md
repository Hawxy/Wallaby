# Marten

The `Wallaby.Marten` package drives capture from a [Marten](https://martendb.io) document store:
Wallaby watches Marten's document tables (`mt_doc_*`), rehydrates each change's JSONB body back into
your document type through the store's own serializer, and routes the documents through the usual
transform → sink pipeline. Soft deletes, conjoined multi-tenancy, and backfills all behave the way a
document database consumer expects.

## Install

```bash
dotnet add package Wallaby
dotnet add package Wallaby.Marten
```

## Register

Chain `UseMarten()` after your usual `AddMarten` registration. Wallaby resolves the `IDocumentStore`
from the container; use the `UseMarten(sp => ...)` overload for multiple stores.

```csharp
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Marten;

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
       .AddMeilisearchSink("meili", m => { /* ... */ })
       .Map<Order>()
            .ToSink("meili", destination: "orders")
            .UsingTransform((session, changes, ct) =>
            {
                var docs = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
                foreach (var c in changes)
                    docs[c.Key] = new WallabyDocument { ["number"] = c.Entity!.Number, ["total"] = c.Entity!.Total };
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
            });
});
```

Transforms receive a leased Marten `IQuerySession` for enrichment lookups — the Marten counterpart of
the EF Core provider's `DbContext`. The three `UsingTransform` overloads mirror the EF Core ones:
an `IWallabyMartenTransform<T>` instance, a container-resolved `UsingTransform<TEntity, TTransform>()`,
or the inline lambda above.

::: warning Register documents up front
Wallaby builds its capture model at startup from the store's registered documents
(`RegisterDocumentType<T>()`, `Schema.For<T>()`, …). Marten's usual lazy discovery — registering a
document type the first time a session touches it — happens too late for capture, so a `Map<T>()` for
an unregistered document fails fast with guidance.
:::

## What gets captured

For each mapped document Wallaby captures the minimal column set: `id`, the `data` JSONB body,
`tenant_id` for conjoined tenancy, and the `mt_deleted`/`mt_deleted_at` flags for soft-deleted
documents. Marten's other metadata columns (`mt_version`, `mt_last_modified`, …) are ignored, and
`ChangeEvent.Changes` is always `null` — change granularity is the whole document.

The change's `Record` carries the id under your document's id member name, plus `TenantId` and
`Deleted` when applicable.

## Soft deletes

A soft delete (`session.Delete<T>(...)` on a `SoftDeleted()` document) flips `mt_deleted` in place —
but consumers of a search index or read model almost never want "deleted but still there" documents.
Wallaby surfaces it as a **Delete event**: the sink document is removed, exactly like a hard delete.
Correspondingly, backfills skip rows where `mt_deleted = true`, and an **un-delete**
(`session.UndoDeleteWhere<T>(...)`) re-emits the full document as an upsert.

Soft-deleted document tables need `REPLICA IDENTITY FULL`: an un-delete's UPDATE doesn't touch `data`,
so for a TOASTed (large) body Postgres omits it from the new tuple, and Wallaby reads it from the old
tuple instead. Self-config detects a missing replica identity at startup and logs the exact
`ALTER TABLE ... REPLICA IDENTITY FULL` to run (set `RequireFullReplicaIdentity` to make it a hard
failure instead).

## Multi-tenancy (conjoined)

For documents with conjoined tenancy the captured key is `[tenant_id, id]`, so equal ids across
tenants stay distinct end to end. Scope a mapping by tenant with `ScopedByTenant()`:

```csharp
cdc.UseMarten()
   .UseTenantSessions()                             // lease store.QuerySession(tenantId) per tenant
   .Map<Order>()
        .ToSink("meili")
        .ScopedByTenant()
        .ScopedDestination(tenant => $"orders-{tenant}")   // optional index-per-tenant
        .UsingTransform(/* ... */);
```

The engine sub-groups each transform batch per tenant, `UseTenantSessions()` hands the transform a
same-tenant `IQuerySession`, and `ScopedDestination` routes per tenant — including deletes, whose
tenant comes from the key columns. Only conjoined (and single) tenancy is supported; separate-database
tenancy is not.

## Running alongside EF Core

Both providers can be registered in one Wallaby instance sharing a single replication
slot/publication/checkpoint — global commit ordering is preserved across EF tables and Marten
document tables:

```csharp
cdc.UseEntityFrameworkCore<AppDbContext>()
   .UseMarten()
   .Map<Product>().ToSink("meili").UsingTransform(/* DbContext transform */)
   .Map<Order>().ToSink("meili").UsingTransform(/* IQuerySession transform */);
```

Each mapping resolves to the provider that models its type; the provider-typed `UsingTransform`
overloads break a tie when both model it, or pin explicitly with `Map<T>().FromProvider("Marten")`.

## Limitations (v1)

- **`DependsOn(...)` is not supported** — documents are self-contained; there are no related tables to
  fan out from.
- **`ChangeEvent.Changes` is always `null`** (no JSON diffing).
- **Document hierarchies** (subclass documents sharing the root table) are not capturable: declared
  ones fail fast, `CaptureAllMappedTables()` skips them.
- Strong-typed identifiers and `Duplicate(...)`d fields are not mapped into `Record`.
- Separate-database tenancy is not supported (conjoined and single tenancy are).
