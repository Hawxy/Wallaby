# Marten

The `Wallaby.Marten` package drives capture from a [Marten](https://martendb.io) document store:
Wallaby watches Marten's document tables (`mt_doc_*`), rehydrates each change's JSONB body back into
your document type through the store's own serializer, and routes the documents through the usual
transform → sink pipeline. Soft deletes, conjoined multi-tenancy, and backfills all behave the way a
Marten consumer expects.

## Install

```bash
dotnet add package Wallaby.Marten
dotnet add package Wallaby.Sinks.Meilisearch #optionally add a sink
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

Transforms receive a leased Marten `IQuerySession` for enrichment lookups. Three `UsingTransform`
overloads are available: an `IWallabyMartenTransform<T>` instance, a container-resolved
`UsingTransform<TEntity, TTransform>()`, or the inline lambda above.

::: warning
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

## Combining providers

Marten can be combined with another storage provider in the same Wallaby instance, sharing a single
replication slot — see the [providers overview](/providers/overview#combining-providers).

## NativeAOT

The core `Wallaby` and `Wallaby.Marten` packages are trim- and NativeAOT-compatible
(`IsAotCompatible`). Running under `PublishAot` additionally needs the Marten store itself configured
for AOT:

- **Pre-generated code**: Marten's runtime Roslyn codegen doesn't work under NativeAOT — use
  `TypeLoadMode.Static` with types generated ahead of time (`dotnet run -- codegen write`).
- **Source-generated serialization**: configure Marten's `System.Text.Json` serializer with a
  source-generated `JsonSerializerContext` covering your document types, so Wallaby's rehydration
  through the store's serializer needs no reflection.
- **Root the Marten assembly**: Marten reflects over its own internals during store configuration, so
  it cannot be trimmed yet — add `<TrimmerRootAssembly Include="Marten" />` to your app until Marten's
  tier-1 AOT mode lands.

One core caveat: when a very large transaction spills to disk/database, column values of common scalar
and scalar-array types are encoded natively, but exotic column types fall back to reflection-based
JSON — under NativeAOT that fallback fails the spill with a descriptive error. Marten's captured
columns (`id`, `data`, `tenant_id`, `mt_deleted`, `mt_deleted_at`) are all natively encoded.

The Meilisearch sink is not yet AOT-compatible.

## Limitations (v1)

- **`DependsOn(...)` is not supported** — documents are self-contained; there are no related tables to
  fan out from.
- **`ChangeEvent.Changes` is always `null`** (no JSON diffing).
- **Document hierarchies** (subclass documents sharing the root table) are not capturable and fail
  fast when mapped.
- Strong-typed identifiers and `Duplicate(...)`d fields are not mapped into `Record`.
- Separate-database tenancy is not supported (conjoined and single tenancy are).
