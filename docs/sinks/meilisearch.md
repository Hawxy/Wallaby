---
description: "Keeping Meilisearch indexes continuously in sync with Postgres tables via idempotent upserts and deletes."
---

# Meilisearch Sink

The `Wallaby.Sinks.Meilisearch` package keeps Meilisearch indexes continuously in sync with your
Postgres tables. Upserts are written with a stable primary key (so updates are idempotent) and
deletions remove by that same id.

## Install

```bash
dotnet add package Wallaby.Sinks.Meilisearch
```

## Register

```csharp
cdc.AddMeilisearchSink("meili", m =>
{
    m.Host = "http://localhost:7700";
    m.ApiKey = key;            // master or a write key; null for an unsecured instance
    m.DefaultIndex = "search"; // optional fallback when a mapping has no destination
});
```

Then attach the entities it indexes, using the destination as the **index name**:

```csharp
cdc.AddMeilisearchSink("meili", m => { /* ... */ })
   .WithMappings(sink => sink
       .Map<Product>()
       .ToDestination("products")
       .UsingTransform(/* ... */));
```

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `Host` | *(required)* | Meilisearch base URL. |
| `ApiKey` | `null` | Master/write key; `null` for unsecured. |
| `DefaultIndex` | `null` | Index used when a routed record has no destination. |
| `PrimaryKey` | `id` | Document key field Wallaby injects into every document. |
| `WaitTimeoutMs` | `60000` | Max wait per indexing task (every task is awaited before the batch is acked). |
| `WaitIntervalMs` | `50` | Poll interval while waiting. |
| `ValidateConfiguredAttributes` | `true` | Check each upsert against its index's [configured attributes](#index-configuration); a document missing one fails delivery **permanently** instead of being silently indexed. |

## Index configuration

By default Meilisearch auto-creates an index on first write (inferring its primary key). To create and
configure an index up front instead, declare it with `ConfigureIndex`. Declared indexes are created (with
the sink's `PrimaryKey`) and have their settings applied on startup.

```csharp
cdc.AddMeilisearchSink("meili", m =>
{
    m.Host = "http://localhost:7700";
    m.ConfigureIndex("products", s =>
    {
        s.SearchableAttributes = ["name", "description"];
        s.FilterableAttributes = ["category", "tenantId"];
        s.SortableAttributes   = ["price"];
    });
});
```

`Settings` is Meilisearch's own settings type, so you have full control (ranking rules, stop words,
synonyms, faceting, …). Setup is idempotent and re-applied on each leadership acquisition.

### Attribute validation

By default (`ValidateConfiguredAttributes = true`), every upsert routed to a `ConfigureIndex`-declared index
is checked against that index's configured **searchable**, **filterable**, and **sortable** attributes: if the
document is missing a key for any of them, delivery fails **permanently** with a
`MeilisearchDocumentValidationException` (which halts the pipeline), rather than silently indexing a
document that has a mismatched configuration. 
A few details:

- A key whose value is `null` counts as present, only an **absent** key is a failure.
- The sink's `PrimaryKey` and Meilisearch's `*` wildcard are exempt.

Set `ValidateConfiguredAttributes = false` to opt out and let Meilisearch accept whatever the transform emits.

::: tip
Per-tenant indexes from [`ScopedDestination`](/providers/entity-framework-core/multi-tenancy) are not supported at the moment. 
They're auto-created on first write with the sink's `PrimaryKey` and use Meilisearch defaults.

If a way to customize this would be useful, open an issue.
:::

## How documents are written

- Your transform's `WallabyDocument` fields become the Meilisearch document. Wallaby stamps the configured
  `PrimaryKey` field with the record's document id (derived from the source primary key, or your
  `KeyedBy(...)` rule) - so you don't include it yourself.
- Document ids are sanitized to Meilisearch's allowed set (`[a-zA-Z0-9-_]`); composite-key separators are
  replaced, so composite keys work transparently.
- A transform that returns `null` for a key (or omits it) issues a **delete** for that id.
- Records are grouped by index; within an index, upserts are applied before deletes, and distinct indexes
  are dispatched in parallel.

## Delivery semantics

Network/HTTP/task failures are reported as **retryable** - the dispatcher retries with exponential
backoff. Every indexing task is awaited to completion; a task that finishes `Failed`/`Canceled` surfaces as
a failure so the batch isn't acked prematurely. Because Meilisearch upserts are by primary key, redelivery
after a crash is safe.

## Per-tenant indexes

Route each tenant to its own index with `ScopedDestination` - see multi-tenancy for
[EF Core](/providers/entity-framework-core/multi-tenancy) or [Marten](/providers/marten/multi-tenancy).
