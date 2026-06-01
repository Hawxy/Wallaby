---
outline: deep
---

# Meilisearch Sink

The `Wallaby.Sinks.Meilisearch` package keeps Meilisearch indexes continuously in sync with your
Postgres tables. Upserts are written with a stable primary key (so updates are idempotent) and
deletions remove by that same id.

## Register

```csharp
cdc.AddMeilisearchSink("meili", m =>
{
    m.Host = "http://localhost:7700";
    m.ApiKey = key;            // master or a write key; null for an unsecured instance
    m.DefaultIndex = "search"; // optional fallback when a mapping has no destination
});
```

Then route mappings to it, using the destination as the **index name**:

```csharp
cdc.Map<Product>()
   .ToSink("meili", destination: "products")
   .UsingTransform(/* ... */);
```

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `Host` | *(required)* | Meilisearch base URL. |
| `ApiKey` | `null` | Master/write key; `null` for unsecured. |
| `DefaultIndex` | `null` | Index used when a routed record has no destination. |
| `PrimaryKey` | `id` | Document key field Wallaby injects into every document. |
| `WaitForCompletion` | `true` | Await each indexing task before the batch is acked (honest at-least-once). |
| `WaitTimeoutMs` | `60000` | Max wait per task when `WaitForCompletion`. |
| `WaitIntervalMs` | `50` | Poll interval while waiting. |

## How documents are written

- Your transform's `CdcDocument` fields become the Meilisearch document. Wallaby stamps the configured
  `PrimaryKey` field with the record's document id (derived from the source primary key, or your
  `KeyedBy(...)` rule) - so you don't include it yourself.
- Document ids are sanitized to Meilisearch's allowed set (`[a-zA-Z0-9-_]`); composite-key separators are
  replaced, so composite keys work transparently.
- A transform that returns `null` for a key (or omits it) issues a **delete** for that id.
- Records are grouped by index; within an index, upserts are applied before deletes, and distinct indexes
  are dispatched in parallel.

## Delivery semantics

Network/HTTP/task failures are reported as **retryable** - the dispatcher retries with exponential
backoff. With `WaitForCompletion = true`, a task that finishes `Failed`/`Canceled` also surfaces as a
failure so the batch isn't acked prematurely. Because Meilisearch upserts are by primary key, redelivery
after a crash is safe.

## Per-tenant indexes

Route each tenant to its own index with `ScopedDestination` — see [Multi-tenancy](/multi-tenancy).
