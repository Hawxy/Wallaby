---
title: "Sync Postgres to Meilisearch from .NET"
description: "Keep a Meilisearch index in sync with Postgres from C#: install Wallaby.Sinks.Meilisearch, map an EF Core entity or Marten document, and changes stream in live."
---

# Meilisearch Sink

Keep a Meilisearch index in sync with Postgres from your .NET application. The
`Wallaby.Sinks.Meilisearch` package streams committed row changes out of Postgres logical
replication and writes them to Meilisearch as idempotent upserts and deletes: no polling, no dual
writes, and no reindex script. The sink also supports
[purge-then-backfill](/backfill#purging-before-a-backfill): a purge deletes all of an index's
documents (the index and its settings survive) so the backfill rebuilds it from scratch.

## Quickstart

```bash
dotnet add package Wallaby.Sinks.Meilisearch
```

Register Wallaby, point it at a storage provider, add the sink, and map an entity. The mapping's
destination is the **index name**.

::: code-group

```csharp [EF Core]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .AddMeilisearchSink("meili", m =>
       {
           m.Endpoint = "http://localhost:7700";
           m.ApiKey = key;
           m.DefaultIndex = "search";
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

```csharp [Marten]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseMarten()
       .UseConnectionString(conn)
       .AddMeilisearchSink("meili", m =>
       {
           m.Endpoint = "http://localhost:7700";
           m.ApiKey = key;
           m.DefaultIndex = "search";
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

```csharp [Plain tables]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseTables(tables => tables.Add<Product>())
       .UseConnectionString(conn)
       .AddMeilisearchSink("meili", m =>
       {
           m.Endpoint = "http://localhost:7700";
           m.ApiKey = key;
           m.DefaultIndex = "search";
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")
           .UsingTransform(/* ... */));
});
```

:::

The transform shapes each change into the document you want indexed; see
[mappings](/mappings#transforms). For the Postgres server settings Wallaby needs, see
[getting started](/getting-started#server-prerequisites).

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `Endpoint` | *(required)* | Meilisearch base URL. |
| `ApiKey` | `null` | Master/write key; `null` for unsecured. |
| `DefaultIndex` | `null` | Index used when a routed record has no destination. |
| `PrimaryKey` | `id` | Document key field Wallaby injects into every document. |
| `WaitTimeout` | `60s` | Max wait per indexing task (every task is awaited before the batch is acked). |
| `WaitInterval` | `50ms` | Poll interval while waiting. |
| `MaxRecordsPerRequest` | `500` | Max records per indexing request; larger batches split into sequential requests, keeping each payload under Meilisearch's body limit. |
| `HttpClientName` | `null` | `IHttpClientFactory` client name to send through; `null` uses `MeilisearchSink.ClientNameFor(name)`. |
| `ValidateConfiguredAttributes` | `true` | Check each upsert against its index's [configured attributes](#index-configuration); a document missing one fails delivery **permanently** instead of being silently indexed. |
| `SerializerOptions` | `null` | Serializer for document values beyond the natively written scalar types (see [how documents are written](#how-documents-are-written)). |

## HttpClient

The underlying HttpClient is configurable via the `IHttpClientFactory`'s named client. Use
`MeilisearchSink.ClientNameFor("meili")`, or the name you set
via `HttpClientName`:

```csharp
builder.Services.AddHttpClient(MeilisearchSink.ClientNameFor("meili"))
    .AddCustomResilienceHandler();
```


## Index configuration

By default Meilisearch auto-creates an index on first write (inferring its primary key). To create and
configure an index up front instead, declare it with `ConfigureIndex`. Declared indexes are created (with
the sink's `PrimaryKey`) and have their settings applied on startup.

```csharp
cdc.AddMeilisearchSink("meili", m =>
{
    m.Endpoint = "http://localhost:7700";
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

### Embedders (vector search)

[AI-powered search](https://www.meilisearch.com/docs/learn/ai_powered_search/getting_started_with_ai_search) can be setup via the 
index configuration:

```csharp
m.ConfigureIndex("products", s =>
{
    s.SearchableAttributes = ["name", "description"];
    s.Embedders = new Dictionary<string, Embedder>
    {
        ["default"] = new Embedder
        {
            Source = EmbedderSource.OpenAi,
            Model = "text-embedding-3-small",
            ApiKey = openAiKey,
            DocumentTemplate = "{{doc.name}}: {{doc.description}}",
        },
    };
});
```

With a server-side source (`OpenAi`, `HuggingFace`, `Ollama`, `Rest`), Meilisearch computes vectors
itself from the synced documents. Optionally, with `EmbedderSource.UserProvided`, the
transform carries the vector in the document's `_vectors` field instead:

```csharp
new WallabyDocument
{
    ["name"] = p.Name,
    ["_vectors"] = new Dictionary<string, object?> { ["default"] = embedding }, // float[]
};
```

See [RAG & Embeddings](/rag) for the full pattern, including re-embedding on model changes.

### Attribute validation

By default (`ValidateConfiguredAttributes = true`), every upsert routed to a `ConfigureIndex`-declared index
is checked against that index's configured **searchable**, **filterable**, and **sortable** attributes: if the
document is missing a key for any of them, delivery fails **permanently** with a
`MeilisearchDocumentValidationException` (which halts the pipeline), rather than silently indexing a
document that has a mismatched configuration. 

- A key whose value is `null` counts as present, only an **absent** key is a failure.
- The sink's `PrimaryKey` and Meilisearch's `*` wildcard are exempt.
- A dotted attribute (`author.name`) matches a literal key first, then resolves segment-by-segment the way
  Meilisearch does: through nested dictionary values and through the elements of an array. Validation only
  inspects dictionary-shaped values (`WallabyDocument`, `Dictionary<string, object?>`); a segment landing
  on anything else (a POCO, an anonymous type, a scalar) passes unchecked, so only a dictionary provably
  missing the key ever fails.

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
- Values are encoded by the same reflection-free writer the other sinks use (dates as ISO 8601,
  `byte[]` as base64, vectors as number arrays); any other value type goes through `SerializerOptions`,
  and a value that cannot be encoded fails delivery permanently.
- Ids are encoded for Meilisearch's alphabet (see [Document ids](#document-ids)). A transform field named
  like `PrimaryKey` that holds a different value fails delivery permanently instead of being overwritten.
- A transform that returns `null` for a key (or omits it) issues a **delete** for that id.
- Records are grouped by index; within an index, upserts are applied before deletes (each split into
  requests of at most `MaxRecordsPerRequest` records), and distinct indexes are dispatched in parallel.

## Document ids

Meilisearch ids allow only `[a-zA-Z0-9-_]` and at most 511 bytes, so the sink re-encodes the
[canonical document id](/mappings#id-format) with `MeilisearchDocumentIds.Encode`:

| Canonical id | Meilisearch id |
| --- | --- |
| A single value of allowed characters, not starting with `_` (`42`, a Guid, `order_line`) | Unchanged |
| A composite id whose values all match `[a-zA-Z0-9-]+` (`tenant-a\|42`) | Values joined with `_`: `tenant-a_42` |
| Anything else (`a.b@x.com`, `acme_eu\|42`) | `_e` + base64url of the UTF-8 id: `_eYS5iQHguY29t` |

The encoding is reversible and never gives two keys of one table the same id. An id that would exceed 511
bytes fails delivery permanently; use `KeyedBy(...)` to derive a shorter one.

::: warning
A search hit's `id` is only your source key for integer, Guid, and similar plain keys. Before using it to
load or authorize a record, decode it with `MeilisearchDocumentIds.Decode(id, keyParts)` (and
`DocumentKey.SplitId` for composite keys), or read the key from a field your transform emits.
:::

## Delivery semantics

Every indexing task is awaited to completion; a task that finishes `Failed`/`Canceled` surfaces as a
failure so the batch isn't acked prematurely. Because Meilisearch upserts are by primary key, redelivery
after a crash is safe.

Failures are classified for the dispatcher by their Meilisearch error code (from the HTTP response or
the failed task):

| Error | Outcome |
| --- | --- |
| Transport failures (connection, socket, timeout), responses without a Meilisearch error code | **Retryable** - the dispatcher retries with exponential backoff. |
| Environment-fixable codes: `index_not_found`, `internal`, disk/queue pressure, … | **Retryable**. Exception: `index_not_found` on a **delete** is treated as success, because deletes don't auto-create indexes, so a delete-only batch to an index that was never written (e.g. a per-tenant `ScopedDestination` index that saw a deletion before any upsert) has nothing to remove and would otherwise retry forever. |
| Deterministic configuration/credential/payload errors: `invalid_api_key`, `missing_authorization_header`, `payload_too_large`, `invalid_document_id`, `missing_document_id`, `invalid_document_fields`, `invalid_document_geo_field`, `invalid_index_uid`, `invalid_index_primary_key`, `index_primary_key_already_exists`, `index_primary_key_multiple_candidates_found`, `bad_request` | **Permanent** - the pipeline halts (a `MeilisearchTaskFailedException` carries the failed task's code). |
| A record with no destination and no `DefaultIndex`, a document missing a [configured attribute](#attribute-validation), or a document value that cannot be encoded | **Permanent**. |

## Per-tenant indexes

Route each tenant to its own index with `ScopedDestination` - see multi-tenancy for
[EF Core](/providers/entity-framework-core/multi-tenancy) or [Marten](/providers/marten/multi-tenancy).
