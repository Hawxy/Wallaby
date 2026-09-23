---
title: "Sync Postgres to Elasticsearch from .NET"
description: "Keep Elasticsearch indices in sync with Postgres from C#: bulk upserts and deletes driven by logical replication, mapped from your EF Core or Marten model."
---

# Elasticsearch Sink

Keep Elasticsearch indices in sync with Postgres from your .NET application. The
`Wallaby.Sinks.Elasticsearch` package streams committed row changes out of Postgres logical
replication and delivers them through the `_bulk` API: upserts are indexed with `_id` set to a
stable document id (so updates are idempotent) and deletions remove by that same id. No polling, no
dual writes, no reindex script. It works with self-managed Elasticsearch and Elastic Cloud.

## Quickstart

```bash
dotnet add package Wallaby.Sinks.Elasticsearch
```

Register Wallaby, point it at a storage provider, add the sink, and map an entity. The mapping's
destination is the **index name** (index names must be lowercase).

::: code-group

```csharp [EF Core]
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .AddElasticsearchSink("search", s =>
       {
           s.Endpoint = "https://localhost:9200";
           s.ApiKey = apiKey;
           s.DefaultIndex = "documents";
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
       .AddElasticsearchSink("search", s =>
       {
           s.Endpoint = "https://localhost:9200";
           s.ApiKey = apiKey;
           s.DefaultIndex = "documents";
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
       .AddElasticsearchSink("search", s =>
       {
           s.Endpoint = "https://localhost:9200";
           s.ApiKey = apiKey;
           s.DefaultIndex = "documents";
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
| `Endpoint` | *(required)* | Elasticsearch base URL. |
| `ApiKey` | `null` | Base64 API key (as issued by Kibana or Elastic Cloud). |
| `Username` / `Password` | `null` | Basic auth; mutually exclusive with `ApiKey`. |
| `ConfigureConnection` | `null` | Full override for the client's settings ([see below](#authentication)). |
| `DefaultIndex` | `null` | Index used when a routed record has no destination; a record with neither fails permanently. |
| `MaxRecordsPerRequest` | `500` | Records per `_bulk` request; larger batches are split into sequential requests, preserving commit order. |
| `Timeout` | `30s` | Per-request timeout. |
| `Refresh` | `false` | When true, bulk requests use `refresh=wait_for` so documents are searchable before the batch is acknowledged. |
| `SerializerOptions` | `null` | Serializer for document values beyond the natively written scalar types (numbers, strings, dates, `byte[]`, vectors). |

## Indices

The sink doesn't create or configure indices. An index is created automatically on first write with
dynamic mapping, as long as the cluster's `action.auto_create_index` setting allows it (it does by
default). For explicit settings or mappings (analyzers, `dense_vector` fields, shard counts, …),
create the index up front with Kibana Dev Tools, your infrastructure tooling, or a deployment script.
In-sink index bootstrapping is planned.

## Vector search

A transform can emit an embedding as a `float[]` or `ReadOnlyMemory<float>` field. The sink writes
either as a plain JSON number array, so no `SerializerOptions` are needed. Dynamic mapping would infer
an ordinary `float` field, so create the index up front with an explicit `dense_vector` mapping:

```json
PUT /products
{
  "mappings": {
    "properties": {
      "embedding": { "type": "dense_vector", "dims": 1536, "index": true, "similarity": "cosine" }
    }
  }
}
```

Don't pass a quantized vector as `byte[]`: byte arrays serialize as base64 strings, not arrays.

Elasticsearch can also do the embedding for you. Map the field as
[`semantic_text`](https://www.elastic.co/docs/solutions/search/semantic-search/semantic-search-semantic-text)
(backed by an inference endpoint) and sync plain text, and the cluster chunks and embeds it at index
time, with no vectors in your pipeline at all. Check two things first:

- The inference API needs an appropriate Elastic subscription, and the default ELSER endpoint needs
  ML nodes.
- Inference runs on every indexed document. Live changes embed incrementally, but a
  [backfill](/backfill) re-runs inference over the whole corpus.

See [RAG & Embeddings](/rag).

## Authentication

`ApiKey` or `Username`/`Password` cover the common schemes. For anything else (Elastic Cloud ids,
certificate fingerprints, client certificates, connection pools, proxies), take over construction of
the client's settings with `ConfigureConnection`:

```csharp
// Self-managed cluster with the self-signed certificate Elasticsearch generates on setup:
cdc.AddElasticsearchSink("search", s =>
{
    s.Endpoint = "https://localhost:9200";
    s.ConfigureConnection = uri => new ElasticsearchClientSettings(uri)
        .CertificateFingerprint("A1:B2:...")   // printed during cluster setup
        .Authentication(new ApiKey(apiKey));
});

// Elastic Cloud (the cloud id encodes the endpoint, so the uri argument is unused):
s.ConfigureConnection = _ => new ElasticsearchClientSettings(cloudId, new ApiKey(apiKey));
```

When `ConfigureConnection` is set, leave `ApiKey`, `Username` and `Password` unset (registration
fails otherwise) and configure authentication on the returned settings. `Timeout` still applies per
request.

## Purging

The sink implements [purge-then-backfill](/backfill#purging-before-a-backfill): a purge runs
`_delete_by_query` with `match_all` against the mapping's index (`conflicts=proceed`, `refresh=true`).
It runs synchronously under the per-request `Timeout`, so a very large index may need a longer
timeout. If the index doesn't exist yet, there's nothing to purge.

## Delivery semantics

Delivery is **at-least-once**: a batch may be re-sent after a transient failure, and every action
is idempotent by `_id`, so replays converge to the same documents. Batches are chunked into
sequential `_bulk` requests so commit order is preserved.

Failures are classified per response *and* per bulk item:

- Throttling and server errors (`408`/`429`/`5xx`, connection failures, timeouts) are **retryable**:
  the dispatcher backs off and re-sends.
- Request or item rejections (e.g. `mapper_parsing_exception` from a mapping conflict) are
  **permanent**. They point to a bug in a transform or the configuration, so the pipeline halts
  rather than silently dropping documents.
- Deleting a document that's already gone reports `404` for that item, which the sink treats as
  success.

By default, documents become searchable on the index's refresh interval (typically 1s) after the
batch is acknowledged. Set `Refresh = true` to make each batch searchable before it's acknowledged,
at a cost to indexing throughput.
