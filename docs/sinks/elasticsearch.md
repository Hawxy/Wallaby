---
description: "Keeping Elasticsearch indices continuously in sync with Postgres tables via idempotent bulk upserts and deletes."
---

# Elasticsearch Sink

The `Wallaby.Sinks.Elasticsearch` package keeps Elasticsearch indices continuously in sync with your
Postgres tables. Changes are delivered through the `_bulk` API: upserts are indexed with `_id` set
to a stable document id (so updates are idempotent) and deletions remove by that same id. It works
with self-managed Elasticsearch and Elastic Cloud.

## Install

```bash
dotnet add package Wallaby.Sinks.Elasticsearch
```

## Register

```csharp
cdc.AddElasticsearchSink("search", s =>
{
    s.Endpoint = "https://localhost:9200";
    s.ApiKey = apiKey;            // or Username/Password; omit both for an unsecured cluster
    s.DefaultIndex = "documents"; // optional fallback when a mapping has no destination
});
```

Then attach the entities it indexes, using the destination as the **index name** (index names must
be lowercase):

```csharp
cdc.AddElasticsearchSink("search", s => { /* ... */ })
   .WithMappings(sink => sink
       .Map<Product>()
       .ToDestination("products")
       .UsingTransform(/* ... */));
```

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `Endpoint` | *(required)* | Elasticsearch base URL. |
| `ApiKey` | `null` | Base64 API key (as issued by Kibana or Elastic Cloud). |
| `Username` / `Password` | `null` | Basic auth; mutually exclusive with `ApiKey`. |
| `ConfigureConnection` | `null` | Full override for the client's settings ([see below](#authentication)). |
| `DefaultIndex` | `null` | Index used when a routed record has no destination. |
| `MaxActionsPerRequest` | `500` | Actions per `_bulk` request; larger batches are split into sequential requests, preserving commit order. |
| `TimeoutMs` | `30000` | Per-request timeout. |
| `Refresh` | `false` | When true, bulk requests use `refresh=wait_for` so documents are searchable before the batch is acknowledged. |
| `SerializerOptions` | `null` | Serializer for document values beyond the natively written scalar types (required for such values on NativeAOT hosts). |

## Indices

The sink does not create or configure indices: an index auto-creates on first write with dynamic
mapping. For explicit settings or mappings (analyzers, `dense_vector` fields, shard counts, …),
create the index up front — via Kibana Dev Tools, your infrastructure tooling, or a deployment
script. In-sink index bootstrapping is planned.

## Authentication

`ApiKey` or `Username`/`Password` cover the common schemes. Everything else — Elastic Cloud ids,
certificate fingerprints, client certificates, connection pools, proxies — is configured by taking
over construction of the client's settings with `ConfigureConnection`:

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

When `ConfigureConnection` is set, `ApiKey`/`Username`/`Password` and `TimeoutMs` are ignored —
configure authentication and timeouts on the returned settings.

## Delivery semantics

Delivery is **at-least-once**: a batch may be re-sent after a transient failure, and every action
is idempotent by `_id`, so replays converge to the same documents. Batches are chunked into
sequential `_bulk` requests so commit order is preserved.

Failures are classified per response *and* per bulk item:

- Throttling and server errors (`408`/`429`/`5xx`, connection failures, timeouts) are **retryable** —
  the dispatcher backs off and re-sends.
- Request or item rejections (e.g. `mapper_parsing_exception` from a mapping conflict) are
  **permanent** — they indicate a transform/configuration bug, so the pipeline halts rather than
  silently dropping documents.
- Deleting an already-absent document reports `404` per item; the sink treats that as success.

By default documents become searchable on the index's refresh interval (typically 1s) after the
batch is acknowledged; set `Refresh = true` to make each batch searchable before it is acknowledged,
at an indexing-throughput cost.
