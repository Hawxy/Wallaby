---
description: "Keeping OpenSearch indexes continuously in sync with Postgres tables via idempotent bulk upserts and deletes."
---

# OpenSearch Sink

The `Wallaby.Sinks.OpenSearch` package keeps OpenSearch indexes continuously in sync with your
Postgres tables. Changes are delivered through the `_bulk` API: upserts are indexed with `_id` set
to a stable document id (so updates are idempotent) and deletions remove by that same id. It works
with self-managed OpenSearch and Amazon OpenSearch Service.

## Install

```bash
dotnet add package Wallaby.Sinks.OpenSearch
```

## Register

```csharp
cdc.AddOpenSearchSink("search", s =>
{
    s.Endpoint = "https://localhost:9200";
    s.Username = "wallaby";       // basic auth; omit for an unsecured cluster
    s.Password = password;
    s.DefaultIndex = "documents"; // optional fallback when a mapping has no destination
});
```

Then attach the entities it indexes, using the destination as the **index name** (index names must
be lowercase):

```csharp
cdc.AddOpenSearchSink("search", s => { /* ... */ })
   .WithMappings(sink => sink
       .Map<Product>()
       .ToDestination("products")
       .UsingTransform(/* ... */));
```

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `Endpoint` | *(required)* | OpenSearch base URL. |
| `Username` / `Password` | `null` | Basic auth; `null` for unsecured. |
| `ConfigureConnection` | `null` | Full override for the client's connection settings ([see below](#authentication)). |
| `DefaultIndex` | `null` | Index used when a routed record has no destination. |
| `MaxActionsPerRequest` | `500` | Actions per `_bulk` request; larger batches are split into sequential requests, preserving commit order. |
| `TimeoutMs` | `30000` | Per-request timeout. |
| `Refresh` | `false` | When true, bulk requests use `refresh=wait_for` so documents are searchable before the batch is acknowledged. |
| `SerializerOptions` | `null` | Serializer for document values beyond the natively written scalar types (required for such values on NativeAOT hosts). |

## Indexes

The sink does not create or configure indexes: an index auto-creates on first write with dynamic
mapping. For explicit settings or mappings (analyzers, `knn_vector` fields, shard counts, …),
create the index up front — via Dev Tools, your infrastructure tooling, or a deployment script.
In-sink index bootstrapping is planned.

## Authentication

`Username`/`Password` cover basic auth. Everything else — AWS SigV4, client certificates,
connection pools, proxies — is configured by taking over construction of the client's connection
settings with `ConfigureConnection`:

```csharp
// Amazon OpenSearch Service, signed with the host's AWS credentials
// (requires the OpenSearch.Net.Auth.AwsSigV4 package):
cdc.AddOpenSearchSink("search", s =>
{
    s.Endpoint = "https://my-domain.eu-west-1.es.amazonaws.com";
    s.ConfigureConnection = uri => new ConnectionSettings(
        new SingleNodeConnectionPool(uri), new AwsSigV4HttpConnection(RegionEndpoint.EUWest1));
});
```

When `ConfigureConnection` is set, `Username`/`Password` are ignored — configure all
authentication on the returned settings.

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
