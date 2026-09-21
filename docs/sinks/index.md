---
title: "Sink destinations for Postgres CDC in .NET"
description: "Every Wallaby destination in one place: Meilisearch, Elasticsearch, OpenSearch, Kafka, pgvector, HTTP webhooks, or your own ISink, and the guarantees they all share."
---

# Sinks

A **sink** is where Wallaby delivers your transformed documents. Wallaby ships sinks for
[Meilisearch](/sinks/meilisearch), [Elasticsearch](/sinks/elasticsearch),
[OpenSearch](/sinks/opensearch), [Kafka](/sinks/kafka), [pgvector](/sinks/pgvector) and
[HTTP webhooks](/sinks/http), and you can deliver anywhere else by
[implementing `ISink`](/sinks/custom).

A single Wallaby host can run several sinks at once. They share one replication slot and one
publication, so adding a second destination costs no extra load on Postgres.

## Choosing a destination

| Destination | Package | Reach for it when | NativeAOT |
| --- | --- | --- | --- |
| [Meilisearch](/sinks/meilisearch) | `Wallaby.Sinks.Meilisearch` | Fast typo-tolerant product or in-app search | no (client SDK) |
| [Elasticsearch](/sinks/elasticsearch) | `Wallaby.Sinks.Elasticsearch` | Search plus analytics, self-managed or Elastic Cloud | no (client SDK) |
| [OpenSearch](/sinks/opensearch) | `Wallaby.Sinks.OpenSearch` | AWS-managed search (Amazon OpenSearch Service) | no (client SDK) |
| [Kafka](/sinks/kafka) | `Wallaby.Sinks.Kafka` | Fanning changes out to other services | yes |
| [pgvector](/sinks/pgvector) | `Wallaby.Sinks.Pgvector` | A RAG corpus that stays inside Postgres | yes |
| [HTTP](/sinks/http) | `Wallaby.Sinks.Http` | Anything that exposes an endpoint | yes |
| [Custom](/sinks/custom) | n/a | Everything else | your call |

Meilisearch, Elasticsearch and OpenSearch depend on client SDKs that are not trim- or
NativeAOT-safe. All other packages are marked `IsAotCompatible`. See each page's NativeAOT
section for the `SerializerOptions` a trimmed host needs.

If you are keeping embeddings fresh rather than building a search index, start at
[RAG & Embeddings](/rag), which covers when to let the destination own embedding and when to do it
in the sink.

## Sink guarantees

These apply to all sinks regardless of what you pick:

- **At-least-once delivery.** The replication slot only advances after a batch is durably delivered,
  so a crash can redeliver the last batch. Every built-in sink upserts and deletes by a stable
  document id.
- **Commit order.** Changes are delivered in the order Postgres committed them, and batches that get
  split into several requests preserve that order.
- **One slot, one ack point.** Every sink on a host reads the same replication stream. Wallaby only
  acknowledges once the batch has reached all of them.
- **Backfill seeding.** A new destination is seeded by [backfill](/backfill), which runs concurrently
  with the live stream and merges with it, so there are no gaps and no stale overwrites.
- **Purge-then-backfill.** Bumping a mapping's backfill version with `purgeOnChange: true` empties
  the destination before rebuilding it, which is how you reshape a document or re-embed a corpus.
- **Permanent failures halt the pipeline.** A misconfiguration or a payload the destination will
  never accept stops the pipeline rather than silently skipping records. Transient failures retry
  with exponential backoff.

## Future plans

There is no built-in sink for Typesense, Algolia, Redis, MongoDB or SQL Server, although these are planned for the future. Any of them is
reachable today through [a custom `ISink`](/sinks/custom), which is a small interface, and sink
contributions are welcome.

## Write your own

[Custom sinks](/sinks/custom) covers the `ISink` contract: batch delivery, how to classify a failure
as retryable or permanent, one-time setup, and purge support.
