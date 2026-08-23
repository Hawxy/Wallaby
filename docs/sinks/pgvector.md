---
description: "Syncing Postgres tables into pgvector vector tables, with optional sink-side embedding that re-embeds only changed text."
---

# Pgvector Sink

The `Wallaby.Sinks.Pgvector` package keeps [pgvector](https://github.com/pgvector/pgvector) tables
continuously in sync with your source tables - the "my RAG corpus is just Postgres" setup, with no
extra search infrastructure. Upserts are idempotent by id and deletes remove by id, so redelivery
converges. Its headline feature is **sink-side embedding**: configure an embedding generator and the
sink embeds documents at delivery time, re-embedding only rows whose text actually changed. The
destination table doubles as the durable embedding cache, so restarts, failovers, and re-backfills
never re-embed unchanged text.

## Install

```bash
dotnet add package Wallaby.Sinks.Pgvector
```

## Register

```csharp
cdc.AddPgvectorSink("vectors", v =>
{
    v.ConnectionString = vectorDbConn;   // often a different database than the CDC source
    v.Dimensions = 1536;
    v.EmbeddingGenerator = generator;    // any Microsoft.Extensions.AI IEmbeddingGenerator
    v.EmbedText = d => $"{d["name"]}\n{d["description"]}";
    v.EmbeddingVersion = "text-embedding-3-small/1";
})
.WithMappings(sink => sink
    .Map<Product>()
    .ToDestination("products")           // the destination table
    .WithBackfillVersion("v1", purgeOnChange: true)
    .UsingTransform(/* emit name + description as plain text */));
```

`generator` is any
[`Microsoft.Extensions.AI`](https://learn.microsoft.com/dotnet/ai/microsoft-extensions-ai)
`IEmbeddingGenerator<string, Embedding<float>>` - OpenAI, Azure, Ollama, Bedrock, ONNX, or your own.
Leave the three embedding options unset and the sink instead stores a vector your transform supplies
in the document's `VectorField` (default `embedding`, as `float[]` or `ReadOnlyMemory<float>`).

## The table

One table per destination, created on initialization (or first delivery) when `CreateTable` is on:

```sql
CREATE TABLE IF NOT EXISTS "public"."products" (
    id         text PRIMARY KEY,
    text_hash  text,
    embedding  vector(1536),
    document   jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
```

- `id` is the record's document id (source primary key or your `KeyedBy(...)` rule).
- `document` is the transform's field bag as jsonb (in pass-through mode, minus the vector field).
- `text_hash` is SHA-256 over `EmbeddingVersion` + the embedded text; `embedding`/`text_hash` are
  null when `EmbedText` returned nothing for the row.
- **No vector index is created.** Build the HNSW index yourself *after* the initial backfill -
  index maintenance during a large bulk load is the slow way around:

```sql
CREATE INDEX ON "public"."products" USING hnsw (embedding vector_cosine_ops);
```

Query it like any pgvector table (`ORDER BY embedding <=> $1 LIMIT 10`), joining fields out of
`document` as needed.

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `ConnectionString` | *(required)* | Destination database. |
| `ConfigureDataSource` | `null` | Extra `NpgsqlDataSourceBuilder` configuration (TLS callbacks, loggers, ...). |
| `Schema` | `public` | Schema holding the destination tables. |
| `DefaultTable` | `null` | Table used when a routed record has no destination. |
| `Dimensions` | *(required)* | The `vector(N)` dimension; every stored vector must match, a mismatch fails permanently. |
| `CreateTable` | `true` | Create missing destination tables on initialization / first delivery. |
| `CreateExtension` | `true` | `CREATE EXTENSION IF NOT EXISTS vector` on initialization (fails with guidance when the role lacks the privilege). |
| `EmbeddingGenerator` | `null` | Embeds at delivery time; configure together with `EmbedText` and `EmbeddingVersion`. |
| `EmbedText` | `null` | Selects the text to embed from a document's field bag; null/empty stores a null vector. |
| `EmbeddingVersion` | `null` | Model/prompt identity folded into `text_hash`; change it (and bump the mapping's backfill version) to re-embed. |
| `MaxEmbeddingBatchSize` | `96` | Texts per embedding call. |
| `MaxEmbeddingConcurrency` | `1` | Embedding calls in flight at once; raise it to overlap calls on large backfills when the provider's rate limits allow. |
| `IsTransientEmbeddingError` | `null` | Classifies embedding exceptions retryable vs permanent; the default retries everything except `ArgumentException`/`NotSupportedException`. |
| `VectorField` | `embedding` | Without a generator: the document field carrying the transform-supplied vector. |
| `MaxRowsPerBatch` | `500` | Rows per database round-trip. |
| `SerializerOptions` | `null` | Serializer for document values beyond the natively written scalar types (required for such values on NativeAOT hosts). |

## How embedding is gated

Per delivered batch (after last-write-wins dedup per id), the sink reads the stored `text_hash` for
the affected ids from the destination table and embeds **only** rows whose hash is missing or
different - one `SELECT`, then one embedding call per `MaxEmbeddingBatchSize` changed texts. Rows
whose hash matches update `document`/`updated_at` and keep their stored vector untouched - and when
the document is unchanged too, the upsert skips the row entirely, so a re-backfill of an unchanged
corpus costs no embedding calls *and* no row rewrites (no WAL or vacuum churn).

Because the gate is the destination row itself, it needs no cache infrastructure and survives
everything the process doesn't: restarts, leader failover, and re-backfills all skip unchanged text.
The two ways to force re-embedding are the ones that should: changing `EmbeddingVersion` (new hash)
and [purging](/backfill#purging-before-a-backfill) (deletes the rows, hashes included) - which is
exactly what `WithBackfillVersion(..., purgeOnChange: true)` does on a model migration.

## Delivery semantics

Delivery is **at-least-once** and idempotent by id. Within a batch the last write per id wins;
writes for a table run in one transaction. Failures classify for the dispatcher:

| Error | Outcome |
| --- | --- |
| Transient database failures (Npgsql transient errors, timeouts, connection loss) | **Retryable** - the dispatcher retries with backoff. |
| Embedding-provider failures classified transient by `IsTransientEmbeddingError` (default: nearly all, e.g. 429/5xx) | **Retryable** - already-stored hashes keep the retry cheap. |
| Non-transient Postgres rejections, dimension mismatches, non-transient embedding errors, a record with no destination and no `DefaultTable`, an invalid destination table name | **Permanent** - the pipeline halts. |

Destination table names (including [`ScopedDestination`](/providers/entity-framework-core/multi-tenancy)
results) must be 1-63 characters of `[a-zA-Z0-9_]`; anything else fails permanently rather than being
quoted into DDL.

The sink implements purge-then-backfill: a [purge](/backfill#purging-before-a-backfill) issues
`DELETE FROM` on the destination table (the table and any indexes survive), so the following
backfill rebuilds - and re-embeds - from scratch. `DELETE` works under ordinary grants but writes
WAL proportional to the corpus; for a very large table you can `TRUNCATE` it manually before the
versioned re-backfill and let the purge find it already empty.

## Performance

Vectors travel in pgvector's binary wire format, and identical redeliveries skip the row write
server-side, so the remaining per-row cost is statement parsing: the sink pipelines its upserts in
batches, and enabling Npgsql's automatic preparation makes the server parse each statement shape
once instead of once per row:

```text
Host=...;Database=...;Max Auto Prepare Statements=16
```

(Or set `MaxAutoPrepare` via `ConfigureDataSource`.) When connecting through a transaction-pooling
PgBouncer, this needs PgBouncer 1.21+ with `max_prepared_statements` set; on older versions leave
it off.

## Per-tenant tables

Route each tenant to its own table with `ScopedDestination` - see multi-tenancy for
[EF Core](/providers/entity-framework-core/multi-tenancy) or [Marten](/providers/marten/multi-tenancy).
Runtime tables are created on first delivery with the same shape (when `CreateTable` is on) and
subject to the identifier rule above.
