---
description: "Keeping embeddings and RAG corpora continuously in sync with Postgres using CDC: destination-side embedding, the pgvector sink, and model migrations."
---

# RAG & Embeddings

A RAG corpus or semantic search index is only as good as its freshness, and keeping vectors in sync
with a database is exactly the shape of problem Wallaby already solves: changes stream through your
transform, deletes propagate, [backfill](/backfill) seeds and re-seeds destinations, and
`WithBackfillVersion` gives embedding-model migrations a one-line answer.

The guiding principle: **let the destination own embedding**. When the party that stores the vector
also computes it, no vectors transit the pipeline, there is no cache to build or invalidate, and
the destination can skip re-embedding text it has already seen. Every path below follows that
shape; computing vectors inside a transform is the fallback, not the default.

## Search sinks: the destination embeds

Each search destination has a native way to embed the text Wallaby syncs:

- **Meilisearch** - declare a [server-side embedder](/sinks/meilisearch#embedders-vector-search)
  (`OpenAi`, `HuggingFace`, `Ollama`, or `Rest`) with a `documentTemplate`; the index becomes
  hybrid-searchable with zero embedding code:

```csharp
cdc.AddMeilisearchSink("meili", m =>
{
    m.Host = "http://localhost:7700";
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
});
```

- **Elasticsearch** - map the field as
  [`semantic_text`](/sinks/elasticsearch#vector-search) backed by an inference endpoint; the
  cluster chunks and embeds at index time. The inference API needs an appropriate Elastic
  subscription, and the default ELSER endpoint needs ML nodes.
- **OpenSearch** - attach a [neural-search ingest pipeline](/sinks/opensearch#vector-search)
  (a `text_embedding` processor over a deployed model) to the index.

In all three, Wallaby delivers plain text and every insert, update, delete, and backfill keeps the
index converged. They differ on re-embedding cost: Meilisearch caches embeddings and calls the
embedder only for documents whose rendered `documentTemplate` changed, while Elasticsearch and
OpenSearch run inference on every indexed document - fine for the live change stream (Wallaby only
delivers rows that changed), but a [backfill](/backfill) re-embeds the whole corpus.

## Postgres as the vector store: the pgvector sink

For "my RAG corpus is just Postgres", the [pgvector sink](/sinks/pgvector) plays the
destination-embeds role itself, since Postgres has no native embedder:

```csharp
cdc.AddPgvectorSink("vectors", v =>
{
    v.ConnectionString = vectorDbConn;
    v.Dimensions = 1536;
    v.EmbeddingGenerator = generator;  // any Microsoft.Extensions.AI IEmbeddingGenerator
    v.EmbedText = d => $"{d["name"]}\n{d["description"]}";
    v.EmbeddingVersion = "text-embedding-3-small/1";
})
.WithMappings(sink => sink
    .Map<Product>()
    .ToDestination("products")
    .WithBackfillVersion("v1", purgeOnChange: true)
    .UsingTransform(/* emit name + description as plain text */));
```

The sink embeds at delivery time and stores a content hash next to each vector, so it
[re-embeds only rows whose text changed](/sinks/pgvector#how-embedding-is-gated) - across restarts,
failovers, and re-backfills, with the destination table itself as the durable cache. Embedding-API
throttling surfaces as a retryable delivery, riding the dispatcher's normal backoff.

## Hand-rolled: embedding in a transform

For destinations that can't embed and can't be read back - Kafka topics, HTTP receivers, or a
Meilisearch `UserProvided` embedder - compute the vector in the transform and emit it as a document
field. A `float[]` or `ReadOnlyMemory<float>` value is written as a plain JSON number array by the
Elasticsearch, OpenSearch, HTTP, and Kafka sinks.

Two rules make this safe and affordable:

- **Batch and retry inside the transform.** Transforms receive whole batches (deduplicated to one
  change per row) - make one provider call per batch, never per row. And a transform exception
  **halts the pipeline** (retry classification exists only at sink delivery), so wrap the embedding
  call in your own retry (e.g. a Polly `ResiliencePipeline` with exponential backoff and jitter)
  and throw only when retries are exhausted; at that point halting is correct backpressure.
- **Skip unchanged text.** On updates, `ChangeEvent.Changes` holds the previous values of changed
  columns - when none of the embedded columns appear in it, skip the API call. Combine with the
  mapping's [column selection](/providers/entity-framework-core/#declaring-consumed-columns) so
  updates to irrelevant columns never reach the transform at all. Note `Changes` is `null` on
  inserts and backfill rows, so a re-backfill re-embeds everything on this path - one more reason
  to prefer the destination-side options above.

## Model migrations

Changing the embedding model (or the prompt template baked into the text) makes every stored vector
stale. Encode the model in the [backfill version](/backfill#automatic-backfill) and bump it:

```csharp
.WithBackfillVersion("text-embedding-3-large/1", purgeOnChange: true)
```

The bump triggers a full re-backfill of the entity, and `purgeOnChange: true`
[purges the destination first](/backfill#purging-before-a-backfill) so no old-model vectors survive
alongside new ones. For the pgvector sink, change `EmbeddingVersion` in the same deploy (it feeds
the stored hash); for destination-side embedders, update the embedder/endpoint configuration.
Dimension changes (e.g. 1536 → 3072) also need the index or column recreated - purge handles the
documents, not the schema.

## Delete propagation

Nothing extra to do: a source row's delete removes its document (and vector) from the destination,
and a transform returning `null` for a key does the same. Stale-vector cleanup is the pipeline's
normal delete path, not a special case.
