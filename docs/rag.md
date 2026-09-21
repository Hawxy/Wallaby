---
title: "Keep RAG embeddings fresh with Postgres CDC"
description: "Keep embeddings and RAG corpora in sync with Postgres from .NET using CDC: destination-side embedding, the pgvector sink, and re-embedding on model changes."
---

# RAG & Embeddings

Keeping vectors in sync as part of a RAG corpus or semantic search index is a critical part of ensuring its freshness. 
Wallaby is perfectly suited for ensuring this requirement.

The ideal solution is to **let the destination own embedding**. When the party that stores the vector
also computes it, no vectors pass through the pipeline, there is no cache to build or invalidate, and
the destination can skip re-embedding text it has already seen.

For a more localized solution, you can also store your vectors in Postgres via `pgvector`.

## Search sinks: the destination embeds

Each search destination has a native way to embed the text that Wallaby syncs:

- **Meilisearch** - declare a [server-side embedder](/sinks/meilisearch#embedders-vector-search)
  (`OpenAi`, `HuggingFace`, `Ollama`, or `Rest`) with a `documentTemplate`; the index becomes
  hybrid-searchable with zero embedding code:

```csharp
cdc.AddMeilisearchSink("meili", m =>
{
    m.Endpoint = "http://localhost:7700";
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
  [`semantic_text`](/sinks/elasticsearch#vector-search) backed by an inference endpoint. The
  cluster chunks and embeds at index time. The inference API needs an appropriate Elastic
  subscription, and the default ELSER endpoint needs ML nodes.
- **OpenSearch** - attach a [neural-search ingest pipeline](/sinks/opensearch#vector-search)
  (a `text_embedding` processor over a deployed model) to the index.

In all three, Wallaby delivers plain text and every insert, update, delete, and backfill keeps the
index converged. 

## Postgres as the vector store: the pgvector sink

For "my RAG corpus is just Postgres", the [pgvector sink](/sinks/pgvector) does the embedding
itself, since Postgres has no native embedder:

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
failovers, and re-backfills, with the destination table itself as the durable cache. 

One row produces one document and one vector. Splitting a long text into several chunks, each with
its own vector, is not supported yet: chunk on the query side, or keep the embedded text short.

## Embedding in a transform

For destinations that can't embed and can't be read back - Kafka topics, HTTP receivers, or a
Meilisearch `UserProvided` embedder - compute the vector in the transform and emit it as a document
field. A `float[]` or `ReadOnlyMemory<float>` value is written as a plain JSON number array by the
Elasticsearch, OpenSearch, HTTP, and Kafka sinks.

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
