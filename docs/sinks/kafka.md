---
description: "Producing Postgres changes to Kafka topics, keyed by document id with tombstone deletes, idempotency keys, and optional topic auto-creation."
---

# Kafka Sink

The `Wallaby.Sinks.Kafka` package produces changes to Kafka topics. Each record becomes one message on
the topic named by its destination, **keyed by the document id**. Every change to a
document lands on the same partition in commit order, and a compacted topic converges to each document's
latest state. Deletes are emitted as **tombstones** (a null value under the same key), aligning with the Kafka-native delete.

Built on [Dekaf](https://github.com/thomhurst/Dekaf), a pure C# Kafka client.

## Install

```bash
dotnet add package Wallaby.Sinks.Kafka
```

## Register

```csharp
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .AddKafkaSink("kafka", k =>
       {
           k.BootstrapServers = "broker-1:9092,broker-2:9092";
           k.Topics.Add(new KafkaTopicConfig       // optional: create on startup
           {
               Name = "products",
               Partitions = 6,
               Config = { ["cleanup.policy"] = "compact" },
           });
       })
       .WithMappings(sink => sink
           .Map<Product>()
           .ToDestination("products")              // the topic
           .UsingTransform(/* ... */));
});
```

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `BootstrapServers` | *(required)* | Comma-separated broker list. |
| `DefaultTopic` | `null` | Topic for records whose mapping declares no destination; a record with neither fails permanently. |
| `Topics` | empty | Topics to [create on startup](#topic-creation); empty skips creation entirely. |
| `ConfigureClient` | `null` | Connection-level settings on the shared client behind the producer and admin client — [TLS/SASL](#authentication), connection timeouts, DNS behaviour. |
| `ConfigureProducer` | `null` | Producer settings the sink does not wrap (batch size, retry policy, socket buffers); runs after the sink's own configuration, so it wins on conflict. |
| `Compression` | `Lz4` | Message batch compression (`None`, `Gzip`, `Snappy`, `Lz4`, `Zstd`). |
| `LingerMs` | `5` | How long the producer lingers to fill a batch before sending. |
| `MessageTimeoutMs` | `30000` | How long the producer retries transient broker errors internally before the failure surfaces as retryable. |
| `AdminTimeoutMs` | `30000` | Ceiling on the startup [topic-creation](#topic-creation) request; an unreachable broker fails the leader session (which retries with backoff) instead of stalling startup. |
| `Annotations` | `null` | Static key/values echoed in every message value. |
| `SerializerOptions` | `null` | Serializer for non-scalar document values — see [NativeAOT](#nativeaot). |

The producer always runs **idempotent** with `acks=all`: broker-side dedup of the producer's internal
retries, no loss on broker failover, and per-partition produce order preserved.

## Authentication

Security is configured through typed builder methods on `ConfigureClient`, applied to both the
producer and the topic-creation admin client:

```csharp
k.ConfigureClient = client => client
    .UseTls()
    .WithSaslScramSha512(user, password);
```

SASL PLAIN, SCRAM-SHA-256/512, OAuth bearer, AWS MSK IAM, Kerberos (GSSAPI), and mutual TLS are all
available. See the [Dekaf security docs](https://thomhurst.github.io/Dekaf/docs/security/tls) for the
full surface.

## The message

**Key**: the record's document id (UTF-8). **Value** for an upsert is a self-contained JSON envelope:

```json
{
  "operation": "upsert",
  "id": "42",
  "idempotencyKey": "27271208:0:products:42",
  "document": { "name": "Kangaroo plush", "price": 19.95 },
  "metadata": {
    "schema": "public",
    "table": "products",
    "action": "insert",
    "commitLsn": "27271208",
    "commitIdx": 0,
    "commitTimestamp": "2026-07-06T03:12:45.100Z",
    "isBackfill": false
  }
}
```

A delete's value is **null** — a tombstone, so compaction removes the document. Its context travels in
the headers, which every message carries:

| Header | Value |
| --- | --- |
| `wallaby.operation` | `upsert` or `delete`. |
| `wallaby.idempotency-key` | The per-change deduplication key (same recipe as the envelope field). |
| `wallaby.table` | Schema-qualified source table, e.g. `public.products`. |
| `wallaby.commit-lsn` | Commit LSN as a decimal string; `0` for backfill rows. |

`commitLsn` is a string - the value can exceed the safe-integer range of JavaScript consumers. With
`Annotations` configured, the envelope carries an `annotations` object with those key/values.

When the host enables tracing with a listener on Dekaf's `ActivitySource`, messages also carry W3C
`traceparent`/`tracestate` headers injected by the client, linking consumer spans to the producing
pipeline.

## Delivery semantics

Delivery is **at-least-once**: a crash can re-produce messages a consumer has already seen. The
`wallaby.idempotency-key` header is unique per delivered change - store it and skip keys you have seen,
or use a compacted topic and let the latest message per key win. A backfill row's key embeds a per-run
token (echoed as `metadata.backfillRunId`): stable within one run, **new for every run**, so a
deliberate re-backfill is never suppressed by stored keys - and a crash-resumed backfill can re-deliver
rows under fresh keys, so gate costly side effects on document state, not on the key alone.

Per-document ordering is guaranteed: same key → same partition, produced in commit order. A batch is
only acknowledged to the replication slot once **every** message's delivery report has succeeded.

Failures are classified for the dispatcher:

| Error | Outcome |
| --- | --- |
| Retriable broker errors, delivery timeouts, connection failures | **Retryable** - the producer retries internally until `MessageTimeoutMs`, then the dispatcher retries with backoff. |
| Non-retriable broker errors (message too large, authorization failures, fenced producer) | **Permanent** - the pipeline halts. |

## Topic creation

Add entries to `Topics` and the sink creates them on the leader before streaming begins (and again,
idempotently, on every leadership takeover); topics that already exist are left untouched. Creation
waits for partition leaders to be elected, so streaming never starts against a topic still propagating.
For entity topics, `cleanup.policy=compact` is the natural fit: the latest message per document id is
the document's current state, and tombstones delete.

Leave `Topics` empty when your platform pre-provisions topics or the broker has
`auto.create.topics.enable` on.

## NativeAOT

The envelope structure and common scalar document values (strings, numbers, booleans, `Guid`, date/time
types, byte arrays, nested dictionaries, and sequences of these) are written without reflection. Any other
value type is serialized through `SerializerOptions`; on trimmed/NativeAOT hosts, point it at a
source-generated context covering the types your transforms emit:

```csharp
k.SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = MyJsonContext.Default };
```

Dekaf is Native AOT compatible with no native dependencies, so the sink no longer carries a
librdkafka binary — validate your publish output if you deploy NativeAOT.
