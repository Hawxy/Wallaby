---
description: "Producing Postgres changes to Kafka topics, keyed by document id with tombstone deletes, idempotency keys, and optional topic auto-creation."
---

# Kafka Sink

The `Wallaby.Sinks.Kafka` package produces changes to Kafka topics. Each record becomes one message on
the topic named by its destination, **keyed by the document id**. Every change to a
document lands on the same partition in commit order, and a compacted topic converges to each document's
latest state. Deletes are emitted as **tombstones** (a null value under the same key), aligning with the Kafka-native delete.

Built on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet).

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
| `ClientConfig` | empty | Raw librdkafka settings merged over the sink's producer config — the escape hatch for [`sasl.*`/`ssl.*`](#authentication) and advanced tuning. |
| `Compression` | `Lz4` | Message batch compression. |
| `LingerMs` | `5` | How long the producer lingers to fill a batch before sending. |
| `MessageTimeoutMs` | `30000` | How long librdkafka retries transient broker errors internally before the failure surfaces as retryable. |
| `AdminTimeoutMs` | `30000` | Ceiling on the startup [topic-creation](#topic-creation) request; an unreachable broker fails the leader session (which retries with backoff) instead of stalling startup. |
| `Annotations` | `null` | Static key/values echoed in every message value. |
| `SerializerOptions` | `null` | Serializer for non-scalar document values — see [NativeAOT](#nativeaot). |

The producer always runs **idempotent** with `acks=all`: broker-side dedup of librdkafka's internal
retries, no loss on broker failover, and per-partition produce order preserved.

## Authentication

Everything librdkafka supports is available through `ClientConfig` (applied to both the producer and
the topic-creation admin client):

```csharp
k.ClientConfig["security.protocol"] = "SASL_SSL";
k.ClientConfig["sasl.mechanism"] = "PLAIN";
k.ClientConfig["sasl.username"] = user;
k.ClientConfig["sasl.password"] = password;
```

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

## Delivery semantics

Delivery is **at-least-once**: a crash can re-produce messages a consumer has already seen. The
`wallaby.idempotency-key` header is unique per delivered change - store it and skip keys you have seen,
or use a compacted topic and let the latest message per key win. Backfill rows share their key across
backfill runs (backfill is upsert-only, so replays are harmless).

Per-document ordering is guaranteed: same key → same partition, produced in commit order. A batch is
only acknowledged to the replication slot once **every** message's delivery report has succeeded.

Failures are classified for the dispatcher:

| Error | Outcome |
| --- | --- |
| Transient broker/transport errors, timeouts | **Retryable** - librdkafka retries internally until `MessageTimeoutMs`, then the dispatcher retries with backoff. |
| Message too large, authorization failures, fatal producer errors | **Permanent** - the pipeline halts. |

## Topic creation

Add entries to `Topics` and the sink creates them on the leader before streaming begins (and again,
idempotently, on every leadership takeover); topics that already exist are left untouched. For entity
topics, `cleanup.policy=compact` is the natural fit: the latest message per document id is the document's
current state, and tombstones delete.

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

Note that `Confluent.Kafka` itself makes no trimming/AOT compatibility claim, so this package does not
either — validate your publish output if you deploy NativeAOT.
