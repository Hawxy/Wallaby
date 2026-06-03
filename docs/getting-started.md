---
outline: deep
---

# Getting Started

Wallaby streams row changes from Postgres logical replication, materializes them into your mapped
**EF Core entities**, lets you transform/enrich them, and routes the resulting documents to pluggable
**sinks** (destinations).

## Install

```bash
dotnet add package Wallaby
dotnet add package Wallaby.Sinks.Meilisearch   # optionally add a pre-built sink
```

Wallaby requires .NET 10+.

## Server prerequisites

Your Postgres server must already have:

- **`wal_level = logical`** set in `postgresql.conf`  required for logical replication.
- A role with the **`REPLICATION`** attribute (or superuser) for the connection string you give Wallaby.
- Headroom in `max_replication_slots` and `max_wal_senders` (one slot/sender per Wallaby cluster).

Wallaby validates these on startup and fails fast with an actionable error if something is missing.

## Register

`AddWallaby` is driven by your existing `DbContext`, declared with `UseContext<TContext>()`. Register the
context as usual — a scoped `AddDbContext<TContext>()` is enough (Wallaby uses an `IDbContextFactory<TContext>`
if one is registered, otherwise a DI scope) — and supply a connection string via `UseConnectionString(...)`.

::: tip
Wallaby also supports running in provision-only mode, where slots are provisioned but not consumed and EF Core can be omitted.
See [External slots](/external-slots).
:::

```csharp
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Sinks.Meilisearch;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddWallaby(cdc =>
{
    cdc.UseContext<AppDbContext>()
       .UseConnectionString(conn)
       .ConfigureOptions(o =>
       {
           o.SlotName = "app_cdc";
           o.PublicationName = "app_cdc_pub";
       })
       .AddMeilisearchSink("meili", m => { m.Host = "http://localhost:7700"; m.ApiKey = key; })

       // specify mapping and then configure the transform and destination
       .Map<Product>()
            .ToSink("meili", destination: "products")
            .WithBackfillVersion("v1")
            .UsingTransform((_, changes, _) =>
            {
                var docs = new Dictionary<DocumentKey, CdcDocument?>(changes.Count);
                foreach (var c in changes)
                    docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name, ["price"] = c.Entity!.Price };
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(docs);
            });
});

await builder.Build().RunAsync();
```

On startup Wallaby validates the server, creates the `wallaby` state schema, the publication, and the
replication slot, backfills the mapped tables, then streams live changes. Wallably holds a distributed lock so these operations will only run on a single node within a HA environment.

## What gets tracked

Only entities you **declare** are captured and added to the publication:

- `Map<T>()` declares a table *and* routes it to a sink.
- `CaptureAllMappedTables()` opts every mapped entity in (not recommended).

Tables a mapping [`DependsOn`](/transforms#dependent-tables) are captured automatically. Captured tables must have a primary key.

## Options

`ConfigureOptions(o => ...)` exposes:

| Option | Default | Purpose |
| --- | --- | --- |
| `SlotName` / `PublicationName` | `efcore_cdc_slot` / `efcore_cdc_pub` | Names Wallaby creates/uses. |
| `ChunkSize` | `500` | Backfill keyset page size. |
| `MaxBatchSize` | `1000` | Max records per dispatched batch (and per inline [dependent fan-out](/transforms#dependent-tables) page). Bounds memory and sink batch size for large transactions, fan-out, and backfill. |
| `ManagePublicationTables` | `true` | Reconcile the publication's table set to the model. |
| `RequireFullReplicaIdentity` | `false` | Fail (vs warn) when a table needs `REPLICA IDENTITY FULL`. |
| `AutoBackfillNewTables` | `true` | Backfill a newly declared table on first run. |
| `AutoBackfillOnVersionChange` | `true` | Re-backfill when a mapping's `WithBackfillVersion` changes. |
| `StandbyRetryInterval` / `LeaderRetryInterval` | `5s` | Leader-election retry cadence. |
| `KeepaliveInterval` | `10s` | How often a replication status update is sent while a transaction is processed (keeps the connection alive during slow transforms/sinks). Keep it under the server's `wal_sender_timeout`. |
| `DeadLetterPolicy` | `Halt` | What to do when a batch can't be processed — a permanent **sink** failure, a **transform** exception, or a **materialization** failure. `Halt` stops the pipeline (retried after the leader restarts); `Skip` logs, counts (`wallaby.dead_letter`), and drops the batch, then continues. |

## Next steps

- [Transforms](/transforms) — shaping and enriching documents.
- [Meilisearch sink](/sinks/meilisearch) and [custom sinks](/sinks/custom).
- [Backfill](/backfill) — initial snapshots and version-triggered reindex.
- [Multi-tenancy](/multi-tenancy) — per-row scoped contexts and destinations.
- [External slots](/external-slots) — provision extra publications/slots for an ELT or other CDC consumer.
- [Observability](/observability) — OpenTelemetry metrics and traces.
