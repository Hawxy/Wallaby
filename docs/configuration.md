---
outline: deep
---

# Configuration

## Large transaction handling

Wallaby uses pgoutput **protocol v2**, so a transaction larger than the server's `logical_decoding_work_mem`
(default 64 MB) is streamed before commit and spilled out of memory, then processed in `MaxBatchSize` pages. This ensures a 
single huge transaction won't exhaust the worker's heap. Small transactions, which will be the majority, are kept as in-memory only.

You have a number of choices as to where streamed transactions spill:

- **`cdc.SpillToDatabase()`**: *(default)* Buffers transactions into `wallaby.stream_buffer` `UNLOGGED` table on the source database.
  Disk-free and zero-config (works wherever Wallaby connects). Will cause I/O amplification on the DB during large transactions.  
- **`cdc.SpillToDisk(path?)`**: Writes to local temp files. Needs a writable path and isn't suitable for read-only environments.
- **`cdc.UseTransactionSpill(ctx => ...)`**: Provide your own custom `ITransactionSpill` backend (e.g. an object store). The
  factory is handed a `SpillContext` (the source data source, slot name, and service provider) once per leader
  session and should return a fresh instance. Your backend should spill to a durable/external store, not buffer in-memory.

## Options

`ConfigureOptions(o => ...)` exposes:

| Option | Default | Purpose |
| --- | --- | --- |
| `ConnectionString` | *(required)* | Postgres connection string for replication, state, locks, and backfill reads. `UseConnectionString(...)` is shorthand for setting it. |
| `SlotName` / `PublicationName` | `wallaby_cdc_slot` / `wallaby_cdc_pub` | Names Wallaby creates/uses. |
| `ChunkSize` | `500` | Backfill keyset page size. |
| `MaxBatchSize` | `1000` | Max records per dispatched batch (and per inline [dependent fan-out](/transforms#dependent-tables) page). Bounds memory and sink batch size for large transactions, fan-out, and backfill. |
| `ManagePublicationTables` | `true` | Reconcile the publication's table set to the model. |
| `RequireFullReplicaIdentity` | `false` | Fail (vs warn) when a table needs `REPLICA IDENTITY FULL`. |
| `AutoBackfillNewTables` | `true` | Backfill a newly declared table on first run. |
| `AutoBackfillOnVersionChange` | `true` | Re-backfill when a mapping's `WithBackfillVersion` changes. |
| `StandbyRetryInterval` / `LeaderRetryInterval` | `5s` | Leader-election retry cadence. |
| `KeepaliveInterval` | `10s` | How often a replication status update is sent while a transaction is processed (keeps the connection alive during slow transforms/sinks). Keep it under the server's `wal_sender_timeout`. |
| `DeadLetterPolicy` | `Halt` | What to do when a batch can't be processed — a permanent **sink** failure, a **transform** exception, or a **materialization** failure. `Halt` stops the pipeline (retried after the leader restarts); `Skip` logs, counts (`wallaby.dead_letter`), and drops the batch, then continues. |
| `MaxBufferedChangesPerTransaction` | `1_000_000` | Safety ceiling on a **non-streamed** transaction's in-memory buffer; a larger transaction streams and spills instead. Exceeding it fails fast with guidance rather than exhausting memory. |

## The Options pattern

`CdcOptions` participates in the standard [options pipeline](https://learn.microsoft.com/dotnet/core/extensions/options),
so the usual mechanisms compose with the builder's `ConfigureOptions(...)`:

```csharp
// Bind from configuration (appsettings.json: { "Wallaby": { "ChunkSize": 250 } }):
builder.Services.Configure<CdcOptions>(builder.Configuration.GetSection("Wallaby"));

builder.Services.AddWallaby(cdc => /* ... */);

// PostConfigure always runs last — handy for test hosts:
builder.Services.PostConfigure<CdcOptions>(o => o.SlotName = "tests_slot");
```
