# Configuration

## Large Transaction Handling

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

## General Options

`ConfigureOptions(o => ...)` exposes:

| Option | Default | Purpose |
| --- | --- | --- |
| `ConnectionString` | *(required)* | Postgres connection string for replication, state, locks, and backfill reads. `UseConnectionString(...)` is shorthand for setting it. |
| `SlotName` / `PublicationName` | `wallaby_cdc_slot` / `wallaby_cdc_pub` | Names Wallaby creates/uses. |
| `ChunkSize` | `500` | Backfill keyset page size (1–100 000; chunk rows are held in memory). |
| `MaxBatchSize` | `1000` | Max records per dispatched batch (and per inline [dependent fan-out](/transforms#dependent-tables) page). Bounds memory and sink batch size for large transactions, fan-out, and backfill (1–100 000). |
| `ManagePublicationTables` | `true` | Reconcile the publication's table set to the model. |
| `RequireFullReplicaIdentity` | `false` | Fail (vs warn) when a table needs `REPLICA IDENTITY FULL`. |
| `AutoBackfillNewTables` | `true` | Backfill a newly declared table on first run. |
| `AutoBackfillOnVersionChange` | `true` | Re-backfill when a mapping's `WithBackfillVersion` changes. |
| `SinkRetry.MaxAttempts` | `10` | Retry attempts after the first delivery try for a **retryable** sink failure (0–100). `0` disables in-dispatch retry: the first retryable failure halts the leader session and leader-level backoff takes over. |
| `SinkRetry.BaseDelay` | `200ms` | Delay before the first sink retry; later delays grow exponentially (with jitter). |
| `SinkRetry.MaxDelay` | `3m` | Ceiling on the delay between sink retries. |

### Advanced Options

Internal tuning knobs live under `o.Advanced`. These defaults should work for 99% of deployments. 
You shouldn't modify these unless you know what you're doing:

| Option | Default | Purpose |
| --- | --- | --- |
| `StandbyRetryInterval` | `10s` | How long a standby waits before retrying to acquire leadership. |
| `LeaderRetryInterval` | `5s` | How long to wait before retrying after a failed leader session. |
| `LeaderHeartbeatInterval` | `10s` | How often the leader verifies it still holds the cluster lock while streaming. If the lock's connection dropped (so Postgres auto-released it), the leader steps down within roughly this interval and re-elects. |
| `KeepaliveInterval` | `10s` | How often a replication status update is sent while a transaction is processed (keeps the connection alive during slow transforms/sinks). Keep it under the server's `wal_sender_timeout`. |
| `FanoutPollInterval` | `30s` | Fallback poll cadence for the dependent [fan-out](/transforms#scaling-fan-out) queue. The worker is woken on demand via `LISTEN`/`NOTIFY` the instant a job is enqueued; this interval is only a safety net for a missed notification (e.g. a dropped listening connection). Lower it for tighter worst-case fan-out latency at the cost of more idle queue polls. |
| `MaxBufferedChangesPerTransaction` | `1_000_000` | Safety ceiling on a **non-streamed** transaction's in-memory buffer; a larger transaction streams and spills instead. Exceeding it fails fast with guidance rather than exhausting memory. |
| `CheckpointSaveInterval` | `5s` | Minimum interval between writes of the `wallaby.checkpoint` row, which backs [slot-loss gap detection](/how-it-works#slot-loss-gap-detection).|

## Options Pattern

`WallabyOptions` participates in the standard [options pipeline](https://learn.microsoft.com/dotnet/core/extensions/options),
so the usual mechanisms compose with the builder's `ConfigureOptions(...)`:

```csharp
// Bind from configuration (appsettings.json: { "Wallaby": { "ChunkSize": 250 } }):
builder.Services.Configure<WallabyOptions>(builder.Configuration.GetSection("Wallaby"));

builder.Services.AddWallaby(cdc => /* ... */);

// PostConfigure always runs last — handy for test hosts:
builder.Services.PostConfigure<WallabyOptions>(o => o.SlotName = "tests_slot");
```

## Reading configuration at startup

When the builder needs services use the provider-included overload of `AddWallaby`:

```csharp
builder.Services.AddWallaby((sp, cdc) =>
{
    var config = sp.GetRequiredService<IConfiguration>();

    cdc.UseEntityFrameworkCore<AppDbContext>() // or any other provider
       .UseConnectionString(config.GetConnectionString("App")!)
       // ... sinks and mappings as usual ...
});
```

The callback runs once, when the host first resolves Wallaby's services. Two consequences of the deferred timing: the
callback receives the **root** provider (scoped services are unavailable), and configuration errors surface
at host start instead of at registration.
