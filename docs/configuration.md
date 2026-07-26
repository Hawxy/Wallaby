---
description: "Wallaby's configuration options and how to set them, from slot and publication names to batching, retries, and large-transaction handling."
---

# Configuration

## General Options

`ConfigureOptions(o => ...)` exposes:

| Option | Default | Purpose |
| --- | --- | --- |
| `ConnectionString` | *(required)* | Postgres connection string for replication, state, locks, and backfill reads. `UseConnectionString(...)` is shorthand for setting it. |
| `SlotName` / `PublicationName` | `wallaby_cdc_slot` / `wallaby_cdc_pub` | Names Wallaby creates/uses. |
| `ChunkSize` | `500` | Backfill keyset page size (1–100 000; chunk rows are held in memory). |
| `MaxBatchSize` | `1000` | Max records per dispatched batch (and per inline [dependent fan-out](/providers/entity-framework-core/#dependent-tables) page). Bounds memory and sink batch size for large transactions, fan-out, and backfill (1–100 000). |
| `ManagePublicationTables` | `true` | Reconcile the publication's table set to the model. When `false`, a publication used with a [partitioned table](/how-it-works#partitioned-tables) must have `publish_via_partition_root = true` set yourself; startup fails otherwise. |
| `PublicationColumnLists` | `true` | Enforce declared [column selections](#publication-column-lists) at the publication, so excluded columns never leave the server. Tables you haven't narrowed publish whole rows. Requires `ManagePublicationTables`. |
| `RequireFullReplicaIdentity` | `false` | Fail (vs warn) when a table needs `REPLICA IDENTITY FULL`. |
| `AutoBackfillNewTables` | `true` | Backfill a newly declared table on first run. |
| `AutoBackfillOnVersionChange` | `true` | Re-backfill when a mapping's `WithBackfillVersion` changes. |
| `PurgeOnSlotGapRepair` | `false` | [Purge sink destinations](/backfill#purging-before-a-backfill) before the automatic re-backfill that repairs a [slot-loss gap](/how-it-works#slot-loss-gap-detection), so deletes missed in the gap also converge. Needs sinks that implement `ISinkPurger`; destinations are incomplete while the re-backfill runs. |
| `Suspended` / `SuspensionReason` | `false` / – | Deploy-time [suspension](/operations/major-version-upgrades) flag (set via `Suspend(reason?)` on the builder): the node drops every managed replication slot and idles instead of streaming, so a platform blocked by logical slots (e.g. an RDS/Aurora major-version upgrade) can proceed. A flag-less deployment auto-resumes it. |
| `SinkRetry.MaxAttempts` | `10` | Retry attempts after the first delivery try for a **retryable** sink failure (0–100). `0` disables in-dispatch retry: the first retryable failure halts the leader session and leader-level backoff takes over. |
| `SinkRetry.BaseDelay` | `200ms` | Delay before the first sink retry; later delays grow exponentially (with jitter). |
| `SinkRetry.MaxDelay` | `3m` | Ceiling on the delay between sink retries. |

Wallaby adjusts two Npgsql settings on the connections it builds from `ConnectionString`, each only
when your connection string doesn't set it explicitly: `Max Auto Prepare=64` (auto-prepares the hot
bookkeeping statements) and `Array Nullability Mode=PerInstance` (an array column holding a `NULL`
element decodes as `Nullable<T>[]` instead of failing the stream).

### Advanced Options

Internal tuning knobs live under `o.Advanced`. These defaults should work for 99% of deployments. 
You shouldn't modify these unless you know what you're doing:

| Option | Default | Purpose |
| --- | --- | --- |
| `MaxTransactionsPerBatch` | `100` | Max committed transactions coalesced into one delivery batch: one sink dispatch and one acknowledgement at the last transaction's LSN. Coalescing is opportunistic: transactions are added only while the stream already has more buffered, so a quiet slot delivers each transaction immediately with no added latency. On a delivery failure nothing in the batch is acknowledged and the whole batch is redelivered (at-least-once; idempotent sinks converge). `1` disables coalescing (1–10 000). |
| `StandbyRetryInterval` | `10s` | How long a standby waits before retrying to acquire leadership. |
| `LeaderRetryInterval` | `5s` | How long to wait before retrying after a failed leader session. |
| `KeepaliveInterval` | `10s` | How often a replication status update is sent while a transaction is processed (keeps the connection alive during slow transforms/sinks). Keep it under the server's `wal_sender_timeout`. |
| `MaxFanoutKeysPerTransaction` | `1 000 000` | Safety valve on the distinct [dependent-lookup](/providers/entity-framework-core/#dependent-tables) keys one transaction may fan out per binding. A wide fan-out is offloaded to the queue in bounded chunk jobs as the keys accumulate, so memory stays flat regardless of size. Past the cap the transaction has effectively rewritten the dependent table: the binding's primary table is re-snapshotted whole instead (upsert-only, so it converges the same way) and a warning is logged (1–1 000 000). |
| `FanoutPollInterval` | `30s` | Fallback poll cadence for the dependent [fan-out](/providers/entity-framework-core/#scaling-fan-out) queue. The worker is woken on demand via `LISTEN`/`NOTIFY` the instant a job is enqueued; this interval is only a safety net for a missed notification (e.g. a dropped listening connection). Lower it for tighter worst-case fan-out latency at the cost of more idle queue polls. |
| `BackfillPollInterval` | `30s` | Fallback poll cadence for [manual backfill](/backfill#manual-backfill) requests. The leader's scheduler is woken on demand via `LISTEN`/`NOTIFY` the instant a request is persisted; this interval is only a safety net for a missed notification. |
| `MaxBufferedChangesPerTransaction` | `1_000_000` | Safety ceiling on a **non-streamed** transaction's in-memory buffer; a larger transaction streams and spills instead. Exceeding it fails fast with guidance rather than exhausting memory. |
| `CheckpointSaveInterval` | `5s` | Minimum interval between writes of the `wallaby.checkpoint` row, which backs [slot-loss gap detection](/how-it-works#slot-loss-gap-detection).|
| `HeartbeatInterval` | `30s` | While the pipeline is idle, how often the leader emits a tiny transactional heartbeat message so the slot's `confirmed_flush_lsn` keeps advancing; see [idle slots and WAL retention](/how-it-works#idle-slots-and-wal-retention). Suppressed while real traffic is being acknowledged; `Zero` disables. |
| `ControlPollInterval` | `15s` | Fallback poll cadence for the [suspend/resume](/operations/major-version-upgrades) control state: the leader re-checking for a suspension request and a suspended node re-checking for a resume. Both are woken on demand via `LISTEN`/`NOTIFY` the instant the state changes; this interval is only a safety net for a missed notification. |
| `WatermarkVisibilityFenceTimeout` | `Zero` (off) | Opt-in [visibility fence](/backfill#visibility-fence-opt-in) for watermark backfill: each chunk waits up to this long after its low watermark until no transaction in the current snapshot has already committed, closing the microsecond race where a commit lands just before the watermark but is visible to neither the chunk read nor the window. Polls `pg_xact_status` (must be callable by Wallaby's role); long-running open transactions don't pin it. On timeout a warning is logged and the chunk proceeds unfenced. |
| `SuspensionAutoResumeGraceFloor` | `60s` | Floor on how long a flag-less node waits before auto-resuming a configuration-origin suspension whose liveness heartbeat has gone quiet; the effective grace is `max(ControlPollInterval * 4, floor)`. Keeps a [mixed rolling deployment](/operations/major-version-upgrades#mixed-rollouts) suspended instead of flapping slots, at the cost of the same wait after the last `Suspend()`-flagged node stops. |

## Options Pattern

`WallabyOptions` participates in the standard [options pipeline](https://learn.microsoft.com/dotnet/core/extensions/options),
so the usual mechanisms compose with the builder's `ConfigureOptions(...)`:

```csharp
// Bind from configuration (appsettings.json: { "Wallaby": { "ChunkSize": 250 } }):
builder.Services.Configure<WallabyOptions>(builder.Configuration.GetSection("Wallaby"));

builder.Services.AddWallaby(cdc => /* ... */);

// PostConfigure always runs last - handy for test hosts:
builder.Services.PostConfigure<WallabyOptions>(o => o.SlotName = "tests_slot");
```

## Reading configuration at startup

When option values need services, use the provider-aware value hooks: `UseConnectionString`,
`ConfigureOptions`, and the sinks' options overloads all accept an `IServiceProvider`-taking delegate
that runs on first resolution, while the registration itself stays eager:

```csharp
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>() // or any other provider
       .UseConnectionString(sp => sp.GetRequiredService<IConfiguration>().GetConnectionString("App")!)
       .AddMeilisearchSink("meili", (sp, m) => m.Host = sp.GetRequiredService<IConfiguration>()["Meili:Host"]!)
       // ... mappings as usual ...
});
```

The delegates run once, when the host first resolves Wallaby's services, and receive the **root** provider
(scoped services are unavailable). Resolving Wallaby's own services inside them creates a resolution cycle,
and their configuration errors surface at host start instead of at registration.

## Large Transaction Handling

See [Transaction Spill](/transaction-spill).

### Publication column lists

A table you narrow with a [column selection](/providers/entity-framework-core/#declaring-consumed-columns)
is published with a matching column list - `CREATE PUBLICATION ... TABLE products (id, name, ...)` - so
the excluded columns are filtered inside Postgres: they are never decoded by the WAL sender or sent over
the wire. Dependent-only tables, which Wallaby narrows automatically to their primary key and lookup
columns, are listed for the same reason. Column lists are reconciled on every startup; drift is applied
atomically with a single `ALTER PUBLICATION ... SET TABLE`.

Narrowing is **opt-in per table**. A table you never narrowed publishes whole rows, even when its entity
maps only some of the physical columns, because a column list pins every column in it against schema
changes (see the warning below). Restricting that cost to the tables you deliberately narrowed keeps
ordinary migrations working everywhere else.

`PublicationColumnLists = false` disables column lists altogether, including declared selections. The
selection still governs materialization and backfill; it just stops being enforced at the server.

Tables that require `REPLICA IDENTITY FULL` (scoped destinations, custom document ids, Marten
soft-delete documents) and tables whose live replica identity is `FULL` always publish whole rows: a
column list must cover the table's replica identity, and `FULL` covers every column.
[External slots](/external-slots) are unaffected - their publications always carry whole tables for
the third-party consumer.

::: warning
**Migrating a column-listed table.** Postgres pins the columns in a publication's column list: while the
list is in place, `ALTER TABLE ... ALTER COLUMN ... TYPE` (even a widening) and `DROP COLUMN` on a listed
column are rejected, and `DROP COLUMN ... CASCADE` succeeds by removing the table from the publication
entirely - which silently stops capturing it until the next startup reconciles the publication. To change
a listed column, widen the table to whole-row publishing first
(`ALTER PUBLICATION ... SET TABLE`, keeping the other members' lists intact), run the migration, and let
the next startup re-narrow it. Tables without a declared selection are never listed, so their migrations
are unaffected.
:::

::: warning
Flipping a column-listed table to `REPLICA IDENTITY FULL` while Wallaby is running makes that table's
`UPDATE`/`DELETE` statements fail on the publisher until the next Wallaby startup reconciles it back to
whole-row publishing. Restart Wallaby (or drop the identity change) after such a flip. Tables Wallaby
itself flags for `REPLICA IDENTITY FULL` are never column-listed, so following Wallaby's own startup
guidance is always safe.
:::