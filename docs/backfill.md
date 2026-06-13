---
outline: deep
---

# Backfill

Backfill loads existing rows into a destination. It runs concurrently with the realtime publish and merges with it so there are no
gaps and no stale overwrites.

## Automatic backfill

- **New tables** are backfilled on first run (via `AutoBackfillNewTables`, default on).
- **Version changes** re-backfill an entity when its `WithBackfillVersion` string changes
  (`AutoBackfillOnVersionChange`, default on). Bump it whenever you change a transform's output shape:

```csharp
cdc.Map<Product>()
   .ToSink("meili", "products")
   .WithBackfillVersion("v3")   // bump → re-backfill + reindex just this entity
   .UsingTransform(/* ... */);
```

Each entity is versioned and backfilled independently, so reindexing one doesn't disturb others or the
live stream.

## Manual backfill

Resolve `IWallabyBackfillManager` and request a backfill at runtime (e.g. from an admin endpoint):

```csharp
public sealed class AdminController(IWallabyBackfillManager backfill)
{
    public Task Reindex() => backfill.RequestBackfillAsync<Product>();
}
```

Requests are persisted, so they survive restarts and are executed by whichever node currently holds
leadership, and a request made on a standby node is still honored. `GetStatusAsync()` returns the current
state of every tracked table.

## How it works

Each table is snapshotted in keyset-paged chunks
(ordered by primary key), and each chunk is bracketed by low/high watermark markers
emitted via `pg_logical_emit_message`. The live reader records any keys that change between the
watermarks; at the high watermark the chunk's surviving rows are emitted through the **same transform and
sink path** as live changes. If a row is changed live during the window, the live version wins.

Progress is persisted per table, so a backfill resumes from its last cursor after a restart.

## Scoped (fan-out) backfill

The same engine also re-snapshots a *subset* of a table's rows on demand. When a [dependent fan-out](/transforms#dependent-tables)
is wider than one page, its tail is enqueued as a **scoped backfill job** (filtered to the affected keys)
that runs asynchronously on the leader — so the triggering transaction is acknowledged immediately instead
of blocking on a huge re-index. These jobs coalesce per (table, key set), are chunked and resumable just
like a full backfill, and emit through the same transform/sink path. See [Transforms → Scaling fan-out](/transforms#scaling-fan-out).

## Tuning & safety

- `ChunkSize` (default 500) sets the keyset page size; `MaxBatchSize` (default 1000) bounds each dispatched batch.
- Re-backfills are safe because sinks are idempotent (upsert/delete by id).
- A backfill of a large table is chunked and resumable, so it can be interrupted and will continue.
