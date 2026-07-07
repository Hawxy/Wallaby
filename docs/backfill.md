---
description: "Loading existing rows into destinations: automatic backfill of new tables, version-triggered reindexing, and gap-free merging with live changes."
---

# Backfill

Backfill loads existing rows into a destination. It runs concurrently with the realtime publish and merges with it so there are no
gaps and no stale overwrites.

## Automatic backfill

- **New tables** are backfilled on first run (via `AutoBackfillNewTables`, default on).
- **Version changes** re-backfill an entity when its `WithBackfillVersion` string changes
  (`AutoBackfillOnVersionChange`, default on). Bump it whenever you change a transform's output shape:

```csharp
sink.Map<Product>()
    .ToDestination("products")
    .WithBackfillVersion("v3")   // bump → re-backfill + reindex just this entity
    .UsingTransform(/* ... */);
```

Each entity is versioned and backfilled independently, so reindexing one doesn't disturb others or the
live stream.

When an entity is mapped to **several sinks**, backfill state is still per table: bumping *any*
mapping's version re-snapshots the table, and the snapshot flows through every sink mapped to it
(idempotent upserts make the extra delivery harmless). 

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

### Duplicates across failover

Chunk delivery and cursor persistence are two steps, so there is a small window where a leader dies
*after* a chunk was applied to the sinks but *before* its cursor was saved. The next leader resumes from
the last saved cursor and re-emits that chunk. This is the intended at-least-once behavior: sinks
upsert/delete by document id, so redelivered rows converge to the same state and nothing is lost — see
[How it works](/why-wallaby#how-it-works).

## Scoped (fan-out) backfill

The same engine also re-snapshots a *subset* of a table's rows on demand. When a [dependent fan-out](/providers/entity-framework-core/#dependent-tables)
is wider than one page, its tail is enqueued as a **scoped backfill job** (filtered to the affected keys)
that runs asynchronously on the leader — so the triggering transaction is acknowledged immediately instead
of blocking on a huge re-index. These jobs coalesce per (table, key set), are chunked and resumable just
like a full backfill, and emit through the same transform/sink path. See [EF Core → Scaling fan-out](/providers/entity-framework-core/#scaling-fan-out).

## Tuning & safety

- `ChunkSize` (default 500) sets the keyset page size; `MaxBatchSize` (default 1000) bounds each dispatched batch. Both are held fully in memory per chunk/batch, so both are capped at 100,000.
- Re-backfills are safe because sinks are idempotent (upsert/delete by id).
- A backfill of a large table is chunked and resumable, so it can be interrupted and will continue.
