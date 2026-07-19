---
description: "Automatic backfill of new tables, version-triggered reindexing, and gap-free merging with live changes."
---

# Backfill

Backfill loads existing rows into a destination and runs until completion. It runs concurrently with the realtime publish and merges with its stream,
 so there are no gaps and no stale overwrites.

## Automatic backfill

- **New tables** are backfilled on first run (via `AutoBackfillNewTables`, default on).
- **Version changes** re-backfill an entity when its `WithBackfillVersion` string changes
 Bump it whenever you change a transform's output shape:

```csharp
sink.Map<Product>()
    .ToDestination("products")
    .WithBackfillVersion("v3")   // bump → re-backfill + reindex just this entity
    .UsingTransform(/* ... */);
```

Each entity is versioned and backfilled independently, so reindexing one doesn't disturb others or the
live stream.

When an entity is mapped to **several sinks**, backfill state is still per table: bumping *any*
mapping's version re-snapshots the table, and the snapshot flows through every sink mapped to it.

## Manual backfill

You might want to integrate backfill management as part of a wider backoff solution and run it manually.
To do this, resolve `IWallabyBackfillManager` and request a backfill via `RequestBackfillAsync`

```csharp
public sealed class AdminController(IWallabyBackfillManager backfill)
{
    public Task Reindex() => backfill.RequestBackfillAsync<Product>();
}
```

Requests are persisted, so they survive restarts, and the requesting node signals the current leader
via `LISTEN`/`NOTIFY` so the backfill starts immediately. A request made while the table is already backfilling wins: the run's remaining progress writes are
discarded and the table re-runs from the start.

`GetStatusAsync()` is also available and returns the current state of every tracked table.

Pass `purge: true` to [empty the sink destinations first](#purging-before-a-backfill), so the backfill
converges them to exactly the current table contents:

```csharp
await backfill.RequestBackfillAsync<Product>(purge: true);
```

### From outside the application

The [Wallaby.Client](/operations/external-control) package drives the same mechanism from **any process
with a connection string** — an ops console, a deployment script — with no Wallaby host reference. It
has no entity model, so tables are addressed by schema-qualified name:

```csharp
await using var control = new WallabyControlClient(connectionString);

await control.RequestBackfillAsync("public.products");
await control.RequestBackfillAsync("public.products", purge: true);   // purge destinations first

var status = await control.GetBackfillStatusAsync();   // every tracked table's state
```

The request behaves exactly like the in-host manager's: persisted, served instantly by the current
leader, and winning over an in-flight run. A request for a table Wallaby doesn't capture stays
`Requested` until a mapping for it deploys.

## Purging before a backfill

Backfill is upsert-only, so it cannot remove documents whose
source rows are gone (such as a [truncate](/how-it-works#truncate-is-not-propagated), or deletes that fell inside
a [slot-loss gap](/how-it-works#slot-loss-gap-detection)). To converge those, a backfill can **purge**
its sink destinations first so the snapshot rebuilds each destination from scratch

A purge runs before a fresh backfill when:

- a **manual request** asks for it (`RequestBackfillAsync(..., purge: true)`, in-host or via Wallaby.Client);
- **slot-gap repair** is configured to purge (`PurgeOnSlotGapRepair`, default off), making slot-loss
  recovery fully convergent;
- a **version change** triggers the re-backfill and the mapping opted in
  (`WithBackfillVersion("v4", purgeOnChange: true)`), so documents whose ids or shape changed don't
  linger under old keys.

Purging is an optional sink capability (`ISinkPurger`). The Meilisearch sink is the only sink that implements it right now.
A sink without the capability, such as the Kafka
and HTTP sinks, or a custom sink that doesn't opt in, is skipped with a warning and its destinations
keep any stale documents.

Two caveats:

- **The destination is temporarily incomplete** between the purge and the backfill's completion.
  Don't opt in where an empty index mid-rebuild is worse than stale documents.
- **Purging is per destination, and backfill is per table.** A destination shared by several tables
  loses the other tables' documents too, and only the requested table is re-backfilled. Scoped
  (per-tenant) destinations cannot be enumerated and are skipped with a warning.

## How it works

Each table is snapshotted in keyset-paged chunks
(ordered by primary key), and each chunk is bracketed by low/high watermark markers
emitted via `pg_logical_emit_message`. The live reader records any keys that change between the
watermarks, so at the high watermark the chunk's surviving rows are emitted through the **same transform and
sink path** as live changes. If a row is changed live during the window, the live version wins.

Progress is persisted per table, so a backfill resumes from its last cursor after a restart.

A [partitioned table](/how-it-works#partitioned-tables) is snapshotted through its root, so one
backfill covers every partition. That makes a [purge backfill](#purging-before-a-backfill) the
remediation for `ATTACH`/`DETACH PARTITION`: attached rows were never streamed and detached rows
leave no delete events, so purge-then-backfill is what converges the sinks after a partition swap.

### Duplicates across failover

Chunk delivery and cursor persistence are two steps, so there is a small window where a leader could die
*after* a chunk was applied to the sinks but *before* its cursor was saved. The next leader will resume from
the last saved cursor and re-emit that chunk. This is the intended at-least-once behavior.

## Scoped (fan-out) backfill

The same engine also re-snapshots a *subset* of a table's rows on demand. When a [dependent fan-out](/providers/entity-framework-core/#dependent-tables)
is wider than one page, its enqueued as a **scoped backfill job** (filtered to the affected keys)
that runs asynchronously on the leader - so the triggering transaction is acknowledged immediately instead
of blocking on a huge re-index. These jobs are chunked and resumable just
like a full backfill, and emit through the same transform/sink path. 
See [EF Core → Scaling fan-out](/providers/entity-framework-core/#scaling-fan-out).

## Tuning & safety

- `ChunkSize` (default 500) sets the keyset page size; `MaxBatchSize` (default 1000) bounds each dispatched batch. Both are held fully in memory per chunk/batch, so both are capped at 100,000.
- Re-backfills are safe because sinks are idempotent (upsert/delete by id).
- A backfill of a large table is chunked and resumable, so it can be interrupted and will continue.
