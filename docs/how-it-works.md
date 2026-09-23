---
description: "Every data flow in the engine on one interactive diagram: live changes, backfill, dependent fan-out, slot loss, and sink outages."
outline: deep
---

<script setup>
import InternalsFlow from './.vitepress/theme/InternalsFlow.vue'
</script>

# How It Works

Wallaby is quite complex internally and instead of writing a wall of text, I thought a diagram 
that explains a few primary flows would make more sense.

Pick a flow to watch it move through the engine and click any stage for what it does:

<InternalsFlow />

## Additional Notes

These cover edge cases the engine handles on its own. You don't need them to use Wallaby, but they
explain the warnings you may see in the logs and the options that tune each behavior.

### Slot-loss gap detection

The replication slot is Wallaby's only source of live changes, and it can be lost in several ways:

- The server invalidates it once it retains more WAL than `max_slot_wal_keep_size`.
- The server invalidates it once no consumer has streamed from it for longer than
  `idle_replication_slot_timeout` (PostgreSQL 18+, off by default). Keep this above any planned Wallaby
  downtime, or [suspend](/operations/major-version-upgrades) for maintenance windows instead of relying on it.
- A failover to a promoted replica loses it (before PostgreSQL 17 slot sync, or on providers that don't sync slots).
- Someone drops it by accident.
- Wallaby drops it itself during a [suspension](/operations/major-version-upgrades), for example for an
  RDS/Aurora major-version upgrade. Resuming recovers through this same mechanism.

A recreated slot only streams from its creation point forward, so every change between the last applied
one and that point would be silently missed.

**Detection.** Alongside acknowledgements, Wallaby records a checkpoint on the slot's
`wallaby.slot_registry` row (at most one write per [`CheckpointSaveInterval`](/configuration)). When a
node becomes leader, it compares that checkpoint with the slot's consistent point:

- **Checkpoint behind the consistent point.** The slot was recreated after the checkpoint was written,
  so changes were missed.
- **Checkpoint ahead of a just-recreated slot's consistent point.** This is impossible in a continuous
  WAL history: the checkpoint predates the drop, and any WAL written since puts the new slot above it.
  The history was rewound (a restore, or a blue/green-style upgrade rebuilt the cluster), so the stale
  LSN is treated as a gap rather than misread as continuity.
- **Slot invalidated (`wal_status = lost`).** Wallaby drops and recreates the slot, then treats it as a gap.

**Repair.** Wallaby logs an error naming the missed LSN range and marks every mapped table for
[re-backfill](/backfill), converging the sinks. Duplicates from the re-snapshot are harmless because
sinks upsert by id.

The re-backfill only upserts, so rows deleted or truncated inside the missed range stay in the sinks.
Enable [`PurgeOnSlotGapRepair`](/configuration) to [purge sink destinations](/backfill#purging-before-a-backfill)
before the re-backfill, making recovery fully convergent. For a planned suspend/resume, a single resume
can request the same with [`ResumeAsync(purge: true)`](/operations/external-control#suspend-and-resume)
without the global option.

### Unavailable-value self-healing (reselect)

Under `REPLICA IDENTITY DEFAULT`, an `UPDATE` that leaves a TOASTed column (large text, jsonb) unchanged
omits that value from the WAL record, and there is no old tuple to recover it from, so the change can't
be materialized from the stream alone. Switching the table to `REPLICA IDENTITY FULL` doesn't fix
changes that are already written, because replica identity only affects records written after the
`ALTER TABLE`. Without intervention, the pipeline would halt on a change it can never deliver.

Wallaby heals this by **reselect**, enabled by default via
[`ReselectUnavailableValues`](/configuration). When materialization reports an unavailable value, the
row is re-read by primary key over the primary connection and the change materializes from the current
row state. Each healed change logs a warning pointing here and increments the
`wallaby.changes.reselected` counter.

Setting the table to `REPLICA IDENTITY FULL` avoids the per-change re-read. Apply it through your
provider's managed path: the
[EF Core migration helpers](/providers/entity-framework-core/#replica-identity-in-migrations) or
Marten's [`ManageWallabyReplicaIdentity()`](/providers/marten/#managed-replica-identity) schema
feature.

Two properties to be aware of:

- **Converge-forward, not point-in-time.** The re-read returns the row as it is *now*, which may be
  newer than the change being processed. Any later update is also in the stream and re-upserts, so
  sinks converge to current state, the same contract [backfill](/backfill) relies on.
- **A vanished row's change is dropped.** If the re-read finds no row, the row was deleted after this
  change committed. Its `DELETE` follows later in the stream and removes the document, so the
  incomplete change is logged and skipped instead of halting the pipeline.

A failing re-read (for example, the database is unreachable) halts the pipeline like any other fault:
nothing is acknowledged and the transaction re-streams, preserving at-least-once delivery. Reselect
can't heal deletes whose routing needs the full old row (`KeyedBy`, entity-scoped destinations) because
the row is already gone, which is why those mappings [require `REPLICA IDENTITY FULL`](/configuration)
up front. Set `ReselectUnavailableValues = false` to halt on any unavailable value instead.

### Idle slots and WAL retention

A replication slot only lets the server recycle WAL up to the position its consumer has acknowledged.
Postgres skips transactions that touch no published table, so on a shared database where the mapped
tables are quiet while other tables churn, Wallaby receives nothing and acknowledges nothing. The slot
then pins an ever-growing range of WAL until `max_slot_wal_keep_size` invalidates it, forcing a
[full re-backfill](#slot-loss-gap-detection).

The leader prevents this with a **heartbeat**. Whenever no transaction has been acknowledged for
[`HeartbeatInterval`](/configuration), it emits a tiny transactional message
(`pg_logical_emit_message`) on a normal connection. The message flows through the replication stream
as an empty committed transaction and is acknowledged through the ordinary delivery path, advancing
`confirmed_flush_lsn` (and the checkpoint) without touching any tables, running DDL, or needing extra
grants. Heartbeats are suppressed while real changes are being processed, so a busy system never emits them.

::: tip
`pg_logical_emit_message` is executable by any role by default. A hardened database that revokes
default function `EXECUTE` privileges needs to re-grant it to Wallaby's role.
:::

### Truncate is not propagated

`TRUNCATE` on a captured table is replicated, but it names no rows, so there is nothing for Wallaby to
translate into sink operations. Wallaby logs a warning naming the truncated table(s) and keeps streaming.
To converge the sinks, request a [backfill with `purge: true`](/backfill#purging-before-a-backfill) for
the affected tables, on sinks that support purging.

One case goes undetected: when publishing via the partition root, a `TRUNCATE` run directly on a leaf
partition isn't replicated at all, so Wallaby can't warn about it. Truncating the root warns as usual.

### Partitioned tables

Partitioned tables work transparently. Every publication Wallaby manages is created with
`publish_via_partition_root = true`, so changes in any leaf partition stream under the **root's** schema
and name, and routing, transforms, and backfill all see a single table. The same applies to
[external slots](/external-slots): their consumers see partitioned tables under the root name.

If you manage the publication yourself (`ManagePublicationTables = false`), it must set this parameter
too. Otherwise changes arrive under leaf names, match no mapping, and are silently dropped.

Some limitations remain:

- **Replica identity does not propagate from the root.** For mappings that need
  `REPLICA IDENTITY FULL`, every leaf must be set to FULL individually, including partitions created
  or attached later. Validation checks each leaf and names the ones that need changing.
- **`ATTACH`/`DETACH PARTITION` is invisible to the stream.** Rows in a newly attached partition were
  never streamed, and detached rows disappear without delete events.
- **Publication column lists are not used for partitioned tables** (the whole row is published):
  a list must cover the replica identity of every leaf, which cannot be validated at the root.

Mapping a leaf partition directly also works, since a leaf is an ordinary table, but map either the root
or the leaf, not both. When both are published, all changes are attributed to the root.
