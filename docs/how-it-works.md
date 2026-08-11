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

Pick a flow to watch it move through the engine, click any stage for what it does, or take the
diagram full screen:

<InternalsFlow />

## Additional Notes

### Slot-loss gap detection

The replication slot is the only source of live changes, and a slot can be destroyed for a number of reasons: 

- The server invalidates it when it retains more WAL than `max_slot_wal_keep_size`.
- A failover to a promoted replica loses it (before Postgres 17 slot sync, or on providers that don't sync slots).
- It's accidentally dropped by someone.
- Wallaby itself dropped it for a [suspension](/operations/major-version-upgrades) (e.g. an RDS/Aurora
  major-version upgrade) — resuming deliberately recovers through this same mechanism.
  
A freshly created slot only streams from its creation point forward, so everything between the last applied
change and that point would be silently missed.

Wallaby closes that hole with the checkpoint it maintains on the slot's `wallaby.slot_registry` row alongside acknowledgements
(throttled to one write per [`CheckpointSaveInterval`](/configuration)). On leadership start, if the
checkpoint is **behind the slot's consistent point**, the slot must have been recreated after that
checkpoint was written. If so, Wallaby logs an error naming the missed LSN range and automatically marks every
mapped table for [re-backfill](/backfill), converging the sinks. An invalidated slot (`wal_status=lost`)
is detected the same way, it is dropped and recreated, then repaired via the same path. A **rewound WAL
history** is caught too: a checkpoint *ahead* of a just-recreated slot's consistent point is impossible
in a continuous history (the checkpoint predates the drop, and any WAL since puts the new slot above
it), so it can only mean the cluster was rebuilt (a restore, or a blue/green-style upgrade) and the
repair fires rather than misreading the stale LSN as continuity. Duplicates from
the re-snapshot are absorbed by the idempotent upsert-by-id sink contract. The re-backfill is
upsert-only, so deletes (and truncates) that happened inside the missed range are not converged —
removing those stale documents needs a destination purge. Enable
[`PurgeOnSlotGapRepair`](/configuration) to have the repair
[purge sink destinations](/backfill#purging-before-a-backfill) before it re-backfills, making the
recovery fully convergent; for the planned suspend/resume case, a single resume can request the same
via [`ResumeAsync(purge: true)`](/operations/external-control#suspend-and-resume) without the global
option.

### Unavailable-value self-healing (reselect)

Under `REPLICA IDENTITY DEFAULT`, an `UPDATE` that leaves a TOASTed column (large text, jsonb) unchanged
omits its value from the WAL record, and there is no old tuple to fall back to. The value was never on
the wire, so the change cannot be materialized faithfully — and because replica identity only affects
records written *after* an `ALTER TABLE ... REPLICA IDENTITY FULL`, a change written before the flip
stays incomplete forever, wedging the pipeline on a change it can never deliver.

Wallaby heals this by **reselect** (enabled by default via
[`ReselectUnavailableValues`](/configuration)): when materialization reports an unavailable value, the
row is re-read by primary key over the primary connection and the change materializes from current row
state. Each healed change logs a warning pointing here, and increments the
`wallaby.changes.reselected` counter. Setting the table to `REPLICA IDENTITY FULL` removes the
per-change re-read cost; apply it through your provider's managed path — the
[EF Core migration helpers](/providers/entity-framework-core/#replica-identity-in-migrations) or
Marten's [`ManageWallabyReplicaIdentity()`](/providers/marten/#managed-replica-identity) schema
feature.

Two properties to be aware of:

- **Converge-forward, not point-in-time.** The re-read returns the row as it is *now*, which may be
  newer than the change being processed. Any later update is itself in the stream and re-upserts, so
  sinks converge to current state — the same contract [backfill](/backfill) already relies on.
- **A vanished row's change is dropped.** If the re-read finds no row, the row was deleted after this
  change committed; the `DELETE` necessarily follows later in the stream and removes the document, so
  the incomplete change is logged and skipped rather than wedging the pipeline.

A failing re-read (e.g. the database is unreachable) halts the pipeline exactly like any other fault:
nothing is acknowledged and the transaction re-streams, preserving at-least-once delivery. Deletes
whose routing needs the full old row (`KeyedBy`, entity-scoped destinations) cannot be healed this way
— the row is gone — which is why those mappings [require `REPLICA IDENTITY FULL`](/configuration) up
front. Set `ReselectUnavailableValues = false` to restore strict halt-on-poison behavior.

### Idle slots and WAL retention

A replication slot only lets the server recycle WAL up to the position the consumer has acknowledged.
Postgres skips transactions that touch no published table, so on a shared database where the mapped tables 
are quiet while other tables churn,
Wallaby receives nothing, acknowledges nothing, and the slot pins an ever-growing range of WAL, until
`max_slot_wal_keep_size` invalidates it and forces the full re-backfill.

The leader solves this with a **heartbeat**. Whenever no transaction has been acknowledged for
[`HeartbeatInterval`](/configuration), it emits a tiny transactional message
(`pg_logical_emit_message`) on a normal connection. The message flows through the replication stream
as an empty committed transaction and is acknowledged through the ordinary delivery path, advancing
`confirmed_flush_lsn` (and the checkpoint) with no tables, DDL, or extra grants involved. Heartbeats
are suppressed while real changes are beingn processed, so a busy system never emits them.

::: tip
`pg_logical_emit_message` is executable by any role by default. A hardened database that revokes
default function `EXECUTE` privileges needs to re-grant it to Wallaby's role.
:::

### Truncate is not propagated

`TRUNCATE` of a captured table is replicated but names no rows, so there is nothing Wallaby can translate it into. 
When one arrives, Wallaby logs a warning naming the truncated table(s) and continues streaming. To converge, request a
[backfill with `purge: true`](/backfill#purging-before-a-backfill) (if supported) for the affected tables.

One blind spot: a `TRUNCATE` executed **directly on a leaf partition** of a
partitioned table is not replicated at all when publishing via the partition
root, so Wallaby cannot even warn. Truncating the root warns as usual.

### Partitioned tables

Partitioned tables work transparently. Every
publication Wallaby manages is created with `publish_via_partition_root = true`, so changes made in any leaf partition stream under the **root's**
name and schema. This results in routing, transforms, and backfill all seeing one table. This also applies to
[external slots](/external-slots): their third-party consumers see partitioned tables
under the root name. If you manage the publication yourself (`ManagePublicationTables = false`), it
must have the parameter set, because leaf-named changes would match no
mapping and be silently dropped.

Some quirks still remain:

- **Replica identity does not propagate from the root.** For mappings that need
  `REPLICA IDENTITY FULL`, every leaf must be set to FULL individually, including partitions created
  or attached later. Validation will check each leaf and name the ones that need changing.
- **`ATTACH`/`DETACH PARTITION` is invisible to the stream.** Rows in a newly attached partition were
  never streamed, and detached rows disappear without delete events.
- **Publication column lists are not used for partitioned tables** (the whole row is published):
  a list must cover the replica identity of every leaf, which cannot be validated at the root.

Mapping a leaf partition directly also works, as a leaf is an ordinary table, but map either the root
or the leaf, not both. When both are published, everything is attributed to the root.
