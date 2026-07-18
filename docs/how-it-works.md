---
description: "Every data flow in the engine on one interactive diagram: live changes, backfill, dependent fan-out, slot loss, and sink outages."
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

## Slot-loss gap detection

The replication slot is the only source of live changes, and a slot can be destroyed for a number of reasons: 

- The server invalidates it when it retains more WAL than `max_slot_wal_keep_size`.
- A failover to a promoted replica loses it (before Postgres 17 slot sync, or on providers that don't sync slots).
- It's accidentally dropped by someone.
- Wallaby itself dropped it for a [suspension](/operations/major-version-upgrades) (e.g. an RDS/Aurora
  major-version upgrade) — resuming deliberately recovers through this same mechanism.
  
A freshly created slot only streams from its creation point forward, so everything between the last applied
change and that point would be silently missed.

Wallaby closes that hole with the `wallaby.checkpoint` row it maintains alongside acknowledgements
(throttled to one write per [`CheckpointSaveInterval`](/configuration)). On leadership start, if the
checkpoint is **behind the slot's consistent point**, the slot must have been recreated after that
checkpoint was written. If so, Wallaby logs an error naming the missed LSN range and automatically marks every
mapped table for [re-backfill](/backfill), converging the sinks. An invalidated slot (`wal_status=lost`)
is detected the same way, it is dropped and recreated, then repaired via the same path. Duplicates from
the re-snapshot are absorbed by the idempotent upsert-by-id sink contract. The re-backfill is
upsert-only, so deletes (and truncates) that happened inside the missed range are not converged —
removing those stale documents needs a destination purge.

## Idle slots and WAL retention

A replication slot only lets the server recycle WAL up to the position the consumer has acknowledged,
and Wallaby only acknowledges delivered transactions. Postgres skips transactions that touch no
published table, so on a shared database where the mapped tables are quiet while other tables churn,
Wallaby receives nothing, acknowledges nothing, and the slot pins an ever-growing range of WAL — until
`max_slot_wal_keep_size` invalidates it and forces the full re-backfill described above.

The leader closes this with a **heartbeat**: whenever no transaction has been acknowledged for
[`HeartbeatInterval`](/configuration), it emits a tiny transactional message
(`pg_logical_emit_message`) on a normal connection. The message flows through the replication stream
as an empty committed transaction and is acknowledged through the ordinary delivery path, advancing
`confirmed_flush_lsn` (and the checkpoint) with no tables, DDL, or extra grants involved. Heartbeats
are suppressed while real traffic flows, so a busy system never emits them.

::: tip
`pg_logical_emit_message` is executable by any role by default. A hardened database that revokes
default function `EXECUTE` privileges needs to re-grant it to Wallaby's role.
:::

## Truncate is not propagated

`TRUNCATE` of a captured table is replicated but names no rows, and the sink contract is
upsert/delete-by-id, so there is nothing Wallaby can translate it into. When one arrives, Wallaby logs
a warning naming the truncated table(s) and continues streaming — documents already delivered for
those tables remain in their sinks, which now diverge from the database. To converge, purge the
destination and re-run a [backfill](/backfill) for the affected tables.
