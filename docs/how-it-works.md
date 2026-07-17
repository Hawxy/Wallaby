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
the re-snapshot are absorbed by the idempotent upsert-by-id sink contract.
