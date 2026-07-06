<script setup>
import PipelineFlow from './.vitepress/theme/PipelineFlow.vue'
</script>

# How It Works


<PipelineFlow />

- **Read + Materialize**: stream committed transactions from the logical replication slot in commit
  order, decode each row change, and materialize it into your mapped entity type through the registered [storage provider](/providers/overview).
- **Transform + Route**: group a transaction's changes by entity mapping, run each [transform](/transforms)
  to shape the output documents, resolve their destinations, and slice
  the result into batches of at most [`MaxBatchSize`](/configuration#general-options).
- **Sinks**: deliver each batch to its sink with retry/backoff. Independent sinks are written
  concurrently; a batch that exhausts its retries halts the pipeline rather than being skipped.
- **Acknowledge + Checkpoint**: only after **every** sink has accepted the transaction's batches,
  acknowledge the commit, thus advancing the replication slot and persisting the checkpoint. That ordering is
  what gives at-least-once delivery: a crash before the acknowledgement simply re-streams from the last
  acknowledged position, and the sinks' idempotent upsert-by-id contract absorbs the redelivery.

[Backfill](/backfill) (initial snapshots) and [dependent fan-out](/transforms#dependent-tables) (re-emitting
an entity when a related table changes) run on the leader and feed rows through the **same** transform and
sink path, so there is only ever one ordered stream.

## Slot-loss gap detection

The replication slot is the only source of live changes, and a slot can be destroyed for a number of reasons: 

- The server invalidates it when it retains more WAL than `max_slot_wal_keep_size`.
- A failover to a promoted replica loses it (before Postgres 17 slot sync, or on providers that don't sync slots).
- It's accidentally dropped by someone.
  
A freshly created slot only streams from its creation point forward, so everything between the last applied
change and that point would be silently missed.

Wallaby closes that hole with the `wallaby.checkpoint` row it maintains alongside acknowledgements
(throttled to one write per [`CheckpointSaveInterval`](/configuration)). On leadership start, if the
checkpoint is **behind the slot's consistent point**, the slot must have been recreated after that
checkpoint was written. If so, Wallaby logs an error naming the missed LSN range and automatically marks every
mapped table for [re-backfill](/backfill), converging the sinks. An invalidated slot (`wal_status=lost`)
is detected the same way, it is dropped and recreated, then repaired via the same path. Duplicates from
the re-snapshot are absorbed by the idempotent upsert-by-id sink contract.

## Prior Art

Many concepts this package uses are based on existing patterns within the wider ecosystem:
- The transformation, enrichment and watermarking pipeline is inspired by [Sequin](https://sequinstream.com/)
- Some general concepts are based on Netflix's [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b)