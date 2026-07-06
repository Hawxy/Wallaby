<script setup>
import ConfigPicker from './.vitepress/theme/ConfigPicker.vue'
</script>

# Getting Started

Wallaby streams row changes from Postgres logical replication, materializes them into your mapped
**entities or documents**, lets you transform/enrich them, and routes the resulting documents to pluggable
**sinks** (destinations).

## Server prerequisites

Your Postgres server must already have:

- **`wal_level = logical`** set in `postgresql.conf`  required for logical replication.
- A role with the **`REPLICATION`** attribute (or superuser) for the connection string you give Wallaby.
- Headroom in `max_replication_slots` and `max_wal_senders` (at least one slot/sender per Wallaby cluster).

Wallaby validates these on startup and fails fast with an actionable error if something is missing.

## Choose your configuration

Wallaby is configured through `AddWallaby`, with a **storage provider** supplying the model of what to
capture and a **sink** as an output destination. Pick the setup that fits your application. Optionally, if you don't want Wallaby to capture
anything itself, let it provision and maintain publications and replication slots for an external
pgoutput consumer:

<ConfigPicker />

::: tip Mixing providers
The EF Core and Marten providers can run side by side in one Wallaby instance, sharing a single
replication slot — see the [providers overview](/providers/overview#combining-providers).
:::

## Deployment

It's highly recommended to deploy Wallaby as a separate service, not as an integrated part of your main application. This allows you to scale CDC operations independently as the need arises.

## Next steps

- [How it works](/how-it-works) - the capture pipeline end to end.
- [Configuration](/configuration) - All configuration options
- [Transforms](/transforms) - shaping and enriching documents.
- [Meilisearch sink](/sinks/meilisearch) and [custom sinks](/sinks/custom).
- [Backfill](/backfill) - initial snapshots and version-triggered reindex.
- [Multi-tenancy](/multi-tenancy) - per-row scoped contexts and destinations.
- [Observability](/operations/observability) - OpenTelemetry metrics and traces.
