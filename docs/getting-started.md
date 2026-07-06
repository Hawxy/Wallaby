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
capture. Pick the setup that matches your application:

### Entity Framework Core

Your tables are modeled by an EFCore `DbContext`, see:

[EFCore Setup →](/providers/entity-framework-core){.wb-btn}

### Marten

Your data lives in a [Marten](https://martendb.io) document store, see:

[Marten Setup →](/providers/marten){.wb-btn}

### External slots (provision-only)

You don't want Wallaby to capture anything itself, instead you want it to provision and maintain
publications and replication slots for an external pgoutput consumer such as Airbyte, Debezium, or
Fivetran, as part of your normal deployment.

[External Slots →](/external-slots){.wb-btn}

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
