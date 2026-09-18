---
description: "What a storage provider contributes and how to choose between the EF Core, Marten and plain-table providers."
---

# Overview

A storage provider tells Wallaby *what* to capture and *how* to turn raw row changes back into your
types. The core `Wallaby` package is provider-agnostic, with it owning the replication slot, publication,
checkpointing, backfills, and sink delivery, while a provider contributes the capture model and the materialization back into your CLR types.

Three providers are available:

- **[EF Core](/providers/entity-framework-core/)**: Captures the tables behind your `DbContext`'s
  entity mappings; transforms receive a leased `DbContext`.
- **[Marten](/providers/marten/)**: Captures Marten document tables and rehydrates each change through
  the store's own serializer; transforms receive a leased `IQuerySession`.
- **[Plain Tables](/providers/tables/)**: Captures any table into a POCO you annotate or configure, with
  no ORM in between; transforms receive an `NpgsqlDataSource`.

## Combining providers

Any combination of providers can be registered in one Wallaby instance sharing a single replication
slot/publication/checkpoint. Global commit ordering is preserved across every captured table:

```csharp
cdc.UseEntityFrameworkCore<AppDbContext>()
   .UseMarten()
   .AddMeilisearchSink("meili", m => { /* ... */ })
   .WithMappings(sink =>
   {
       sink.Map<Product>().UsingTransform(/* DbContext transform */);
       sink.Map<Order>().UsingTransform(/* IQuerySession transform */);
   });
```

Slot/publication names, batching, backfill versions, and sinks are all configured once and shared between providers.

## How mappings resolve to a provider

Each mapped entity type resolves to the provider that models it:

- If exactly one registered provider models the type, that provider wins - nothing to configure.
- If both model it, a provider-typed `UsingTransform` overload breaks the tie: each provider's
  overloads pin the mapping to that provider.
- Pin explicitly with `Map<T>().FromProvider(...)`; each package exposes its name as a constant
  (`EfCoreWallabyBuilderExtensions.ProviderName`, `MartenWallabyBuilderExtensions.ProviderName`,
  `TablesWallabyBuilderExtensions.ProviderName`).
- Remaining ambiguity, or a `FromProvider` pin that contradicts the transform's provider will fail
  fast at startup with guidance.
- A type mapped under several sinks resolves once - all its mappings share one table, so a pin on any
  of them decides for all (conflicting pins fail fast).

## Enrichment sessions

Transforms are handed the session type native to their mapping's provider: a `DbContext` for EF Core
mappings, an `IQuerySession` for Marten mappings, an `NpgsqlDataSource` for plain-table mappings.
Tenant-scoped session leasing is likewise per provider - see multi-tenancy for
[EF Core](/providers/entity-framework-core/multi-tenancy) and [Marten](/providers/marten/multi-tenancy).

[External slots](/external-slots) can be declared alongside any combination of the above, or on their
own in provision-only mode. `ForTable` is provider-independent; `ForEntity` and `ForAllEntities` resolve
against the registered providers.
