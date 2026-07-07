---
description: "What a storage provider contributes and how to choose between the EF Core and Marten providers."
---

# Overview

A storage provider tells Wallaby *what* to capture and *how* to turn raw row changes back into your
types. The core `Wallaby` package is provider-agnostic, with it owning the replication slot, publication,
checkpointing, backfills, and sink delivery, while a provider contributes the capture model and the materialization back into your CLR types.

Two providers are available:

- **[EF Core](/providers/entity-framework-core/)**: Captures the tables behind your `DbContext`'s
  entity mappings; transforms receive a leased `DbContext`.
- **[Marten](/providers/marten/)**: Captures Marten document tables and rehydrates each change through
  the store's own serializer; transforms receive a leased `IQuerySession`.

## Combining providers

Both providers can be registered in one Wallaby instance sharing a single replication
slot/publication/checkpoint. Global commit ordering is preserved across EF tables and Marten
document tables:

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

- If exactly one registered provider models the type, that provider wins — nothing to configure.
- If both model it, a provider-typed `UsingTransform` overload breaks the tie: each provider's
  overloads pin the mapping to that provider.
- Pin explicitly with `Map<T>().FromProvider(...)` (provider names: `"EntityFrameworkCore"`,
  `"Marten"`).
- Remaining ambiguity, or a `FromProvider` pin that contradicts the transform's provider will fail
  fast at startup with guidance.
- A type mapped under several sinks resolves once — all its mappings share one table, so a pin on any
  of them decides for all (conflicting pins fail fast).

## Enrichment sessions

Transforms are handed the session type native to their mapping's provider, a `DbContext` for EF Core
mappings, an `IQuerySession` for Marten mappings. Tenant-scoped session leasing is likewise per
provider — see multi-tenancy for [EF Core](/providers/entity-framework-core/multi-tenancy) and
[Marten](/providers/marten/multi-tenancy).

[External slots](/external-slots) are provider-independent and can be declared alongside any
combination of the above, or on their own in provision-only mode.
