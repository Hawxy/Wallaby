# Wallaby 0.17.0 — storage providers

Draft release notes for the provider split (paste into the GitHub release).

## Breaking: EF Core support moved to `Wallaby.EntityFrameworkCore`

The core `Wallaby` package is now provider-agnostic — it no longer references EF Core. EF Core support
lives in the new **`Wallaby.EntityFrameworkCore`** package, and Marten support ships in the new
**`Wallaby.Marten`** package (see below).

### Migrating

1. Add the provider package:

   ```bash
   dotnet add package Wallaby.EntityFrameworkCore
   ```

2. Replace `UseContext<TContext>()` with `UseEntityFrameworkCore<TContext>()`:

   ```csharp
   builder.Services.AddWallaby(cdc =>
   {
       cdc.UseEntityFrameworkCore<AppDbContext>()   // was: cdc.UseContext<AppDbContext>()
          .UseConnectionString(conn)
          // ... sinks and mappings unchanged ...
   });
   ```

3. `UseScopedContext(...)` is now `UseScopedDbContext(...)` (same signature).

4. `IWallabyTransform<TEntity>` and `DelegateTransform<TEntity>` moved from `Wallaby.Abstractions`
   to the `Wallaby.EntityFrameworkCore` namespace/package. The `TransformAsync(DbContext, ...)`
   signature is unchanged — transform bodies need no edits, only a `using` update.

5. The container-resolved transform registration gains a type parameter (extension methods cannot
   infer it): `UsingTransform<TTransform>()` becomes `UsingTransform<TEntity, TTransform>()`.

6. `Map<T>().DependsOn(...)` is now an EF Core-typed extension in `Wallaby.EntityFrameworkCore` —
   call sites are unchanged beyond the package's `using`.

7. `CaptureAllMappedTables()` is removed. Capture is always declared explicitly: `Map<T>()` each
   table you want routed (dependent tables still come in via `DependsOn(...)`).

Everything else — sinks, options, backfill, external slots, health checks, `Wallaby.Testing` — is
unchanged.

## New: Marten storage provider (`Wallaby.Marten`)

Drive capture from a Marten document store: Wallaby watches the `mt_doc_*` tables, rehydrates each
change's JSONB body into your document type through the store's own serializer, and routes documents
through the usual transform → sink pipeline.

- `cdc.UseMarten()` (resolves the container's `IDocumentStore`; an overload takes a store factory).
- Transforms receive an `IQuerySession`; the `UsingTransform` overloads mirror the EF Core ones.
- **Soft deletes surface as Delete events** — the sink document is removed, backfills skip
  `mt_deleted` rows, and an un-delete re-emits the document (soft-deleted tables need
  `REPLICA IDENTITY FULL`; self-config logs the DDL).
- **Conjoined tenancy**: the captured key is `[tenant_id, id]`; `ScopedByTenant()` +
  `UseTenantSessions()` give per-tenant transform batches, sessions, and (optionally) destinations.
- Documents must be registered with the store up front (`RegisterDocumentType<T>()`,
  `Schema.For<T>()`, …).
- v1 limitations: no `DependsOn`, `ChangeEvent.Changes` is always `null`, document hierarchies are
  not capturable, duplicated fields/strong-typed ids are not mapped, no separate-database tenancy.

## New: multiple storage providers on one slot

`UseProvider` no longer enforces a single provider: EF Core and Marten can be registered in the same
Wallaby instance, sharing ONE replication slot/publication/checkpoint so global commit ordering is
preserved across both providers' tables.

- Each mapping auto-resolves to the provider that models its type; the provider-typed
  `UsingTransform` overloads break ties, and `Map<T>().FromProvider("name")` pins explicitly.
  Ambiguity or an unclaimed type fails fast at startup with the fixes spelled out.
- `UseScopedEnrichmentSessions` now targets a provider by name — `UseScopedDbContext(...)` (EF Core)
  and `UseTenantSessions()` (Marten) each affect only their own provider's mappings, and must be
  called after that provider is registered.
- Mappings on different providers lease enrichment sessions independently within a batch (an EF
  transform gets its `DbContext` while a Marten transform gets its `IQuerySession` in the same
  transaction).

## New: trimming + NativeAOT support (core and Marten)

The `Wallaby` and `Wallaby.Marten` packages are now marked `IsAotCompatible` and build clean under
the trim/AOT analyzers. Internal JSON state (spilled transactions, keyset cursors, fan-out queues) is
serialized with source-generated or hand-written codecs — no reflection.

- Marten under `PublishAot` needs the store configured for AOT: `TypeLoadMode.Static` with
  pre-generated code, and a source-generated `System.Text.Json` serializer for document types.
- Spilled (very large) transactions encode common scalar and scalar-array column types natively;
  exotic column types still use a reflection-based JSON fallback, which is unavailable under
  NativeAOT and fails the spill with a descriptive error there.
- The EF Core provider and the Meilisearch sink are not yet AOT-compatible.

## For provider authors

The new public `Wallaby.Providers` namespace in the core package defines the storage-provider seams:
`IWallabyModelProvider`/`CapturePlan` (model discovery), `IRowMaterializer` (row materialization),
`IEnrichmentSession(Provider)` (transform enrichment), and `IWallabyTransformInvoker` (typed transform
invocation), registered through `WallabyBuilder.UseProvider(...)`.

Breaking for provider authors in this release: `IWallabyModelProvider` gains `bool Handles(Type)`
(claim a CLR type for multi-provider affinity), `WallabyProviderRegistration` gains an optional
`ScopedEnrichmentSessions` factory, and `MaterializedRow` now carries the routed `ChangeAction`
(letting a provider re-interpret a change, e.g. Marten's soft-delete UPDATE → Delete).
