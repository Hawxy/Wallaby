# Wallaby 0.17.0 — storage providers

Draft release notes for the provider split (paste into the GitHub release).

## Breaking: EF Core support moved to `Wallaby.EntityFrameworkCore`

The core `Wallaby` package is now provider-agnostic — it no longer references EF Core. EF Core support
lives in the new **`Wallaby.EntityFrameworkCore`** package, and a **`Wallaby.Marten`** provider is in
development (a preview placeholder project exists to prove the seams).

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

Everything else — sinks, options, backfill, external slots, health checks, `Wallaby.Testing` — is
unchanged.

## For provider authors

The new public `Wallaby.Providers` namespace in the core package defines the storage-provider seams:
`IWallabyModelProvider`/`CapturePlan` (model discovery), `IRowMaterializer` (row materialization),
`IEnrichmentSession(Provider)` (transform enrichment), and `IWallabyTransformInvoker` (typed transform
invocation), registered through `WallabyBuilder.UseProvider(...)`.
