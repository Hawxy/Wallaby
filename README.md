# EFCore.CDC

Postgres Change Data Capture for .NET, driven by your **EF Core model**.

EFCore.CDC streams row changes from Postgres logical replication, materializes them into your mapped
EF Core entities, lets you **transform/enrich** them with a single interface (project from the row, or
join/flatten an aggregate via EF Core LINQ or raw SQL), and routes the resulting documents to pluggable
**destinations** (sinks). It **self-configures** the publication and replication slot from your model,
supports **backfill** (initial snapshot) coordinated with the live stream so there are no gaps or
duplicates, and is **cluster-safe** via leader election.

The first shipped sink is **Meilisearch** — keep a search index continuously in sync with your tables.

## Packages

| Project | Purpose |
| --- | --- |
| `EFCore.CDC` | Core: self-config, logical replication, transforms, routing, backfill, leader election, the in-process `DelegateSink`. Depends only on Npgsql + EF Core + `Microsoft.Extensions.*` + Polly. |
| `EFCore.CDC.Meilisearch` | Meilisearch destination sink. |

## Quick start

```csharp
builder.Services.AddDbContextFactory<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddCdc<AppDbContext>(cdc =>
{
    cdc.UseConnectionString(conn)
       .ConfigureOptions(o => { o.SlotName = "app_cdc"; o.PublicationName = "app_cdc_pub"; })
       .AddMeilisearchSink("meili", m => { m.Host = "http://localhost:7700"; m.ApiKey = key; })

       // Mapping = routing only. The transform does the data shaping.
       .Map<Product>()
            .ToSink("meili", destination: "products")
            .WithBackfillVersion("v1")           // bump to force a reindex/backfill
            .UsingTransform<Dictionary<string, object?>>((db, changes, ct) =>
            {
                var docs = new Dictionary<DocumentKey, Dictionary<string, object?>?>();
                foreach (var c in changes)
                    docs[c.Key] = new Dictionary<string, object?> { ["name"] = c.Entity!.Name };
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, Dictionary<string, object?>?>>(docs);
            });
});
```

On startup the library validates the server (`wal_level=logical`, replication role, slot headroom),
creates the `cdc` state schema, the publication, and the replication slot, backfills the mapped tables,
then streams live changes — all on a single elected leader.

### Transforms

All enrichment/transformation goes through one interface:

```csharp
public interface ICdcTransform<TEntity, TDocument> where TEntity : class
{
    Task<IReadOnlyDictionary<DocumentKey, TDocument?>> TransformAsync(
        DbContext db, IReadOnlyList<ChangeEvent<TEntity>> changes, CancellationToken ct);
}
```

Implement it as a class (`.UsingTransform<MyTransform, MyDoc>()`, DI-constructed) or inline
(`.UsingTransform<MyDoc>((db, changes, ct) => ...)`). Inside, project from `change.Entity`, or query
`db` (EF Core LINQ with `Include`, or `db.Database.SqlQuery<T>(...)`) to flatten an aggregate. Return one
document per source key; omit a key (or map it to `null`) to delete it from the sink. Deletes are handled
by the engine — a transform never sees them.

### Backfill

- New mapped tables are backfilled automatically on first run.
- Bumping a mapping's `backfillVersion` re-backfills that entity.
- Trigger a manual backfill at runtime: resolve `ICdcBackfillManager` and call
  `RequestBackfillAsync<Product>()`. Requests are persisted and executed by the current leader.

Backfill uses the DBLog/Sequin watermark pattern (keyset pagination + low/high watermarks emitted via
`pg_logical_emit_message` and decoded from pgoutput as generic WAL messages) so the snapshot merges
with the live stream with no gaps and live always wins.

### Per-row scoping (multi-tenancy)

When the enrichment `DbContext` — or the destination — must be derived from the changed row's own data
(e.g. a `TenantId`), declare a scope key and supply a scoped context factory:

```csharp
cdc.UseScopedContext((scopeKey, services) => new AppDbContext(OptionsForTenant(scopeKey)))  // tenant conn or query-filter
   .Map<Order>()
       .ScopedBy(o => o.TenantId)                     // scope key from the change
       .UsingTransform<OrderDoc, OrderTransform>()    // transform receives the tenant-scoped db
       .ScopedDestination(key => $"orders_{key}");    // per-tenant index (optional)
```

The engine sub-groups each transaction's changes by scope key and invokes the transform once per tenant
with a context built for that tenant (one context per tenant per batch). `ScopedDestination` also routes
each document — and deletes — to the tenant's destination; because deletes must resolve the key, that
table is marked to require `REPLICA IDENTITY FULL`. With neither, behavior is unchanged (one shared context).

## Running locally

```bash
docker compose -f samples/docker-compose.yml up -d        # Postgres (wal_level=logical) + Meilisearch
dotnet run --project samples/Sample.WorkerApp             # self-configures, backfills, then streams
```

Insert/update/delete rows in the `products` table and watch the Meilisearch `products` index stay in sync
(`curl http://localhost:7700/indexes/products/search -H "Authorization: Bearer masterKey"`).

## Tests

- Unit tests (no Docker): `dotnet run --project tests/EFCore.CDC.UnitTests`
- Core integration tests (Testcontainers Postgres — needs Docker): `dotnet run --project tests/EFCore.CDC.IntegrationTests`
- Meilisearch integration tests (Testcontainers Postgres + Meilisearch): `dotnet run --project tests/EFCore.CDC.Meilisearch.IntegrationTests`

All test projects use [TUnit](https://tunit.dev/); shared fixtures (e.g. the Postgres container) live in
`tests/EFCore.CDC.TestInfrastructure`.

## Notes & limitations (v1)

- Captured tables must have a primary key (pgoutput requirement).
- One leader owns the slot at a time; standby nodes take over on failure (Postgres advisory lock).
- Tables needing old values / unchanged-TOAST columns on UPDATE should use `REPLICA IDENTITY FULL`
  (the library warns with the exact DDL); transforms can also re-query via `DbContext`.
- The server must already have `wal_level=logical`; the library never edits `postgresql.conf`.
