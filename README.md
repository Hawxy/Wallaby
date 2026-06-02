# Wallaby

Postgres Change Data Capture for .NET, driven by your **EF Core model**.

EFCore.CDC streams row changes from Postgres logical replication, materializes them into your mapped
EF Core entities, lets you **transform/enrich** them, and routes the resulting documents to pluggable
**destinations** (sinks). It **self-configures** the publication and replication slot from your model,
supports **backfill** operations, and is **cluster-safe** via leader election.

The first shipped sink is **Meilisearch** — keep a search index continuously in sync with your tables.

## Packages

| Project                      | Purpose                       |
|------------------------------|-------------------------------|
| `Wallably`                   | Core package.                 |
| `Wallably.Sinks.Meilisearch` | Meilisearch destination sink. |

## Quick start

```csharp
builder.Services.AddDbContextFactory<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddWallaby<AppDbContext>(cdc =>
{
    cdc.UseConnectionString(conn)
       .ConfigureOptions(o => { o.SlotName = "app_cdc"; o.PublicationName = "app_cdc_pub"; })
       .AddMeilisearchSink("meili", m => { m.Host = "http://localhost:7700"; m.ApiKey = key; })

       // Mapping = routing only. The transform does the data shaping.
       .Map<Product>()
            .ToSink("meili", destination: "products")
            .WithBackfillVersion("v1")           // bump to force a reindex/backfill
            .UsingTransform((db, changes, ct) =>
            {
                var docs = new Dictionary<DocumentKey, CdcDocument?>();
                foreach (var c in changes)
                    docs[c.Key] = new CdcDocument { ["name"] = c.Entity!.Name };
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(docs);
            });
});
```

On startup the library validates the server (`wal_level=logical`, replication role, slot headroom),
creates the `wallaby` state schema, the publication, and the replication slot, backfills the mapped tables,
then streams live changes — all on a single elected leader.

## Tests

- Unit tests (no Docker): `dotnet run --project tests/EFCore.CDC.UnitTests`
- Core integration tests (Testcontainers Postgres — needs Docker): `dotnet run --project tests/EFCore.CDC.IntegrationTests`
- Meilisearch integration tests (Testcontainers Postgres + Meilisearch): `dotnet run --project tests/EFCore.CDC.Meilisearch.IntegrationTests`

All test projects use [TUnit](https://tunit.dev/); shared fixtures (e.g. the Postgres container) live in
`tests/EFCore.CDC.TestInfrastructure`.

