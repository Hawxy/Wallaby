# Wallaby

Postgres Change Data Capture for .NET, driven by your **EF Core or Marten model**.

Wallaby streams row changes from Postgres logical replication, materializes them into your mapped
EF Core entities or Marten documents, lets you **transform/enrich** them, and routes the resulting
documents to pluggable **destinations** (sinks). It **self-configures** the publication and replication
slot from your model, supports **backfill** operations, and is **cluster-safe** via leader election.

A **Meilisearch** sink is supported out of the box. Contributions for additional sinks is welcome.

## Packages

| Project                        | Purpose                                  |
|--------------------------------|------------------------------------------|
| `Wallaby`                      | Core package (provider-agnostic).        |
| `Wallaby.EntityFrameworkCore`  | EF Core storage provider.                |
| `Wallaby.Marten`               | Marten storage provider.                 |
| `Wallaby.Sinks.Meilisearch`    | Meilisearch destination sink.            |

## Quick start

```csharp
builder.Services.AddDbContextFactory<AppDbContext>(o => o.UseNpgsql(conn));

builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()
       .UseConnectionString(conn)
       .ConfigureOptions(o => { o.SlotName = "app_cdc"; o.PublicationName = "app_cdc_pub"; })
       .AddMeilisearchSink("meili", m => { m.Host = "http://localhost:7700"; m.ApiKey = key; })

       // Mapping = routing only. The transform does the data shaping.
       .WithMappings(sink => sink
            .Map<Product>()
            .ToDestination("products")
            .WithBackfillVersion("v1")           // bump to force a reindex/backfill
            .UsingTransform((db, changes, ct) =>
            {
                var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var c in changes)
                    docs[c.Key] = new WallabyDocument { ["name"] = c.Entity!.Name };
                return Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(docs);
            }));
});
```

## Tests

The test suite requires docker to run and can be executed via `.\build.ps1 Test`

All test projects use [TUnit](https://tunit.dev/); shared fixtures (e.g. the Postgres container) live in
`tests/Wallaby.TestInfrastructure`.

