# Wallaby

Postgres Change Data Capture for .NET, driven by your **EF Core model**.

Wallaby streams row changes from Postgres logical replication, materializes them into your mapped
EF Core entities, lets you **transform/enrich** them, and routes the resulting documents to pluggable
**destinations** (sinks). It **self-configures** the publication and replication slot from your model,
supports **backfill** operations, and is **cluster-safe** via leader election.

A **Meilisearch** sink is supported out of the box. Contributions for additional sinks is welcome.

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

## Tests

The test suite requires docker to run and can be executed via `.\build.ps1 Test`

All test projects use [TUnit](https://tunit.dev/); shared fixtures (e.g. the Postgres container) live in
`tests/Wallaby.TestInfrastructure`.

