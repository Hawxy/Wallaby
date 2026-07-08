# Wallaby

Postgres Change Data Capture for .NET, driven by your **EF Core or Marten model**.

Wallaby streams row changes from Postgres logical replication, materializes them into your mapped
EF Core entities or Marten documents, lets you **transform/enrich** them, and routes the resulting
documents to pluggable **destinations** (sinks). It **self-configures** the publication and replication
slot from your model, supports **backfill** operations, and is **cluster-safe** via leader election.

**Meilisearch** and **HTTP/webhook** sinks are supported out of the box. Contributions for additional sinks is welcome.

📖 **Full documentation: [wallabycdc.net](https://wallabycdc.net/)**

## Packages

| Project                        | Purpose                                  |
|--------------------------------|------------------------------------------|
| `Wallaby`                      | Core package (provider-agnostic).        |
| `Wallaby.Providers.EntityFrameworkCore`  | EF Core storage provider.                |
| `Wallaby.Providers.Marten`               | Marten storage provider.                 |
| `Wallaby.Sinks.Http`           | HTTP/webhook destination sink.           |
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

Each package has one test project under `tests/` (`Wallaby.Tests`, `Wallaby.Providers.*.Tests`,
`Wallaby.Sinks.*.Tests`) with `Unit/` and `Integration/` folders inside; namespaces follow the folders.
All test projects use [TUnit](https://tunit.dev/); shared fixtures (e.g. the Postgres container) live in
`tests/Wallaby.TestInfrastructure`.

- Everything (what CI runs; integration tests need Docker): `dotnet test` or `.\build.ps1 Test`
- One package: `dotnet run -c Release --project tests/Wallaby.Providers.EntityFrameworkCore.Tests`
- Unit tests only (no Docker): append `-- --treenode-filter "/*/*.Unit*/*/*"`

### Viewing traces

`tests/Wallaby.TraceDemo` (a console app, not a test project) runs a curated CDC scenario — live
changes with a sink retry, dependent fan-out, whole-table backfill — and exports the resulting traces
and metrics to a local [Aspire Dashboard](https://learn.microsoft.com/dotnet/aspire/fundamentals/dashboard/standalone)
container:

```
dotnet run --project tests/Wallaby.TraceDemo
# then open http://localhost:18888/traces
```

The dashboard keeps running (and accumulates traces across runs); remove it with
`docker rm -f wallaby-trace-dashboard`.

