---
description: "End-to-end pipeline testing with the Wallaby.Testing package: real logical replication, production transforms, and captured sink output."
---

# Testing

The `Wallaby.Testing` package helps you test your CDC pipeline **end-to-end**: write rows through your
real `DbContext`, let real logical replication pick them up, run your production transforms, and assert
on the documents that would have been delivered.

```bash
dotnet add package Wallaby.Testing
```

The package depends only on core Wallaby, so it carries no test-framework or host opinions — it works
with TUnit, xUnit or NUnit, and with `WebApplicationFactory`, `HostApplicationBuilder` or a plain
`ServiceCollection`.

## The recipe

An end-to-end CDC test has four moving parts, and the package provides a piece for each:

1. A Postgres with **`wal_level = logical`** (see [Test database](#test-database) below).
2. Your application's **real `AddWallaby` registration**, with the production sink swapped for a
   [`CaptureSink`](#capturing-output-capturesink) and the slot/publication retargeted via the
   [post-registration overrides](#overriding-the-app-s-registration).
3. A [readiness gate](#waiting-for-streaming): rows written before the replication slot exists are never
   captured, so wait for streaming before seeding.
4. Asynchronous assertions: delivery happens some time after the triggering commit, so wait for the
   expected documents instead of asserting immediately.

## Test database

Logical replication needs `wal_level = logical`. A [Testcontainers](https://dotnet.testcontainers.org/) container configured at startup is the reliable
option locally and in your CI:

```csharp
var postgres = new PostgreSqlBuilder("postgres:17")
    .WithCommand("-c", "wal_level=logical")
    .Build();
await postgres.StartAsync();
```

Ensure you create the schema (e.g. `MigrateAsync()`) before starting the Wallaby host.

## Capturing output: `CaptureSink`

`CaptureSink` is an `ISink` that records every delivered `SinkRecord` for assertions. It is thread-safe
and understands that CDC delivery is asynchronous:

```csharp
var sink = new CaptureSink();

// Wait until specific documents have been delivered (default timeout 30s):
await sink.WaitForDocumentsAsync([product.Id.ToString()]);

// Or wait on any condition over everything delivered so far:
await sink.WaitForAsync(records => records.Any(r => r.DocumentId == id && r.IsDeletion));

// "Current state" view: the last record per DocumentId, optionally per destination.
var latest = sink.LatestByDocumentId(destination: "products");
var record = latest[product.Id.ToString()];

sink.Records;        // everything delivered, in delivery order
sink.For("products") // filtered by source table
sink.Clear();        // reset between tests
```

When a wait times out, the `TimeoutException` lists every record received so far, so a failing test tells
you what actually arrived.

::: tip
Delivery is **at-least-once**, so avoid asserting on record counts as a redelivered batch would make the
test flaky. `LatestByDocumentId()` gives you the stable end-state view, which also pairs well with
snapshot testing tools like [Verify](https://github.com/VerifyTests/Verify).
:::

Deletions show up as records with `IsDeletion = true` — both *tombstones* (your transform returned a
`null` document) and *hard deletes* (the row was deleted; routed by key without invoking the transform).

## Overriding the app's registration

The point of end-to-end testing is to run your application's own `AddWallaby` call, not a parallel test copy. Two `IServiceCollection` extensions apply test
overrides *after* the app has registered itself:

```csharp
services
    .ConfigureWallabyOptions(o =>
    {
        // Give the test host its own slot/publication so it can't collide with other environments.
        o.SlotName = "app_tests_slot";
        o.PublicationName = "app_tests_pub";
    })
    .ReplaceWallabySink("meili", sink);
```

`ReplaceWallabySink` swaps the sink registered under that name for your replacement — the sink's
`WithMappings(...)` mappings stay attached, so the capture sink receives exactly the records the
production sink would have, with their original `SinkRecord.Destination` values. Replacing the sink also prevents the original's `ISinkInitializer` from
running, so no connection to the real destination is ever attempted. Unknown names throw with the list of
registered sinks.

Both extensions are designed for `WebApplicationFactory.ConfigureTestServices`, which runs after the
app's `ConfigureServices`, but they work on any `IServiceCollection` you may provide. Since `WallabyOptions`
participates in the standard options pattern, `ConfigureWallabyOptions` is equivalent to
`services.PostConfigure<WallabyOptions>(...)`, use whichever reads better in your test host.

## Waiting for streaming

Rows committed before the replication slot exists are invisible to the pipeline (unless a backfill picks
them up later). `WallabyReadiness.WaitForStreamingAsync` blocks until the host is actually capturing:
the node has become the **leader**, and the configured slot exists with an attached walsender
(`pg_replication_slots.active`). It throws immediately if the background service faults, instead of
letting your test time out.

```csharp
await WallabyReadiness.WaitForStreamingAsync(factory.Services);
```

## Full example

A session-scoped fixture that boots a worker's real `Program` with `WebApplicationFactory`, and a test
against it. Wire the fixture into your framework's session lifecycle (TUnit `ClassDataSource` +
`IAsyncInitializer`, xUnit `IAsyncLifetime`, …).

```csharp
public sealed class CdcWorkerFixture : IAsyncDisposable
{
    private PostgreSqlContainer _postgres = null!;
    public WebApplicationFactory<Program> Factory { get; private set; } = null!;
    public CaptureSink Sink { get; } = new();

    public async Task InitializeAsync()
    {
        _postgres = new PostgreSqlBuilder("postgres:17")
            .WithCommand("-c", "wal_level=logical")
            .Build();
        await _postgres.StartAsync();

        // Remember to override the application's DB connection string, do this however you see fit.
        Environment.SetEnvironmentVariable("ConnectionStrings__App", _postgres.GetConnectionString());

        // Mapped tables must exist before self-config runs.
        await using (var db = CreateContext())
        {
            await db.Database.MigrateAsync();
        }

        Factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.ConfigureTestServices(services => services
                .ConfigureWallabyOptions(o =>
                {
                    o.SlotName = "app_tests_slot";
                    o.PublicationName = "app_tests_pub";
                })
                .ReplaceWallabySink("meili", Sink)));

        _ = Factory.Services; // builds and starts the host
        await WallabyReadiness.WaitForStreamingAsync(Factory.Services);
    }

    public AppDbContext CreateContext() => /* a context pointed at _postgres */;

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}
```

```csharp
[Test]
public async Task Product_is_projected_to_the_search_index()
{
    await using var db = fixture.CreateContext();
    var product = new Product { Id = 42, Name = "Lamington" };
    db.Add(product);
    await db.SaveChangesAsync(); // real WAL → pipeline → transform → capture

    await fixture.Sink.WaitForDocumentsAsync([product.Id.ToString()]);

    var record = fixture.Sink.LatestByDocumentId(destination: "products")[product.Id.ToString()];
    await Assert.That(record.IsDeletion).IsFalse();
    await Assert.That(record.Document!["name"]).IsEqualTo("Lamington");
}
```

## Isolation between tests

The pipeline streams continuously across the whole session, so keep tests from observing each other:

- Run tests against the shared fixture **sequentially**, and start each one with `Sink.Clear()`.
- Assert only on the document ids the test itself seeded. Database cleanup tools (Respawn etc.) issue
  real `DELETE`s that arrive in the capture sink as deletion records for *previous* tests' rows.
- If you reset data between tests, leave Wallaby's `wallaby` state schema untouched so checkpoints and
  backfill state survive the reset.
