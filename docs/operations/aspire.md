---
description: "Run and operate Wallaby from an Aspire AppHost: logical replication on Aspire's Postgres container, plus a dashboard resource with slot, backfill and suspension state and commands to backfill, suspend and resume."
---

# Aspire

The **Wallaby.Aspire.Hosting** package integrates Wallaby with an [Aspire](https://aspire.dev) AppHost. It
does two things:

- Starts Aspire's Postgres container with **logical replication** enabled.
- Adds a **Wallaby resource** to the dashboard, nested under the worker that runs Wallaby. It shows the
  installation's replication slots, backfill progress and suspension state, and has commands to backfill
  a table, suspend, resume and widen publications.

```bash
dotnet add package Wallaby.Aspire.Hosting
```

Add it to the **AppHost** project. Your worker keeps its normal `AddWallaby` registration; the Wallaby
resource reads and writes Wallaby's control plane through the database, the same way
[Wallaby.Client](/operations/external-control) does, and never starts a process of its own.

## Setup

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres").WithLogicalReplication();
var db = postgres.AddDatabase("app");

var worker = builder.AddProject<Projects.Cdc>("worker")
    .WithReference(db)
    .WaitFor(db)
    .WithWallaby(db);

builder.Build().Run();
```

- `WithLogicalReplication()` starts Postgres with `wal_level=logical` and 10 replication slots and WAL
  senders (pass `maxReplicationSlots` to change it). Aspire's default user is a superuser, so no
  `REPLICATION` grant is needed locally.
- `WithWallaby(db)` adds a child resource named `{worker}-wallaby`. `db` can be any resource with a
  connection string, including `AddConnectionString` for an existing database. Pass `name:` to rename it,
  and configure it through the callback, e.g. `WithWallaby(db, w => w.WithPollInterval(TimeSpan.FromSeconds(1)))`
  (the default interval is 2 seconds).

`WithWallaby` works on any resource that runs Wallaby, including a worker added by path with
`AddDotnetProject` from the prerelease `Aspire.Hosting.Dotnet` package, which needs no project reference
from the AppHost. The [sample AppHost](https://github.com/Hawxy/Wallaby/tree/main/samples/Wallaby.Aspire.Sample.AppHost)
wires the EF Core sample worker to Meilisearch that way:

```csharp
builder.AddDotnetProject("worker", "../Sample.WorkerApp/Sample.WorkerApp.csproj")
    .WithReference(db, connectionName: "Postgres")
    .WaitFor(db)
    .WithWallaby(db);
```

## What the dashboard shows

The resource's details list:

| Property | Shows |
| --- | --- |
| Suspension | `Running`, or who suspended Wallaby and why |
| Publications | when publications are [widened](/operations/external-control#widening-publications-for-schema-migrations) |
| Slot *name* | streaming or not, retained WAL, and whether the server invalidated it |
| Backfill *table* | progress and ETA while running, row count once complete, and the last error while retrying |

Its health reports whether Wallaby is **caught up**:

| Health | When |
| --- | --- |
| Healthy | every managed slot exists, Wallaby's own slot is streaming, and no backfill is requested or running |
| Degraded | waiting for the worker to provision its slot, a backfill is pending, running or retrying, or Wallaby is suspended |
| Unhealthy | a slot was invalidated, the database can't be polled, or the database holds state from a newer Wallaby version |

Widened publications stay Healthy, since replication keeps streaming. Slots provisioned for
[external consumers](/external-slots) only need to exist; their consumer may be offline in development.

State changes also appear in the resource's console log: backfills starting, completing or failing,
slots streaming or being dropped, and every command's result.

## Commands

| Command | Does |
| --- | --- |
| Backfill table | Re-snapshots one table, with an option to purge its destinations first. The table list comes from the tables Wallaby has backfilled; you can also type a schema-qualified name. |
| Backfill all tracked tables | Requests a backfill of every table Wallaby has backfilled before. |
| Cancel backfill | Withdraws a request the leader hasn't started. |
| Suspend / Resume | [Suspends](/operations/external-control#suspend-and-resume) Wallaby (its slots are dropped) and resumes it, re-backfilling every table. |
| Widen / Restore publications | Temporarily publishes whole tables so a migration blocked by publication column lists can run. |

Each command is a call into `WallabyControlClient`, so it behaves exactly like the client. Commands that
don't apply to the current state are disabled.

## Waiting for CDC

`WaitForWallaby` delays another resource until the worker's Wallaby installation is caught up:

```csharp
builder.AddProject<Projects.Api>("api").WaitForWallaby(worker);
```

On a fresh database that means the initial backfill has finished, so the API starts against a populated
index. Integration tests built on `Aspire.Hosting.Testing` can wait the same way before asserting:

```csharp
await app.Services.GetRequiredService<ResourceNotificationService>()
    .WaitForResourceHealthyAsync("worker-wallaby", ct);
```

## Telemetry

The Wallaby resource only shows what the database records. The worker's own traces and metrics reach the
dashboard through OpenTelemetry; add Wallaby's meter and activity source to the worker's telemetry setup,
usually in ServiceDefaults:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("Wallaby"))
    .WithTracing(t => t.AddSource("Wallaby"));
```

The worker's [health check](/operations/health-checks) covers what only the running node knows (its role,
crash loops and ingestion lag). See [observability](/operations/observability) for the full list.

## Limits

- The resource is for development and operations: it is excluded from the deployment manifest.
- It sees one database's Wallaby state. If several workers share a database with different slots, every
  slot appears on each worker's Wallaby resource.
