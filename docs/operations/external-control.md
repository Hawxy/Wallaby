---
description: "Wallaby.Client: suspend, resume, trigger backfills, and inspect a Wallaby installation from any process with a connection string — no Wallaby host reference required."
---

# External Control

The **Wallaby.Client** package is a standalone remote control plane for a Wallaby installation. It talks
only to the **source Postgres database** so any process with a connection string can drive it. Useful for an ops console, an admin endpoint in
another service, or a deployment script. It references no Wallaby host packages and needs no running node.

```bash
dotnet add package Wallaby.Client
```

The package depends only on `Npgsql` (plus the logging/DI abstractions) and is AOT-compatible, so it
suits small operational tools.

## Creating a client

```csharp
// Owns a pooled data source built from the connection string, disposed with the client:
await using var control = new WallabyControlClient(connectionString);

// Or over an existing NpgsqlDataSource (not disposed with the client):
await using var control = new WallabyControlClient(dataSource);
```

A multi-host data source is automatically targeted at the primary. For dependency injection, three
idempotent `AddWallabyControlClient` overloads register a singleton:

```csharp
builder.Services.AddWallabyControlClient(connectionString);      // client owns the data source
builder.Services.AddWallabyControlClient(sp => myDataSource);    // caller owns the data source
builder.Services.AddWallabyControlClient();                      // resolves NpgsqlDataSource from the container
```

Everything below shares one design: state changes are **guarded updates on durable rows**, so every
operation is idempotent, safe to run concurrently from multiple actors, and interoperates with the
host-side equivalents.

## Inspecting state

```csharp
var state = await control.GetStateAsync();
// state.State          Running | SuspendRequested | Suspended
// state.Origin         who initiated a suspension: Client | Configuration
// state.Reason / RequestedBy / RequestedAt / SuspendedAt / ResumedAt
// state.Slots          every managed slot: name, publication, kind, exists-on-server, active,
//                      retained-WAL bytes (null when the slot is gone or read from a standby)
```

A slot's `RetainedWalBytes` is how much WAL the server must keep for it. Watch it especially for
[external slots](/external-slots): they pin WAL from the moment they exist, and Wallaby's own
heartbeat does not advance them, so a consumer that lags (or never connects) grows this number
until `max_slot_wal_keep_size` invalidates the slot.

## Suspend and resume

```csharp
var suspended = await control.SuspendAsync(new WallabySuspendOptions
{
    Reason = "PG18 major-version upgrade",
});

// ...later...
await control.ResumeAsync();
```

`SuspendAsync` durably drops **every** replication slot Wallaby manages (primary and external) and idles
the installation across restarts and database outages until an explicit `ResumeAsync`.
On resume, nodes recreate their slots and re-backfill every mapped table. 
For more information on this feature, see the [major-version upgrade runbook](/operations/major-version-upgrades).

Options on `WallabySuspendOptions`:

| Option | Default | Meaning |
|---|---|---|
| `Reason` | `null` | Free-text reason, surfaced in `GetStateAsync` and health-check data. |
| `RequestedBy` | machine name | Recorded with the request. |
| `WaitForCompletion` | `true` | Wait until every managed slot is verified dropped; `false` fires the request and returns immediately. |
| `HostGracePeriod` | 15 s | How long to give a running host to drop the slots before the client drops them itself (safe: an actively streamed slot refuses the drop). |
| `Timeout` | 2 min | Deadline for completion; expiry throws `WallabyControlTimeoutException` with the last observed state. The request stays persisted. |
| `Progress` | `null` | `IProgress<WallabyControlState>` reporting each poll. |

::: warning
A **client-requested** suspension is never auto-resumed not by restarts, not by fresh deployments.
Only an explicit `ResumeAsync` ends it.
:::

## Triggering backfills

```csharp
await control.RequestBackfillAsync("public.products");
await control.RequestBackfillAsync("public.products", purge: true);   // purge destinations first

await control.CancelBackfillAsync("public.products");  // withdraw a queued request (clears its purge mark)

var status = await control.GetBackfillStatusAsync();   // every tracked table's state
```

Identical semantics to the in-host manager: the request is persisted (it survives restarts), the current
leader is signalled instantly, and a request made while the table is already backfilling wins — the
table re-runs from the start. The client has no entity model, so tables are addressed by
schema-qualified name; a request for a table Wallaby doesn't capture stays `Requested` until a mapping
for it deploys (the leader warns once per term about such requests).
`CancelBackfillAsync` withdraws a queued request before the leader serves it — including its pending
purge mark, the escape hatch for a mis-fired `purge: true` — and returns whether a request was
withdrawn; see [cancelling a queued request](/backfill#cancelling-a-queued-request) for the exact
semantics. See [Backfill](/backfill) for how snapshots run and what
[`purge: true`](/backfill#purging-before-a-backfill) converges.

## Requirements and privileges

- **The client never performs DDL.** The control tables are created by the Wallaby host at startup, so
  each operation requires a host to have run against the database at least once (a suspension-aware
  version for suspend/resume); `SuspendAsync` and `RequestBackfillAsync` throw a descriptive
  `InvalidOperationException` otherwise. Reads (`GetStateAsync`, `GetBackfillStatusAsync`) will always work.
- Suspending needs the same rights Wallaby itself uses: drop its replication slots and update the
  `wallaby` schema's tables.
- Operations are visible to the installation's own diagnostics: a suspension turns the
  [health check](/operations/health-checks) `Degraded` with the reason in its data, and backfill
  progress shows in `GetBackfillStatusAsync` and the host's logs.
