---
description: "ASP.NET Core health check for the Wallaby background service across leader and standby nodes."
outline: deep
---

# Health Checks

Wallaby runs as a hosted background service on each node of a cluster. The
`Wallaby.AspNetCore.HealthChecks` package exposes a health check so you can
wire a liveness probe to a Wallaby worker.

## Install & register

```bash
dotnet add package Wallaby.AspNetCore.HealthChecks
```

```csharp
builder.Services.AddHealthChecks().AddWallaby();
```

The package only depends on `Microsoft.Extensions.Diagnostics.HealthChecks`, so it also works in a plain
generic host if required.

## The `wallaby` check

Registered as **`wallaby`** (tag `wallaby`). It reports:

- **Unhealthy**: When the CDC background service has **terminated** (faulted out of its hosted loop), or
  when the leader is **crash-looping**: sessions keep dying before a single transaction is fully delivered
  and acknowledged (a poison event, e.g. a throwing transform or a sink permanently rejecting a batch).
  Delivery does not advance in that state, so after `CrashLoopFailureThreshold` consecutive leader-session
  failures (default **3**) the check goes Unhealthy and its description carries the last error.
- **Degraded**: While the installation is [suspended](/operations/major-version-upgrades): the node is
  alive (an orchestrator shouldn't restart-loop it) but replication is deliberately stopped and the
  managed slots are dropped. Expected during a planned upgrade window; alert if it persists after.
  Also when [dependent fan-out](/providers/entity-framework-core/#dependent-tables) keeps failing: after
  `FanoutFailureThreshold` consecutive job failures (default **5**) the documents that depend on those
  tables are going stale, while live replication carries on unaffected - so this is loud but is not a
  restart signal either. And likewise when a table's [backfill](/backfill#failure-handling) keeps failing:
  after `BackfillFailureThreshold` consecutive failures (default **5**) that table's sinks are not
  converging, while other tables and live replication carry on.
- **Healthy**: In every other state: a **leader** streaming changes, a **standby** waiting to take over,
  or a node still **starting**.

All thresholds are adjustable (set any to `0` to disable that arm):

```csharp
builder.Services.AddHealthChecks().AddWallaby(configure: o =>
{
    o.CrashLoopFailureThreshold = 5;
    o.FanoutFailureThreshold = 10;
    o.BackfillFailureThreshold = 10;
});
```

The check attaches a `data` dictionary for diagnostics. Keys with no value yet (for example
`leaderSince` on a standby) are omitted:

| Key | Meaning |
| --- | --- |
| `role` | `Starting`, `Leader`, `Standby`, `Stopped`, or `Suspended`. |
| `faulted` | `true` once the background service has terminated. |
| `startedAt` | When the background service started on this node. |
| `lastError` | The most recent leader-session, fan-out, or backfill error. |
| `leaderSince` | When this node took leadership. |
| `suspendedSince`, `suspensionReason` | When and why the installation was [suspended](/operations/major-version-upgrades). |
| `publicationsWidened`, `publicationsWidenedAt` | Present while managed publications are temporarily [widened](/operations/external-control#widening-publications-for-schema-migrations) to whole-table membership. Capture is fully functional, so the check stays Healthy. |
| `lastAcknowledgedLsn` | The last LSN acknowledged back to the replication slot. |
| `lastProgressAt` | When a transaction was last fully delivered and acknowledged. |
| `lastIngestionLagSeconds` | Ingestion lag measured at the most recently received transaction. |
| `consecutiveLeaderFailures` | Leader sessions that died in a row without acknowledging progress; the counter behind the crash-loop grade. |
| `consecutiveFanoutFailures` | The worst failing fan-out job's persisted attempt count. Holds while that job is backed off (healthy jobs draining alongside cannot mask it) and clears once the job finally completes; the rest of the queue keeps draining throughout. |
| `consecutiveFanoutPassFailures` | The fan-out worker's own loop failing outright (the queue or state store unreachable) rather than one job. |
| `consecutiveBackfillFailures` | The worst failing table's persisted backfill attempt count, cleared when its run finally starts fresh or completes. Other tables and live replication carry on. |
| `consecutiveBackfillPassFailures` | The backfill worker's own loop failing outright rather than one table. |
| `slotName` | The replication slot this node manages. |
| `lastSinkDeliveryAt:<sink>` | When each sink last accepted a batch; one entry per sink that has delivered this session. |

Each subsystem's Degraded grade fires on the worse of its job and pass counters against the same
threshold. `consecutiveLeaderFailures` only resets on real progress or a clean step-down - not just
because a failing session ran for a while first - so the crash-loop grade holds even when each session
streams briefly before failing. On recovery (the poison transaction delivers, or a fixed transform is
deployed) the first acknowledged transaction resets it and the check returns to Healthy.

::: warning
The `data` dictionary can include exception text. Don't expose a detailed `/health` response on a public
endpoint.
:::

## Reading status directly

The check reads a public `IWallabyStatus` singleton that the core runtime maintains in memory (role, leadership,
last acknowledged LSN, last ingestion lag, leader-session failures, fault). `AddWallaby` registers it, so
you can resolve `IWallabyStatus` and read `Current` to surface CDC status in your own diagnostics.

::: tip Readiness
A richer **readiness** check (graded on replication lag / retained WAL) may be added later. Lag is best watched through
[metrics](/operations/observability) (`wallaby.ingestion.lag`).
:::
