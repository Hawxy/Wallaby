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
- **Healthy**: In every other state: a **leader** streaming changes, a **standby** waiting to take over,
  or a node still **starting**.

The crash-loop threshold is adjustable (set it to `0` to disable that arm):

```csharp
builder.Services.AddHealthChecks().AddWallaby(configure: o => o.CrashLoopFailureThreshold = 5);
```

The check attaches a `data` dictionary for diagnostics: `role`, `faulted`, `lastError`, `startedAt`,
`leaderSince`, `suspendedSince`, `suspensionReason`, `lastAcknowledgedLsn`, `lastProgressAt`, `lastIngestionLagSeconds`,
`consecutiveLeaderFailures`, `consecutiveFanoutFailures`, `slotName`, and one
`lastSinkDeliveryAt:<sink>` entry per sink that has accepted a batch this session. A nonzero
`consecutiveFanoutFailures` means the fan-out worker is stuck retrying with backoff. 
Live replication keeps flowing, so the node stays Healthy, but the value is worth alerting on.

`consecutiveLeaderFailures` (the counter behind the crash-loop grade) only resets on real progress or a
clean step-down - not just because a failing session ran for a while first - so the grade holds even when
each session streams briefly before failing. On recovery (the poison transaction delivers, or a fixed
transform is deployed) the first acknowledged transaction resets it and the check returns to Healthy.

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
