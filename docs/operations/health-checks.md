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

- **Unhealthy** - When the CDC background service has **terminated** (faulted out of its hosted loop).
- **Healthy** - In every other state: a **leader** streaming changes, a **standby** waiting to take over,
  or a node still **starting**.

The check attaches a `data` dictionary for diagnostics: `role`, `faulted`, `lastError`, `startedAt`,
`leaderSince`, `lastAcknowledgedLsn`, `lastProgressAt`, `lastIngestionLagSeconds`,
`consecutiveLeaderFailures`, `consecutiveFanoutFailures`, `slotName`, and one
`lastSinkDeliveryAt:<sink>` entry per sink that has accepted a batch this session. A nonzero
`consecutiveFanoutFailures` means the fan-out worker is stuck retrying with backoff. 
Live replication keeps flowing, so the node stays Healthy, but the value is worth alerting on.

A climbing `consecutiveLeaderFailures` means the leader is crash-looping: sessions keep dying before a
single transaction is fully delivered and acknowledged (e.g. a sink permanently rejecting a batch). The
counter only resets on real progress or a clean step-down — not just because a failing session ran for a
while first — so it is a reliable alerting signal even when each session streams briefly before failing.

::: WARNING
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
