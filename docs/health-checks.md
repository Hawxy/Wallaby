---
outline: deep
---

# Health Checks

Wallaby runs as a hosted background service on each node of a cluster. The
`Wallaby.AspNetCore.HealthChecks` package exposes a ASP.NET Core health check so you can
wire a liveness probe to a Wallaby worker.

## Install & register

```bash
dotnet add package Wallaby.AspNetCore.HealthChecks
```

```csharp
builder.Services.AddHealthChecks().AddWallaby();
```

The package only depends on `Microsoft.Extensions.Diagnostics.HealthChecks`, so it also works in a plain
generic host

## The `wallaby` check

Registered as **`wallaby`** (tag `wallaby`). It reports:

- **Unhealthy** - When the CDC background service has **terminated** (faulted out of its hosted loop).
- **Healthy** - In every other state: a **leader** streaming changes, a **standby** waiting to take over,
  or a node still **starting**.

The check attaches a `data` dictionary for diagnostics: `role`, `faulted`, `lastError`, `startedAt`,
`leaderSince`, `lastAcknowledgedLsn`, `lastProgressAt`, `lastIngestionLagSeconds`,
`consecutiveLeaderFailures`, and `slotName`.

::: warning Don't expose full detail publicly
The `data` dictionary can include exception text. Don't expose a detailed `/health` response on a public
endpoint.
:::

## Reading status directly

The check reads a public `ICdcStatus` singleton that the core runtime maintains in memory (role, leadership,
last acknowledged LSN, last ingestion lag, leader-session failures, fault). `AddWallaby` registers it, so
you can resolve `ICdcStatus` and read `Current` to surface CDC status in your own diagnostics.

::: tip Readiness
A richer **readiness** check (graded on replication lag / retained WAL) may be added later. Lag is best watched through
[metrics](/observability) (`wallaby.ingestion.lag`).
:::
