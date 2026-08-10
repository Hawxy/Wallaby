---
description: "Suspending Wallaby so RDS/Aurora can run a Postgres major-version upgrade: drop the managed replication slots, upgrade, resume, re-backfill."
---

# Upgrading Postgres

Managed platforms like RDS and Aurora refuse a Postgres major-version upgrade while **any logical
replication slot exists**, failing the precheck with:

> The cluster could not be upgraded because one or more databases have logical replication slots.
> Please drop all logical replication slots and try again.

Wallaby's **suspension** solves this: it drops every replication slot Wallaby manages (its primary slot
*and* any [external slots](/external-slots)), then idles durably across app restarts and the database
outage during the upgrade until you explicitly resume. On resume, Wallaby recreates its slots and
publications and recovers via [slot-loss gap detection](/how-it-works#slot-loss-gap-detection): every
mapped table is **fully re-backfilled**, so idempotent sinks converge on their own.

There are two ways to drive this feature. Both share the same durable state, so they interoperate.

## Option A: two-phase deployment

No admin endpoint or extra tooling, suspension is managed via your deployment pipeline.

1. **Deploy suspended.** Add `Suspend()` to your Wallaby registration and deploy it to every node:

   ```csharp
   builder.Services.AddWallaby(cdc =>
   {
       cdc.UseEntityFrameworkCore<AppDbContext>()
          .UseConnectionString(connectionString)
          .Suspend("PG18 major-version upgrade")   // [!code ++]
          .AddSink(...)...
   });
   ```

   On startup the nodes drop every managed slot and idle. Optionally verify no slots remain:

   ```sql
   SELECT slot_name FROM pg_replication_slots;   -- must be empty for the precheck
   ```

2. **Run the engine upgrade.** The suspension state lives in a regular table, so it survives
   `pg_upgrade` and the outage. Nodes ride out the downtime and stay suspended when the database
   returns.

3. **Deploy without the flag.** Remove `Suspend()` and deploy again. The flag-less nodes detect the
   configuration-driven suspension, resume automatically, recreate the slots, and re-backfill every
   mapped table.

While the flagged build is deployed, the suspension is **re-asserted**, so a remote `ResumeAsync` gets
re-suspended by the running nodes. Resume by deploying the flag-less build.

### Mixed rollouts

A rolling deploy inevitably mixes flagged and flag-less nodes for a while. The flagged nodes continuously
stamp a liveness heartbeat (`wallaby.control.configuration_asserted_at`) while their suspension is in
force, and a flag-less node only auto-resumes once that heartbeat has been quiet for a grace window, computed as
`max(ControlPollInterval × 4, SuspensionAutoResumeGraceFloor)`, set to one minute by default. So a mixed
rollout simply **stays suspended** until the last flagged node is gone, then resumes exactly once,
rather than the two sides flapping the slots (each flap forces a full re-backfill).

The grace is also the dead time between the last flagged node stopping and the resume; lower
`Advanced.SuspensionAutoResumeGraceFloor` if your deploys are single-node and the wait matters.
A flag-less node waiting out the grace will log that it is doing so.

## Option B: runtime control (Wallaby.Client)

Suspend and resume at runtime from **any process with a connection string**. The app itself doesn't need to restart or redeploy.
The client itself is covered in [External Control](/operations/external-control).

```bash
dotnet add package Wallaby.Client
```

```csharp
await using var control = new WallabyControlClient(connectionString);

// Before the upgrade: drops every managed slot, waits until they're verified gone.
var suspended = await control.SuspendAsync(new WallabySuspendOptions
{
    Reason = "PG18 major-version upgrade",
});

// ... run the engine upgrade ...

// After the upgrade: nodes wake, recreate slots, and re-backfill. purge: true also empties each
// mapped destination first, so deletes committed during the window converge too (needs ISinkPurger).
await control.ResumeAsync(purge: true);

// Inspect at any time: state, who/why/when, and every managed slot's live server status.
var state = await control.GetStateAsync();
```

Or register it in a host's container (e.g. to expose from your own admin endpoint, alongside
`IWallabyBackfillManager`):

```csharp
builder.Services.AddWallabyControlClient(connectionString);
```

How a suspension executes:

- A **running node** observes the request instantly (LISTEN/NOTIFY), winds down its streaming session,
  drops the slots under the cluster lock, and marks the suspension finalized.
- If there's **no host running** (scaled to zero, or a provision-only worker that already exited), the client
  drops the slots itself after `HostGracePeriod` (default 15 s). This is safe against a live host: an
  actively streamed slot refuses the drop, so the client keeps waiting for the host instead.
- `SuspendAsync` waits until every managed slot is verified gone (`Timeout`, default 2 minutes, then
  `WallabyControlTimeoutException` with the last observed state — the request stays persisted). Pass
  `WaitForCompletion = false` to fire-and-forget and poll `GetStateAsync`.

A client-requested suspension is **never auto-resumed**. Only an explicit `ResumeAsync` (or raw SQL) ends it.

### Raw SQL fallback

For psql-only operators, the control row can be driven directly:

```sql
-- Request suspension (a running Wallaby node drops the slots and marks it 'Suspended'):
INSERT INTO wallaby.control (scope, state, origin, reason, requested_at)
VALUES ('wallaby', 'SuspendRequested', 'client', 'PG18 upgrade', now())
ON CONFLICT (scope) DO UPDATE
    SET state = 'SuspendRequested', origin = 'client', reason = EXCLUDED.reason,
        requested_at = now(), updated_at = now()
    WHERE control.state = 'Running';
SELECT pg_notify('wallaby_control', '');

-- Resume:
UPDATE wallaby.control SET state = 'Running', resumed_at = now(), updated_at = now()
WHERE state IN ('SuspendRequested', 'Suspended');
SELECT pg_notify('wallaby_control', '');
```

## What to expect on resume

- The primary slot and publications are recreated by normal self-configuration on the next leader
  election, and slot-loss gap detection marks **every mapped table for a full re-backfill**.
- **External slots** are recreated too, but their consumers' positions are gone: the external tool must
  re-sync from scratch (the same situation as any dropped slot). Plan its re-sync alongside the upgrade.
- Until the re-backfill completes, sinks are stale by however long the suspension lasted.
- The re-backfill is upsert-only: rows **deleted** during the window leave stale documents behind
  unless the resume requested a [destination purge](/backfill#purging-before-a-backfill)
  (`ResumeAsync(purge: true)`, or the `PurgeOnSlotGapRepair` option).
- The repair also fires when the upgrade **rebuilt the cluster** (restore, blue/green) instead of
  upgrading in place: the rewound WAL history is
  [detected](/how-it-works#slot-loss-gap-detection) even though the surviving checkpoint's LSN reads
  as ahead of the new slot.

## Operational notes

- **Health checks** report `Degraded` while suspended. Don't alert-page on it
  during a planned upgrade window; do alert if it persists after.
- **Backfill requests** made while suspended stay persisted and are absorbed by the resume's full
  re-backfill.
- The suspension is **installation-wide**: one control row governs every node and every managed slot on
  that database.
