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

`SuspendAsync` durably drops **every** replication slot Wallaby manages (primary and external) — and
every **Wallaby-managed publication** with them, so the database is fully quiesced: the upgrade precheck
passes, and schema migrations blocked by publication column lists or row filters
(`cannot alter type of a column used by a publication...`) run freely during the window. A publication
Wallaby doesn't own (`ManagePublicationTables = false`) is never touched. The suspension idles the
installation across restarts and database outages until an explicit `ResumeAsync`.
On resume, nodes recreate their slots and publications and re-backfill every mapped table. 
For more information on this feature, see the [major-version upgrade runbook](/operations/major-version-upgrades).

The re-backfill is upsert-only, so documents whose **deletes** were committed while suspended would
linger in sinks. `ResumeAsync(purge: true)` additionally
[purges each mapped destination](/backfill#purging-before-a-backfill) before its re-backfill, converging
sinks to exactly the current table contents (sinks must implement `ISinkPurger`; destinations are
temporarily incomplete while the re-backfill runs). The purge request is persisted with the resume, so
it survives restarts and is honored by whichever node repairs the gap.

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
A **client-requested** suspension is never auto-resumed: not by restarts, not by fresh deployments.
Only an explicit `ResumeAsync` ends it.
:::

## Widening publications for schema migrations

Postgres refuses `ALTER TABLE ... ALTER COLUMN ... TYPE` (and `DROP COLUMN`) on any column pinned by a
[publication column list](/configuration#publication-column-lists) or row filter. Suspension clears
this — but at the cost of a capture outage and a full re-backfill, which is overkill when nothing is
being upgraded and the operator just needs to run a migration. **Widening** is the lighter tool:

```csharp
// Temporarily reconcile every managed publication to whole-table membership (no column lists).
await control.WidenPublicationsAsync();

// ... run the blocked migration ...

// Clear the flag; the next leader term reapplies the narrow lists from the captured model.
await control.RestorePublicationsAsync();
```

No slot is dropped and the checkpoint stays continuous, so there is **no capture gap and no
re-backfill**. A running host applies the widen by bouncing its leader session (the membership rewrite
is one atomic `ALTER PUBLICATION ... SET TABLE`; streaming pauses for one re-election). With no host
running, the client rewrites the publications itself after
`WallabyWidenOptions.HostGracePeriod` — it reads each publication's current membership from the catalog,
so no entity model is needed. `WidenPublicationsAsync` waits (up to `Timeout`) until no managed
publication carries a list or filter — i.e. until the migration will actually run — and reports
per-publication progress via each slot's `PublicationNarrowed` in `GetStateAsync`.

`RestorePublicationsAsync` returns immediately: the narrow lists come from the captured model, so only
a host can reapply them. With hosts running that lands within seconds; scaled to zero, the publications
stay wide until the next host starts. Nothing is blocked either way.

Semantics worth knowing:

- **While widened, deliberately excluded columns are published.** Data minimization via
  `Consumes`/`ConsumesAllExcept` is temporarily lifted at the server (client-side selection still
  filters what sinks receive). The leader logs a warning each term while the flag is set, and the
  [health check](/operations/health-checks) stays **Healthy** with `publicationsWidened`/`publicationsWidenedAt`
  in its data — capture is fully functional.
- **Unmanaged publications** (`ManagePublicationTables = false`) are never touched; clear their lists
  or filters manually before the migration.
- **Widen while suspended is refused** — a suspension already dropped the managed publications, so the
  migration runs freely; resume first if widening was what you meant.
- **Suspending while widened ends the widening**: the suspension drops the managed publications
  outright, and resume recreates them with their configured narrow lists. Re-widen afterwards if a
  migration is still pending.
- Choosing between the two: engine upgrade (slots must not exist) ⇒
  [suspend](/operations/major-version-upgrades); schema migration only ⇒ widen.

For psql-only operators, the raw-SQL equivalent of the guarded transitions:

```sql
-- Widen (only while Running; a host bounce or the client fallback applies it):
UPDATE wallaby.control SET publications_widened = true, widened_at = now(), updated_at = now()
WHERE state = 'Running' AND NOT publications_widened;
SELECT pg_notify('wallaby_control', '');

-- Restore:
UPDATE wallaby.control SET publications_widened = false, widened_at = NULL, widened_by = NULL, updated_at = now()
WHERE publications_widened;
SELECT pg_notify('wallaby_control', '');
```

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

- **The client never creates schema objects.** The `wallaby` schema is created and migrated by the
  Wallaby host at startup, and the client checks its version ledger (`wallaby.schema_version`) instead
  of probing: `SuspendAsync`, `WidenPublicationsAsync`, and `RestorePublicationsAsync` require the
  schema version the client was built against and throw a descriptive `InvalidOperationException`
  naming the found version otherwise (`RequestBackfillAsync` only needs a host to have run at all).
  `ResumeAsync` deliberately works against any schema version, so an old installation can always be
  unsuspended. Reads (`GetStateAsync`, `GetBackfillStatusAsync`) will always work — their column set
  adapts to the ledger. The only DDL it ever runs targets Wallaby-managed publications, and only in the
  no-host fallbacks: dropping them when it finalizes a suspension, and rewriting their membership when
  it applies a widening — in both cases objects the host recreates or re-narrows from configuration.
- Suspending needs the same rights Wallaby itself uses: drop its replication slots and update the
  `wallaby` schema's tables.
- Operations are visible to the installation's own diagnostics: a suspension turns the
  [health check](/operations/health-checks) `Degraded` with the reason in its data, and backfill
  progress shows in `GetBackfillStatusAsync` and the host's logs.
