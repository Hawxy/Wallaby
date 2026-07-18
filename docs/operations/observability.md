---
description: "The OpenTelemetry metrics and traces built into Wallaby and how to add them to your application's telemetry pipeline."
---

# Observability

Wallaby is instrumented with **OpenTelemetry metrics and traces** using the built-in .NET
primitives (`System.Diagnostics.Metrics.Meter` and `System.Diagnostics.ActivitySource`). You can configure OTEL for
Wallaby by adding its meter and activity source to your telemetry pipeline.

## Enabling

```csharp
services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddMeter("Wallaby")
    .WithTracing(t => t
        .AddSource("Wallaby")
        .AddNpgsql());                 // nests enrichment queries under Wallaby's transform spans
```

The meter and source names are also exposed as constants: `WallabyInstrumentation.MeterName` and
`WallabyInstrumentation.ActivitySourceName`.

## Metrics

Durations are in **seconds** (OpenTelemetry convention);

| Metric | Type | Attributes | Description |
| --- | --- | --- | --- |
| `wallaby.changes.received` | Counter | `wallaby.slot`, `wallaby.action`, `wallaby.source` | Materialized change events received (live and backfill). |
| `wallaby.ingestion.lag` | Histogram (s) | `wallaby.slot` | Delay between a source transaction's commit and Wallaby receiving it. |
| `wallaby.dependent.synthetic` | Counter | `wallaby.table` | Synthetic parent changes emitted inline by dependent-table fan-out (a wide fan-out's offloaded tail is counted by the `backfill.*` metrics instead). |
| `wallaby.transform.duration` | Histogram (s) | `wallaby.entity`, `wallaby.sink` | Time spent invoking a mapping's transform for a batch. |
| `wallaby.sink.delivery.duration` | Histogram (s) | `wallaby.sink`, `wallaby.delivery.outcome` | Duration of a single sink delivery attempt (its count by outcome gives attempts and retries). |
| `wallaby.sink.records.delivered` | Counter | `wallaby.sink` | Records accepted by a sink. |
| `wallaby.sink.delivery.failures` | Counter | `wallaby.sink`, `wallaby.delivery.outcome` | Failed deliveries (`retryable`/`permanent`). |
| `wallaby.sink.delivery.lag` | ObservableGauge (s) | `wallaby.sink` | Seconds since each sink last accepted a batch. Climbs while a sink is stuck retrying (or the pipeline is halted), so alert on it per sink. Absent until a sink's first delivery. |
| `wallaby.fanout.queue.depth` | ObservableGauge | - | Scoped fan-out jobs currently due (`Requested`/`InProgress`), sampled once per drain pass on the leader. A persistently growing depth means fan-out is falling behind its triggers. |
| `wallaby.backfill.rows` | Counter | `wallaby.table` | Rows copied during backfill. |
| `wallaby.backfill.active` | UpDownCounter | - | Tables currently being backfilled. |
| `wallaby.backfill.chunk.duration` | Histogram (s) | `wallaby.table` | Time to read and emit one backfill chunk. |

The main questions you'll want to ask are:

- **What's our throughput?** Watch `rate(wallaby.changes.received)`.
- **Are we keeping up?** Track `wallaby.ingestion.lag`.
- **Is every sink healthy?** Track `wallaby.sink.delivery.lag` (per sink).

.NET runtime metrics should also be monitored to ensure CPU and memory usage is acceptable.

## Traces

The activity source `Wallaby` emits one span per unit of work:

| Span | Kind | Notable attributes |
| --- | --- | --- |
| `transaction.process` | Consumer | `wallaby.slot`, `wallaby.txn.lsn.commit`, `wallaby.txn.lsn.end`, `wallaby.txn.size`, `wallaby.txn.streamed`, `wallaby.ingestion.lag_s`, `wallaby.watermark` (`low`/`high`, only on the tiny transactions that bracket a backfill chunk), `wallaby.heartbeat` (`true`, only on [idle-slot heartbeat](/how-it-works#idle-slots-and-wal-retention) transactions — filter these out in trace viewers); status `Error` on fault |
| `dependent.resolve` | Internal | `wallaby.table`, `wallaby.dependent.count`, `wallaby.fanout.offloaded` (bindings whose tail was queued as a scoped backfill) |
| `route` | Internal | `wallaby.batch.size`, `wallaby.source` (`live`/`fanout`/`backfill`) |
| `transform` | Internal | `wallaby.entity`, `wallaby.batch.size` |
| `sink.deliver` | Producer | `wallaby.sink`, `wallaby.destination`, `wallaby.batch.size` (retries recorded as span events; status `Error` on terminal failure) |
| `backfill` | Internal | `wallaby.table`, `wallaby.backfill.kind` (`table`/`fanout`), `wallaby.fanout.keys` (fanout only), `wallaby.backfill.rows` (one root span per backfill run; status `Error` on fault) |
| `backfill.chunk` | Internal | `wallaby.table`, `wallaby.chunk.size` (span link to the `backfill` span of the run that produced the chunk) |
| `ack` | Internal | `wallaby.slot`, `wallaby.txn.lsn.end` |
| `leader.bootstrap` | Internal | `wallaby.slot` (one per leadership term: self-config, slot-gap repair, and sink initialization before streaming; status `Error` on fault) |
| `selfconfig` | Internal | `wallaby.slot`; server validation and publication/slot/state-schema setup (child of `leader.bootstrap` when hosted); status `Error` on fault |
| `slot.repair` | Internal | child of `leader.bootstrap`: slot-loss gap detection (and re-backfill marking when one is found) |
| `sink.initialize` | Internal | `wallaby.sink`; child of `leader.bootstrap`, one per sink with one-time setup |

Live spans nest under the `transaction.process` root, so a single trace shows a committed transaction flowing
through routing, each transform, and each sink delivery. If you also enable Npgsql tracing, the
queries your transforms run appear nested under the `transform` (and `dependent.resolve`) spans.

Anomalies are recorded as **span events**, so they show up on the span's timeline exactly when they
happened: `retry` on `sink.deliver` (with `attempt` and `error`), `fanout.offloaded` on
`dependent.resolve` (one per binding whose tail was queued, with `wallaby.table`), and `slot.gap` on
`slot.repair` (with the checkpoint and consistent-point LSNs). None of these will appear on the happy path.

A backfill run gets its own `backfill` root span covering the run end-to-end, from the first chunk read
until the last chunk's delivery is acknowledged. Each chunk is delivered *inside* a slot commit, so its
`backfill.chunk` span appears in that transaction's trace and carries a **span link** back to the
`backfill` root. From a slow backfill you can jump to the commits that delivered its chunks, and from
an odd-looking transaction you can jump to the backfill run it was carrying. A `backfill` span that
stays open far longer than its chunks take to read means the run is waiting on the pipeline to
reach its watermarks (for example, a sink retrying).

To browse example traces locally, run `dotnet run --project tests/Wallaby.TraceDemo` (needs Docker).
It runs a scenario covering every span above and exports to an Aspire Dashboard at `http://localhost:18888/traces`.

## Cardinality

Metric attributes are deliberately low-cardinality: `wallaby.slot`, `wallaby.sink`, `wallaby.entity`,
`wallaby.table`, `wallaby.action`, `wallaby.source`, and `wallaby.delivery.outcome`.

Per-row values such as tenant/scope keys, document ids, and per-tenant destinations are **never** used as
metric attributes as they would explode cardinality. `wallaby.destination` appears only as a **span**
attribute, where sampling keeps the cost bounded.