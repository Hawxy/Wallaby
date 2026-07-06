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
| `wallaby.fanout.queue.depth` | ObservableGauge | — | Scoped fan-out jobs currently due (`Requested`/`InProgress`), sampled once per drain pass on the leader. A persistently growing depth means fan-out is falling behind its triggers. |
| `wallaby.backfill.rows` | Counter | `wallaby.table` | Rows copied during backfill. |
| `wallaby.backfill.active` | UpDownCounter | — | Tables currently being backfilled. |
| `wallaby.backfill.chunk.duration` | Histogram (s) | `wallaby.table` | Time to read and emit one backfill chunk. |

The main questions you'll want to ask are:

- **What's our throughput?**, which can be seen via `rate(wallaby.changes.received)`; 
- **Are we keeping up?** which is tracked via `wallaby.ingestion.lag`;
- **Is every sink healthy?** which is tracked via `wallaby.sink.delivery.lag` (per sink);

.NET runtime metrics should also be monitored to ensure CPU and memory usage is acceptable.

## Traces

The activity source `Wallaby` emits one span per unit of work:

| Span | Kind | Notable attributes |
| --- | --- | --- |
| `transaction` | Consumer | `wallaby.slot`, `wallaby.txn.lsn.commit`, `wallaby.txn.lsn.end`, `wallaby.txn.size`, `wallaby.ingestion.lag_s` |
| `dependent.resolve` | Internal | `wallaby.table`, `wallaby.dependent.count` |
| `route` | Internal | `wallaby.batch.size` |
| `transform` | Internal | `wallaby.entity`, `wallaby.batch.size` |
| `sink.deliver` | Producer | `wallaby.sink`, `wallaby.destination`, `wallaby.batch.size` (retries recorded as span events; status `Error` on terminal failure) |
| `backfill.chunk` | Internal | `wallaby.table`, `wallaby.chunk.size` |
| `ack` | Internal | `wallaby.slot`, `wallaby.txn.lsn.end` |

Spans nest under the `transaction` root, so a single trace shows a committed transaction flowing
through routing, each transform, and each sink delivery. If you also enable Npgsql tracing, the EF Core
queries your transforms run appear nested under the `transform` (and `dependent.resolve`) spans.

## Cardinality

Metric attributes are deliberately low-cardinality: `wallaby.slot`, `wallaby.sink`, `wallaby.entity`,
`wallaby.table`, `wallaby.action`, `wallaby.source`, and `wallaby.delivery.outcome`.

Per-row values - tenant/scope keys, document ids, and per-tenant destinations - are **never** used as
metric attributes (they would explode cardinality). `wallaby.destination` appears only as a **span**
attribute, where sampling keeps the cost bounded.