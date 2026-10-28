---
title: "CDC vs dual writes, Debezium and Sequin for .NET"
description: "Why change data capture beats publish-on-write for keeping search indexes and read models in sync, and how Wallaby compares to Debezium and Sequin."
---

# Why Wallaby?

The typical way to keep a search index or read model in sync is to publish a message whenever the
application writes to the database, usually driven by a change detector. This is a reasonable solution, but it introduces a number of new problems for engineers to contend with. 
Change data capture works by reading committed changes from the database's write-ahead log instead, which sidesteps the structural problems of the messaging approach:

- **Nothing gets missed**: Every insert, update, and delete is captured, including writes that never go
  through your publishing code: bulk updates, migrations, admin scripts, other services sharing the
  database.
- **No dual writes**: A message published alongside a database write can be lost (commit succeeds,
  publish fails) or orphaned (publish succeeds, commit rolls back). The outbox pattern fixes this at the
  cost of an extra table, a dispatcher, and ceremony on every write. With CDC the commit *is* the event & source of truth, there is nothing to keep in sync.
- **No domain duplication & PR noise**: You don't need to author event contracts or add overhead by trying to keep publish calls in step with your
  schema. Wallaby materializes changes into the entities you already have, and a [transform](/mappings#transforms) shapes the output document.
- **Ordering and delivery guarantees**: Changes arrive in commit order with
  [at-least-once delivery](/how-it-works), and a new sink is seeded through the same pipeline
  via [backfill](/backfill). There is no separate replay mechanism to build.

The only thing to note is that CDC captures *state changes*, not intent. If consumers need to know **why** something changed, such as a
domain event carrying business meaning, a message bus is still the right tool. Wallaby covers the cases
where the goal is "the data changed, keep the downstream copy in sync".

## Compared to Debezium and Sequin

[Debezium](https://debezium.io) and [Sequin](https://sequinstream.com) are both excellent CDC tools, and
all three read committed changes from Postgres logical replication. The difference is where the engine
lives and what your code works with.

Debezium and Sequin are **services you run next to your database**. They emit a generic row-change
event, and you shape it with their transform mechanism or in a consumer downstream. Wallaby is a
**.NET library**: it runs inside a .NET host, materializes each change into your EF Core entity, Marten
document or POCO, and a C# transform shapes the document before it reaches the sink.

| | Wallaby | Debezium | Sequin |
| --- | --- | --- | --- |
| **Runs as** | NuGet package in a .NET host | Kafka Connect connector, Debezium Server, or embedded engine (JVM) | Standalone Docker container (Elixir) |
| **Sources** | Postgres 15+ | Postgres, MySQL, SQL Server, Oracle, MongoDB and more | Postgres 14+ |
| **What you transform** | Your EF Core entities, Marten documents or POCOs | Debezium change event (`before`/`after` envelope) | Row-change message |
| **Transform language** | C#, with your `DbContext` or `IQuerySession` for enrichment | Java SMTs, or a downstream consumer | Elixir functions |
| **Configuration** | Code, derived from your model; publication and slot self-configured | Connector properties | Web console, YAML, CLI or API |
| **Backfill** | Versioned: a mapping's version bump triggers a reindex | Initial and incremental snapshots | Initial and on-demand backfills |
| **Delivery** | At-least-once, commit order per document | At-least-once by default | Ordered, with idempotency keys |

### Choose Wallaby when

- **Your app is already .NET on Postgres.** Mappings are derived from the model you already built, so a
  schema change that breaks a mapping is an instead compile error.
- **The destination needs a shaped document, not a row.** Transforms are plain C#: join related data
  through your `DbContext`, flatten it, and send it on. There is no event format to learn and no separate
  consumer to write.
- **You don't want another service to operate.** No Kafka, no JVM and no extra container. Wallaby runs in
  a .NET host you deploy like any other, with [leader election](/configuration) across nodes,
  [OpenTelemetry](/operations/observability) and [health checks](/operations/health-checks) built in.
- **Your output shape changes over time.** Bump a mapping's version and Wallaby
  [backfills](/backfill) the destination while live changes keep flowing.

### Choose Debezium or Sequin when

- **You capture from more than Postgres.** Debezium covers most major databases; Wallaby is Postgres only.
- **Your services aren't .NET**, or you want CDC owned by a platform team rather than an application.
- **You need a destination Wallaby doesn't ship**, such as SQS, NATS or Pub/Sub, and don't want to write
  a [custom sink](/sinks/custom).

If another tool already reads from the same database, Wallaby can still
[provision and maintain its publication and slot](/external-slots).

## Prior Art

Many concepts this package uses are based on existing patterns within the wider ecosystem:
- The transformation, enrichment and watermarking pipeline is inspired by [Sequin](https://sequinstream.com/).
- Some general concepts are based on Netflix's [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b).