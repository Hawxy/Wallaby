---
description: "Why change data capture beats publish-on-write for keeping search indexes and read models in sync, and what Wallaby handles for you."
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

## Prior Art

Many concepts this package uses are based on existing patterns within the wider ecosystem:
- The transformation, enrichment and watermarking pipeline is inspired by [Sequin](https://sequinstream.com/).
- Some general concepts are based on Netflix's [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b).