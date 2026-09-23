---
description: "Provisioning and maintaining publications and replication slots for external pgoutput consumers like Airbyte, Debezium, or Fivetran."
---

# External Slots

Wallaby provisions one publication and one logical replication slot for **its own** capture. If the same
database also feeds a separate CDC consumer (an ELT or replication tool such as Airbyte, Debezium, or
Fivetran running in **pgoutput** mode), that consumer needs its own publication and slot, scoped to its own
tables.

`AddExternalSlot` has Wallaby create and maintain those for you, so the slot is provisioned and kept in
sync as part of your normal deployment instead of being managed separately. **Wallaby never consumes from
an external slot**; it only manages it.

## Install (Optional)

External slots are part of the core package. If you only want Wallaby to provision slots for other tools,
with no sinks of its own, the core package is all you need:

```bash
dotnet add package Wallaby
```

With no sinks registered, Wallaby runs **provision-only**: it creates and reconciles the external slots,
but never opens a slot of its own or streams changes. `ForEntity<T>()` and `ForAllEntities()` resolve
against a storage provider's model, so they still need a provider (EF Core or Marten) registered;
`ForTable(...)` works without one.

## Declare a slot

```csharp
builder.Services.AddWallaby(cdc =>
{
    cdc.UseEntityFrameworkCore<AppDbContext>()        // needed for ForEntity / ForAllEntities
       .UseConnectionString(conn)

       // Provision a publication + slot for an external ELT tool.
       .AddExternalSlot("elt", s => s
            .WithPublication("elt_pub")          // optional; defaults to "elt_pub" (= "{slot}_pub")
            .ForTable("public", "orders")        // by schema-qualified name
            .ForTable("customers")               // schema defaults to "public"
            .ForEntity<Product>());              // or by EF/Marten entity type (resolved to its table)
});
```

Point your external tool at the **slot name** (`elt`) and **publication name** (`elt_pub`) in its
pgoutput configuration.

For a large model, include everything the registered providers map and carve out the exceptions:

```csharp
cdc.AddExternalSlot("elt", s => s
    .ForAllEntities()                    // every table in the EF Core / Marten model
    .Except<AuditLog>()                  // by entity type
    .Except("public", "outbox")          // or by name (schema defaults to "public")
    .ForTable("legacy", "invoices"));    // explicit tables are added on top
```

`ForAllEntities()` is resolved at startup, so an entity added to the model joins the publication on the
next start. It includes:

- **EF Core**: every table the relational model maps, including owned types with their own table, TPT/TPC
  tables, and many-to-many join tables. A join table has no CLR type, so exclude it by name.
  Same-table and JSON-mapped owned types belong to their owner's table, so naming one with
  `ForEntity`/`Except` fails. A TPH hierarchy is a single table, so excluding one member excludes the
  whole table.
- **Marten**: the table of every document type registered up front (`StoreOptions.RegisterDocumentType` or
  `Schema.For<T>()`). The event store is never included.
- **Keyless tables**: tables without a primary key are skipped, because a table with no replica identity inside a
  publication makes the application's own `UPDATE`/`DELETE` statements fail. Only add such a table with
  `ForTable` if you've given it a replica identity.

Names are case-sensitive. Startup fails if an `Except` matches nothing, or names a table the slot also
declares explicitly. Because every entity is included, a new entity whose migration hasn't run yet fails
provisioning until the migration is applied.

External publications always publish **whole tables**. The
[`PublicationColumnLists`](/configuration#publication-column-lists) option only narrows Wallaby's own
publication, never one consumed by a third-party tool.

### API

| Member | Purpose |
| --- | --- |
| `AddExternalSlot(name, configure)` | Declare an external pgoutput slot named `name`. |
| `WithPublication(name)` | Override the publication name (default `"{slot}_pub"`). |
| `ForTable(table)` / `ForTable(schema, table)` | Add a table by name (schema defaults to `public`). |
| `ForEntity<T>()` | Add the table mapped to `T`, resolved against the EF Core or Marten model. |
| `ForAllEntities()` | Add every table the registered providers model (see above). |
| `Except<T>()` / `Except(table)` / `Except(schema, table)` | Remove a table from the `ForAllEntities()` set. |

Each slot needs at least one table, since a pgoutput publication can't be empty. Slot and publication
names must differ from Wallaby's own slot and publication, and from each other.

## Lifecycle & semantics

- **Leader-only and idempotent**: External slots are created in the same self-config step as Wallaby's
  own slot: on the leader, before streaming, every time a node becomes leader. Creating a missing slot or
  re-applying a publication is safe to repeat.
- **Reconciled**: On each startup, Wallaby reconciles the external publication's tables to your declared
  list (`ALTER PUBLICATION ... ADD/DROP TABLE`). Wallaby **owns** that list, so a table added to the
  publication by hand is dropped on the next run. Manage membership through `AddExternalSlot`.
- **Pre-existing slots are adopted (and validated)**: If a slot with the declared name already exists,
  Wallaby reuses it instead of recreating it, and records it in `wallaby.slot_registry`. It **fails fast**
  if that slot isn't a pgoutput *logical* slot (e.g. a physical slot, or one using a different output
  plugin), so a name clash with an unrelated slot shows up as a clear startup error rather than a silent
  mismatch.
- **Never auto-dropped**: Removing an `AddExternalSlot(...)` declaration does **not** drop the slot or
  publication. You have to remove a retired slot yourself:

  ```sql
  SELECT pg_drop_replication_slot('elt');
  DROP PUBLICATION elt_pub;
  ```

  The one exception is a [suspension](/operations/major-version-upgrades). Suspending Wallaby (e.g. for
  an RDS/Aurora major-version upgrade) drops **every** managed slot, external ones included, because the
  platform's precheck rejects any logical slot. The slot is recreated on resume, but the external
  consumer's position is lost and it has to re-sync.

- **Slot headroom**: Wallaby's startup validation counts every slot it will create (its own plus all
  external ones) and fails fast with `max_replication_slots` guidance if there isn't room.
- **Bookkeeping**: Each provisioned slot is recorded in `wallaby.slot_registry`. External slots are marked
  `kind = 'external'`, and Wallaby's own slot is `'primary'`.

::: warning
An external slot is created inactive and **pins WAL** from the moment it exists. If nothing consumes it,
WAL keeps accumulating on the server. Only declare slots an external tool will actually read.
:::

## Scope

Wallaby only *provisions* external slots. It doesn't consume them, monitor their lag, or manage roles and
grants for the external tool. Only **pgoutput** consumers are supported.
