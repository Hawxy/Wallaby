# External Slots

Wallaby provisions one publication and one logical replication slot for **its own** capture. If the same
database also feeds a separate CDC consumer - an ELT or replication tool such as Airbyte, Debezium, or
Fivetran running in **pgoutput** mode - that consumer needs its own publication + slot, scoped to its own
tables.

`AddExternalSlot` lets Wallaby create and maintain those for you, so the slot is provisioned (and kept in
sync) as part of your normal deployment instead of requiring seperate management. **Wallaby never consumes from an external slot**,
it just manages it for you.

## Install (Optional)

If you don't have (or need) a provider installed, simply install the core Wallaby package:

```bash
dotnet add package Wallaby
```

## Declare a slot

```csharp
builder.Services.AddWallaby(cdc =>
{
    cdc.
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

### API

| Member | Purpose |
| --- | --- |
| `AddExternalSlot(name, configure)` | Declare an external pgoutput slot named `name`. |
| `WithPublication(name)` | Override the publication name (default `"{slot}_pub"`). |
| `ForTable(table)` / `ForTable(schema, table)` | Add a table by name (schema defaults to `public`). |
| `ForEntity<T>()` | Add the table mapped to `T`, resolved against the EF Core model. |

At least one table is required (a pgoutput publication needs tables). Slot and publication names must be
distinct from Wallaby's own slot/publication and from each other.

## Lifecycle & semantics

- **Leader-only and idempotent**: External slots are created during the same self-config step as Wallaby's
  own slot - on the leader, before streaming, on every leadership acquisition. Creating a missing slot or
  re-applying a publication is safe to repeat.
- **Reconciled**: On each startup Wallaby reconciles the external publication's table set to your declared
  list (`ALTER PUBLICATION ... ADD/DROP TABLE`). Wallaby **owns** the table set: a table added to the
  publication out-of-band is dropped on the next run. Manage membership through `AddExternalSlot`.
- **Pre-existing slots are adopted (and validated)**: If a slot with the declared name already exists,
  Wallaby reuses it rather than recreating it, and records it in `wallaby.slot_registry`. It **fails fast**
  if that slot isn't a pgoutput *logical* slot (e.g. a physical slot or one on a different output plugin),
  so a name clash with an unrelated slot surfaces as a clear startup error rather than a silent mismatch.
- **Never auto-dropped**: Removing an `AddExternalSlot(...)` declaration does **not** drop the slot or
  publication. A retired slot must be removed yourself:

  ```sql
  SELECT pg_drop_replication_slot('elt');
  DROP PUBLICATION elt_pub;
  ```

- **Slot headroom.** Wallaby's startup validation accounts for every slot it will create (its own plus all
  external ones) and fails fast with `max_replication_slots` guidance if there isn't room.
- **Bookkeeping.** Each provisioned slot is recorded in `wallaby.slot_registry`; external slots are marked
  `kind = 'external'` (Wallaby's own slot is `'primary'`).

::: warning
An external slot is created inactive and **pins WAL** the moment it exists. If nothing consumes it, WAL
accumulates on the server. Only declare slots an external tool will actually read.
:::

## Scope

Wallaby only *provisions* external slots, it does not consume them, monitor their lag, or manage roles
and grants for the external tool. Only **pgoutput** consumers are supported.
