---
title: "Postgres CDC without an ORM"
description: "Postgres change data capture with no ORM in .NET: annotated POCOs, naming conventions, keys, Dapper-style transforms, column selection and backfills."
---

# Plain Tables

The `Wallaby.Providers.Tables` package drives capture from tables you describe with plain C# types.
There is no ORM in between: you register a POCO per table, Wallaby derives the table, columns and key
from `System.ComponentModel.DataAnnotations` attributes, fluent overrides or a naming convention, and
each change materializes into an instance of your type. Transforms receive an `NpgsqlDataSource`, so
enrichment queries use Dapper, raw Npgsql, or anything else that takes a connection.

Use it when the application is not on EF Core or Marten, or for the odd table that lives outside your
model. It [combines](/providers/overview#combining-providers) with the other providers on one slot.

## Install

```bash
dotnet add package Wallaby.Providers.Tables
```

## Register

Chain `UseTables(...)` and register every type to capture. Only registered types are handled, so a
class that also belongs to an EF Core model is never claimed twice.

You must also supply a connection string via `UseConnectionString(...)`, or any other [options-pattern mechanism](/configuration#options-pattern) such as configuration binding. Multi-host connection strings are supported, but Wallaby will only connect to your primary node.

```csharp
using System.ComponentModel.DataAnnotations.Schema;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;
using Wallaby.Providers.Tables;

public enum OrderStatus { Pending, Paid, Shipped }

[Table("orders", Schema = "sales")]
public sealed record Order(int Id, string CustomerRef, decimal Total, OrderStatus Status, DateTimeOffset CreatedAt);

builder.Services.AddWallaby(cdc =>
{
    cdc.UseTables(tables =>
       {
           tables.UseSnakeCase();                                   // CustomerRef -> customer_ref
           tables.Add<Order>();
           tables.Add<OrderLine>().HasKey(l => l.OrderId, l => l.LineNo);
       })
       .UseConnectionString(conn)

        // Sink configuration below - example
       .AddMeilisearchSink("meili", m => { /* ... */ })
       .WithMappings(sink => sink
            .Map<Order>()
            .ToDestination("orders")
            .UsingTransform(async (db, changes, ct) =>
            {
                await using var connection = await db.OpenConnectionAsync(ct);
                // Dapper, raw Npgsql, anything that takes a connection.
                var docs = new Dictionary<DocumentKey, WallabyDocument?>();
                foreach (var change in changes)
                    docs[change.Key] = new WallabyDocument { ["customer"] = change.Entity!.CustomerRef };
                return docs;
            }));
});
```

Registration errors (no key, an unsupported property type, an ambiguous constructor) throw from
`UseTables` itself, before the host starts.

## Mapping rules

Each registered type maps public instance properties with a getter to columns. The rules, in order of
precedence:

| Aspect | Fluent | Attribute | Convention |
|---|---|---|---|
| Table | `ToTable("orders", "sales")` | `[Table("orders", Schema = "sales")]` | The type name, in the default schema (`public`, or `DefaultSchema(...)`) |
| Column | `Column(o => o.CustomerRef, "cust")` | `[Column("cust")]` | The property name |
| Key | `HasKey(o => o.A, o => o.B)`, in argument order | `[Key]` members, ordered by `[Column(Order = n)]` then declaration | A property named `Id` or `{Type}Id` |
| Skip | `Ignore(o => o.Scratch)` | `[NotMapped]` | A property with no getter |

`UseSnakeCase()` turns unannotated names into snake_case with the same rules as EFCore.NamingConventions
(`OrderLine` to `order_line`, `HTTPStatus` to `http_status`); attribute and fluent names are always used
verbatim. Names are matched against the
relation exactly as Postgres reports them, so a quoted mixed-case identifier needs the same casing.

### Types

A property maps to a single column when its type is a scalar the pgoutput decoder produces or value
coercion can bridge: the numeric types, `string`, `char`, `bool`, `Guid`, `DateTime`, `DateTimeOffset`,
`DateOnly`, `TimeOnly`, `TimeSpan`, `byte[]`, `IPAddress`, `PhysicalAddress`, `BitArray`, enums (from
text or number), nullable versions of those, and
single-dimension arrays of them. A property of any other type (a nested class, a collection,
`JsonElement`) fails registration with the remedy: mark it `[NotMapped]` or `Ignore(...)` it. JSON
columns can only be captured into a `string` property in this version.

Key properties are further limited to the types a backfill cursor can persist: numbers, strings,
`Guid`, dates and times, and `byte[]`. An enum key is rejected at registration.

### Records and constructors

A type needs a public parameterless constructor, or exactly one public constructor whose parameters
all match public properties by name (case-insensitive). Positional records satisfy the second rule:

```csharp
public sealed record Order(int Id, string CustomerRef, decimal Total);
```

Constructor parameters take the row values, and a parameter for an ignored or `[NotMapped]` property
receives its default; remaining properties with a setter (`init` included) are assigned afterwards. A property with a getter only is still captured into `ChangeEvent.Record` but
never assigned. A column absent from the change (a narrowed selection, or a delete under
`REPLICA IDENTITY DEFAULT`) leaves its member at the default value.

## Transforms

Transforms receive the provider's `NpgsqlDataSource`. Open a pooled connection for each batch and let
it return to the pool when the transform ends. Three `UsingTransform` overloads are available: one
taking a standalone `IWallabyTablesTransform<T>` instance, a container-resolved
`UsingTransform<TEntity, TTransform>()`, or an inline lambda.

The data source is the `NpgsqlDataSource` registered in the container (`AddNpgsqlDataSource`, or your
own singleton) when there is one, else the one Wallaby builds from its own connection string, which
also carries any [password provider](/configuration#authentication-with-short-lived-tokens). Pass a
factory to `UseTables(sp => ..., tables => ...)` to choose explicitly, for example a read replica.

### Class-based transforms

For anything with dependencies, implement `IWallabyTablesTransform<TEntity>` as a class. It is resolved
from the container:

```csharp
public sealed class OrderSearchTransform : IWallabyTablesTransform<Order>
{
    public async Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        NpgsqlDataSource dataSource, IReadOnlyList<ChangeEvent<Order>> changes, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var customerIds = changes.Select(c => c.Entity!.CustomerId).Distinct().ToArray();
        var names = (await connection.QueryAsync<(int Id, string Name)>(               // Dapper
                "SELECT id, name FROM crm.customers WHERE id = ANY(@ids)", new { ids = customerIds }))
            .ToDictionary(c => c.Id, c => c.Name);

        var docs = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
        foreach (var c in changes)
            docs[c.Key] = new WallabyDocument
            {
                ["number"] = c.Entity!.Number,
                ["customer"] = names.GetValueOrDefault(c.Entity!.CustomerId),
            };
        return docs;
    }
}

// register:
sink.Map<Order>()
    .ToDestination("orders")
    .UsingTransform<Order, OrderSearchTransform>();
```

::: tip
The registration itself can also live outside `AddWallaby` as a
[mapping class](/mappings#mapping-classes): `sink.Apply<OrderSearchMapping>()`.
:::

## Declaring consumed columns

By default Wallaby captures every mapped property. A mapping can narrow that with `Consumes(...)` or
`ConsumesAllExcept(...)`; the entity's captured set is the union across its mappings plus the key,
and the table is published with a [column list](/configuration#publication-column-lists) so
unselected columns never leave the server:

```csharp
sink.Map<Order>()
    .Consumes(o => o.CustomerRef, o => o.Total)      // key columns are always kept
    .UsingTransform(...);

sink.Map<Document>()
    .ConsumesAllExcept(d => d.Body)                  // drop a TOAST-prone column no transform reads
    .UsingTransform(...);
```

Unselected properties keep their default value on the materialized entity and are absent from
`ChangeEvent.Record`. A key property cannot be excluded.

## Deletes and the old tuple

A delete's `ChangeEvent.Entity` is built from the old tuple: only the key columns under
`REPLICA IDENTITY DEFAULT`, the whole row under `REPLICA IDENTITY FULL`. `ChangeEvent.Changes` on an
update likewise lists the previous values of the columns the old tuple carries. Mappings that derive
delete-time identity or routing from the entity ([`KeyedBy(...)`](/mappings#document-ids), an
entity-scoped destination) therefore need `REPLICA IDENTITY FULL`; self-config reports the missing
identity at startup.

## Replica identity

Wallaby never alters your tables. Apply the identity in your own migrations where a table needs it:

```sql
ALTER TABLE sales.orders REPLICA IDENTITY FULL;
```

Without it, an update that leaves a large (TOASTed) column untouched omits that value from the change,
and Wallaby [heals the change by re-reading the row](/how-it-works#unavailable-value-self-healing-reselect)
(a warning per healed change; a hard failure when [`ReselectUnavailableValues`](/configuration) is
disabled). `ConsumesAllExcept(...)` on the large column avoids both.

## Backfills

Backfills read rows by key in [keyset order](/backfill) and materialize them through the same path as
live changes, so `ChangeEvent.Metadata.IsBackfill` is the only difference a transform sees. Composite
keys page in the declared key order.

## NativeAOT

`Wallaby.Providers.Tables` is trim- and NativeAOT-compatible (`IsAotCompatible`). `Add<T>()` carries
the annotations the trimmer needs to keep your type's public properties and constructors; no
serializer is involved.

## Limitations (v1)

- **JSON columns are not supported**: a `jsonb` column can only be captured into a `string` property.
- **`DependsOn(...)` is not supported**: a POCO has no navigations to resolve; capture the dependent
  table as its own type instead.
- **`ScopedByTenant()` is not supported**: scope by a property with `ScopedBy(e => e.TenantId)`.
- **No catalog discovery**: every type is registered explicitly; Wallaby does not read `pg_catalog` to
  find tables or keys.
