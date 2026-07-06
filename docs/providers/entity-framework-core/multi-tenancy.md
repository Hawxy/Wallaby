# Multi-tenancy (EF Core)

Sometimes the enrichment `DbContext` and/or destination must be derived from the **changed row's own
data**. The canonical case is multi-tenancy: a row carries a `TenantId`, enrichment must run against a
context scoped to that tenant (a tenant connection, or a global query filter), and the result must land in
a per-tenant destination (e.g. an index per tenant).

## API

```csharp
cdc.UseScopedDbContext((scopeKey, services) => new AppDbContext(OptionsForTenant(scopeKey)));

sink.Map<Order>()
    .ScopedBy(o => o.TenantId)                  // derive the scope key from the change
    .UsingTransform<Order, OrderTransform>()           // transform receives the tenant-scoped DbContext
    .ScopedDestination(key => $"orders_{key}"); // per-tenant destination (optional)
```

- **`ScopedBy(o => o.TenantId)`** extracts a scope key from each change's entity. When the key isn't a CLR
  property of the entity, e.g. a shadow `tenant_id` column added by a multi-tenancy library, use the
  `ChangeEvent` overload instead: `ScopedBy(c => c.Record["TenantId"])`.
- **`UseScopedDbContext((key, services) => ...)`** builds the enrichment `DbContext` for a scope key - point
  it at a tenant connection string, or hand the context the tenant so a global query filter applies. `services`
  is a DI scope that disposes together with the returned context, so scoped services are safe to resolve.
- **`ScopedDestination(key => ...)`** computes the destination per scope key. Without it, the scope only
  affects the enrichment context and the fixed `ToDestination(...)` value (or the sink default) is used.

Each is opt-in and independent; with neither, behavior is exactly as normal (one shared context per batch).

## Internals

For each transaction, Wallaby will **sub-group the changes by scope key** and invoke the transform once per scope
with a `DbContext` built for that scope. Backfill operations flow through the same router, so the scope will apply too.

## Deletes and `REPLICA IDENTITY`

Deletes never reach a transform, but a **scoped destination** still needs the scope key to target the
right destination, and a default delete only carries the primary key. So `ScopedDestination` marks its
table to require **`REPLICA IDENTITY FULL`**. With full replica identity the old-row values carry
the scope key on delete. Enrichment-only scoping (`ScopedBy` without `ScopedDestination`) has no such
requirement, since non-delete changes carry the full new row.


