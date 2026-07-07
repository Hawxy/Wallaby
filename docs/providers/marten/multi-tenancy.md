---
description: "Conjoined multi-tenancy with Marten: tenant ids captured in document keys, ScopedByTenant, and tenant-scoped query sessions."
---

# Multi-tenancy (Marten)

Marten's **conjoined** multi-tenancy stores every tenant's documents in one table with a `tenant_id`
column. Wallaby captures that column as part of the document key, so per-tenant scoping needs no
scope-key selector, simply adding `ScopedByTenant()` reads the tenant straight from the change:

```csharp
cdc.UseMarten()
   .UseTenantSessions();                            // lease store.QuerySession(tenantId) per tenant

sink.Map<Order>()
    .ScopedByTenant()
    .ScopedDestination(tenant => $"orders-{tenant}")   // optional index-per-tenant
    .UsingTransform(/* ... */);
```

- **`ScopedByTenant()`** scopes the mapping by the change's tenant.
- **`UseTenantSessions()`** hands each transform invocation a same-tenant `IQuerySession`
  (`store.QuerySession(tenantId)`), so enrichment queries only see that tenant's documents.
- **`ScopedDestination(tenant => ...)`** routes per tenant - including deletes.
  This is optional, without it, the fixed `ToDestination(...)` value (or the sink default) is used.

Backfills flow through the same router, so per-tenant sessions and destinations apply to backfilled
documents too.

A mapping with a scoped destination flags its table for `REPLICA IDENTITY FULL`, see [Managed Replica Identity](/providers/marten/#managed-replica-identity).

## Limitations

Only conjoined (and single) tenancy is supported; separate-database tenancy is not.
