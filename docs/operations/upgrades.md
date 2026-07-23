---
description: "How Wallaby versions and migrates its internal state schema across package upgrades."
---

# Upgrading Wallaby

Wallaby keeps its bookkeeping state in a `wallaby` schema co-located in the source database. 
That schema is **versioned** and a `wallaby.schema_version` ledger records each applied step with a
timestamp and the package version that applied it.

## What happens on startup

When a node becomes leader (or the provision-only service starts), Wallaby compares the database's schema
version to the version the binary requires:

- **Equal**: nothing to do; the bootstrap is a single read and runs no DDL.
- **Behind**: the pending migration steps are applied in order and stamped, atomically, under a dedicated advisory lock. 
- **Ahead**: the node **refuses to start** with a `WallabyConfigurationException`. A schema version newer
  than the binary means a newer Wallaby has already migrated this database and running an older build against
  it risks silent misbehavior, so roll the package forward instead of downgrading in place.

## Rolling upgrades

Deployments that run several nodes (leader + standbys, blue/green) can upgrade one node at a time:

- Migration steps are **additive** (new columns always carry a server-side `DEFAULT`, and columns are
  never renamed), so a not-yet-upgraded node keeps reading and writing correctly against an
  already-migrated schema.
- The first upgraded node to win leadership applies the migration, the rest fast-path past it.
- The remote [control client](/operations/external-control) tolerates schema drift in the same way: it
  performs no DDL and treats missing tables as benign.

The one direction that is not supported is **downgrading a deployed binary below the schema version**.
If you must roll back a Wallaby upgrade that shipped a schema change, restore the database 
(or drop the `wallaby` schema and accept a full [re-backfill](/backfill)).
