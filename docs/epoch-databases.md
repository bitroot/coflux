# Epoch Databases

## Motivation

Coflux currently uses a single SQLite database per project for orchestration. For a cloud-hosted version (e.g., on Fly with Litestream for backup/restore to object storage), this creates a scaling problem: the database grows indefinitely, making restores increasingly slow. We need the ability to shut down VMs when projects are idle and restore quickly on demand.

## Design

### Epoch-based splitting

Instead of one monolithic database, each project has a series of **epoch databases**. One epoch is "active" (receiving writes); the rest are archived and immutable, stored in object storage.

All data lives in epoch databases - there is no separate "config" or "metadata" database. Shared configuration (workspaces, manifests, pools, tokens, etc.) is duplicated into each epoch as needed. This avoids having a separate database that could itself accumulate and grow.

### Epoch rotation

A new epoch is created when the active epoch exceeds a size threshold. The rotation process:

1. Create a new empty epoch database.
2. The old epoch becomes archived and immutable.
3. Upload the archived epoch to object storage (via Litestream).
4. Update the index file (see below).

There is no special handling for "in-progress" runs during rotation. Runs don't have a strict concept of completion, so they're all treated the same way (see "Duplication" below).

### Duplication on reference

Whenever the active epoch needs to reference data from an old epoch - for any reason - the entire run (and its dependencies) is copied into the active epoch first. This applies to:

- **Re-runs**: A step in an old run is re-run.
- **Cache hits**: A cache lookup matches an execution in an old epoch.
- **Any other cross-epoch reference**.

The duplication is **recursive**: if the copied run itself references executions in other runs (e.g., via cached result successor chains), those runs are copied too. Chains are typically short (1-2 hops).

This ensures the active epoch is always **fully self-contained** - no cross-epoch references exist within its database. Old epochs remain immutable after rotation.

### Index file

A single binary index file is stored in object storage alongside the epoch databases. It contains, for each epoch:

- Epoch ID, creation timestamp, object storage key.
- A **Bloom filter** of run external IDs in that epoch.
- A **Bloom filter** of cache keys in that epoch.

On cold start, the VM downloads this one file and loads the Bloom filters into memory. Only the active epoch database is restored; old epochs are loaded on demand.

**Bloom filter sizing**: Since lookups search backwards (newest epoch first) and stop at the first match, false positives only cost an extra epoch load before continuing. This means filters can be relatively small (5-10% false positive rate is acceptable). For cache lookups, `max_age` provides a natural cutoff - no need to search epochs older than the maximum cache age.

The index file is append-only (new epoch = new entry) and rewritten when old epochs are deleted.

### Archival and retention

Since the active epoch is always self-contained (anything it references has been copied forward), deleting old epochs is trivially safe. Data that is still "alive" (being cache-hit, re-run, etc.) naturally migrates forward via the duplication mechanism. Only truly dormant historical data is lost.

Retention policies can be tiered by customer plan:

- Free: keep last N days of epochs.
- Paid: keep last M months.
- Enterprise: keep everything.

### Cold start sequence

1. Download index file from object storage.
2. Load Bloom filters into memory.
3. Restore active epoch database (Litestream restore).
4. Start serving. Old epoch data is available on demand (fetched from object storage when needed).

## ID scheme

Epoch splitting requires that IDs are globally unique across epochs, since data can be duplicated between them. Integer autoincrement primary keys would collide.

### Execution IDs

Currently, `executions.id` (integer PK) is exposed to workers in the assignment/result protocol. This changes to a derived external ID:

```
<run_external_id>:<step_number>:<attempt>
```

For example, `R1a2b:5:1` is the first attempt of the fifth step in run `R1a2b`.

- `step_number` is a sequential integer per run (replacing the current random external ID for steps).
- `attempt` is already sequential per step.
- The combination is globally unique and deterministic.

Integer PKs are retained for internal joins within an epoch, but are never exposed externally.

### Workspace IDs

Workspaces gain an `external_id` (random string), providing a stable reference that survives name changes. The integer PK is internal to each epoch.

### Pool IDs

Similarly, pools gain an `external_id` for stable cross-epoch identity.

### Other entities

- **Runs**: Already have `external_id` (random string). Safe.
- **Sessions**: Already have `external_id`. Safe.
- **Assets**: Already have `external_id`. Safe.
- **Hash-based entities** (manifests, values, errors, fragments, cache configs, tag sets, parameter sets, instructions): Identified by content hash. When duplicated between epochs, they're matched by hash and get whatever integer PK the new epoch assigns. Safe.
- **Blobs**: Identified by `key TEXT UNIQUE`. Matched by key during duplication. Safe.

## Hash-based deduplication across epochs

Entities identified by content hash (manifests, values, errors, fragments, etc.) are naturally handled during duplication. When copying a run to a new epoch:

1. Insert each hash-based entity with its hash.
2. If the hash already exists in the new epoch, reuse the existing row's ID.
3. Remap all foreign keys from old IDs to new IDs.

This is the same deduplication mechanism that already operates within a single database.

## Relationship to logs

Log messages are already stored in a separate `logs.sqlite` database. The logs DB can adopt the same epoch strategy independently, with run external IDs used to route queries to the correct epoch.
