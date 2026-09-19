-- Concurrency limits — a cap on how many executions sharing a key may run at
-- once, enforced by the scheduler before an execution is assigned to a
-- worker (so a gated execution never occupies a worker slot).
--
-- The key is built like defer_key: a hash of the namespace (defaulting to
-- "module:target") plus the selected argument values. A NULL key means the
-- step declares no limit.
--
-- The limit is stored per step rather than folded into the key, so several
-- targets can share one namespace while each is judged against its own
-- declaration. Admission is `holders(key) < this step's limit`.
--
-- Nothing here records who holds a permit: a holder is an execution with an
-- assignments row and no completions row, so the scheduler's in-memory
-- ledger is derivable from the database and is rebuilt at boot.
ALTER TABLE steps ADD COLUMN concurrency_key BLOB;
ALTER TABLE steps ADD COLUMN concurrency_limit INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_steps_concurrency_key ON steps(concurrency_key) WHERE concurrency_key IS NOT NULL;

-- Manifest-registered workflows: stored inline like defer_params.
-- concurrency_params uses the encode_params_list convention:
-- NULL = no params, '*' = all, '0,2' = indexes.
ALTER TABLE workflows ADD COLUMN concurrency_limit INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflows ADD COLUMN concurrency_params TEXT;
ALTER TABLE workflows ADD COLUMN concurrency_namespace TEXT;

-- Group concurrency: a cap on how many direct children submitted inside one
-- cf.group() may run at once. Registered with the group, then copied onto
-- each child step at schedule time so the scheduler treats it exactly like
-- a task limit (a second permit key on the step).
ALTER TABLE groups ADD COLUMN concurrency INTEGER NOT NULL DEFAULT 0;

-- The key is "<parent run>:<step>:<attempt>/<group id>" — the parent's
-- external id, so it survives epoch rotation without remapping, and reads
-- meaningfully on the queue. Not hashed: there are no arguments to fold in.
-- NULL key = the step is in no limited group. children.group_id still
-- records display membership (including memo-hit links, which never take
-- a permit); this pair records what the step counts against.
ALTER TABLE steps ADD COLUMN group_key TEXT;
ALTER TABLE steps ADD COLUMN group_limit INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_steps_group_key ON steps(group_key) WHERE group_key IS NOT NULL;

-- Catalog — paths holding versioned values.
--
-- A version is one publish at a path: which value, who published it and
-- when. Rows are append-only and never modified. The value
-- is an ordinary `values_` row — an asset, a data structure holding assets,
-- a reference to something external — content-hashed like any other, which
-- is what makes the dedup below exact.
--
-- `id` is the catalog's clock. It is the rowid, allocated as max + 1, and
-- since rows are never deleted it is monotonic; rotation copies rows with
-- their ids, so it stays monotonic across epochs. Each execution is pinned
-- to the clock at the moment it is assigned (`assignments.catalog_sequence`)
-- and reads resolve against versions with `id <= pin`, plus anything its own
-- run published since — so an execution sees one consistent catalog for its
-- whole life.
--
-- `number` is the per-path ordinal humans use (`path@6`). It is allocated
-- once per path across all workspaces, the way attempts are allocated once
-- per step, so a number names exactly one version wherever you are. Within
-- a path, number order and id order agree — both are allocated in the same
-- insert — so waits can be stated in numbers.
--
-- Visibility follows the cache rule, not the checkpoint rule: a read from
-- workspace W sees every version published in W or any of its bases, and
-- the latest is the newest by id regardless of which workspace it came from.
CREATE TABLE catalog_versions (
  id INTEGER PRIMARY KEY,
  path TEXT NOT NULL,
  workspace_id INTEGER NOT NULL,
  number INTEGER NOT NULL,
  value_id INTEGER NOT NULL,
  -- Publishing execution, as a stable ref (rotation-safe). NULL when the
  -- version was published through the API rather than by an execution.
  execution_ref_id INTEGER,
  created_by INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE RESTRICT,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT,
  FOREIGN KEY (execution_ref_id) REFERENCES execution_refs ON DELETE RESTRICT,
  FOREIGN KEY (created_by) REFERENCES principals ON DELETE SET NULL
) STRICT;

CREATE UNIQUE INDEX idx_catalog_versions_path_number ON catalog_versions(path, number);
CREATE INDEX idx_catalog_versions_path_ws ON catalog_versions(path, workspace_id, id);

-- Lineage: which versions an execution resolved — a `current()`, a
-- `version(n)`, the version a `next()`/select handed back, or a version's
-- value read by an execution it was passed to. Mirrors asset_dependencies
-- (which records an asset itself when it is restored); this adds "via
-- which path and version".
CREATE TABLE catalog_reads (
  execution_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, version_id),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (version_id) REFERENCES catalog_versions ON DELETE RESTRICT
) STRICT;

-- Gate for an execution that suspended waiting on a path: the successor is
-- held until the path, as seen from its workspace chain, has a version with
-- `number` greater than this. Read only by compute_pending_dependencies,
-- like stream_dependencies.sequence, and it stays on the row afterwards as
-- a record of what the attempt resumed from. One row per path the select
-- was waiting on; together with the successor's other recorded
-- dependencies they form an any-of group.
CREATE TABLE catalog_waits (
  execution_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  number INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, path),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE
) STRICT;

-- The execution's snapshot: a `catalog_versions.id`, fixed at the moment
-- the execution is handed to a worker. Reads resolve against versions with
-- an id at or below it. NULL for assignments made before the catalog
-- existed, which read as "no pin" — the latest of everything.
--
-- How it is chosen (Catalog.resolve_pin): an override on the execution,
-- else the clock if the execution is gated on a catalog wait (a `next()`
-- successor has to see past its predecessor), else the pin of the attempt
-- before it — so a retry, a re-run, or the execution that resumes a
-- suspension sees what the previous attempt saw — unless that attempt
-- recurred, else the run's override, else the clock.
ALTER TABLE assignments ADD COLUMN catalog_sequence INTEGER;

-- Snapshot overrides, as `catalog_versions.id`s. On a run, the snapshot
-- every execution in the run starts from (`submit --catalog path@n`); on an
-- execution, the snapshot that one attempt takes (`runs rerun --catalog`).
-- NULL means "not chosen": resolve by the rule above.
ALTER TABLE runs ADD COLUMN catalog_sequence INTEGER;
ALTER TABLE executions ADD COLUMN catalog_sequence INTEGER;

-- How long a pool keeps an idle worker before stopping it, in seconds.
-- NULL leaves it to the scheduler's default.
ALTER TABLE pool_definitions ADD COLUMN idle_timeout INTEGER;

-- Tokens have moved to the admin store, which isn't rotated, so a
-- principal names its token by external id rather than by a row in this
-- database - the way it already names a user.
--
-- The `tokens` table stays for now: the server copies it across to the
-- admin store when it starts, then drops it (`Coflux.Admin.Tokens`).
CREATE TABLE principals_new (
  id INTEGER PRIMARY KEY,
  user_external_id TEXT UNIQUE,
  token_external_id TEXT UNIQUE,
  CHECK ((user_external_id IS NOT NULL AND token_external_id IS NULL) OR (user_external_id IS NULL AND token_external_id IS NOT NULL))
) STRICT;

INSERT INTO principals_new (id, user_external_id, token_external_id)
SELECT p.id, p.user_external_id, t.external_id
FROM principals AS p
LEFT JOIN tokens AS t ON t.id = p.token_id
WHERE p.user_external_id IS NOT NULL OR t.external_id IS NOT NULL;

DROP TABLE principals;
ALTER TABLE principals_new RENAME TO principals;
