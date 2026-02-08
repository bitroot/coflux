CREATE TABLE tag_sets (id INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE) STRICT;

CREATE TABLE tag_set_items (
  tag_set_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  FOREIGN KEY (tag_set_id) REFERENCES tag_sets ON DELETE CASCADE
) STRICT;

CREATE TABLE parameter_sets (id INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE) STRICT;

CREATE TABLE parameter_set_items (
  parameter_set_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  name TEXT NOT NULL,
  default_ TEXT,
  annotation TEXT,
  PRIMARY KEY (parameter_set_id, position),
  FOREIGN KEY (parameter_set_id) REFERENCES parameter_sets ON DELETE CASCADE
) STRICT;

CREATE TABLE manifests (id INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE) STRICT;

CREATE TABLE instructions (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  content TEXT NOT NULL
) STRICT;

CREATE TABLE cache_configs (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  params TEXT NOT NULL,
  max_age INTEGER,
  namespace TEXT,
  version TEXT
) STRICT;

CREATE TABLE workflows (
  id INTEGER PRIMARY KEY,
  manifest_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  parameter_set_id INTEGER NOT NULL,
  instruction_id INTEGER,
  wait_for INTEGER NOT NULL,
  cache_config_id INTEGER,
  defer_params TEXT,
  delay INTEGER NOT NULL,
  retry_limit INTEGER NOT NULL,
  retry_delay_min INTEGER NOT NULL,
  retry_delay_max INTEGER NOT NULL,
  recurrent INTEGER NOT NULL DEFAULT 0,
  requires_tag_set_id INTEGER,
  UNIQUE (manifest_id, name),
  FOREIGN KEY (manifest_id) REFERENCES manifests ON DELETE CASCADE,
  FOREIGN KEY (instruction_id) REFERENCES instructions ON DELETE RESTRICT,
  FOREIGN KEY (parameter_set_id) REFERENCES parameter_sets ON DELETE RESTRICT,
  FOREIGN KEY (cache_config_id) REFERENCES cache_configs ON DELETE RESTRICT,
  FOREIGN KEY (requires_tag_set_id) REFERENCES tag_sets ON DELETE RESTRICT
) STRICT;

CREATE TABLE workspaces (
  id INTEGER PRIMARY KEY,
  external_id TEXT
) STRICT;

CREATE UNIQUE INDEX idx_workspaces_external_id ON workspaces(external_id);

CREATE TABLE workspace_manifests (
  workspace_id INTEGER NOT NULL,
  module TEXT NOT NULL,
  manifest_id INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE,
  FOREIGN KEY (manifest_id) REFERENCES manifests ON DELETE CASCADE
) STRICT;

CREATE TABLE workspace_states (
  workspace_id INTEGER NOT NULL,
  state INTEGER NOT NULL, -- 0: active, 1: paused, 2: archived
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE
) STRICT;

CREATE TABLE workspace_names (
  workspace_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE
) STRICT;

CREATE TABLE workspace_bases (
  workspace_id INTEGER NOT NULL,
  base_workspace_id INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE,
  FOREIGN KEY (base_workspace_id) REFERENCES workspaces ON DELETE CASCADE
) STRICT;

CREATE TABLE launchers (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  type INTEGER NOT NULL,
  config TEXT NOT NULL
) STRICT;

CREATE TABLE pool_definitions (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  launcher_id INTEGER,
  provides_tag_set_id INTEGER,
  FOREIGN KEY (launcher_id) REFERENCES launchers ON DELETE RESTRICT,
  FOREIGN KEY (provides_tag_set_id) REFERENCES tag_sets ON DELETE RESTRICT
) STRICT;

CREATE TABLE pool_definition_modules (
  pool_definition_id INTEGER NOT NULL,
  pattern TEXT NOT NULL,
  FOREIGN KEY (pool_definition_id) REFERENCES pool_definitions ON DELETE CASCADE
) STRICT;

CREATE TABLE pools (
  id INTEGER PRIMARY KEY,
  external_id TEXT,
  workspace_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  pool_definition_id INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE,
  FOREIGN KEY (pool_definition_id) REFERENCES pool_definitions ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX idx_pools_external_id ON pools(external_id);

CREATE TABLE workers (
  id INTEGER PRIMARY KEY,
  external_id TEXT,
  pool_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (pool_id) REFERENCES pools ON DELETE CASCADE
) STRICT;

CREATE UNIQUE INDEX idx_workers_external_id ON workers(external_id);

CREATE TABLE worker_launch_results (
  worker_id INTEGER PRIMARY KEY,
  data BLOB,
  error TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (worker_id) REFERENCES workers,
  CHECK (
    data IS NULL
    OR error IS NULL
  )
) STRICT;

CREATE TABLE worker_states (
  worker_id INTEGER NOT NULL,
  state INTEGER NOT NULL, -- 0: active, 1: paused, 2: draining
  -- TODO: reason? (0: user, 1: scaling down?, 2: config update?)
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (worker_id) REFERENCES workers
) STRICT;

CREATE TABLE worker_stops (
  id INTEGER PRIMARY KEY,
  worker_id INTEGER NOT NULL,
  -- TODO: reason? (manual, scaling down, pool removed, ?)
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (worker_id) REFERENCES workers
) STRICT;

CREATE TABLE worker_stop_results (
  worker_stop_id INTEGER PRIMARY KEY,
  error TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (worker_stop_id) REFERENCES worker_stops
) STRICT;

CREATE TABLE worker_deactivations (
  worker_id INTEGER PRIMARY KEY,
  error TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (worker_id) REFERENCES workers
) STRICT;

CREATE TABLE sessions (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  workspace_id INTEGER NOT NULL,
  worker_id INTEGER,
  provides_tag_set_id INTEGER,
  concurrency INTEGER NOT NULL DEFAULT 0,
  activation_timeout INTEGER,
  reconnection_timeout INTEGER,
  secret_hash BLOB,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE,
  FOREIGN KEY (worker_id) REFERENCES workers ON DELETE RESTRICT,
  FOREIGN KEY (provides_tag_set_id) REFERENCES tag_sets ON DELETE RESTRICT
) STRICT;

CREATE TABLE session_activations (
  session_id INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (session_id) REFERENCES sessions ON DELETE CASCADE
) STRICT;

CREATE TABLE session_expirations (
  session_id INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (session_id) REFERENCES sessions ON DELETE CASCADE
) STRICT;

CREATE TABLE runs (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  parent_id INTEGER,
  idempotency_key TEXT UNIQUE,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (parent_id) REFERENCES executions ON DELETE SET NULL
) STRICT;

CREATE TABLE steps (
  id INTEGER PRIMARY KEY,
  number INTEGER,
  run_id INTEGER NOT NULL,
  parent_id INTEGER, -- TODO: remove?
  module TEXT NOT NULL,
  target TEXT NOT NULL,
  type INTEGER NOT NULL,
  priority INTEGER NOT NULL, -- TODO: move to executions?
  wait_for INTEGER NOT NULL,
  cache_config_id INTEGER,
  cache_key BLOB,
  defer_key BLOB,
  memo_key BLOB,
  retry_limit INTEGER NOT NULL,
  retry_delay_min INTEGER NOT NULL,
  retry_delay_max INTEGER NOT NULL,
  recurrent INTEGER NOT NULL DEFAULT 0,
  delay INTEGER NOT NULL DEFAULT 0,
  requires_tag_set_id INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs ON DELETE CASCADE,
  FOREIGN KEY (parent_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (cache_config_id) REFERENCES cache_configs ON DELETE RESTRICT,
  FOREIGN KEY (requires_tag_set_id) REFERENCES tag_sets ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX steps_initial_step ON steps (run_id)
WHERE
  parent_id IS NULL;

CREATE INDEX steps_cache_key ON steps (cache_key);

CREATE TABLE step_arguments (
  step_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  value_id INTEGER NOT NULL,
  PRIMARY KEY (step_id, position),
  FOREIGN KEY (step_id) REFERENCES steps ON DELETE CASCADE,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT
) STRICT;

CREATE TABLE executions (
  id INTEGER PRIMARY KEY,
  step_id INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  workspace_id INTEGER NOT NULL,
  execute_after INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  UNIQUE (step_id, attempt),
  FOREIGN KEY (step_id) REFERENCES steps ON DELETE CASCADE,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE
) STRICT;

CREATE TABLE assets (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  name TEXT,
  hash BLOB NOT NULL UNIQUE
) STRICT;

CREATE TABLE asset_entries (
  entry_id INTEGER PRIMARY KEY,
  asset_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  blob_id INTEGER NOT NULL,
  UNIQUE (asset_id, path),
  FOREIGN KEY (asset_id) REFERENCES assets ON DELETE CASCADE,
  FOREIGN KEY (blob_id) REFERENCES blobs ON DELETE RESTRICT
) STRICT;

CREATE TABLE asset_entry_metadata (
  asset_entry_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (asset_entry_id, key),
  FOREIGN KEY (asset_entry_id) REFERENCES asset_entries ON DELETE CASCADE
) STRICT;

CREATE TABLE execution_assets (
  execution_id INTEGER NOT NULL,
  asset_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, asset_id),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (asset_id) REFERENCES assets ON DELETE CASCADE
) STRICT;

CREATE TABLE groups (
  execution_id INTEGER NOT NULL,
  group_id INTEGER NOT NULL,
  name TEXT,
  PRIMARY KEY (execution_id, group_id),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE
) STRICT;

-- TODO: add 'type' (e.g., 'regular', memoised)
CREATE TABLE children (
  parent_id INTEGER NOT NULL,
  child_id INTEGER NOT NULL,
  group_id INTEGER,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (parent_id, child_id),
  FOREIGN KEY (parent_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (child_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (parent_id, group_id) REFERENCES groups ON DELETE SET NULL
) STRICT;

CREATE TABLE assignments (
  execution_id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (session_id) REFERENCES sessions ON DELETE CASCADE
) STRICT;

CREATE TABLE result_dependencies (
  execution_id INTEGER NOT NULL,
  dependency_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, dependency_id),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (dependency_id) REFERENCES executions ON DELETE RESTRICT
) STRICT;

CREATE TABLE asset_dependencies (
  execution_id INTEGER NOT NULL,
  asset_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, asset_id),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (asset_id) REFERENCES assets ON DELETE RESTRICT
) STRICT;

CREATE TABLE heartbeats (
  id INTEGER PRIMARY KEY,
  execution_id INTEGER NOT NULL,
  status INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE
) STRICT;

CREATE TABLE blobs (
  id INTEGER PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  size INTEGER NOT NULL
) STRICT;

CREATE TABLE fragment_formats (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT;

CREATE TABLE fragments (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  format_id INTEGER NOT NULL,
  blob_id INTEGER NOT NULL,
  FOREIGN KEY (format_id) REFERENCES fragment_formats ON DELETE RESTRICT,
  FOREIGN KEY (blob_id) REFERENCES blobs ON DELETE RESTRICT
) STRICT;

CREATE TABLE fragment_metadata (
  fragment_id INTEGER NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (fragment_id, key),
  FOREIGN KEY (fragment_id) REFERENCES fragments ON DELETE CASCADE
) STRICT;

CREATE TABLE values_ (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  content BLOB,
  blob_id INTEGER,
  FOREIGN KEY (blob_id) REFERENCES blobs ON DELETE RESTRICT,
  CHECK ((content IS NULL) != (blob_id IS NULL))
) STRICT;

CREATE TABLE value_references (
  value_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  fragment_id INTEGER,
  execution_id INTEGER,
  asset_id INTEGER,
  PRIMARY KEY (value_id, position),
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE CASCADE,
  FOREIGN KEY (fragment_id) REFERENCES fragments ON DELETE RESTRICT,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE RESTRICT,
  FOREIGN KEY (asset_id) REFERENCES assets ON DELETE RESTRICT,
  CHECK (
    (fragment_id IS NOT NULL) + (execution_id IS NOT NULL) + (asset_id IS NOT NULL) = 1
  )
) STRICT;

CREATE TABLE errors (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  type TEXT NOT NULL,
  message TEXT NOT NULL
) STRICT;

CREATE TABLE error_frames (
  error_id INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  file TEXT NOT NULL,
  line INTEGER NOT NULL,
  name TEXT,
  code TEXT,
  PRIMARY KEY (error_id, depth),
  FOREIGN KEY (error_id) REFERENCES errors ON DELETE CASCADE
) STRICT;

CREATE TABLE results (
  execution_id INTEGER PRIMARY KEY,
  type INTEGER NOT NULL,
  error_id INTEGER,
  value_id INTEGER,
  successor_id INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (error_id) REFERENCES errors ON DELETE RESTRICT,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT,
  FOREIGN KEY (successor_id) REFERENCES executions ON DELETE RESTRICT,
  CHECK (
    CASE type
      WHEN 0 THEN error_id
      AND NOT value_id
      WHEN 1 THEN value_id
      AND NOT (
        successor_id
        OR error_id
      )
      WHEN 2 THEN NOT (
        error_id
        OR value_id
      )
      WHEN 3 THEN NOT (
        error_id
        OR successor_id
        OR value_id
      )
      WHEN 4 THEN successor_id
      AND NOT (
        error_id
        OR value_id
      )
      WHEN 5 THEN successor_id
      AND NOT (
        error_id
        OR value_id
      )
      WHEN 6 THEN successor_id
      AND NOT (
        error_id
        OR value_id
      )
      WHEN 7 THEN successor_id
      AND NOT (
        error_id
        OR value_id
      )
      ELSE FALSE
    END
  )
) STRICT;

CREATE TABLE message_templates (
  id INTEGER PRIMARY KEY,
  template TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE messages (
  id INTEGER PRIMARY KEY,
  execution_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  level INTEGER NOT NULL,
  template_id INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (template_id) REFERENCES message_templates ON DELETE RESTRICT
) STRICT;

CREATE TABLE message_labels (
  id INTEGER PRIMARY KEY,
  label TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE message_values (
  message_id INTEGER NOT NULL,
  label_id INTEGER NOT NULL,
  value_id INTEGER NOT NULL,
  PRIMARY KEY (message_id, label_id),
  FOREIGN KEY (message_id) REFERENCES messages ON DELETE CASCADE,
  FOREIGN KEY (label_id) REFERENCES message_labels ON DELETE RESTRICT,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT
) STRICT;

-- Principal and token tracking

CREATE TABLE tokens (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  token_hash TEXT NOT NULL UNIQUE,
  name TEXT,
  workspaces TEXT,  -- JSON array of workspace patterns, NULL for all workspaces
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER,
  revoked_at INTEGER
) STRICT;

CREATE TABLE principals (
  id INTEGER PRIMARY KEY,
  user_external_id TEXT UNIQUE,  -- For users (from JWT sub claim), NULL for tokens
  token_id INTEGER UNIQUE REFERENCES tokens ON DELETE CASCADE,
  CHECK ((user_external_id IS NOT NULL AND token_id IS NULL) OR (user_external_id IS NULL AND token_id IS NOT NULL))
) STRICT;
