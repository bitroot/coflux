-- Drop external_id from steps (replaced by number) and add number column.
-- SQLite doesn't support DROP COLUMN on tables with constraints, so recreate.
CREATE TABLE steps_new (
  id INTEGER PRIMARY KEY,
  number INTEGER,
  run_id INTEGER NOT NULL,
  parent_id INTEGER,
  module TEXT NOT NULL,
  target TEXT NOT NULL,
  type INTEGER NOT NULL,
  priority INTEGER NOT NULL,
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

INSERT INTO steps_new (id, run_id, parent_id, module, target, type, priority, wait_for,
  cache_config_id, cache_key, defer_key, memo_key, retry_limit, retry_delay_min,
  retry_delay_max, recurrent, delay, requires_tag_set_id, created_at)
SELECT id, run_id, parent_id, module, target, type, priority, wait_for,
  cache_config_id, cache_key, defer_key, memo_key, retry_limit, retry_delay_min,
  retry_delay_max, recurrent, delay, requires_tag_set_id, created_at
FROM steps;

DROP TABLE steps;
ALTER TABLE steps_new RENAME TO steps;

-- Workspace external IDs
ALTER TABLE workspaces ADD COLUMN external_id TEXT;
CREATE UNIQUE INDEX idx_workspaces_external_id ON workspaces(external_id);

-- Pool external IDs
ALTER TABLE pools ADD COLUMN external_id TEXT;
CREATE UNIQUE INDEX idx_pools_external_id ON pools(external_id);

-- Worker external IDs (UUIDs)
ALTER TABLE workers ADD COLUMN external_id TEXT;
CREATE UNIQUE INDEX idx_workers_external_id ON workers(external_id);
