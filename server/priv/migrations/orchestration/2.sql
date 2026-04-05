CREATE TABLE metrics (
  id INTEGER PRIMARY KEY,
  execution_id INTEGER NOT NULL REFERENCES executions(id),
  key TEXT NOT NULL,
  "group" TEXT,
  scale TEXT,
  units TEXT,
  progress BOOLEAN NOT NULL DEFAULT 0,
  lower REAL,
  upper REAL,
  group_units TEXT,
  group_lower REAL,
  group_upper REAL,
  created_at INTEGER NOT NULL,
  UNIQUE(execution_id, key)
);

-- Add timeout column to workflows (for manifest-stored workflow timeouts)
ALTER TABLE workflows ADD COLUMN timeout INTEGER NOT NULL DEFAULT 0;

-- Add timeout column to steps (for per-step timeout from decorator/submission)
ALTER TABLE steps ADD COLUMN timeout INTEGER NOT NULL DEFAULT 0;

-- Recreate results table to add type 8 (timeout) to CHECK constraint.
-- SQLite doesn't support ALTER TABLE ... ALTER CONSTRAINT, so we recreate.
CREATE TABLE results_new (
  execution_id INTEGER PRIMARY KEY,
  type INTEGER NOT NULL,
  error_id INTEGER,
  value_id INTEGER,
  successor_id INTEGER,
  successor_ref_id INTEGER,
  retryable INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (error_id) REFERENCES errors ON DELETE RESTRICT,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT,
  FOREIGN KEY (successor_id) REFERENCES executions ON DELETE RESTRICT,
  FOREIGN KEY (successor_ref_id) REFERENCES execution_refs ON DELETE RESTRICT,
  CHECK (
    CASE type
      WHEN 0 THEN error_id
      AND NOT value_id
      AND NOT successor_ref_id
      WHEN 1 THEN value_id
      AND NOT (
        successor_id
        OR error_id
        OR successor_ref_id
      )
      WHEN 2 THEN NOT (
        error_id
        OR value_id
        OR successor_ref_id
      )
      WHEN 3 THEN NOT (
        error_id
        OR successor_id
        OR value_id
        OR successor_ref_id
      )
      WHEN 4 THEN NOT error_id
      AND (
        (successor_id AND NOT successor_ref_id AND NOT value_id)
        OR (successor_ref_id AND value_id AND NOT successor_id)
      )
      WHEN 5 THEN NOT error_id
      AND (
        (successor_id AND NOT successor_ref_id AND NOT value_id)
        OR (successor_ref_id AND value_id AND NOT successor_id)
      )
      WHEN 6 THEN successor_id
      AND NOT (
        error_id
        OR value_id
        OR successor_ref_id
      )
      WHEN 7 THEN NOT error_id
      AND (
        (successor_id AND NOT successor_ref_id AND NOT value_id)
        OR (successor_ref_id AND value_id AND NOT successor_id)
      )
      WHEN 8 THEN NOT (
        error_id
        OR value_id
        OR successor_ref_id
      )
      ELSE FALSE
    END
  )
) STRICT;

INSERT INTO results_new SELECT * FROM results;
DROP TABLE results;
ALTER TABLE results_new RENAME TO results;

CREATE INDEX idx_results_successor_id ON results(successor_id);
CREATE INDEX idx_results_successor_ref_id ON results(successor_ref_id);

CREATE TABLE pool_states (
  pool_name TEXT NOT NULL,
  workspace_id INTEGER NOT NULL,
  state INTEGER NOT NULL, -- 0: active, 1: disabled
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE
) STRICT;

CREATE INDEX idx_pool_states_ws_name ON pool_states(workspace_id, pool_name, created_at);

-- Add accepts_tag_set_id to pool_definitions and sessions
ALTER TABLE pool_definitions ADD COLUMN accepts_tag_set_id INTEGER REFERENCES tag_sets ON DELETE RESTRICT;
ALTER TABLE sessions ADD COLUMN accepts_tag_set_id INTEGER REFERENCES tag_sets ON DELETE RESTRICT;

-- Add requires_tag_set_id to runs (inherited from workflow definition)
ALTER TABLE runs ADD COLUMN requires_tag_set_id INTEGER REFERENCES tag_sets ON DELETE RESTRICT;
