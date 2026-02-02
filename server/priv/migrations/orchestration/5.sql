-- User tracking: record which user performs actions

-- Users table to store user references
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE
) STRICT;

-- Add created_by to tables that record user-initiated actions:

-- Workspace changes
ALTER TABLE workspace_names ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE workspace_states ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE workspace_bases ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE workspace_manifests ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;

-- Pool and worker changes
ALTER TABLE pools ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE worker_stops ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE worker_states ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;

-- Run and session changes
ALTER TABLE runs ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE sessions ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;

-- Execution and result changes (for reruns and cancellations)
ALTER TABLE executions ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
ALTER TABLE results ADD COLUMN created_by INTEGER REFERENCES users ON DELETE SET NULL;
