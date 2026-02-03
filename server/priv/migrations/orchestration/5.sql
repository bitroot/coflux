-- Principal and token tracking

-- Tokens table for API token authentication
CREATE TABLE tokens (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,  -- tok_xxx
  token_hash TEXT NOT NULL UNIQUE,
  name TEXT,
  workspaces TEXT,  -- JSON array of workspace patterns, NULL for all workspaces
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER,
  revoked_at INTEGER
) STRICT;

-- Principals table to store references to users (JWT) or tokens
CREATE TABLE principals (
  id INTEGER PRIMARY KEY,
  user_external_id TEXT UNIQUE,  -- For users (from JWT sub claim), NULL for tokens
  token_id INTEGER UNIQUE REFERENCES tokens ON DELETE CASCADE,
  CHECK ((user_external_id IS NOT NULL AND token_id IS NULL) OR (user_external_id IS NULL AND token_id IS NOT NULL))
) STRICT;

-- Add created_by to tables that record user-initiated actions:

-- Workspace changes
ALTER TABLE workspace_names ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE workspace_states ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE workspace_bases ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE workspace_manifests ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;

-- Pool and worker changes
ALTER TABLE pools ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE worker_stops ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE worker_states ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;

-- Run and session changes
ALTER TABLE runs ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE sessions ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;

-- Execution and result changes (for reruns and cancellations)
ALTER TABLE executions ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
ALTER TABLE results ADD COLUMN created_by INTEGER REFERENCES principals ON DELETE SET NULL;
