-- Template deduplication (biggest space savings)
CREATE TABLE templates (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  template TEXT NOT NULL
) STRICT;

-- Log messages
CREATE TABLE messages (
  id INTEGER PRIMARY KEY,
  run_id TEXT NOT NULL,
  execution_id INTEGER NOT NULL,
  workspace_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  level INTEGER NOT NULL,
  template_id INTEGER REFERENCES templates(id),
  values_json TEXT,
  created_at INTEGER NOT NULL
) STRICT;

-- Indexes for common query patterns
CREATE INDEX idx_messages_run_workspace_ts ON messages(run_id, workspace_id, timestamp);
CREATE INDEX idx_messages_execution_ts ON messages(execution_id, timestamp);
CREATE INDEX idx_messages_created ON messages(created_at);
