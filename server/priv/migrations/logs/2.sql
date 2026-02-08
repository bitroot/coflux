-- Change execution_id and workspace_id from INTEGER to TEXT for external IDs.
-- SQLite STRICT mode doesn't allow altering column types, so recreate the table.

CREATE TABLE messages_new (
  id INTEGER PRIMARY KEY,
  run_id TEXT NOT NULL,
  execution_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  level INTEGER NOT NULL,
  template_id INTEGER REFERENCES templates(id),
  values_json TEXT,
  created_at INTEGER NOT NULL
) STRICT;

INSERT INTO messages_new SELECT * FROM messages;
DROP TABLE messages;
ALTER TABLE messages_new RENAME TO messages;

CREATE INDEX idx_messages_run_workspace_ts ON messages(run_id, workspace_id, timestamp);
CREATE INDEX idx_messages_execution_ts ON messages(execution_id, timestamp);
CREATE INDEX idx_messages_created ON messages(created_at);
