ALTER TABLE sessions ADD COLUMN concurrency INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN activation_timeout INTEGER;
ALTER TABLE sessions ADD COLUMN reconnection_timeout INTEGER;

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
