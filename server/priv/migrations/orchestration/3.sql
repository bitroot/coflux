ALTER TABLE sessions ADD COLUMN concurrency INTEGER NOT NULL DEFAULT 0;

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
