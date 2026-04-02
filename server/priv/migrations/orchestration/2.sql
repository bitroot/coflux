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
