-- Metric data points
CREATE TABLE metrics (
  id INTEGER PRIMARY KEY,
  run_id TEXT NOT NULL,
  execution_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value REAL NOT NULL,
  at REAL NOT NULL,
  created_at INTEGER NOT NULL
) STRICT;

-- Primary query: metrics for a run, filtered by workspace and key
CREATE INDEX idx_metrics_run_workspace_key_at ON metrics(run_id, workspace_id, key, at);

-- Execution-scoped queries
CREATE INDEX idx_metrics_execution_key_at ON metrics(execution_id, key, at);

-- For rotation/cleanup
CREATE INDEX idx_metrics_created ON metrics(created_at);
