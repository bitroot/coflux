-- Input schemas (content-addressed, deduplicated)
CREATE TABLE input_schemas (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  schema TEXT NOT NULL
) STRICT;

-- Input prompts (content-addressed, deduplicated)
-- Hash covers template + placeholder value references
CREATE TABLE input_prompts (
  id INTEGER PRIMARY KEY,
  hash BLOB NOT NULL UNIQUE,
  template TEXT NOT NULL
) STRICT;

CREATE TABLE input_prompt_placeholders (
  prompt_id INTEGER NOT NULL,
  placeholder TEXT NOT NULL,
  value_id INTEGER NOT NULL,
  PRIMARY KEY (prompt_id, placeholder),
  FOREIGN KEY (prompt_id) REFERENCES input_prompts ON DELETE CASCADE,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT
) STRICT;

-- Inputs: requested by an execution, optionally keyed per run
CREATE TABLE inputs (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  execution_id INTEGER NOT NULL,
  workspace_id INTEGER NOT NULL,
  key TEXT,
  prompt_id INTEGER NOT NULL,
  schema_id INTEGER,
  title TEXT,
  actions TEXT,
  initial TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (workspace_id) REFERENCES workspaces ON DELETE CASCADE,
  FOREIGN KEY (prompt_id) REFERENCES input_prompts ON DELETE RESTRICT,
  FOREIGN KEY (schema_id) REFERENCES input_schemas ON DELETE RESTRICT
) STRICT;

CREATE INDEX idx_inputs_key ON inputs(key) WHERE key IS NOT NULL;
CREATE INDEX idx_inputs_execution_id ON inputs(execution_id);

-- Input responses: type 1 = value, type 2 = dismissed
CREATE TABLE input_responses (
  input_id INTEGER PRIMARY KEY,
  type INTEGER NOT NULL CHECK (type IN (1, 2)),
  value TEXT,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (input_id) REFERENCES inputs ON DELETE CASCADE
) STRICT;

-- Recreate value_references to add input_id column and update CHECK constraint
CREATE TABLE value_references_new (
  value_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  fragment_id INTEGER,
  execution_ref_id INTEGER,
  asset_id INTEGER,
  input_id INTEGER,
  PRIMARY KEY (value_id, position),
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE CASCADE,
  FOREIGN KEY (fragment_id) REFERENCES fragments ON DELETE RESTRICT,
  FOREIGN KEY (execution_ref_id) REFERENCES execution_refs ON DELETE RESTRICT,
  FOREIGN KEY (asset_id) REFERENCES assets ON DELETE RESTRICT,
  FOREIGN KEY (input_id) REFERENCES inputs ON DELETE RESTRICT,
  CHECK (
    (fragment_id IS NOT NULL) + (execution_ref_id IS NOT NULL) + (asset_id IS NOT NULL) + (input_id IS NOT NULL) = 1
  )
) STRICT;

INSERT INTO value_references_new (value_id, position, fragment_id, execution_ref_id, asset_id)
  SELECT value_id, position, fragment_id, execution_ref_id, asset_id FROM value_references;

DROP TABLE value_references;
ALTER TABLE value_references_new RENAME TO value_references;

-- Input dependencies: which executions depend on which inputs
CREATE TABLE input_dependencies (
  execution_id INTEGER NOT NULL,
  input_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, input_id),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (input_id) REFERENCES inputs ON DELETE RESTRICT
) STRICT;

