-- Secrets: values a pool needs that must never appear in its
-- configuration. Encrypted with a key derived from COFLUX_SECRET and bound
-- to their row (project, scope, name, version), so a ciphertext moved
-- elsewhere won't decrypt. One current value per scope and name: setting
-- it again replaces the value and bumps the version, deleting it removes
-- it, and nothing older is kept.
--
-- `scope` is a workspace name, or a prefix of one ('development' covers
-- 'development/joe'); '' is the whole project.
CREATE TABLE secrets (
  id INTEGER PRIMARY KEY,
  scope TEXT NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  key_id TEXT NOT NULL,
  nonce BLOB NOT NULL,
  ciphertext BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  updated_by_type TEXT,  -- 'user' or 'token'
  updated_by_external_id TEXT,
  UNIQUE (scope, name),
  CHECK ((updated_by_type IS NULL) = (updated_by_external_id IS NULL))
) STRICT;
