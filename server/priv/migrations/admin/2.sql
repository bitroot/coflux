-- Secrets: values a pool needs that must never appear in its
-- configuration. Encrypted with a key derived from COFLUX_SECRET and bound
-- to their row (project, workspaces, name, version), so a ciphertext moved
-- elsewhere won't decrypt. One current value per pattern and name: setting
-- it again replaces the value and bumps the version, deleting it removes
-- it, and nothing older is kept.
--
-- `workspaces` is one workspace pattern - 'development', 'development/*'
-- or '*'. A secret set for several is stored once per pattern, so each is
-- versioned and deleted on its own. (Contrast `tokens.workspaces`, which
-- is a JSON array: a token holds its patterns together.)
CREATE TABLE secrets (
  id INTEGER PRIMARY KEY,
  workspaces TEXT NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  key_id TEXT NOT NULL,
  nonce BLOB NOT NULL,
  ciphertext BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  updated_by_type TEXT,  -- 'user' or 'token'
  updated_by_external_id TEXT,
  UNIQUE (workspaces, name),
  CHECK ((updated_by_type IS NULL) = (updated_by_external_id IS NULL))
) STRICT;
