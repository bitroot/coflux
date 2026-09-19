-- The admin store: identity and configuration that outlive any epoch.
-- Unlike the orchestration store it is never rotated, so what is here is
-- here once, and deleting something deletes it.

-- Service tokens. A token's principal lives in the orchestration store
-- (named by `external_id`), where everything it does is attributed.
CREATE TABLE tokens (
  id INTEGER PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  token_hash TEXT NOT NULL UNIQUE,
  name TEXT,
  workspaces TEXT,  -- JSON array of workspace patterns, NULL for all workspaces
  created_by_type TEXT,  -- 'user' or 'token'
  created_by_external_id TEXT,
  created_at INTEGER NOT NULL,
  expires_at INTEGER,
  revoked_at INTEGER,
  CHECK ((created_by_type IS NULL) = (created_by_external_id IS NULL))
) STRICT;
