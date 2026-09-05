-- Checkpoints — durable, named state scoped to a step within a workspace.
--
-- Motivation: an execution that suspends, retries or recurs re-runs from the
-- beginning, so anything it needs to carry forward (a cursor, a page token,
-- an accumulator) has nowhere to live. Memoised child tasks cover values that
-- are naturally task results; checkpoints cover the rest.
--
-- Scope is (step, workspace). Reads walk the workspace chain nearest-first
-- and take the latest attempt that touched checkpoints in the nearest
-- workspace that has any. A re-run in a descendant workspace therefore reads
-- the base's state but writes only to its own overlay — inheritance is
-- read-only and one-directional.
--
-- Each execution's row-set is a complete snapshot, not a delta: on an
-- execution's first checkpoint write the server materialises the previous
-- effective set and then applies the delta, in one transaction. The worker
-- only ever sends what changed, so unchanged values are carried forward as
-- values_ FK copies and are never re-serialised or re-uploaded.
--
-- DELIBERATE EXCEPTION TO THE APPEND-ONLY CONVENTION: rows are updated in
-- place within an execution. Nothing can observe an intra-execution
-- overwrite — the execution reads its own writes from memory, and the
-- worker-side throttle discards intermediate values before they reach the
-- server. History across attempts is preserved by the (execution_id, name)
-- key, and is compacted to the effective snapshot at epoch rotation.
--
-- Three states per name:
--   * row with value_id      — set to a value (possibly the null value)
--   * row with NULL value_id — explicitly reset at this attempt (tombstone)
--   * no row                 — never existed
--
-- reset() ALWAYS writes a tombstone, even for a name that was never set.
-- That is what keeps an execution's row-set non-empty whenever it touched
-- checkpoints at all, so "reset everything" can never be mistaken for "wrote
-- nothing" and resurrect the pre-reset state from an earlier attempt.

CREATE TABLE checkpoints (
  execution_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  value_id INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, name),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT
) STRICT;

-- Resolving "the latest attempt of this step in this workspace" is the read
-- path for every execution start. executions has UNIQUE (step_id, attempt)
-- and a separate idx_executions_workspace_id, neither of which can satisfy
-- that in one seek.
CREATE INDEX idx_executions_step_workspace ON executions(step_id, workspace_id, attempt);
