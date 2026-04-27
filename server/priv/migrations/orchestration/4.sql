-- Split the old `results` table into two:
--
--   * `results`      — pure payload: value_id XOR error_id (+ retryable flag
--                       for errors). Written by the worker when the task body
--                       produces a value/error.
--   * `completions`  — terminal-state marker for the execution: kind (with
--                       a broader vocabulary than results' old `type`),
--                       optional successor (retry / suspended / recurred /
--                       deferred / cached / spawned), and created_by (which
--                       moved from results).
--
-- The lifecycle primary is the completion: an execution is "running" iff no
-- completions row exists. A value result is available for downstream
-- resolution as soon as it's written (before completion, so consumers don't
-- block on stream drain); an error result can't resolve until the completion
-- tells us whether there's a retry successor.

-- The new results table. Payload only. `value_id` and `error_id` are
-- mutually exclusive, enforced by CHECK. `retryable` is the `when`-callback
-- result from the worker and only meaningful for errors.
CREATE TABLE results_new (
  execution_id INTEGER PRIMARY KEY,
  value_id INTEGER,
  error_id INTEGER,
  retryable INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT,
  FOREIGN KEY (error_id) REFERENCES errors ON DELETE RESTRICT,
  CHECK ((value_id IS NULL) != (error_id IS NULL))
) STRICT;

-- The completions table. Terminal state for the execution. `kind` values:
--   0 = succeeded       — value result recorded, process ended cleanly
--   1 = errored         — error result recorded, process ended cleanly
--   2 = abandoned       — session expired before notify_terminated
--   3 = crashed         — notify_terminated without prior result
--   4 = timeout         — execution hit its timeout
--   5 = cancelled       — user cancelled (may or may not have a result row)
--   6 = suspended       — body called suspend; successor resumes later
--   7 = recurred        — recurrent execution scheduled its next run
--   8 = deferred        — execution deferred to another (memoisation / defer)
--   9 = cached          — execution resolved to an existing cache hit
--  10 = spawned         — execution spawned a continuation
--  11 = stream_errored  — value result recorded but a stream errored mid-flight;
--                         counted as a failure for retry / cache eligibility
--  12 = stream_timeout  — value result recorded but a stream timed out;
--                         logically a success but ineligible for cache.
--                         Distinct from (execution-level) `timeout` = 4.
--
-- `successor_id` points at an execution in the same epoch; used for retry
-- chains and in-flight handoffs. `successor_ref_id` points at an
-- `execution_refs` row and is used post-epoch-rotation, when the target
-- integer id is no longer resolvable in the active DB. At most one is set.
CREATE TABLE completions (
  execution_id INTEGER PRIMARY KEY,
  kind INTEGER NOT NULL,
  successor_id INTEGER,
  successor_ref_id INTEGER,
  created_at INTEGER NOT NULL,
  created_by INTEGER REFERENCES principals ON DELETE SET NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (successor_id) REFERENCES executions ON DELETE RESTRICT,
  FOREIGN KEY (successor_ref_id) REFERENCES execution_refs ON DELETE RESTRICT
) STRICT;

CREATE INDEX idx_completions_successor_id ON completions(successor_id);
CREATE INDEX idx_completions_successor_ref_id ON completions(successor_ref_id);

-- Migrate completions first (one row per existing results row). Map the old
-- `type` enum onto the new `kind` enum. created_by moves from results to
-- completions.
INSERT INTO completions (execution_id, kind, successor_id, successor_ref_id, created_at, created_by)
  SELECT
    execution_id,
    CASE type
      WHEN 0 THEN 1   -- errored
      WHEN 1 THEN 0   -- succeeded
      WHEN 2 THEN 2   -- abandoned
      WHEN 3 THEN 5   -- cancelled
      WHEN 4 THEN 8   -- deferred
      WHEN 5 THEN 9   -- cached
      WHEN 6 THEN 6   -- suspended
      WHEN 7 THEN 10  -- spawned
      WHEN 8 THEN 4   -- timeout
      WHEN 9 THEN 7   -- recurred
    END,
    successor_id,
    successor_ref_id,
    created_at,
    created_by
  FROM results;

-- Migrate payloads into new results. Only rows that actually carry a value
-- or error get copied — in-flight deferred/cached/spawned (types 4/5/7
-- without a value_id) don't get a results row. `retryable` is cleared for
-- non-error rows to keep the new CHECK/intent tight.
INSERT INTO results_new (execution_id, value_id, error_id, retryable, created_at)
  SELECT
    execution_id,
    value_id,
    error_id,
    CASE WHEN error_id IS NOT NULL THEN retryable ELSE NULL END,
    created_at
  FROM results
  WHERE error_id IS NOT NULL OR value_id IS NOT NULL;

-- Replace the old results table.
DROP INDEX idx_results_successor_id;
DROP INDEX idx_results_successor_ref_id;
DROP TABLE results;
ALTER TABLE results_new RENAME TO results;

-- Stream config attached to a workflow definition (from the manifest)
-- and to each step (copied at submit time, optionally overridden per
-- call via cf.Target.with_streams). NULL means "unset" — the adapter
-- falls back to the decorator-level default.
--
-- streams_buffer: backpressure budget in number of items (0 = strict
-- lockstep, N = allow N items ahead, NULL = unbounded).
-- streams_timeout_ms: idle-timeout budget in milliseconds; NULL means
-- no timeout.
ALTER TABLE workflows ADD COLUMN streams_buffer INTEGER;
ALTER TABLE workflows ADD COLUMN streams_timeout_ms INTEGER;
ALTER TABLE steps ADD COLUMN streams_buffer INTEGER;
ALTER TABLE steps ADD COLUMN streams_timeout_ms INTEGER;

-- Streams — ordered, append-only sequences of values produced by an
-- execution. Each stream is identified by (execution_id, index), where
-- `index` is assigned monotonically by the worker when serialising the
-- execution's return value. The worker manages allocation locally, so
-- no server round-trip is needed to mint an id. The column is quoted
-- with backticks throughout because INDEX is a SQLite keyword.
--
-- Invariants:
--   • A stream is owned by exactly one execution (its producer).
--   • stream_items are append-only with monotonic sequence starting at 0.
--   • stream_closures are terminal — no items may be appended after closure.
--   • On execution completion / cancellation / crash, every owned stream
--     that lacks a closure receives one (clean, cancelled, or crashed).
--   • Re-running a producer execution creates fresh streams (new attempt ⇒
--     new execution_id ⇒ new rows). Consumer references are concrete to
--     the original streams.
--   • Consumer cursors are kept in-memory only; re-run consumers subscribe
--     fresh from sequence 0.

CREATE TABLE streams (
  execution_id INTEGER NOT NULL,
  `index` INTEGER NOT NULL,
  -- Producer-side backpressure budget. NULL opts out of flow control
  -- (producer emits freely). Integer N means the producer may run up
  -- to N items ahead of the fastest consumer; N=0 is strict lockstep.
  -- Persisted so the server can reconstruct per-stream flow-control
  -- state on restart and so Studio can display the configuration.
  buffer INTEGER,
  -- Idle-timeout budget in milliseconds. NULL disables the timeout.
  -- Enforced at the worker (CLI) level; persisted here only so Studio
  -- can display the configured value.
  timeout_ms INTEGER,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, `index`),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE
) STRICT;

CREATE TABLE stream_items (
  execution_id INTEGER NOT NULL,
  `index` INTEGER NOT NULL,
  sequence INTEGER NOT NULL,
  value_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, `index`, sequence),
  FOREIGN KEY (execution_id, `index`) REFERENCES streams (execution_id, `index`) ON DELETE CASCADE,
  FOREIGN KEY (value_id) REFERENCES values_ ON DELETE RESTRICT
) STRICT;

-- Closure of a stream. `reason` records *why* it closed:
--   0 = complete   — producer finished normally (no error)
--   1 = errored    — producer raised an error (stored in errors via error_id)
--   2 = lifecycle  — closed implicitly because the producer execution ended
--                    (cancel/crash/abandon/error). The specific error is
--                    derived on read by looking up the execution's completion,
--                    so we don't duplicate that state here.
--   3 = timeout    — closed by the worker because the configured idle
--                    timeout elapsed without a new item being appended.
CREATE TABLE stream_closures (
  execution_id INTEGER NOT NULL,
  `index` INTEGER NOT NULL,
  reason INTEGER NOT NULL,
  error_id INTEGER,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, `index`),
  FOREIGN KEY (execution_id, `index`) REFERENCES streams (execution_id, `index`) ON DELETE CASCADE,
  FOREIGN KEY (error_id) REFERENCES errors ON DELETE RESTRICT,
  CHECK ((reason = 1) = (error_id IS NOT NULL))
) STRICT;

-- Track stream subscriptions as a lineage edge between executions, mirroring
-- result_dependencies / asset_dependencies. A row is written when a consumer
-- subscribes to a producer's stream (regardless of whether items are read),
-- so data lineage is preserved even for subscriptions that yield no values.
--
-- The producer side is referenced via `execution_refs` (not the live
-- `executions` row) so the edge survives epoch rotation, and by `stream_index`
-- so we can distinguish between multiple streams produced by the same
-- execution.
CREATE TABLE stream_dependencies (
  execution_id INTEGER NOT NULL,
  stream_ref_id INTEGER NOT NULL,
  stream_index INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (execution_id, stream_ref_id, stream_index),
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE,
  FOREIGN KEY (stream_ref_id) REFERENCES execution_refs ON DELETE RESTRICT
) STRICT;
