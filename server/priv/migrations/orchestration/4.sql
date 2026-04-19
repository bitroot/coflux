-- Add completions table — a pure termination marker. The existing results
-- table continues to hold the disposition (including any successor), written
-- at result-arrival time. A completions row is written separately at
-- notify_terminated time, so its timestamp reflects when the worker's
-- process actually finished shutting down.
--
-- This enables streaming support: a results row can be written with stream
-- handles while the process keeps running, with completions written later
-- when streams have drained.

CREATE TABLE completions (
  execution_id INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (execution_id) REFERENCES executions ON DELETE CASCADE
) STRICT;

-- Every existing results row represents a terminated execution, so each
-- produces a completions row with the same timestamp.
INSERT INTO completions (execution_id, created_at)
  SELECT execution_id, created_at FROM results;

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
--                    derived on read by looking up the execution's result,
--                    so we don't duplicate that state here.
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
