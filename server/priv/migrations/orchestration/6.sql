-- Concurrency limits — a cap on how many executions sharing a key may run at
-- once, enforced by the scheduler before an execution is assigned to a
-- worker (so a gated execution never occupies a worker slot).
--
-- The key is built like defer_key: a hash of the namespace (defaulting to
-- "module:target") plus the selected argument values. A NULL key means the
-- step declares no limit.
--
-- The limit is stored per step rather than folded into the key, so several
-- targets can share one namespace while each is judged against its own
-- declaration. Admission is `holders(key) < this step's limit`.
--
-- Nothing here records who holds a permit: a holder is an execution with an
-- assignments row and no completions row, so the scheduler's in-memory
-- ledger is derivable from the database and is rebuilt at boot.
ALTER TABLE steps ADD COLUMN concurrency_key BLOB;
ALTER TABLE steps ADD COLUMN concurrency_limit INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_steps_concurrency_key ON steps(concurrency_key) WHERE concurrency_key IS NOT NULL;

-- Manifest-registered workflows: stored inline like defer_params.
-- concurrency_params uses the encode_params_list convention:
-- NULL = no params, '*' = all, '0,2' = indexes.
ALTER TABLE workflows ADD COLUMN concurrency_limit INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflows ADD COLUMN concurrency_params TEXT;
ALTER TABLE workflows ADD COLUMN concurrency_namespace TEXT;
