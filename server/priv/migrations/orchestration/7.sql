-- Group concurrency: a cap on how many direct children submitted inside one
-- cf.group() may run at once. Registered with the group, then copied onto
-- each child step at schedule time so the scheduler treats it exactly like
-- a task limit (a second permit key on the step).
ALTER TABLE groups ADD COLUMN concurrency INTEGER NOT NULL DEFAULT 0;

-- The key is "<parent run>:<step>:<attempt>/<group id>" — the parent's
-- external id, so it survives epoch rotation without remapping, and reads
-- meaningfully on the queue. Not hashed: there are no arguments to fold in.
-- NULL key = the step is in no limited group. children.group_id still
-- records display membership (including memo-hit links, which never take
-- a permit); this pair records what the step counts against.
ALTER TABLE steps ADD COLUMN group_key TEXT;
ALTER TABLE steps ADD COLUMN group_limit INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_steps_group_key ON steps(group_key) WHERE group_key IS NOT NULL;
