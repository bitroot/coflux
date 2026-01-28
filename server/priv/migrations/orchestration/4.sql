ALTER TABLE worker_deactivations ADD COLUMN exit_code INTEGER;
ALTER TABLE worker_deactivations ADD COLUMN oom_killed INTEGER;
ALTER TABLE worker_deactivations ADD COLUMN error TEXT;
