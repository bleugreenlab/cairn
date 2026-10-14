-- Track user-initiated activation of a browser pane so the bare/default
-- `cairn:~/browser` resource can focus-follow the user's most-recently-active
-- open browser instead of spawning a fresh `default` tab. Nullable: a non-NULL
-- value means a human activated the pane and makes it a focus-following
-- candidate; existing rows (and agent-created rows) stay NULL and never
-- focus-follow until the user activates them.
ALTER TABLE job_browsers ADD COLUMN last_active_at INTEGER;
CREATE INDEX idx_job_browsers_active
  ON job_browsers(job_id, status, last_active_at);
