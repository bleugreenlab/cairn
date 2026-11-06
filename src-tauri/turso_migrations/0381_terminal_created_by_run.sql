-- lineage: shared
-- terminal created by run

-- The run that created a terminal, kept through its restarts. A project terminal
-- speaks for no run, so this is the only record of who is answerable for what it
-- does. NULL when no run created it (the app or /api/invoke), and on every row
-- older than this column, which cannot be told apart.
ALTER TABLE job_terminals ADD COLUMN created_by_run_id TEXT;
