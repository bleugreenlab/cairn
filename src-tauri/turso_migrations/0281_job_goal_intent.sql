-- The operator's durable choice for whether a job's native provider goal may
-- continue on its own after a turn ends. Only the operator changes it (the job
-- pane's goal toggle, or Stop, which turns it off); message delivery never does.
-- A process launched for a job whose intent is 'off' starts with its goal paused.
ALTER TABLE jobs ADD COLUMN goal_intent TEXT NOT NULL DEFAULT 'on'
    CHECK (goal_intent IN ('on', 'off'));
