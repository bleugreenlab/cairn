-- Terminal-exit wake subscriptions.
--
-- Retain exited terminals (mark-exited instead of delete) so a subscription can
-- be created after the process already finished and still fire, and so post-exit
-- reads return the exited banner plus a retained output tail. one_shot marks a
-- subscription consumed (deleted) the first time a matching wake routes to it, so
-- a terminal exit can never wake the same subscriber twice.
ALTER TABLE job_terminals ADD COLUMN output_tail TEXT;
ALTER TABLE wake_subscriptions ADD COLUMN one_shot INTEGER NOT NULL DEFAULT 0;
