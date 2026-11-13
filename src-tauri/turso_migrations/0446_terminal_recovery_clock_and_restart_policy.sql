-- lineage: shared
-- cairn/7082: a recovering terminal's frozen clock, and whether recovery may run it again.

-- When Cairn lost sight of the terminal's process: the moment it entered
-- `recovering`, lowered to the residency's last heartbeat once an executor
-- advertises it. A terminal's run clock stops here, and settling it as lost
-- writes `exited_at = COALESCE(exited_at, lost_at, now)` so the clock stays
-- frozen at the loss rather than at the end of the recovery window. NULL for
-- every terminal that has never been in recovery.
ALTER TABLE job_terminals ADD COLUMN lost_at INTEGER;

-- Whether recovery starts the terminal's command again after an involuntary
-- stop: `auto` (today's rule) or `never`, for a command that drives external
-- state a replay would spoil. Set at creation and carried to the row a
-- recovery restart re-mints.
ALTER TABLE job_terminals ADD COLUMN restart_policy TEXT NOT NULL DEFAULT 'auto';
