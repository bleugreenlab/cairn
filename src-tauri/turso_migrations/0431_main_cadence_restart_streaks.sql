-- lineage: private
-- cairn/6969: runner restarts that keep cancelling one main-cadence lane are one
-- incident per suite and platform, counted across boots until the lane's next
-- complete row clears it. `main_cadence_interruptions` stays the per-boot
-- citation queue; this is the durable count beside it.
--
-- An interruption now names the machine its pass ran on and, when it was
-- recorded by a boot rather than by the stop, the restart that cancelled it.
-- A stop cannot know the build it is stopping for, so its rows leave both
-- empty and the boot that reads them stamps its own.
ALTER TABLE main_cadence_interruptions ADD COLUMN executor_id TEXT;
ALTER TABLE main_cadence_interruptions ADD COLUMN build_id TEXT;
ALTER TABLE main_cadence_interruptions ADD COLUMN source_commit TEXT;

-- `cancels_json` is the ordered list of
-- `{stoppedAtMs, cancelledCommit, buildId, sourceCommit, executorId}`;
-- `last_complete_*` is the lane's newest complete cadence row when the streak
-- began, which is what the lane has gone unobserved since. `announced_count` is
-- the count the notify thread was last told of, so a rise is announced once,
-- whenever it is first known, and never again after a restart.
CREATE TABLE main_cadence_restart_streaks (
    project_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    cancels_json TEXT NOT NULL,
    last_complete_commit TEXT,
    last_complete_ran_at_ms INTEGER,
    announced_count INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (project_id, check_name, platform)
);
