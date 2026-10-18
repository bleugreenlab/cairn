-- How each recent real completion of a canonical command identity ended, per
-- machine. Placement prices a machine by its own recent same-signature failures
-- of a command (a stall, a timeout, a signal death), so a platform whose lanes
-- keep dying the same way ranks behind one that answers.
--
-- Separate from `command_duration_profiles` on purpose: a duration profile
-- learns only from work that succeeded, because a lane that died 30 minutes in
-- is not a sample of how long the lane takes. This table is where that death is
-- remembered instead of discarded.
--
-- `outcome` is `completed` (any exit the command itself chose, red or green: a
-- red is a verdict about the code, not the machine), `stalled` (the executor's
-- silence stop), `timed_out`, or `killed` (a signal death, such as a kernel OOM
-- kill, which is not evidence the machine works). `signature` names the failure
-- (`silent_stop`, `timed_out`, `signal:<n>`) and is NULL for `completed`.
--
-- Machine-local, rows older than seven days are pruned on insert, and it holds
-- no commands, environments, credentials, or paths.
CREATE TABLE command_outcome_history (
    identity_version INTEGER NOT NULL,
    command_identity TEXT NOT NULL,
    os TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    signature TEXT,
    duration_ms INTEGER,
    finished_at_unix_ms INTEGER NOT NULL
);

CREATE INDEX idx_command_outcome_history_machine
    ON command_outcome_history (identity_version, command_identity, executor_id, finished_at_unix_ms);

CREATE INDEX idx_command_outcome_history_finished
    ON command_outcome_history (finished_at_unix_ms);
