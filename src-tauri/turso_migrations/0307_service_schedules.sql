-- lineage: private
-- Service schedules (cairn/5611, tenet 1313): a service's manifest may name
-- `schedule: [{every, method, args}]`, and the supervisor calls each method on
-- that cadence as its owner, with no turn. One row per declared entry holds the
-- entry's anchored recurrence and what its last occurrence did, so a runner
-- restart resumes the cadence rather than re-anchoring it. Rows are kept in
-- step with the declarations whenever the supervisor applies an owner's source.
--
-- owner_thread is '' for a project's service and the owning thread's id for a
-- thread's, as in service_log. Times are epoch seconds, like wake_schedules,
-- whose recurrence arithmetic the cadence shares.
CREATE TABLE service_schedules (
    project_id               TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    owner_thread             TEXT NOT NULL DEFAULT '',
    service                  TEXT NOT NULL,
    -- method, interval, and args, hashed: the entry's identity across versions
    entry_key                TEXT NOT NULL,
    method                   TEXT NOT NULL,
    args_json                TEXT NOT NULL DEFAULT 'null',
    every_ms                 INTEGER NOT NULL,
    anchor_at                INTEGER NOT NULL,
    last_fired_occurrence_at INTEGER,
    last_run_at              INTEGER,
    last_status              TEXT CHECK (last_status IN ('ok', 'failed', 'skipped')),
    last_error               TEXT,
    consecutive_failures     INTEGER NOT NULL DEFAULT 0,
    created_at               INTEGER NOT NULL,
    updated_at               INTEGER NOT NULL,
    PRIMARY KEY (project_id, owner_thread, service, entry_key)
);

-- Automations were cron triggers on thread procedures; a scheduled service
-- method replaces both, and their owning threads were sent their conversions.
DROP TABLE thread_automations;
