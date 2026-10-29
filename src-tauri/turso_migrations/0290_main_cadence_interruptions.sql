-- lineage: private
-- cairn/5441: a main-cadence lane pass this runner's own restart cancelled.
-- Written as the runner stops, read back and deleted by the next boot, whose
-- re-arm cites it, so a restart reads as a cancelled pass with a successor
-- rather than a death followed by silence. Private: it describes this
-- runner's restart, not shared project state. One row per lane: a later
-- restart's cancel replaces an earlier one no boot has cited yet.
CREATE TABLE main_cadence_interruptions (
    project_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    stopped_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, check_name, platform)
);
