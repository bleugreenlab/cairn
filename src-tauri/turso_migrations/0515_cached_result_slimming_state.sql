-- lineage: private
-- cairn/7748: progress of the one-time rewrite that drops plain passes from
-- `check_result_cache.target_results_json` rows earlier builds stored. The walk
-- goes by rowid a page at a time and records how far it got, so a restart
-- resumes rather than rescans; `completed_at` is set once it reaches the end,
-- after which boots skip it. Runner-transient, like
-- `delegated_packet_backfill_state`.
CREATE TABLE cached_result_slimming_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cursor_rowid INTEGER NOT NULL DEFAULT 0,
    rewritten INTEGER NOT NULL DEFAULT 0,
    completed_at INTEGER
);

INSERT INTO cached_result_slimming_state (id, cursor_rowid, rewritten, completed_at)
VALUES (1, 0, 0, NULL);
