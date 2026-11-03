-- lineage: private
-- The daily storage census (cairn-db storage::census): one row per census of
-- this install's database, the newest 400 kept, pruned by the write that
-- records the next. The typed columns are what readers select on;
-- report_json holds the per-table rows, the history calibration, the archive
-- size and the projection.
CREATE TABLE storage_census (
    id INTEGER PRIMARY KEY,
    taken_at INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    slowest_read_ms INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL,
    file_bytes INTEGER NOT NULL,
    bytes_per_event_row REAL,
    report_json TEXT NOT NULL
);
