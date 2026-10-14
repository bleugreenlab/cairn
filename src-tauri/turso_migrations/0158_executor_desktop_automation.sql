CREATE TABLE executor_desktop_automation (
    executor_id TEXT PRIMARY KEY NOT NULL,
    probed_at INTEGER NOT NULL,
    health_json TEXT,
    verbs_json TEXT NOT NULL,
    probe_error TEXT
);