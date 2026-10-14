-- Local runner build identity and frozen, per-session rebuild notices. These are
-- operator-install facts and must not replicate with project data.
CREATE TABLE app_boot_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version TEXT NOT NULL,
    build_id TEXT NOT NULL,
    booted_at INTEGER NOT NULL
);

CREATE TABLE build_change_notifications (
    id TEXT PRIMARY KEY,
    recipient TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    from_version TEXT NOT NULL,
    from_build_id TEXT NOT NULL,
    to_version TEXT NOT NULL,
    to_build_id TEXT NOT NULL,
    booted_at INTEGER NOT NULL
);