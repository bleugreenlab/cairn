-- lineage: private
-- self rebuild outcomes

-- A self-rebuild's pending outcome: recorded by the run that starts the
-- rebuild, delivered at most once by the detached installer, which outlives
-- that run and holds no run credential of its own.
CREATE TABLE self_rebuild_outcomes (
    id TEXT PRIMARY KEY,
    report_to TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    delivered_at INTEGER
);
