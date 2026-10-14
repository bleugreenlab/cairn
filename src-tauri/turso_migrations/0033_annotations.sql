CREATE TABLE annotations (
    id TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    origin_job_id TEXT,
    origin_branch TEXT,
    target_uri TEXT NOT NULL,
    target_path TEXT,
    region TEXT NOT NULL,
    snapshot TEXT,
    kind TEXT NOT NULL,
    body TEXT NOT NULL,
    data TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    created_at INTEGER NOT NULL,
    resolved_at INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_annotations_target_uri ON annotations(target_uri);
CREATE INDEX idx_annotations_target_path ON annotations(target_path);
CREATE INDEX idx_annotations_origin_branch ON annotations(origin_branch);
CREATE INDEX idx_annotations_status ON annotations(status);

-- Intentional data-loss cleanup: embedded artifact annotations were the retired
-- dev-era storage model and cannot be safely backfilled here without duplicating
-- runtime URI construction for schema-named artifact anchors in SQL. New
-- annotations are durable rows in the table above; remove the legacy arrays so
-- callers cannot observe or mutate two divergent annotation stores.
UPDATE artifacts
SET data = json_remove(data, '$.annotations'),
    updated_at = unixepoch()
WHERE json_type(data, '$.annotations') = 'array';