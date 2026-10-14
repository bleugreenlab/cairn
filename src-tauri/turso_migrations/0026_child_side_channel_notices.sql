CREATE TABLE child_side_channel_notices (
    id            TEXT PRIMARY KEY NOT NULL,
    parent_job_id TEXT NOT NULL,
    child_uri     TEXT NOT NULL,
    content       TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    delivered_at  INTEGER
);

CREATE INDEX idx_side_channel_pending
    ON child_side_channel_notices(parent_job_id, delivered_at);
