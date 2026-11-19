-- lineage: shared
-- Session-local cache invalidation hints, exact under one writer per session.
-- Application mutation seams update these rows in their own write transaction.
-- No session FK: retained tombstones prevent delete/reinsert identity reuse.
CREATE TABLE session_skyline_watermarks (
    session_id TEXT PRIMARY KEY NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0 CHECK (typeof(event_count) = 'integer' AND event_count >= 0),
    event_revision INTEGER NOT NULL DEFAULT 0 CHECK (typeof(event_revision) = 'integer' AND event_revision >= 0 AND event_revision <= 9007199254740991),
    event_reset_revision INTEGER NOT NULL DEFAULT 0 CHECK (typeof(event_reset_revision) = 'integer' AND event_reset_revision >= 0 AND event_reset_revision <= 9007199254740991),
    vibe_count INTEGER NOT NULL DEFAULT 0 CHECK (typeof(vibe_count) = 'integer' AND vibe_count >= 0),
    vibe_revision INTEGER NOT NULL DEFAULT 0 CHECK (typeof(vibe_revision) = 'integer' AND vibe_revision >= 0 AND vibe_revision <= 9007199254740991),
    edge_present INTEGER NOT NULL DEFAULT 0,
    edge_run_unknown INTEGER NOT NULL DEFAULT 0,
    edge_run_created_at INTEGER NOT NULL DEFAULT 0,
    edge_run_rowid INTEGER NOT NULL DEFAULT 0,
    edge_sequence INTEGER NOT NULL DEFAULT 0,
    edge_event_rowid INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE skyline_vibe_mutations (
    session_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    PRIMARY KEY (session_id, event_id)
);
CREATE INDEX idx_skyline_vibe_mutations_revision ON skyline_vibe_mutations(session_id, revision);
INSERT INTO skyline_vibe_mutations SELECT session_id, event_id, 1
FROM event_vibes WHERE session_id IS NOT NULL;
INSERT INTO session_skyline_watermarks
    (session_id, event_count, event_revision, event_reset_revision)
SELECT session_id, COUNT(*), 1, 1
FROM events WHERE session_id IS NOT NULL GROUP BY session_id;
INSERT INTO session_skyline_watermarks (session_id)
SELECT id FROM sessions WHERE true ON CONFLICT(session_id) DO NOTHING;
INSERT INTO session_skyline_watermarks (session_id)
SELECT DISTINCT session_id FROM event_vibes WHERE session_id IS NOT NULL
ON CONFLICT(session_id) DO NOTHING;
UPDATE session_skyline_watermarks
SET vibe_count = (SELECT COUNT(*) FROM event_vibes v
    WHERE v.session_id = session_skyline_watermarks.session_id),
    vibe_revision = 1;
UPDATE session_skyline_watermarks SET edge_present = 1,
    (edge_run_unknown, edge_run_created_at, edge_run_rowid, edge_sequence, edge_event_rowid) = (SELECT CASE WHEN r.id IS NULL THEN 1 ELSE 0 END, COALESCE(r.created_at, 0),
        COALESCE(r.rowid, 0), e.sequence, e.rowid
    FROM events e LEFT JOIN runs r ON r.id = e.run_id AND r.session_id = e.session_id
    WHERE e.session_id = session_skyline_watermarks.session_id
    ORDER BY CASE WHEN r.id IS NULL THEN 1 ELSE 0 END DESC,
        COALESCE(r.created_at, 0) DESC, COALESCE(r.rowid, 0) DESC,
        e.sequence DESC, e.rowid DESC LIMIT 1)
WHERE event_count > 0;
ALTER TABLE session_skyline_cache RENAME COLUMN latest_event_created_at TO event_revision;
ALTER TABLE session_skyline_cache ADD COLUMN event_reset_revision INTEGER NOT NULL DEFAULT 0;
DELETE FROM session_skyline_cache;
