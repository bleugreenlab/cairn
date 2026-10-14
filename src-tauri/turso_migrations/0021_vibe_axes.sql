-- Replace 5-emotion vibe loci with 2 behavioral axes (CAIRN-1217).
--
-- The vibe-coloring math changed from (5 near-collinear emotion centroids +
-- winner-take-all + absolute-cosine->chroma) to (2 behaviorally-grounded
-- contrast axes -- PHASE and FRICTION -- projected and percentile-rank-normalized
-- to an OKLCH color). Each event now stores its two axis coordinates instead of
-- a winning locus + similarity.
--
-- Wipe and recreate: the old per-event colors are meaningless under the new
-- palette. The lazy backfill path (get_session_vibe_colors) recolors each
-- session's events the next time it is viewed; new events color at store time.
DROP TABLE IF EXISTS event_vibes;
CREATE TABLE event_vibes (
    event_id   TEXT PRIMARY KEY NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    css_color  TEXT NOT NULL,
    phase      REAL NOT NULL,
    friction   REAL NOT NULL,
    model      TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
