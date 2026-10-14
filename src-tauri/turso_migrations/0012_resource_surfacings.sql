-- Resource surfacing feedback log (CAIRN-1139).
--
-- One row per recommended resource each time the recommender surfaces a
-- `## related` section on a cairn:// resource read. The (query_pos,
-- candidate_set, read/ignored) calibration signal is captured as the session's
-- current_pos at surfacing time (reconstructable, not stored) plus these rows
-- and their read_at. No inline 1536-d query vector — full-fidelity calibration,
-- if ever wanted, goes to the JSON-lines structured log instead.
--
-- read_at closes the feedback loop: when the session later reads a uri it was
-- shown, the matching open rows are stamped. NULL read_at = surfaced-and-ignored,
-- which drives the soft-decay drop.
CREATE TABLE resource_surfacings (
    id          TEXT PRIMARY KEY NOT NULL,
    session_id  TEXT NOT NULL,
    uri         TEXT NOT NULL,        -- recommended resource URI
    rank        INTEGER NOT NULL,     -- position in the surfaced list (0-based)
    sim         REAL NOT NULL,        -- cosine similarity at surfacing time
    surfaced_at INTEGER NOT NULL,
    read_at     INTEGER               -- set when the session later reads this uri; NULL = ignored
);
CREATE INDEX idx_resource_surfacings_session ON resource_surfacings(session_id);
CREATE INDEX idx_resource_surfacings_session_uri ON resource_surfacings(session_id, uri);
