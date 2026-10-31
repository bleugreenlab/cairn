-- lineage: private
-- Vibe axes for every embedding model, not only Cohere v4 (cairn/5022).
--
-- Vibe colours project an event's vector onto two behavioural axes (PHASE,
-- FRICTION) learned in one model's space at one width. The Cohere v4 set ships
-- bundled as `vibe_axes.json`; for any other model the app learns a set from
-- this workspace's own history and keeps it here, keyed by the space it was
-- learned in. Rows are never deleted on a model switch: switching back finds
-- its set still here and recomputes nothing.
--
-- `axes` is the set as JSON, the same shape as the bundled file.
-- `counts` is how many embedded events each class and the percentile
-- population held when it was learned.
CREATE TABLE vibe_axes (
    model      TEXT NOT NULL,
    dims       INTEGER NOT NULL,
    axes       TEXT NOT NULL,
    counts     TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (model, dims)
);

-- The sample a recompute in progress is learning from, so it resumes after a
-- restart instead of embedding its events again. One sample at a time, for the
-- active model; a model switch deletes the rest with every other vector of the
-- old space. Rows are written all at once when the sample is drawn, so their
-- presence means sampling finished; `embedding` fills in chunk by chunk, and
-- every row is deleted once the set is stored.
CREATE TABLE vibe_axis_samples (
    model           TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    run_progress    REAL NOT NULL,
    -- Event steps back to the nearest errored tool result; NULL for none.
    dist_prev_error INTEGER,
    -- Drawn regardless of class, for the percentile table's population.
    in_distribution INTEGER NOT NULL,
    embedding       BLOB,                -- f32 little-endian, `dims` wide
    dims            INTEGER,
    PRIMARY KEY (model, event_id)
);
