-- Cohere Embed v4 storage model (CAIRN-1137).
--
-- Corpus vectors live in resource_embeddings (recall targets, queried in-engine
-- via vector_distance_cos). Per-event vectors are transient: the async worker
-- embeds at store time, assigns a vibe color, persists the color here, and
-- discards the vector. Retire the MiniLM-era event/issue embedding stores.

CREATE TABLE resource_embeddings (
    uri        TEXT PRIMARY KEY NOT NULL,   -- canonical cairn:// resource URI
    embedding  BLOB NOT NULL,               -- 1536-d f32 little-endian
    model      TEXT NOT NULL,               -- e.g. 'cohere-embed-v4'
    dims       INTEGER NOT NULL,            -- 1536
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_resource_embeddings_model ON resource_embeddings(model);

-- Persisted per-event vibe color (the vector itself is discarded).
CREATE TABLE event_vibes (
    event_id   TEXT PRIMARY KEY NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    locus      TEXT NOT NULL,
    css_color  TEXT NOT NULL,
    similarity REAL NOT NULL,
    model      TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

-- Retire the MiniLM-era stores.
DROP TABLE IF EXISTS event_embeddings;
DROP TABLE IF EXISTS issue_embeddings;
