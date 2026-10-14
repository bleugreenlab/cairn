-- Per-turn semantic vectors for the search lane (CAIRN-3810).
--
-- One row per conversation turn holding a reduced-dimension Cohere Embed v4
-- vector of that turn's concatenated prose. Turn granularity is the whole
-- point: a session's folded vector in `resource_embeddings` blurs every topic
-- the session touched, while a single event is a fragment of a thought. A turn
-- is the smallest unit that carries a complete exchange.
--
-- Private lineage only, matching `resource_embeddings`. Sharing
-- remotely-computed vectors into team replicas is the deferred question
-- CAIRN-2210 already owns; answering it differently here would fork it.
--
-- `project_id`, `job_id`, `session_id`, and `sequence` are denormalized from
-- the turn's run so a project-scoped brute-force cosine scan reads one table
-- with no joins. At 256 dimensions a workspace of 100k turns is tens of
-- megabytes, well inside scan range, so no vector index is needed.
--
-- A row with `dims = 0` and an empty `embedding` is a tombstone: the turn was
-- swept and carried no embeddable prose (a turn of pure tool results, say).
-- It records the sweep so the turn is not reconsidered on every pass.

CREATE TABLE turn_embeddings (
    turn_id    TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    job_id     TEXT,
    project_id TEXT,
    sequence   INTEGER NOT NULL,
    embedding  BLOB NOT NULL,             -- f32 little-endian, `dims` wide
    model      TEXT NOT NULL,
    dims       INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_turn_embeddings_project ON turn_embeddings(project_id);
