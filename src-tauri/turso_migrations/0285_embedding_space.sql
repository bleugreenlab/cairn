-- The embedding model is a workspace setting (cairn/5379), so the vector space
-- every stored vector shares has to be recorded, not assumed.
--
-- `turn_embedding_state` held only the install boundary. It becomes the record
-- of the whole embedding state: the boundary, plus the space (provider, model,
-- and the width each corpus is stored at) that every row with `dims > 0` in
-- `turn_embeddings`, `resource_embeddings` and `session_positions` belongs to.
-- On startup, after a settings change, and before every sweep pass, the active
-- offer is compared with this record; a difference purges the other space's
-- vectors and records the new one.
--
-- Every vector stored before this migration was made by the Cairn gateway's
-- Cohere Embed v4, so an install with vectors is seeded as that space. The
-- literals here must equal `CAIRN_GATEWAY_KEY` and `CAIRN_GATEWAY_EMBED_MODEL`
-- (asserted by the upgrade test in `orchestrator::embed`): a mismatch would
-- read as a model switch on first start and purge every vector this install
-- has. An install with no vectors records nothing; its first reconcile records
-- whatever it embeds with.

ALTER TABLE turn_embedding_state RENAME TO embedding_state;
ALTER TABLE embedding_state ADD COLUMN provider TEXT;
ALTER TABLE embedding_state ADD COLUMN model TEXT;
ALTER TABLE embedding_state ADD COLUMN turn_dims INTEGER;
ALTER TABLE embedding_state ADD COLUMN resource_dims INTEGER;

UPDATE embedding_state
   SET provider = 'cairn',
       model = 'cohere-embed-v4',
       turn_dims = (SELECT dims FROM turn_embeddings WHERE dims > 0 LIMIT 1),
       resource_dims = (SELECT dims FROM resource_embeddings WHERE dims > 0 LIMIT 1)
 WHERE id = 1
   AND (EXISTS (SELECT 1 FROM turn_embeddings WHERE dims > 0)
        OR EXISTS (SELECT 1 FROM resource_embeddings WHERE dims > 0));

-- Whether a turn is settled now depends on the model and width of its row,
-- not only on its existence. Both columns sit after the embedding blob, so
-- reading them from the table walks the blob's overflow pages: an anti-join
-- over 115k turns, or a progress count, went from an index probe per turn to
-- seconds. This index answers both without touching a row.
CREATE INDEX idx_turn_embeddings_space ON turn_embeddings(turn_id, dims, model);

-- A session's live semantic position, keyed by the space it was folded in.
-- It moves out of `sessions.current_pos` because `sessions` is in the team
-- lineage, where a column cannot be dropped or re-keyed; that column stays,
-- dormant. Private for the same reason `resource_embeddings` is.
CREATE TABLE session_positions (
    session_id TEXT PRIMARY KEY NOT NULL,
    embedding  BLOB NOT NULL,             -- f32 little-endian, `dims` wide
    model      TEXT NOT NULL,
    dims       INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO session_positions (session_id, embedding, model, dims, updated_at)
SELECT id, current_pos, 'cohere-embed-v4', length(current_pos) / 4, 0
  FROM sessions
 WHERE current_pos IS NOT NULL AND length(current_pos) > 0;
