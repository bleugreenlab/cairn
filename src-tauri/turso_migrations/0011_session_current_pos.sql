-- Session semantic position (CAIRN-1138).
--
-- The async embed worker maintains a live, recency-biased "current position"
-- vector per session (dual-timescale EMA over event embeddings). Persisting it
-- here lets a resumed session reload its position rather than rebuilding from
-- zero. The per-node/chat summary centroid lives in resource_embeddings keyed
-- by the node/chat URI; only the live position needs a home on the session row.
--
-- Stored as little-endian f32 bytes (same encoding as resource_embeddings.embedding).
ALTER TABLE sessions ADD COLUMN current_pos BLOB;
