ALTER TABLE annotations ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;

CREATE TABLE annotation_seq_counters (
    target_uri TEXT PRIMARY KEY,
    next_seq   INTEGER NOT NULL
);

-- Backfill per-anchor 1-based seqs, ordered by created_at then id for deterministic ties.
UPDATE annotations
SET seq = (
  SELECT COUNT(*) FROM annotations a2
  WHERE a2.target_uri = annotations.target_uri
    AND (a2.created_at <  annotations.created_at
      OR (a2.created_at =  annotations.created_at AND a2.id <= annotations.id))
);

-- Seed counters at the high-water + 1 per target.
INSERT INTO annotation_seq_counters (target_uri, next_seq)
SELECT target_uri, MAX(seq) + 1 FROM annotations GROUP BY target_uri;
