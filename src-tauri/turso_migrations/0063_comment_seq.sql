-- Give each issue comment a stable, human-friendly per-issue sequence so it can
-- be addressed as cairn://p/PROJECT/NUMBER/comments/N instead of by raw UUID.
ALTER TABLE comments ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;

-- Backfill per-issue 1-based seqs, ordered by created_at then id for
-- deterministic ties (matches the comment list ordering).
UPDATE comments
SET seq = (
  SELECT COUNT(*) FROM comments c2
  WHERE c2.issue_id = comments.issue_id
    AND (c2.created_at <  comments.created_at
      OR (c2.created_at =  comments.created_at AND c2.id <= comments.id))
);
