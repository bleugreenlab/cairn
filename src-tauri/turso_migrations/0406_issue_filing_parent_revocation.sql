-- lineage: shared
-- Filing provenance stays immutable while the parent edge remains ordinary
-- attention routing. Its first actual change permanently revokes the filing
-- edge's permission-answering authority, including after re-adoption.
-- There is no complete lifetime reparent journal for existing issues. A
-- current edge matching its creator cannot prove it was never adopted back,
-- so every pre-existing parent edge is revoked rather than backfilled.
--
-- A future issues table rebuild must preserve filing_parent_revoked_at and
-- recreate all three triggers. REPLACE must not erase a stored revocation.
ALTER TABLE issues ADD COLUMN filing_parent_revoked_at INTEGER;
UPDATE issues SET filing_parent_revoked_at = unixepoch()
WHERE parent_issue_id IS NOT NULL OR parent_thread_id IS NOT NULL;

CREATE TRIGGER issues_revoke_filing_parent
AFTER UPDATE OF parent_issue_id, parent_thread_id, parent_job_id ON issues
WHEN OLD.filing_parent_revoked_at IS NULL
 AND (OLD.parent_issue_id IS NOT NEW.parent_issue_id
      OR OLD.parent_thread_id IS NOT NEW.parent_thread_id
      OR OLD.parent_job_id IS NOT NEW.parent_job_id)
BEGIN
    UPDATE issues SET filing_parent_revoked_at = unixepoch() WHERE id = NEW.id;
END;

CREATE TRIGGER issues_preserve_filing_parent_revocation
BEFORE UPDATE OF filing_parent_revoked_at ON issues
WHEN OLD.filing_parent_revoked_at IS NOT NULL
 AND NEW.filing_parent_revoked_at IS NOT OLD.filing_parent_revoked_at
BEGIN
    SELECT RAISE(ABORT, 'issue filing parent revocation is permanent');
END;

CREATE TRIGGER issues_refuse_replacing_revoked_filing_parent
BEFORE INSERT ON issues
WHEN EXISTS (
    SELECT 1 FROM issues WHERE id = NEW.id AND (
        (filing_parent_revoked_at IS NOT NULL
         AND filing_parent_revoked_at IS NOT NEW.filing_parent_revoked_at)
        OR (NEW.filing_parent_revoked_at IS NULL AND (
            parent_issue_id IS NOT NEW.parent_issue_id
            OR parent_thread_id IS NOT NEW.parent_thread_id
            OR parent_job_id IS NOT NEW.parent_job_id
        ))
    )
)
BEGIN
    SELECT RAISE(ABORT, 'cannot replace an issue with a revoked filing parent');
END;
