-- lineage: shared
-- Authority derived from issue parentage follows the parent edges as they stand
-- (cairn/6765): whether an edge was created by filing, adoption or reparenting
-- no longer matters, so nothing reads the revocation stamp migration 0406 added.
-- Its triggers go first, then the column, dropped in place rather than by a
-- table rebuild so the Turso sync triggers on the shared `issues` table stay
-- intact. `issues.parent_job_id` stays: it is the filing job's provenance.
DROP TRIGGER IF EXISTS issues_revoke_filing_parent;
DROP TRIGGER IF EXISTS issues_preserve_filing_parent_revocation;
DROP TRIGGER IF EXISTS issues_refuse_replacing_revoked_filing_parent;
ALTER TABLE issues DROP COLUMN filing_parent_revoked_at;
