-- lineage: shared
-- cairn/5989: the plan audit's findings as a lane's stored rows, beside its tests.
--
-- A finding is not a test: counting one in `check_test_results` would inflate the
-- run's tally and turn "all N passed" into "1 failed", and "not evaluated" could
-- not be said there at all, because that table's `status` is CHECK-constrained
-- and widening it needs the rebuild 0276 explains. So the audit is additive: its
-- state on the observation, and its findings in their own table, which the main
-- cadence diffs by `finding_id` the way it diffs tests by `test_id`.
--
-- `finding_id` is line-insensitive: `file|rule|table|statement`. `status` is
-- deliberately unconstrained (`failed`, `allowlisted`, `reported`, `stale` today),
-- for the reason above.

-- `evaluated` or `not-evaluated`. NULL: the lane carries no audit. A consumer that
-- acts without a person reads `not-evaluated` here as the audit half unverified.
ALTER TABLE check_result_observations ADD COLUMN plan_audit_state TEXT;
-- Why it was not evaluated.
ALTER TABLE check_result_observations ADD COLUMN plan_audit_note TEXT;

CREATE TABLE check_audit_findings (
    observation_id TEXT NOT NULL REFERENCES check_result_observations(id) ON DELETE CASCADE,
    finding_id TEXT NOT NULL,
    status TEXT NOT NULL,
    rule TEXT NOT NULL,
    table_name TEXT NOT NULL,
    file TEXT NOT NULL,
    statement TEXT NOT NULL,
    issue TEXT,
    known_shape TEXT,
    excerpt TEXT,
    PRIMARY KEY (observation_id, finding_id)
);

CREATE TRIGGER check_audit_findings_immutable
BEFORE UPDATE ON check_audit_findings
BEGIN SELECT RAISE(ABORT, 'check audit findings are immutable'); END;
