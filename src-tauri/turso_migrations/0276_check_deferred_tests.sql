-- cairn/5141: the rust-tests fast tier's verdict-free per-test outcome.
--
-- A fast-tier lane terminates any test that runs past its per-test budget and
-- records it as DEFERRED: started, never finished, neither passed nor failed.
-- `check_test_results.status` is CHECK-constrained to passed/failed/skipped, and
-- widening that constraint needs a create-copy-drop-rename rebuild, which breaks
-- Turso sync triggers on team replicas (see 0227_attachments.sql). So deferral
-- is ADDITIVE: its own table, which no query over `check_test_results` can count
-- as passed, plus the observation's tier and deferred tally.

-- The population the observation's test rows describe: full, filtered, empty,
-- fast, or unknown, as the producer's structured result declared it. NULL on
-- rows recorded before this column existed and on results with no parse.
ALTER TABLE check_result_observations ADD COLUMN selection TEXT;

-- How many tests the fast tier deferred. A test with no row at all on a fast
-- observation was not selected, never "unobserved": every deferral has a row.
ALTER TABLE check_result_observations ADD COLUMN deferred_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE check_deferred_tests (
    observation_id TEXT NOT NULL REFERENCES check_result_observations(id) ON DELETE CASCADE,
    test_id TEXT NOT NULL,
    duration_ms INTEGER,
    attempt_count INTEGER,
    PRIMARY KEY (observation_id, test_id)
);

CREATE TRIGGER check_deferred_tests_immutable
BEFORE UPDATE ON check_deferred_tests
BEGIN SELECT RAISE(ABORT, 'check deferred tests are immutable'); END;

-- One terminal outcome per test per observation: a deferred test has no result
-- row, and a test with a result row cannot also be deferred.
CREATE TRIGGER check_deferred_tests_disjoint
BEFORE INSERT ON check_deferred_tests
WHEN EXISTS (SELECT 1 FROM check_test_results
             WHERE observation_id = NEW.observation_id AND test_id = NEW.test_id)
BEGIN SELECT RAISE(ABORT, 'a test with a result row cannot also be deferred'); END;

CREATE TRIGGER check_test_results_not_deferred
BEFORE INSERT ON check_test_results
WHEN EXISTS (SELECT 1 FROM check_deferred_tests
             WHERE observation_id = NEW.observation_id AND test_id = NEW.test_id)
BEGIN SELECT RAISE(ABORT, 'a deferred test cannot also have a result row'); END;

-- The one documented union of an observation's per-test outcomes. Every
-- per-test reader goes through this, so a deferred test is visible as itself.
CREATE VIEW check_test_outcomes AS
SELECT observation_id, test_id, status, duration_ms, attempt_count,
       failure_excerpt, skip_reason, declaration_source, flaky
  FROM check_test_results
UNION ALL
SELECT observation_id, test_id, 'deferred', duration_ms, attempt_count,
       NULL, NULL, NULL, 0
  FROM check_deferred_tests;
