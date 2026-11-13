-- lineage: private
-- rebuild-fk-off: Conserved(request_cohorts), Conserved(requests)
-- cairn/5815: `fenced_command` joins the closed operator-request kinds.
--
-- The kind CHECK on request_cohorts and requests cannot be widened in place,
-- so both are rebuilt with every column and row carried across. Children keep
-- their REFERENCES by name, which resolve to the rebuilt tables. The two
-- triggers on requests are dropped first, so the drop can never be read as a
-- deletion of retained request identities, and recreated exactly as 0395
-- wrote them, with the indexes.
CREATE TABLE request_cohorts_rebuilt (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, coalescing_key TEXT,
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','bump_version','lift_drain','fenced_command')),
 state TEXT NOT NULL CHECK(state IN ('open','accepted','acting','done','failed','declined','withdrawn')),
 revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
INSERT INTO request_cohorts_rebuilt (id,workspace_id,coalescing_key,kind,state,revision,created_at,updated_at)
SELECT id,workspace_id,coalescing_key,kind,state,revision,created_at,updated_at FROM request_cohorts;

CREATE TABLE requests_rebuilt (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','bump_version','lift_drain','fenced_command')),
 target TEXT, target_identity TEXT NOT NULL, coalescing_key TEXT,
 cohort_id TEXT NOT NULL REFERENCES request_cohorts(id),
 state TEXT NOT NULL CHECK(state IN ('open','accepted','acting','done','failed','declined','withdrawn')),
 revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 current_attempt TEXT, fulfilled_by_attempt TEXT, report_json TEXT
);
INSERT INTO requests_rebuilt (id,workspace_id,kind,target,target_identity,coalescing_key,cohort_id,
 state,revision,created_at,updated_at,current_attempt,fulfilled_by_attempt,report_json)
SELECT id,workspace_id,kind,target,target_identity,coalescing_key,cohort_id,
 state,revision,created_at,updated_at,current_attempt,fulfilled_by_attempt,report_json FROM requests;

DROP TRIGGER request_decline_immutable;
DROP TRIGGER request_identity_no_delete;
DROP TABLE requests;
DROP TABLE request_cohorts;
ALTER TABLE request_cohorts_rebuilt RENAME TO request_cohorts;
ALTER TABLE requests_rebuilt RENAME TO requests;

CREATE UNIQUE INDEX request_one_open_cohort ON request_cohorts(workspace_id,coalescing_key)
 WHERE state='open' AND coalescing_key IS NOT NULL;
CREATE INDEX requests_workspace ON requests(workspace_id,updated_at DESC);
CREATE INDEX requests_cohort ON requests(cohort_id);
CREATE TRIGGER request_decline_immutable BEFORE UPDATE ON requests WHEN OLD.state='declined'
 BEGIN SELECT RAISE(ABORT,'decline decision is immutable'); END;
CREATE TRIGGER request_identity_no_delete BEFORE DELETE ON requests
 BEGIN SELECT RAISE(ABORT,'request identities are retained'); END;
