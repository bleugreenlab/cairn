-- lineage: private
-- rebuild-fk-off: Reshaped(request_cohorts), Reshaped(requests), Reshaped(request_cohort_members), Reshaped(request_requesters), Reshaped(request_waiting), Reshaped(request_facts), Reshaped(request_attempts), Reshaped(request_attempt_members), Reshaped(request_outbox), Reshaped(request_report_receipts), Conserved(authorization_events)
-- cairn/7672: `bump_version` leaves the closed operator-request kinds.
--
-- No production runner ever registered a bump_version consumer, and filing and
-- acceptance resolve the consumer before persisting anything, so no such rows
-- are expected. Any that exist are deleted with every row that hangs off them.
-- Authorization journal events stay: the journal is append-only and nothing
-- references the deleted attempts from it. The append-only triggers on
-- attempts, attempt members and receipts are dropped for the deletion and
-- recreated exactly as 0395 wrote them.
--
-- A receipt no longer records the coverage a bump derived for other rebuilds,
-- so the always-empty `covered` key is removed from every remaining receipt.
--
-- The kind CHECK on request_cohorts and requests cannot be narrowed in place,
-- so both are rebuilt as 0452 did, keeping every column, including the address
-- `number` 0506 added to requests, with its unique index, its stamping trigger
-- and its decline trigger exactly as 0506 wrote them.
DROP TRIGGER request_attempt_members_no_delete;
DROP TRIGGER request_attempts_no_delete;
DROP TRIGGER request_receipts_no_update;
DROP TRIGGER request_receipts_no_delete;

DELETE FROM request_report_receipts WHERE attempt_id IN (SELECT id FROM request_attempts
 WHERE cohort_id IN (SELECT id FROM request_cohorts WHERE kind='bump_version'));
DELETE FROM request_attempt_members WHERE attempt_id IN (SELECT id FROM request_attempts
 WHERE cohort_id IN (SELECT id FROM request_cohorts WHERE kind='bump_version'));
DELETE FROM request_attempts WHERE cohort_id IN (SELECT id FROM request_cohorts WHERE kind='bump_version');
DELETE FROM request_facts WHERE cohort_id IN (SELECT id FROM request_cohorts WHERE kind='bump_version');
DELETE FROM request_waiting WHERE contribution_id IN (SELECT id FROM request_requesters
 WHERE request_id IN (SELECT id FROM requests WHERE kind='bump_version'));
DELETE FROM request_requesters WHERE request_id IN (SELECT id FROM requests WHERE kind='bump_version');
DELETE FROM request_outbox WHERE request_id IN (SELECT id FROM requests WHERE kind='bump_version');
DELETE FROM request_cohort_members WHERE cohort_id IN (SELECT id FROM request_cohorts WHERE kind='bump_version')
 OR request_id IN (SELECT id FROM requests WHERE kind='bump_version');

UPDATE request_report_receipts SET report_json=json_remove(report_json,'$.covered')
 WHERE json_type(report_json,'$.covered') IS NOT NULL;

CREATE TRIGGER request_attempt_members_no_delete BEFORE DELETE ON request_attempt_members
 BEGIN SELECT RAISE(ABORT,'acceptance membership is immutable'); END;
CREATE TRIGGER request_attempts_no_delete BEFORE DELETE ON request_attempts
 BEGIN SELECT RAISE(ABORT,'attempt history is immutable'); END;
CREATE TRIGGER request_receipts_no_update BEFORE UPDATE ON request_report_receipts
 BEGIN SELECT RAISE(ABORT,'consumer receipt is immutable'); END;
CREATE TRIGGER request_receipts_no_delete BEFORE DELETE ON request_report_receipts
 BEGIN SELECT RAISE(ABORT,'consumer receipt is immutable'); END;

CREATE TABLE request_cohorts_rebuilt (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, coalescing_key TEXT,
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','lift_drain','fenced_command')),
 state TEXT NOT NULL CHECK(state IN ('open','accepted','acting','done','failed','declined','withdrawn')),
 revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
INSERT INTO request_cohorts_rebuilt (id,workspace_id,coalescing_key,kind,state,revision,created_at,updated_at)
SELECT id,workspace_id,coalescing_key,kind,state,revision,created_at,updated_at FROM request_cohorts
 WHERE kind!='bump_version';

CREATE TABLE requests_rebuilt (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','lift_drain','fenced_command')),
 target TEXT, target_identity TEXT NOT NULL, coalescing_key TEXT,
 cohort_id TEXT NOT NULL REFERENCES request_cohorts(id),
 state TEXT NOT NULL CHECK(state IN ('open','accepted','acting','done','failed','declined','withdrawn')),
 revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 current_attempt TEXT, fulfilled_by_attempt TEXT, report_json TEXT, number INTEGER
);
INSERT INTO requests_rebuilt (id,workspace_id,kind,target,target_identity,coalescing_key,cohort_id,
 state,revision,created_at,updated_at,current_attempt,fulfilled_by_attempt,report_json,number)
SELECT id,workspace_id,kind,target,target_identity,coalescing_key,cohort_id,
 state,revision,created_at,updated_at,current_attempt,fulfilled_by_attempt,report_json,number FROM requests
 WHERE kind!='bump_version';

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
CREATE UNIQUE INDEX idx_requests_number ON requests(number);
CREATE TRIGGER request_decline_immutable BEFORE UPDATE ON requests
 WHEN OLD.state='declined' AND OLD.number IS NOT NULL
 BEGIN SELECT RAISE(ABORT,'decline decision is immutable'); END;
CREATE TRIGGER requests_number AFTER INSERT ON requests
WHEN NEW.number IS NULL
BEGIN
    INSERT INTO address_counters (kind, scope, last) VALUES ('operator_request', '', 1)
        ON CONFLICT (kind, scope) DO UPDATE SET last = last + 1;
    UPDATE requests
       SET number = (SELECT last FROM address_counters
                      WHERE kind = 'operator_request' AND scope = '')
     WHERE id = NEW.id;
END;
CREATE TRIGGER request_identity_no_delete BEFORE DELETE ON requests
 BEGIN SELECT RAISE(ABORT,'request identities are retained'); END;
