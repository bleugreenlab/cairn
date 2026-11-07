-- lineage: private
-- Authorization is install-local. The migration runner holds its exclusive
-- schema/write transaction for this entire rebuild, including trigger creation.
-- This establishes append-only triggers absent on earlier schemas. Future journal
-- rebuilds must preserve these triggers and the request_attempts journal FK.
CREATE TABLE authorization_events_rebuilt (
 id TEXT PRIMARY KEY, scope_key TEXT NOT NULL, place_kind TEXT NOT NULL,
 action TEXT NOT NULL, scope_json TEXT NOT NULL,
 mutation TEXT NOT NULL CHECK(mutation IN ('create','update','delete')),
 summary TEXT NOT NULL,
 outcome TEXT NOT NULL CHECK(outcome IN ('allowed_by_grant','approval_required','forbidden','direct','request_accepted')),
 reason TEXT NOT NULL, principal_json TEXT NOT NULL, audience_json TEXT NOT NULL,
 workspace_id TEXT NOT NULL, run_id TEXT, request_uri TEXT, grant_id TEXT,
 decided_at INTEGER NOT NULL, actor_principal_json TEXT, appearance_snapshot_json TEXT,
 request_acceptance_json TEXT,
 CHECK(outcome != 'request_accepted' OR (request_acceptance_json IS NOT NULL
 AND actor_principal_json IS NOT NULL AND appearance_snapshot_json IS NOT NULL AND grant_id IS NULL))
);
INSERT INTO authorization_events_rebuilt
 (id,scope_key,place_kind,action,scope_json,mutation,summary,outcome,reason,
 principal_json,audience_json,workspace_id,run_id,request_uri,grant_id,decided_at,
 actor_principal_json,appearance_snapshot_json)
SELECT id,scope_key,place_kind,action,scope_json,mutation,summary,outcome,reason,
 principal_json,audience_json,workspace_id,run_id,request_uri,grant_id,decided_at,
 actor_principal_json,appearance_snapshot_json FROM authorization_events;
DROP TABLE authorization_events;
ALTER TABLE authorization_events_rebuilt RENAME TO authorization_events;
CREATE INDEX idx_authorization_events_scope ON authorization_events(workspace_id,scope_key,decided_at DESC);
CREATE INDEX idx_authorization_events_grant ON authorization_events(grant_id);
CREATE INDEX idx_authorization_events_actor ON authorization_events(workspace_id,actor_principal_json,decided_at DESC);
CREATE TRIGGER authorization_events_no_update BEFORE UPDATE ON authorization_events
 BEGIN SELECT RAISE(ABORT,'authorization journal is append-only'); END;
CREATE TRIGGER authorization_events_no_delete BEFORE DELETE ON authorization_events
 BEGIN SELECT RAISE(ABORT,'authorization journal is append-only'); END;

CREATE TABLE request_cohorts (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, coalescing_key TEXT,
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','bump_version','lift_drain')),
 state TEXT NOT NULL CHECK(state IN ('open','accepted','acting','done','failed','declined','withdrawn')),
 revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX request_one_open_cohort ON request_cohorts(workspace_id,coalescing_key)
 WHERE state='open' AND coalescing_key IS NOT NULL;
CREATE TABLE requests (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','bump_version','lift_drain')),
 target TEXT, target_identity TEXT NOT NULL, coalescing_key TEXT,
 cohort_id TEXT NOT NULL REFERENCES request_cohorts(id),
 state TEXT NOT NULL CHECK(state IN ('open','accepted','acting','done','failed','declined','withdrawn')),
 revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 current_attempt TEXT, fulfilled_by_attempt TEXT, report_json TEXT
);
CREATE INDEX requests_workspace ON requests(workspace_id,updated_at DESC);
CREATE TABLE request_cohort_members (
 cohort_id TEXT NOT NULL REFERENCES request_cohorts(id),
 request_id TEXT NOT NULL REFERENCES requests(id),
 PRIMARY KEY(cohort_id,request_id)
);
CREATE TABLE request_requesters (
 id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES requests(id),
 origin_uri TEXT NOT NULL, run_id TEXT NOT NULL, principal_json TEXT NOT NULL,
 wake_destination TEXT NOT NULL, rationale TEXT NOT NULL, requirements_json TEXT NOT NULL,
 minimum_target TEXT, normalized_target_json TEXT NOT NULL, filing_key TEXT NOT NULL,
 active INTEGER NOT NULL CHECK(active IN (0,1)), satisfied_by_attempt TEXT, created_at INTEGER NOT NULL,
 UNIQUE(request_id,origin_uri,rationale,filing_key)
);
CREATE TABLE request_waiting (
 contribution_id TEXT NOT NULL REFERENCES request_requesters(id) ON DELETE CASCADE,
 uri TEXT NOT NULL, PRIMARY KEY(contribution_id,uri)
);
CREATE TABLE request_facts (
 cohort_id TEXT NOT NULL REFERENCES request_cohorts(id),
 version INTEGER NOT NULL, revision INTEGER NOT NULL, schema_version INTEGER NOT NULL,
 observed_at INTEGER NOT NULL, document_json TEXT NOT NULL,
 PRIMARY KEY(cohort_id,version)
);
CREATE TABLE request_attempts (
 id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL REFERENCES request_cohorts(id),
 authorization_event_id TEXT NOT NULL UNIQUE REFERENCES authorization_events(id),
 acceptance_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('accepted','acting','done','failed')),
 claim_id TEXT, outcome TEXT, report_json TEXT, updated_at INTEGER NOT NULL, settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0,1))
);
CREATE UNIQUE INDEX request_one_unsettled_attempt ON request_attempts(cohort_id) WHERE state IN ('accepted','acting') OR (state='failed' AND settled=0);
CREATE TABLE request_attempt_members (
 attempt_id TEXT NOT NULL REFERENCES request_attempts(id),
 request_id TEXT NOT NULL REFERENCES requests(id), contributions_json TEXT NOT NULL,
 PRIMARY KEY(attempt_id,request_id)
);
CREATE TRIGGER request_attempt_members_no_update BEFORE UPDATE ON request_attempt_members
 BEGIN SELECT RAISE(ABORT,'acceptance membership is immutable'); END;
CREATE TRIGGER request_attempt_members_no_delete BEFORE DELETE ON request_attempt_members
 BEGIN SELECT RAISE(ABORT,'acceptance membership is immutable'); END;
CREATE TRIGGER request_attempt_inputs_immutable BEFORE UPDATE ON request_attempts
 WHEN NEW.id != OLD.id OR NEW.cohort_id != OLD.cohort_id
 OR NEW.authorization_event_id != OLD.authorization_event_id OR NEW.acceptance_json != OLD.acceptance_json
 BEGIN SELECT RAISE(ABORT,'acceptance inputs are immutable'); END;
CREATE TRIGGER request_attempts_no_delete BEFORE DELETE ON request_attempts
 BEGIN SELECT RAISE(ABORT,'attempt history is immutable'); END;
CREATE TRIGGER request_accepted_facts_no_update BEFORE UPDATE ON request_facts
 WHEN EXISTS (SELECT 1 FROM request_attempts WHERE cohort_id=OLD.cohort_id
 AND json_extract(acceptance_json,'$.facts_version')=OLD.version)
 BEGIN SELECT RAISE(ABORT,'accepted facts are immutable'); END;
CREATE TRIGGER request_accepted_facts_no_delete BEFORE DELETE ON request_facts
 WHEN EXISTS (SELECT 1 FROM request_attempts WHERE cohort_id=OLD.cohort_id
 AND json_extract(acceptance_json,'$.facts_version')=OLD.version)
 BEGIN SELECT RAISE(ABORT,'accepted facts are immutable'); END;
CREATE TABLE request_outbox (
 id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES requests(id),
 kind TEXT NOT NULL CHECK(kind IN ('changed','wake','reference','dispatch')),
 destination TEXT, payload_json TEXT NOT NULL, created_at INTEGER NOT NULL, delivered_at INTEGER,
 last_attempt_at INTEGER, last_error TEXT
);
CREATE INDEX request_outbox_pending ON request_outbox(created_at) WHERE delivered_at IS NULL;
CREATE TABLE request_report_receipts (
 id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES request_attempts(id),
 claim_id TEXT NOT NULL, report_json TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE TRIGGER request_receipts_no_update BEFORE UPDATE ON request_report_receipts
 BEGIN SELECT RAISE(ABORT,'consumer receipt is immutable'); END;
CREATE TRIGGER request_receipts_no_delete BEFORE DELETE ON request_report_receipts
 BEGIN SELECT RAISE(ABORT,'consumer receipt is immutable'); END;

CREATE TRIGGER request_decline_immutable BEFORE UPDATE ON requests WHEN OLD.state='declined'
 BEGIN SELECT RAISE(ABORT,'decline decision is immutable'); END;
CREATE TRIGGER request_identity_no_delete BEFORE DELETE ON requests
 BEGIN SELECT RAISE(ABORT,'request identities are retained'); END;

-- Unconditional child-key indexes serve FK checks and retained-history lookups;
-- the unsettled-attempt partial index cannot serve settled acceptance history.
CREATE INDEX requests_cohort ON requests(cohort_id);
CREATE INDEX request_cohort_members_request ON request_cohort_members(request_id);
CREATE INDEX request_attempts_cohort ON request_attempts(cohort_id);
CREATE INDEX request_attempt_members_request ON request_attempt_members(request_id);
CREATE INDEX request_outbox_request ON request_outbox(request_id);
CREATE INDEX request_report_receipts_attempt ON request_report_receipts(attempt_id);
