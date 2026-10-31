-- lineage: private
-- cairn/5517: a write through a page is ordinary (direct) work by policy, but
-- its principal is a verified operator acting through a page version rather
-- than a run, so nothing else records who made it. Those writes are journaled
-- with outcome `direct`. The outcome CHECK names every value and cannot be
-- widened in place, so the table is rebuilt and its rows copied across.
-- Nothing references this table, and it is private, so no team lineage carries
-- it.
CREATE TABLE authorization_events_rebuilt (
  id             TEXT PRIMARY KEY,
  scope_key      TEXT NOT NULL,
  place_kind     TEXT NOT NULL,
  action         TEXT NOT NULL,
  scope_json     TEXT NOT NULL,
  mutation       TEXT NOT NULL CHECK(mutation IN ('create','update','delete')),
  summary        TEXT NOT NULL,
  outcome        TEXT NOT NULL CHECK(outcome IN ('allowed_by_grant','approval_required','forbidden','direct')),
  reason         TEXT NOT NULL,
  principal_json TEXT NOT NULL,
  audience_json  TEXT NOT NULL,
  workspace_id   TEXT NOT NULL,
  run_id         TEXT,
  request_uri    TEXT,
  grant_id       TEXT,
  decided_at     INTEGER NOT NULL,
  actor_principal_json TEXT,
  appearance_snapshot_json TEXT
);

INSERT INTO authorization_events_rebuilt (
  id, scope_key, place_kind, action, scope_json, mutation, summary, outcome,
  reason, principal_json, audience_json, workspace_id, run_id, request_uri,
  grant_id, decided_at, actor_principal_json, appearance_snapshot_json
)
SELECT
  id, scope_key, place_kind, action, scope_json, mutation, summary, outcome,
  reason, principal_json, audience_json, workspace_id, run_id, request_uri,
  grant_id, decided_at, actor_principal_json, appearance_snapshot_json
FROM authorization_events;

DROP TABLE authorization_events;

ALTER TABLE authorization_events_rebuilt RENAME TO authorization_events;

CREATE INDEX idx_authorization_events_scope
  ON authorization_events(workspace_id, scope_key, decided_at DESC);

CREATE INDEX idx_authorization_events_grant
  ON authorization_events(grant_id);

CREATE INDEX idx_authorization_events_actor
  ON authorization_events(workspace_id, actor_principal_json, decided_at DESC);
