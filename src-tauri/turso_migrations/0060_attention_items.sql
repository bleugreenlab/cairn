-- CAIRN-1647: Attention ledger — durable level-triggered attention items.
--
-- Replaces the fire-and-forget "[Child update] … Read X." frozen system-direct
-- model with a reconcilable ledger. Items OPEN when something starts needing a
-- watcher's attention, BUMP (version++) when their content meaningfully changes,
-- and RESOLVE when handled through any path. Watcher resumes are evaluated
-- against current item state at delivery time, so a fact resolved between
-- trigger and delivery simply drops out of the deliverable set.
--
-- This migration only creates the tables; emission/delivery wiring lands in
-- later steps while the existing wake path keeps running. The ledger is inert
-- until callers open/resolve items and the delivery engine reads them.

CREATE TABLE attention_items (
  id            TEXT PRIMARY KEY,
  source_kind   TEXT NOT NULL,                 -- 'issue' (this redesign); extensible
  source_ref    TEXT NOT NULL,                 -- issue URI
  issue_id      TEXT REFERENCES issues(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,                 -- 'question'|'permission'|'review'|'resolved'|'message'
  key           TEXT NOT NULL,                 -- stable identity within (source, kind)
  state         TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'resolved')),
  version       INTEGER NOT NULL DEFAULT 1,
  escalate      INTEGER NOT NULL DEFAULT 0,
  content_json  TEXT NOT NULL,                 -- typed per-kind content, serialized
  detail_uri    TEXT,
  opened_at     INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL,
  resolved_at   INTEGER,
  resolved_by   TEXT                           -- 'user'|'agent'|'system'
);

-- One item per stable identity: re-opening the same fact updates in place
-- (idempotent open / content-change bump) instead of accumulating duplicates.
CREATE UNIQUE INDEX idx_attention_items_identity
  ON attention_items(source_kind, source_ref, kind, key);

-- Cascade/join by issue, and scan the deliverable set per source efficiently.
CREATE INDEX idx_attention_items_issue ON attention_items(issue_id);
CREATE INDEX idx_attention_items_source_state ON attention_items(source_ref, state);

-- Per-watcher delivery cursor: the version of each item this job last saw.
CREATE TABLE attention_seen (
  item_id      TEXT NOT NULL REFERENCES attention_items(id) ON DELETE CASCADE,
  job_id       TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  seen_version INTEGER NOT NULL,
  seen_at      INTEGER NOT NULL,
  PRIMARY KEY (item_id, job_id)
);

CREATE INDEX idx_attention_seen_job ON attention_seen(job_id);

-- Durable, coalesced wake trigger: at most one pending evaluation per watcher
-- job. N triggers while the watcher is busy collapse to one evaluation; the
-- highest urgency requested since the last evaluation wins.
CREATE TABLE attention_evaluations (
  job_id       TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  requested_at INTEGER NOT NULL,
  urgency      TEXT NOT NULL DEFAULT 'queue'
);
