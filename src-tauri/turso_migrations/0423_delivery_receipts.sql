-- lineage: shared
-- cairn/6391: one durable receipt per (fact, stable recipient).
--
-- A fact the recipient resolver reaches a recipient by is owed to that
-- recipient exactly once, however many derivations reach it (parent edge,
-- subscription, camp role, trigger, direct address) and however often the
-- fact is re-indexed, re-routed or retried. `fact_key` names the fact:
-- `fact_log:<source>:<row id>` for a logged fact, or the push fact key of an
-- issue push or condition broadcast. The recipient is stable, never a
-- session: `thread` with a `threads.id`, or `job` with the id of a non-session
-- job (a coordinator, a sub-agent task). The session that receives it is
-- resolved at delivery and recorded in `delivered_job_id`.
--
-- `fact_row_id` references the original content (a fact log row) and the body
-- is never copied here. `level` is the maximum any reason reaches, and
-- `reasons_json` keeps every reason; both may only rise while the receipt is
-- `queued`, so a consumed receipt is never re-opened.
--
-- A receipt lives in the database where its delivery settles: a push receipt
-- beside its push in the issue's owning database, a fact-log receipt in the
-- runner's private database beside its coalesced wake. `delivering` binds a
-- receipt to the deterministic `message_id` its settlement writes, so a retry
-- re-settles that same message and never carries the receipt twice.
CREATE TABLE delivery_receipts (
  id TEXT PRIMARY KEY,
  fact_key TEXT NOT NULL,
  recipient_kind TEXT NOT NULL CHECK (recipient_kind IN ('thread', 'job')),
  recipient_id TEXT NOT NULL,
  project_id TEXT,
  fact_row_id INTEGER,
  level TEXT NOT NULL CHECK (level IN ('passive', 'wake', 'interrupt')),
  reasons_json TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'queued'
    CHECK (state IN ('queued', 'delivering', 'delivered', 'withdrawn', 'dropped')),
  wake_id INTEGER,
  message_id TEXT,
  delivered_job_id TEXT,
  push_id TEXT,
  last_refusal TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  settled_at INTEGER,
  UNIQUE (fact_key, recipient_kind, recipient_id)
);
CREATE INDEX idx_delivery_receipts_wake ON delivery_receipts(wake_id, state);
CREATE INDEX idx_delivery_receipts_recipient ON delivery_receipts(recipient_kind, recipient_id, state, created_at);
CREATE INDEX idx_delivery_receipts_settled ON delivery_receipts(state, settled_at);
