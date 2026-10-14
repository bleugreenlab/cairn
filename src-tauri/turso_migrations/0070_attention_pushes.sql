-- Attention push-queue substrate (CAIRN-1880, slice A of the attention/wake
-- rebuild; see docs/attention-redesign.md).
--
-- One delivery queue per agent. Everything that reaches an agent from outside
-- its own turn is a *push*: recipient + content_ref + wake + boundary + key.
-- A push is delivered the instant a durable event carries it, recorded by
-- stamping delivered_event_id (atomic with that event's persistence in a later
-- slice). Supersession is by (recipient, key) among undelivered rows, enforced
-- by the partial unique index below.
--
-- delivered_event_id has NO foreign key to events on purpose. events are pruned
-- under archival (events.run_id REFERENCES runs(id) ON DELETE CASCADE), and a
-- delivered push must STAY delivered: a SET NULL FK would resurrect a delivered
-- push as pending (a spurious re-wake), and a CASCADE FK would erase delivery
-- history and needlessly couple the queue to event pruning. The column is a
-- durable delivery marker, not a navigable relation; recovery reasons over
-- event durability directly (slice E).

CREATE TABLE IF NOT EXISTS attention_pushes (
  id                  TEXT PRIMARY KEY,
  -- The watcher job this push is for. Matches how wake_subscriptions.job_id
  -- identifies a watcher; CASCADE is correct (watcher gone -> nothing to
  -- deliver).
  recipient           TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  -- URI of the underlying resolvable thing (a PR/plan, a prompt, a permission
  -- row, a child chat window).
  content_ref         TEXT NOT NULL,
  wake                TEXT NOT NULL CHECK (wake IN ('passive','wake','interrupt')),
  boundary            TEXT NOT NULL CHECK (boundary IN ('event','turn')),
  -- Supersession identity, e.g. review:{issue_uri}, catchup:{child_uri},
  -- question:{detail_uri}. The prefix also discriminates which referent table
  -- lazy-resolution checks at drain.
  key                 TEXT NOT NULL,
  created_at          INTEGER NOT NULL,
  -- NULL = undelivered. When set, the durable event that sealed delivery.
  delivered_event_id  TEXT
);

-- Pending-drain scan: undelivered pushes for a recipient.
CREATE INDEX IF NOT EXISTS idx_attention_pushes_pending
  ON attention_pushes(recipient, delivered_event_id);

-- Supersede-by-key: at most one UNDELIVERED push per (recipient, key). A
-- delivered push leaves the partial index, so a later same-key push inserts a
-- fresh row (a future child idle + open PR is a NEW review).
CREATE UNIQUE INDEX IF NOT EXISTS idx_attention_pushes_supersede
  ON attention_pushes(recipient, key)
  WHERE delivered_event_id IS NULL;
