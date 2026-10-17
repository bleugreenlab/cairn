-- Tenet evidence (cairn/5032): concluded work judged against standing tenets.
--
-- An issue resolution or a thread era becomes one evaluation EVENT carrying a
-- text snapshot of the conclusion. A worker embeds it, picks the nearest
-- approved tenets, and asks a model whether the work supports or contradicts
-- each; confident verdicts become EVIDENCE rows. Contradictions accumulate a
-- decayed weight that wakes the proposing thread. Each wake consumes exactly
-- the rows it counted (`consumed_by_wake`), so what the next wake may count is
-- decided per row, never by a timestamp edge a late-replicated row could fall
-- behind. `tenets.contradiction_alerted_at` only records when the last wake
-- fired.
--
-- Both tables replicate to team replicas, where Turso Sync replays remote rows
-- as an upsert on the primary key. Ids are therefore derived from each table's
-- logical unique key, so two machines writing the same logical row collide on
-- the primary key (an upsert) rather than on the secondary UNIQUE (an error).
--
-- `owner_device_id` is the machine that wrote the conclusion. Only it
-- evaluates the event, and the wake fires only on the machine owning the
-- newest contributing contradiction: a compare-and-swap is atomic on one
-- machine, never across replicas.
--
-- Timestamps are unix seconds, like `tenets`.

CREATE TABLE tenet_evidence_events (
  -- uuid derived from sha256(dedupe_key)
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  source_uri TEXT NOT NULL,
  source_kind TEXT NOT NULL CHECK (source_kind IN ('issue_resolution','era')),
  -- issue:<id>:<resolution>:<resolution_generation> or era:<job_id>:<seq>
  dedupe_key TEXT NOT NULL UNIQUE,
  owner_device_id TEXT NOT NULL,
  text TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','done','skipped','failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  evaluated_at INTEGER
);
CREATE INDEX tenet_evidence_events_due
  ON tenet_evidence_events(owner_device_id, state, next_attempt_at);

CREATE TABLE tenet_evidence (
  -- uuid derived from sha256(tenet_id || ':' || source_uri)
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  tenet_id TEXT NOT NULL REFERENCES tenets(id) ON DELETE CASCADE,
  event_id TEXT NOT NULL REFERENCES tenet_evidence_events(id) ON DELETE CASCADE,
  source_uri TEXT NOT NULL,
  sign TEXT NOT NULL CHECK (sign IN ('supports','contradicts')),
  -- The judge's probability that its verdict is right; ordinal, not calibrated.
  confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  model TEXT NOT NULL,
  owner_device_id TEXT NOT NULL,
  evaluated_at INTEGER NOT NULL,
  -- The wake that counted this contradiction (its message id) and when it
  -- fired; NULL until a wake counts it. Only a contradiction is ever consumed.
  consumed_by_wake TEXT,
  consumed_at INTEGER,
  UNIQUE (tenet_id, source_uri)
);
CREATE INDEX tenet_evidence_by_tenet ON tenet_evidence(tenet_id, sign, evaluated_at);
-- The wake sweep's candidates: contradictions no wake has counted yet.
CREATE INDEX tenet_evidence_unconsumed ON tenet_evidence(tenet_id)
  WHERE sign = 'contradicts' AND consumed_by_wake IS NULL;
