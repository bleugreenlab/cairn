-- lineage: private
-- rebuild-fk-off: Conserved(request_outbox)
-- cairn/7672: a request whose verified `Done` report lands a commit, and whose
-- frozen act names a `then` kind, chains into one request of that kind for
-- exactly that commit.
--
-- One row per parent attempt, and one per (kind, commit), so a second `Done`
-- on a commit that already chained (a re-accept after Inconclusive, or
-- recovery adopting the landed commit) writes no row. `deadline_unix_ms` bounds
-- how long after the parent's report the chained acceptance may still be
-- written; past it the chained request is an ordinary card.
--
-- The outbox gains the `chain` kind the processor is woken by. Its kind CHECK
-- cannot widen in place, so request_outbox is rebuilt with every row and both
-- of its 0395 indexes.
CREATE TABLE request_chains (
 parent_attempt_id TEXT PRIMARY KEY REFERENCES request_attempts(id),
 kind TEXT NOT NULL CHECK(kind IN ('rebuild','lift_drain','fenced_command')),
 commit_id TEXT NOT NULL,
 deadline_unix_ms INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','accepted','satisfied','refused')),
 child_request_id TEXT REFERENCES requests(id),
 child_attempt_id TEXT,
 refusal TEXT,
 wake_destinations_json TEXT NOT NULL,
 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 UNIQUE(kind, commit_id),
 CHECK(state != 'accepted' OR (child_request_id IS NOT NULL AND child_attempt_id IS NOT NULL)),
 CHECK(state != 'refused' OR refusal IS NOT NULL)
);
CREATE INDEX request_chains_pending ON request_chains(kind) WHERE state='pending';
CREATE INDEX request_chains_child ON request_chains(child_request_id);

CREATE TABLE request_outbox_rebuilt (
 id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES requests(id),
 kind TEXT NOT NULL CHECK(kind IN ('changed','wake','reference','dispatch','chain')),
 destination TEXT, payload_json TEXT NOT NULL, created_at INTEGER NOT NULL, delivered_at INTEGER,
 last_attempt_at INTEGER, last_error TEXT
);
INSERT INTO request_outbox_rebuilt (id,request_id,kind,destination,payload_json,created_at,delivered_at,last_attempt_at,last_error)
SELECT id,request_id,kind,destination,payload_json,created_at,delivered_at,last_attempt_at,last_error FROM request_outbox;
DROP TABLE request_outbox;
ALTER TABLE request_outbox_rebuilt RENAME TO request_outbox;
CREATE INDEX request_outbox_pending ON request_outbox(created_at) WHERE delivered_at IS NULL;
CREATE INDEX request_outbox_request ON request_outbox(request_id);
