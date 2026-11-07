-- A thread's running record (tenet 182): an `intent` singleton (where the
-- thread is going and what is in motion) and a kinded `journal`. Both names are
-- app-known without the `_cairn_` prefix: their shape is Cairn's, their rows
-- are the thread's. Their indexes and triggers keep the prefix, because index
-- and trigger names are database-wide and a thread's own objects must never
-- collide with them. A thread table already holding either name is renamed to
-- `<name>_own` by the store's open before this runs (`db_store::open_path`).
CREATE TABLE intent (
  id             INTEGER PRIMARY KEY CHECK (id = 1),
  current_intent TEXT NOT NULL DEFAULT '',
  working        TEXT NOT NULL DEFAULT '',
  -- NULL until first written; the boot import from the retired arc fills only
  -- a row nobody has written.
  updated_at     TEXT
);
INSERT INTO intent(id) VALUES (1);
CREATE TRIGGER _cairn_intent_touch AFTER UPDATE ON intent
  WHEN NEW.updated_at IS OLD.updated_at
  BEGIN UPDATE intent SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = NEW.id; END;
CREATE TRIGGER _cairn_intent_singleton BEFORE DELETE ON intent
  BEGIN SELECT RAISE(ABORT, 'intent is one row: patch cairn:~/db/intent/1 instead'); END;

CREATE TABLE journal (
  id          INTEGER PRIMARY KEY,
  ts          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  kind        TEXT NOT NULL CHECK (kind IN
                ('event','decision','lesson','practice','reference','watch','question','queue')),
  note        TEXT NOT NULL CHECK (length(trim(note)) > 0),
  refs        TEXT NOT NULL DEFAULT '[]'
                CHECK (json_valid(refs) AND json_type(refs) = 'array'),
  -- Only the open kinds (watch, question, queue) resolve.
  resolution  TEXT CHECK (resolution IS NULL OR kind IN ('watch','question','queue')),
  resolved_at TEXT CHECK (resolved_at IS NULL OR kind IN ('watch','question','queue'))
);
CREATE INDEX _cairn_journal_ts ON journal(ts);
CREATE INDEX _cairn_journal_open ON journal(kind) WHERE resolved_at IS NULL;
-- Retiring an open row is one patch, `{resolution:"…"}`; this stamps the time.
CREATE TRIGGER _cairn_journal_resolve AFTER UPDATE OF resolution ON journal
  WHEN NEW.resolution IS NOT NULL AND NEW.resolved_at IS NULL
  BEGIN UPDATE journal SET resolved_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = NEW.id; END;

-- One table for the running record: open questions fold in as kind 'question'.
INSERT INTO journal(ts, kind, note, refs)
  SELECT strftime('%Y-%m-%dT%H:%M:%SZ', created_at, 'unixepoch'), 'question', question,
         COALESCE(provenance, '[]')
  FROM _cairn_open_questions ORDER BY id;
DROP TABLE _cairn_open_questions;
