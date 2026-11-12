-- A thread's headlines: one short line each for an operator who has been away
-- eight hours (a notable event, a decision made on the thread's own
-- authority, an assumption it proceeds on, a change in what the operator can
-- do or expect), with the one canonical link that carries the detail. App-known
-- beside `intent` and `journal`: the shape is Cairn's, the rows are the
-- thread's. A line is live while `until` is in the future; retiring it is one
-- patch of `until` to now, and rows are never deleted, so a reader's cursor id
-- stays meaningful. A thread table already holding the name is renamed to
-- `headlines_own` by the store's open before this runs (`db_store::open_path`).
CREATE TABLE headlines (
  id    INTEGER PRIMARY KEY,
  ts    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  line  TEXT NOT NULL CHECK (length(trim(line)) BETWEEN 1 AND 250),
  ref   TEXT NOT NULL CHECK (ref LIKE 'cairn://%' OR ref LIKE 'https://%'),
  until TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now','+3 days'))
         -- Not strict: retiring a line in the second it was written sets
         -- until to its ts, which is no longer live.
         CHECK (until >= ts)
);
CREATE INDEX _cairn_headlines_live ON headlines(until);
-- A screen long: at most 10 live lines per thread.
CREATE TRIGGER _cairn_headlines_cap BEFORE INSERT ON headlines
  WHEN (SELECT count(*) FROM headlines WHERE until > strftime('%Y-%m-%dT%H:%M:%SZ','now')) >= 10
  BEGIN SELECT RAISE(ABORT, '10 headlines are live: retire one (patch its until to now) before adding'); END;
-- The CHECKs above hold the shape; these name the fix where a writer's likely
-- mistake would otherwise read as a bare constraint failure. A relative
-- address means nothing to a reader in another thread, and `until` is compared
-- as text against the second-precision UTC stamp, so any other shape would
-- make a line live or retired at the wrong time.
CREATE TRIGGER _cairn_headlines_ref_insert BEFORE INSERT ON headlines
  WHEN NOT (NEW.ref LIKE 'cairn://%' OR NEW.ref LIKE 'https://%')
  BEGIN SELECT RAISE(ABORT, 'a headline ref is one canonical link, cairn://p/<project>/... or https://...: cairn:~/ and cairn:@/ are relative to the writer, so write the cairn://p/ address they resolve to'); END;
CREATE TRIGGER _cairn_headlines_ref_update BEFORE UPDATE OF ref ON headlines
  WHEN NOT (NEW.ref LIKE 'cairn://%' OR NEW.ref LIKE 'https://%')
  BEGIN SELECT RAISE(ABORT, 'a headline ref is one canonical link, cairn://p/<project>/... or https://...: cairn:~/ and cairn:@/ are relative to the writer, so write the cairn://p/ address they resolve to'); END;
CREATE TRIGGER _cairn_headlines_until_insert BEFORE INSERT ON headlines
  WHEN NEW.until NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z'
  BEGIN SELECT RAISE(ABORT, 'a headline until is UTC to the second, YYYY-MM-DDTHH:MM:SSZ (e.g. 2026-11-14T17:00:00Z)'); END;
CREATE TRIGGER _cairn_headlines_until_update BEFORE UPDATE OF until ON headlines
  WHEN NEW.until NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z'
  BEGIN SELECT RAISE(ABORT, 'a headline until is UTC to the second, YYYY-MM-DDTHH:MM:SSZ (e.g. 2026-11-14T17:00:00Z)'); END;
