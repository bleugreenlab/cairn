-- CAIRN-3264: the write-replay ledger.
--
-- A `write` batch is not idempotent when its patch is insertion-shaped. Such a
-- patch matches an anchor and re-emits that anchor inside the text it writes, so
-- after it applies its own `old_string` is still present: a second delivery of
-- the same call matches again, inserts a second copy, and reports success.
--
-- Nothing above this layer made a re-delivery a no-op. The store lock serializes
-- writers but cannot tell two deliveries of one call from two calls; the
-- expected-head compare-and-swap is satisfied by whatever the first delivery
-- left behind. So the identity has to be recorded here.
--
-- A row is one provider `write` call that has already published file changes,
-- with the report it produced. A later delivery of that same call finds the row
-- and returns the report instead of applying anything.
--
-- Host-local (RunnerTransient): it guards one host's in-flight deliveries over
-- seconds to minutes and means nothing to a teammate, so it stays out of the
-- team lineage. Rows die with their run.
CREATE TABLE write_replay_ledger (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  tool_use_id TEXT NOT NULL,
  report TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, tool_use_id)
);
