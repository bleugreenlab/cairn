-- The ledger of the boot-time import that turned each thread's rulings into
-- migrated tenets (cairn/5030). One row per thread the importer reconciled,
-- written in the same transaction as the tenets it inserted for the project,
-- so a row here means every ruling the thread held at import time is a tenet.
--
-- Thread migration 0002 drops a thread's local `_cairn_rulings` table only
-- once its rulings are imported; this ledger is what lets a later boot skip a
-- thread whose local table is already gone.
CREATE TABLE tenet_ruling_imports (
  thread_id   TEXT PRIMARY KEY REFERENCES threads(id) ON DELETE CASCADE,
  project_id  TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  imported_at INTEGER NOT NULL
);
