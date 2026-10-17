-- Rulings are project tenets now (cairn/5030). The boot import copies every
-- row into `tenets` first; the thread store's open holds this migration back
-- on any store whose rows have not been imported.
DROP TABLE IF EXISTS _cairn_rulings;
