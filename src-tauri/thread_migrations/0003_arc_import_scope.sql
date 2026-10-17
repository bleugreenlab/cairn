-- What an arc-import marker vouches for (cairn/5030). Before tenets, the lazy
-- arc import copied rulings AND open questions into this store, so every
-- existing marker means the arc's rulings are already in `_cairn_rulings`;
-- those rows keep the default 1. After the cutover the import copies only open
-- questions and writes 0, so the boot-time ruling import still reads the arc's
-- rulings for a store it has not yet imported.
ALTER TABLE _cairn_arc_imports ADD COLUMN rulings_copied INTEGER NOT NULL DEFAULT 1;
