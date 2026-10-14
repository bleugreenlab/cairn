-- Migration: integrity sweep state
--
-- Whole-database `PRAGMA integrity_check` used to run inside every fk_off table
-- rebuild, where its cost scaled with the whole database rather than the
-- migration's blast radius and where pre-existing index-entry drift from an
-- unrelated table could fail an unrelated migration and, through the runner's
-- init_database expect(), brick startup (CAIRN-3103). Migrations now verify their
-- own rebuilt tables; whole-database verification moved to a background
-- maintenance sweep that reports and repairs rather than gating a boot.
--
-- That sweep is roughly a minute of CPU on a multi-gigabyte database, so it runs
-- on a cadence rather than every boot. This single-row table records when it last
-- ran. NULL means never swept.
CREATE TABLE integrity_sweep_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_swept_at INTEGER
);

INSERT INTO integrity_sweep_state (id, last_swept_at) VALUES (1, NULL);
