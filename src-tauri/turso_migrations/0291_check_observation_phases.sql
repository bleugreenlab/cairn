-- lineage: shared
-- cairn/5428: where a lane's minutes go. One `duration_ms` cannot say whether
-- a twelve-minute rust-tests lane spent its time compiling or testing, and the
-- answer decides what a merge gate can promise. The runner divides its
-- duration into phases on one timeline (scripts/lib/lane-phases.ts), so they
-- sum to the command's wall clock; each total is its own column so cairn://db
-- can group and sum them without parsing anything.
--
-- All NULL on rows recorded before this column existed and on lanes whose
-- runner does not measure phases.
ALTER TABLE check_result_observations ADD COLUMN phase_setup_ms INTEGER;
ALTER TABLE check_result_observations ADD COLUMN phase_compile_ms INTEGER;
ALTER TABLE check_result_observations ADD COLUMN phase_test_ms INTEGER;
ALTER TABLE check_result_observations ADD COLUMN phase_doctest_ms INTEGER;
ALTER TABLE check_result_observations ADD COLUMN phase_publish_ms INTEGER;

-- `executor` when the timeline starts at the executor's spawn stamp, so setup
-- includes the command's prelude; `process` when it starts at the runner
-- script and the prelude is outside every phase.
ALTER TABLE check_result_observations ADD COLUMN phase_origin TEXT;

-- Crates cargo compiled across the lane's passes. The same tree compiles in
-- eight minutes or twenty-five seconds depending only on what is stale, so a
-- compile duration is read beside this count, `execution_warmth`, and the cell
-- origin.
ALTER TABLE check_result_observations ADD COLUMN compiled_units INTEGER;

-- The per-pass breakdown, [{name, wallMs, compileMs, testMs, compiledUnits,
-- summaryMs?}], keeping the fast tier's local, warm-up, and global passes apart.
ALTER TABLE check_result_observations ADD COLUMN phase_passes_json TEXT;

-- The executor's own account of the cell, under its own names, so the phase
-- ledger and the executor speak one vocabulary. NULL when the executor made no
-- claim, including every run on the coordinator itself.
--
-- `execution_warmth` is `ExecutionWarmth`: what the cell held when the command
-- started (`preparedWarmSlot`, `repositoryOnly`, `cold`).
ALTER TABLE check_result_observations ADD COLUMN execution_warmth TEXT;

-- `cell_origin_kind` is `CellOrigin`'s provenance: the seed on the cell's first
-- occupancy (`clone`, `catalog`, `cold`) or `reused` after it. The distance is
-- the `CacheDistance` from the cell's tree to the command's commit, carried by
-- a clone or a reuse when the executor measured it.
ALTER TABLE check_result_observations ADD COLUMN cell_origin_kind TEXT;
ALTER TABLE check_result_observations ADD COLUMN cell_origin_distance_files INTEGER;
ALTER TABLE check_result_observations ADD COLUMN cell_origin_distance_crates INTEGER;
