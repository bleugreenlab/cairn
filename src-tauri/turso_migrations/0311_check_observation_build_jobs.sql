-- lineage: shared
-- cairn/5665: a compile's length depends on how many jobs it ran, and on a
-- small machine that is the jobs its memory could hold rather than its core
-- count. The executor states the number it gave cargo and nextest beside the
-- CPU width and the memory reading that may have held it lower
-- (`BuildJobs`), so a slow lane on a small machine is read beside its cause.
--
-- `build_jobs` is the number, its own column so cairn://db can group by it;
-- `build_jobs_json` is the whole `BuildJobs` record (cpuWidth, and memory
-- {jobs, availableBytes, measuredAtUnixMs, perJobReserveBytes, sharers} when
-- memory was read). Both NULL on rows recorded before these columns existed,
-- on runs on the coordinator itself, and from executors that do not state it.
ALTER TABLE check_result_observations ADD COLUMN build_jobs INTEGER;
ALTER TABLE check_result_observations ADD COLUMN build_jobs_json TEXT;
