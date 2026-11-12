-- lineage: private
-- cairn/6446: a test lane's memory charge steps down at its observed
-- compile-to-test boundary, to what the suite's test phase has been measured
-- to hold on the machine.
--
-- upper_test_phase_peak_bytes is the largest process tree a lane of the suite
-- held in this machine context while nextest was running its tests, and
-- upper_test_phase_bytes_per_test that peak divided by the tests that could
-- run at once (the smaller of the lane's test threads and its tests), each
-- rise-only and taken independently. NULL until a test phase has been
-- measured here, which keeps the lane on its whole-suite charge.
ALTER TABLE suite_memory_evidence ADD COLUMN upper_test_phase_peak_bytes INTEGER;
ALTER TABLE suite_memory_evidence ADD COLUMN upper_test_phase_bytes_per_test INTEGER;
