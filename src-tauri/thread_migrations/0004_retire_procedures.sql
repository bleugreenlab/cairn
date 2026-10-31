-- Thread procedures are retired (tenet 1313): thread code that runs without a
-- turn is a service method, and a scheduled one is a `schedule` entry in the
-- service's manifest. Each thread that held procedures was sent its
-- conversion before this landed; the resource that owned this table is gone.
DROP TABLE IF EXISTS _cairn_procedures;
