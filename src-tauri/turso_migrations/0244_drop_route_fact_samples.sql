-- The route Playground's sampled fact is now the latest `fact_log` row for a
-- source in a scope, so the one-row-per-(scope, source) sample table is gone.
DROP TABLE route_fact_samples;
