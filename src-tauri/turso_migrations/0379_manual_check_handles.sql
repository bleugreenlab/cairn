-- lineage: shared
-- A request's durable receipt is distinct from the immutable verdict it used.
CREATE TABLE manual_check_handles (
    handle TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    observation_id TEXT REFERENCES check_result_observations(id) ON DELETE CASCADE,
    publication_id TEXT,
    response_json TEXT,
    commit_sha TEXT,
    tree_hash TEXT,
    input_hash TEXT,
    disposition TEXT,
    context_json TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_manual_check_handles_project ON manual_check_handles(project_id);
CREATE INDEX idx_manual_check_handles_observation ON manual_check_handles(observation_id);

-- Recovery must not depend on the evictable hot cache for the producer's parse.
ALTER TABLE check_result_observations ADD COLUMN verdict_result_json TEXT;
ALTER TABLE check_result_observations ADD COLUMN publication_id TEXT;
CREATE UNIQUE INDEX idx_check_observations_publication
    ON check_result_observations(project_id,publication_id) WHERE publication_id IS NOT NULL;
CREATE INDEX idx_manual_handle_backfill_source ON check_result_cache(source_observation_id);
DROP TRIGGER check_result_observations_immutable;
UPDATE check_result_observations
   SET verdict_result_json = (
       SELECT CASE WHEN json_valid(c.target_results_json) THEN CASE WHEN
           json_type(c.target_results_json,'$.parser')='text'
           AND json_type(c.target_results_json,'$.passed')='integer'
           AND json_type(c.target_results_json,'$.failed')='integer'
           AND json_type(c.target_results_json,'$.skipped')='integer'
           AND json_extract(c.target_results_json,'$.passed')>=0
           AND json_extract(c.target_results_json,'$.failed')>=0
           AND json_extract(c.target_results_json,'$.skipped')>=0
       THEN json_object(
           'parser',json_extract(c.target_results_json,'$.parser'),
           'complete',json(CASE WHEN json_extract(c.target_results_json,'$.complete')=1
                               THEN 'true' ELSE 'false' END),
           'selection',COALESCE(json_extract(c.target_results_json,'$.selection'),'unknown'),
           'passed',json_extract(c.target_results_json,'$.passed'),
           'failed',json_extract(c.target_results_json,'$.failed'),
           'skipped',json_extract(c.target_results_json,'$.skipped'),
           'deferred',COALESCE(json_extract(c.target_results_json,'$.deferred'),0),
           'unverified',json_extract(c.target_results_json,'$.unverified'),
           'failures',json('[]')) END END FROM check_result_cache c
       WHERE c.source_observation_id=check_result_observations.id
         -- Unary + keeps the planner off the (project_id, ...) primary key:
         -- probed by project, each observation walks its project's whole cache
         -- and its overflowed JSON (~10^10 row visits on a 137k-row install).
         AND +c.project_id=check_result_observations.project_id
         AND json_valid(c.target_results_json) LIMIT 1
   )
 WHERE id IN (SELECT source_observation_id FROM check_result_cache
              WHERE source_observation_id IS NOT NULL AND json_valid(target_results_json));
DROP INDEX idx_manual_handle_backfill_source;
CREATE TRIGGER check_result_observations_immutable
BEFORE UPDATE ON check_result_observations
BEGIN SELECT RAISE(ABORT, 'check result observations are immutable'); END;
