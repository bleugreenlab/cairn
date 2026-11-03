-- lineage: shared
-- cairn/5956: a full index leading with every foreign-key child column both
-- lineages carry. Deleting a parent row runs each child key's action (or, for
-- NO ACTION, its constraint check) as a lookup on the child column; with no
-- index leading with it the engine walks the whole child table once per
-- deleted parent, and a cascade multiplies that by the rows it deletes. A
-- partial index does not serve the lookup, because the action's `col = ?`
-- does not imply its predicate to Turso, so these are full even where a
-- partial index on the same column exists (agent_waits.job_id,
-- attention_pushes.recipient, jobs.parent_job_id, provider_limit_holds.job_id).
-- `storage::migrations::tests::every_foreign_key_child_column_leads_an_index`
-- holds the rule.
CREATE INDEX idx_action_runs_execution_id ON action_runs(execution_id);
CREATE INDEX idx_action_runs_issue_id ON action_runs(issue_id);
CREATE INDEX idx_action_runs_project_id ON action_runs(project_id);
CREATE INDEX idx_agent_waits_job_id ON agent_waits(job_id);
CREATE INDEX idx_agent_waits_predecessor_turn_id ON agent_waits(predecessor_turn_id);
CREATE INDEX idx_agent_waits_session_id ON agent_waits(session_id);
CREATE INDEX idx_agent_waits_successor_turn_id ON agent_waits(successor_turn_id);
CREATE INDEX idx_artifact_content_execution_id ON artifact_content(execution_id);
CREATE INDEX idx_artifact_content_job_id ON artifact_content(job_id);
CREATE INDEX idx_ask_conditions_job_id ON ask_conditions(job_id);
CREATE INDEX idx_attention_pushes_recipient ON attention_pushes(recipient);
CREATE INDEX idx_check_result_cache_source_observation_id
    ON check_result_cache(source_observation_id);
CREATE INDEX idx_check_result_commit_aliases_source_observation_id
    ON check_result_commit_aliases(source_observation_id);
CREATE INDEX idx_condition_evaluations_execution_id ON condition_evaluations(execution_id);
CREATE INDEX idx_executions_project_id ON executions(project_id);
CREATE INDEX idx_issues_parent_thread_id ON issues(parent_thread_id);
CREATE INDEX idx_jobs_current_turn_id ON jobs(current_turn_id);
CREATE INDEX idx_jobs_parent_job_id ON jobs(parent_job_id);
CREATE INDEX idx_jobs_resume_session_id ON jobs(resume_session_id);
CREATE INDEX idx_jobs_turn_end_check_attempt_id ON jobs(turn_end_check_attempt_id);
CREATE INDEX idx_memories_project_id ON memories(project_id);
CREATE INDEX idx_merge_attempt_events_merge_request_id ON merge_attempt_events(merge_request_id);
-- In the key's own column order, so the cascade from one pack_catalog row
-- seeks on all four columns.
CREATE INDEX idx_pack_catalog_references_catalog
    ON pack_catalog_references(project_id, repository_id, object_format, content_hash);
CREATE INDEX idx_provider_limit_holds_job_id ON provider_limit_holds(job_id);
CREATE INDEX idx_provider_limit_holds_run_id ON provider_limit_holds(run_id);
CREATE INDEX idx_provider_limit_holds_turn_id ON provider_limit_holds(turn_id);
CREATE INDEX idx_suppressed_wakes_subscription_id ON suppressed_wakes(subscription_id);
CREATE INDEX idx_tenet_evidence_event_id ON tenet_evidence(event_id);
CREATE INDEX idx_tenet_evidence_project_id ON tenet_evidence(project_id);
CREATE INDEX idx_tenet_evidence_events_project_id ON tenet_evidence_events(project_id);
CREATE INDEX idx_tenet_ruling_imports_project_id ON tenet_ruling_imports(project_id);
