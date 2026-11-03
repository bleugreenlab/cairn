-- lineage: private
-- cairn/5956: the private-only half of 0360_index_foreign_key_children. These
-- tables (and `projects.workspace_id`, whose team counterpart is
-- `projects.team_id`) exist only in this install's database. Full indexes, for
-- the reason NEXT1 gives. `codex_watchdog_lifecycle`'s two keys are not here:
-- at about 1.0M rows their builds cost 45 s of boot, so they wait for
-- cairn/6037's eviction and land under cairn/6039.
CREATE INDEX idx_build_change_notifications_recipient
    ON build_change_notifications(recipient);
CREATE INDEX idx_codex_watchdog_leases_successor_run_id
    ON codex_watchdog_leases(successor_run_id);
CREATE INDEX idx_project_routes_team_id ON project_routes(team_id);
CREATE INDEX idx_projects_workspace_id ON projects(workspace_id);
CREATE INDEX idx_quarantined_records_incident_id ON quarantined_records(incident_id);
CREATE INDEX idx_thread_services_project_id ON thread_services(project_id);
