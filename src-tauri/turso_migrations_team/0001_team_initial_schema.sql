-- Team-rooted shared-space schema (TEAM_HEAD, lineage 0001).
--
-- This is the team-DB counterpart of the private lineage's 0001..00NN history:
-- a one-time snapshot of the FINAL schema of every project-scoped ("shared")
-- table, re-anchored from the private `workspaces` root to a `teams` root.
-- It is authored once because shipped private history must never be rewritten;
-- from here forward a shared-table change is written ONCE in the SHARED_TAIL
-- (see `with_shared_tail!` in storage/migrations.rs) and both lineages compose
-- it. The schema-equivalence test in storage/migrations.rs proves, after
-- whitespace normalization, that every shared table here is byte-identical to
-- the private lineage except the four intentional re-rootings:
--   * projects:        workspace_id -> team_id (FK -> teams); server_id FK dropped
--                      (servers is private/workspace-scoped; no cross-DB FK).
--   * action_configs:  workspaces FK dropped only (workspaces is
--                      private/workspace-scoped; no cross-DB FK). workspace_id
--                      is KEPT (nullable, always NULL for team rows) so the one
--                      shared action_configs query layer stays lineage-agnostic;
--                      the CHECK keeps every team row project-anchored.
--   * skill_configs:   workspace arm dropped (project_id-only, NOT NULL).
--   * issue_labels:    labels FK dropped (labels stays workspace/private-scoped;
--                      team-scoped labels are a deferred cross-scope decision).
--
-- Host-local runtime tables (job_terminals, job_browsers, checkpoint_command_cache,
-- checkpoint_runs) live here ONLY because they FK to jobs. Whether their ROWS
-- should replicate to other members or stay per-replica is a later content-
-- filtering decision, NOT settled here.

CREATE TABLE teams (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE "action_configs" (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', command_template TEXT, input_schema TEXT, output_schema TEXT, is_builtin INTEGER NOT NULL DEFAULT 0, workspace_id TEXT, project_id TEXT REFERENCES projects (id) ON DELETE CASCADE, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, tool_name TEXT, tool_description TEXT, CHECK ((workspace_id IS NULL) != (project_id IS NULL)));
CREATE TABLE action_runs (id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, recipe_node_id TEXT NOT NULL, action_config_id TEXT NOT NULL, issue_id TEXT, project_id TEXT NOT NULL, status TEXT NOT NULL, inputs TEXT, output TEXT, error_message TEXT, started_at INTEGER, completed_at INTEGER, created_at INTEGER NOT NULL, parent_job_id TEXT, uri_segment TEXT, FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE, FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE SET NULL, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE CASCADE);
CREATE TABLE "artifact_content" (id TEXT PRIMARY KEY, artifact_node_id TEXT NOT NULL, execution_id TEXT NOT NULL REFERENCES executions (id) ON DELETE CASCADE, job_id TEXT REFERENCES jobs (id) ON DELETE SET NULL, data TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE artifacts (id TEXT PRIMARY KEY, job_id TEXT, artifact_type TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1, data TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, parent_version_id TEXT, output_name TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, seen_at INTEGER, confirmed INTEGER NOT NULL DEFAULT 0, FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE, FOREIGN KEY (parent_version_id) REFERENCES artifacts(id));
CREATE TABLE attention_pushes (id TEXT PRIMARY KEY, recipient TEXT NOT NULL, content_ref TEXT NOT NULL, wake TEXT NOT NULL CHECK(wake IN ('passive', 'wake', 'interrupt')), boundary TEXT NOT NULL CHECK(boundary IN ('event', 'turn')), "key" TEXT NOT NULL, created_at INTEGER NOT NULL, delivered_event_id TEXT, fingerprint TEXT, FOREIGN KEY (recipient) REFERENCES jobs(id) ON DELETE CASCADE);
CREATE TABLE attention_read_cursors (recipient TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, source TEXT NOT NULL, position INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (recipient, source));
CREATE TABLE checkpoint_command_cache (id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, command TEXT NOT NULL, normalized_command TEXT NOT NULL, exit_code INTEGER NOT NULL, commit_sha TEXT NOT NULL, is_dirty INTEGER NOT NULL DEFAULT 0, ran_at INTEGER NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE checkpoint_runs (id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, attempt INTEGER NOT NULL, command TEXT, commit_sha TEXT, exit_code INTEGER NOT NULL, passed INTEGER NOT NULL, stdout_tail TEXT, stderr_tail TEXT, ran_at INTEGER NOT NULL);
CREATE TABLE comments (id TEXT PRIMARY KEY, issue_id TEXT NOT NULL, content TEXT NOT NULL, source TEXT NOT NULL CHECK(source IN ('user', 'agent')), created_at INTEGER NOT NULL, seq INTEGER NOT NULL DEFAULT 0, FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE);
CREATE TABLE "condition_evaluations" (id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions (id) ON DELETE CASCADE, recipe_node_id TEXT NOT NULL, result_port TEXT NOT NULL, raw_result TEXT, error_message TEXT, evaluated_at INTEGER NOT NULL);
CREATE TABLE doc_references (id TEXT PRIMARY KEY, issue_id TEXT NOT NULL REFERENCES issues (id) ON DELETE CASCADE, doc_path TEXT NOT NULL, created_at INTEGER NOT NULL, UNIQUE (issue_id, doc_path));
CREATE TABLE event_read_tokens (event_id TEXT PRIMARY KEY NOT NULL REFERENCES events (id) ON DELETE CASCADE, segments_json TEXT NOT NULL, total_tokens INTEGER NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE event_vibes (event_id TEXT NOT NULL PRIMARY KEY, css_color TEXT NOT NULL, phase REAL NOT NULL, friction REAL NOT NULL, model TEXT NOT NULL, created_at INTEGER NOT NULL, session_id TEXT, FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE);
CREATE TABLE events (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, session_id TEXT, sequence INTEGER NOT NULL, timestamp INTEGER NOT NULL, event_type TEXT NOT NULL, data TEXT NOT NULL, parent_tool_use_id TEXT, created_at INTEGER NOT NULL, input_tokens INTEGER, cache_read_tokens INTEGER, cache_create_tokens INTEGER, output_tokens INTEGER, turn_id TEXT, change_preview_status TEXT, change_applied_at INTEGER, thinking_tokens INTEGER, storage_mode TEXT DEFAULT 'full' CHECK(storage_mode IN ('full', 'gitcoord', 'zstd')), content_commit TEXT, content_render_sha TEXT, data_blob BLOB, codec TEXT DEFAULT 'none' CHECK(codec IN ('none', 'zstd_v1')), cost_usd REAL, content_change_id TEXT, FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE, FOREIGN KEY (turn_id) REFERENCES turns(id));
CREATE TABLE execution_history (execution_id TEXT PRIMARY KEY NOT NULL REFERENCES executions (id) ON DELETE CASCADE, base_sha TEXT NOT NULL, tip_sha TEXT NOT NULL, pack BLOB, pack_idx BLOB);
CREATE TABLE execution_trigger_sources (id TEXT NOT NULL PRIMARY KEY, source_job_id TEXT NOT NULL REFERENCES jobs (id), triggered_execution_id TEXT NOT NULL REFERENCES executions (id), created_at INTEGER NOT NULL);
CREATE TABLE "executions" (id TEXT PRIMARY KEY, recipe_id TEXT NOT NULL, issue_id TEXT REFERENCES issues (id) ON DELETE CASCADE, project_id TEXT REFERENCES projects (id) ON DELETE CASCADE, status TEXT NOT NULL, started_at INTEGER NOT NULL, completed_at INTEGER, snapshot TEXT, seq INTEGER, initiator_sub TEXT, initiator_auth_mode TEXT, initiator_org_id TEXT, triggered_by TEXT NOT NULL DEFAULT 'manual');
CREATE TABLE file_changes (id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, file_path TEXT NOT NULL, status TEXT NOT NULL, additions INTEGER, deletions INTEGER, previous_path TEXT, created_at INTEGER NOT NULL);
CREATE TABLE issue_dependencies (issue_id TEXT NOT NULL REFERENCES issues (id) ON DELETE CASCADE, depends_on_uri TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (issue_id, depends_on_uri));
CREATE TABLE issue_labels (issue_id TEXT NOT NULL REFERENCES issues (id) ON DELETE CASCADE, label_id TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY (issue_id, label_id));
CREATE TABLE issue_workspaces (issue_id TEXT NOT NULL REFERENCES issues (id) ON DELETE CASCADE, execution_id TEXT NOT NULL, surface TEXT NOT NULL, layout_json TEXT NOT NULL, schema_version INTEGER NOT NULL, updated_at INTEGER NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY (issue_id, execution_id));
CREATE TABLE issues (id TEXT NOT NULL PRIMARY KEY, project_id TEXT NOT NULL, number INTEGER NOT NULL, title TEXT NOT NULL, description TEXT, status TEXT NOT NULL DEFAULT 'backlog', priority INTEGER, completed_at INTEGER, dismissed_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, model TEXT, merged_at INTEGER, closed_at INTEGER, progress TEXT NOT NULL DEFAULT 'backlog', attention TEXT NOT NULL DEFAULT 'none', parent_issue_id TEXT, parent_job_id TEXT, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (parent_issue_id) REFERENCES issues(id) ON DELETE SET NULL, FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE SET NULL);
CREATE TABLE job_browsers (id TEXT NOT NULL PRIMARY KEY, job_id TEXT, project_id TEXT, slug TEXT NOT NULL, webview_label TEXT NOT NULL, url TEXT, title TEXT, status TEXT NOT NULL, created_at INTEGER NOT NULL, closed_at INTEGER, last_active_at INTEGER, FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE);
CREATE TABLE job_terminals (id TEXT PRIMARY KEY, job_id TEXT, project_id TEXT, run_id TEXT, session_id TEXT NOT NULL, command TEXT NOT NULL, title TEXT, description TEXT, status TEXT NOT NULL, exit_code INTEGER, created_at INTEGER NOT NULL, exited_at INTEGER, slug TEXT, output_tail TEXT, FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE);
CREATE TABLE jobs (id TEXT NOT NULL PRIMARY KEY, execution_id TEXT, recipe_node_id TEXT, parent_job_id TEXT, worktree_path TEXT, branch TEXT, base_commit TEXT, current_session_id TEXT, status TEXT NOT NULL DEFAULT 'pending', agent_config_id TEXT, issue_id TEXT, project_id TEXT NOT NULL, task_description TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, completed_at INTEGER, parent_tool_use_id TEXT, task_index INTEGER, started_at INTEGER, model TEXT, node_name TEXT, base_branch TEXT, current_turn_id TEXT, resume_session_id TEXT, uri_segment TEXT, memory_review_state TEXT CHECK(memory_review_state IN ('sent', 'done')), memory_review_requested_turn INTEGER, pack_anchor TEXT, needs_fresh_session INTEGER NOT NULL DEFAULT 0, FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE, FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE SET NULL, FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (current_turn_id) REFERENCES turns(id), FOREIGN KEY (resume_session_id) REFERENCES sessions(id));
CREATE TABLE memories (id TEXT PRIMARY KEY NOT NULL, name TEXT, project_id TEXT REFERENCES projects (id), content TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'pending', 'claimed', 'promoted', 'discarded', 'deferred')), scope TEXT NOT NULL DEFAULT 'workspace' CHECK (scope IN ('project', 'role', 'workspace')), scope_value TEXT NOT NULL DEFAULT 'workspace', job_id TEXT NOT NULL REFERENCES jobs (id), node_seq INTEGER NOT NULL, promoted_commit_sha TEXT, reason TEXT, triage_decision TEXT CHECK (triage_decision IN ('promote', 'discard', 'defer')), deferred_scope TEXT, deferred_scope_value TEXT, provenance_uri TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE memory_triage_issue_memories (issue_id TEXT NOT NULL REFERENCES issues (id) ON DELETE CASCADE, memory_id TEXT NOT NULL REFERENCES memories (id) ON DELETE CASCADE, PRIMARY KEY (issue_id, memory_id));
CREATE TABLE merge_requests (id TEXT PRIMARY KEY, job_id TEXT NOT NULL, project_id TEXT NOT NULL, issue_id TEXT, title TEXT NOT NULL, body TEXT, source_branch TEXT NOT NULL, target_branch TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open', merge_method TEXT NOT NULL DEFAULT 'squash', additions INTEGER, deletions INTEGER, changed_files INTEGER, commit_count INTEGER, merged_commit TEXT, checks_json TEXT, checks_status TEXT, opened_at INTEGER NOT NULL, merged_at INTEGER, closed_at INTEGER, updated_at INTEGER NOT NULL, github_pr_number INTEGER, github_pr_url TEXT UNIQUE, github_state TEXT, github_review TEXT, github_mergeable TEXT, github_fetched_at INTEGER, is_local INTEGER NOT NULL DEFAULT 0, head_sha TEXT, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (issue_id) REFERENCES issues(id));
CREATE TABLE message_stream_chunks (id TEXT PRIMARY KEY NOT NULL, stream_id TEXT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('content', 'thinking')), chunk_index INTEGER NOT NULL, data TEXT NOT NULL, char_count INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, FOREIGN KEY (stream_id) REFERENCES message_streams (id) ON DELETE CASCADE);
CREATE TABLE message_streams (id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL, session_id TEXT, turn_id TEXT, backend TEXT NOT NULL, sequence INTEGER NOT NULL, status TEXT NOT NULL CHECK (status IN ('open', 'finalizing', 'finalized', 'aborted')), version INTEGER NOT NULL DEFAULT 0, content_chars INTEGER NOT NULL DEFAULT 0, thinking_chars INTEGER NOT NULL DEFAULT 0, chunk_count INTEGER NOT NULL DEFAULT 0, final_event_id TEXT, abort_reason TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, finalized_at INTEGER, FOREIGN KEY (run_id) REFERENCES runs (id) ON DELETE CASCADE, FOREIGN KEY (final_event_id) REFERENCES events (id) ON DELETE SET NULL);
CREATE TABLE messages (id TEXT NOT NULL PRIMARY KEY, channel_type TEXT NOT NULL CHECK(channel_type IN ('project', 'issue', 'direct')), channel_id TEXT, sender_run_id TEXT, sender_name TEXT NOT NULL, recipient_run_id TEXT, content TEXT NOT NULL, created_at INTEGER NOT NULL, urgency TEXT);
CREATE TABLE permission_requests (id TEXT NOT NULL PRIMARY KEY, run_id TEXT NOT NULL, tool_use_id TEXT NOT NULL, tool_name TEXT NOT NULL, tool_input TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', response TEXT, created_at INTEGER NOT NULL, responded_at INTEGER, turn_id TEXT, job_id TEXT, uri_segment TEXT, FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE, FOREIGN KEY (turn_id) REFERENCES turns(id), UNIQUE (run_id, tool_use_id));
CREATE TABLE pr_node_port_fires (id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions (id) ON DELETE CASCADE, recipe_node_id TEXT NOT NULL, port TEXT NOT NULL CHECK (port IN ('open', 'merge', 'close')), fired_at INTEGER NOT NULL, UNIQUE (execution_id, recipe_node_id, port));
CREATE TABLE projects (id TEXT PRIMARY KEY, team_id TEXT NOT NULL, name TEXT NOT NULL, "key" TEXT NOT NULL UNIQUE, repo_path TEXT NOT NULL, context TEXT DEFAULT '', docs_enabled INTEGER DEFAULT 1, default_branch TEXT DEFAULT 'main', next_issue_number INTEGER DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, ci_commands TEXT, setup_commands TEXT, terminal_commands TEXT, config TEXT, remote_url TEXT, hidden INTEGER NOT NULL DEFAULT 0, is_workspace INTEGER NOT NULL DEFAULT 0, FOREIGN KEY (team_id) REFERENCES teams(id));
CREATE TABLE prompts (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, questions TEXT NOT NULL, response TEXT, created_at INTEGER NOT NULL, answered_at INTEGER, turn_id TEXT, uri_segment TEXT, job_id TEXT, tool_use_id TEXT, FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE, FOREIGN KEY (turn_id) REFERENCES turns(id));
CREATE TABLE queued_messages (id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL, content TEXT NOT NULL, delivery TEXT NOT NULL, created_at INTEGER NOT NULL, delivered_at INTEGER);
CREATE TABLE resource_surfacings (id TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, uri TEXT NOT NULL, rank INTEGER NOT NULL, sim REAL NOT NULL, surfaced_at INTEGER NOT NULL, read_at INTEGER);
CREATE TABLE runs (id TEXT PRIMARY KEY, issue_id TEXT, project_id TEXT, job_id TEXT, status TEXT, session_id TEXT, error_message TEXT, started_at INTEGER, exited_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, chat_id TEXT, exit_reason TEXT, start_mode TEXT, FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE);
CREATE TABLE search_outbox (id TEXT PRIMARY KEY NOT NULL, source_table TEXT NOT NULL, source_id TEXT NOT NULL, content_type TEXT NOT NULL, op TEXT NOT NULL CHECK (op IN ('upsert', 'delete')), status TEXT NOT NULL CHECK (status IN ('pending', 'applied')), created_at INTEGER NOT NULL);
CREATE TABLE session_skyline_cache (session_id TEXT NOT NULL PRIMARY KEY, event_count INTEGER NOT NULL, latest_event_created_at INTEGER NOT NULL, bars_json TEXT NOT NULL, updated_at INTEGER NOT NULL, vibe_count INTEGER NOT NULL DEFAULT 0, latest_vibe_created_at INTEGER NOT NULL DEFAULT 0, FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE);
CREATE TABLE sessions (id TEXT PRIMARY KEY NOT NULL, job_id TEXT, chat_id TEXT, backend TEXT NOT NULL DEFAULT 'claude', status TEXT NOT NULL DEFAULT 'open', replaced_by_id TEXT, terminal_reason TEXT, sequence INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, closed_at INTEGER, updated_at INTEGER NOT NULL, backend_id TEXT, parent_session_id TEXT REFERENCES sessions (id), current_pos BLOB, channel_cursor_rowid INTEGER, FOREIGN KEY (job_id) REFERENCES jobs (id), FOREIGN KEY (replaced_by_id) REFERENCES sessions (id), CHECK ((job_id IS NOT NULL AND chat_id IS NULL) OR (job_id IS NULL AND chat_id IS NOT NULL)));
CREATE TABLE "skill_configs" (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', prompt TEXT NOT NULL, allowed_tools TEXT, model TEXT, project_id TEXT NOT NULL REFERENCES projects (id) ON DELETE CASCADE, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE suppressed_wakes (id TEXT PRIMARY KEY, subscription_id TEXT REFERENCES wake_subscriptions (id) ON DELETE SET NULL, job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, source_kind TEXT NOT NULL, source_ref TEXT, fact_kind TEXT, occurrences INTEGER NOT NULL DEFAULT 1, latest_detail_uri TEXT, content TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, delivered_at INTEGER);
CREATE TABLE todos (id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, todo_id TEXT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed')), priority TEXT CHECK (priority IN ('high', 'medium', 'low')), active_form TEXT, position INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE token_rollup (id TEXT PRIMARY KEY, day INTEGER NOT NULL, project_id TEXT, run_id TEXT NOT NULL, session_id TEXT, job_id TEXT, node_name TEXT, model TEXT, agent_config_id TEXT, backend TEXT NOT NULL, input_tokens INTEGER NOT NULL DEFAULT 0, cache_read_tokens INTEGER NOT NULL DEFAULT 0, cache_create_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0, thinking_tokens INTEGER NOT NULL DEFAULT 0, billable_tokens INTEGER NOT NULL DEFAULT 0, token_event_count INTEGER NOT NULL DEFAULT 0, exact_cost REAL NOT NULL DEFAULT 0, exact_cost_count INTEGER NOT NULL DEFAULT 0);
CREATE TABLE token_rollup_runs (run_id TEXT PRIMARY KEY, processed_through INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE tool_invocation_runs (run_id TEXT PRIMARY KEY, processed_through INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE tool_invocations (id TEXT PRIMARY KEY, event_id TEXT NOT NULL, tool_use_id TEXT, run_id TEXT NOT NULL, ts INTEGER NOT NULL, verb TEXT NOT NULL, tool_name TEXT NOT NULL, target_scheme TEXT NOT NULL, target_kind TEXT NOT NULL, target_path TEXT, is_error INTEGER NOT NULL DEFAULT 0);
CREATE TABLE turns (id TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, run_id TEXT, job_id TEXT, sequence INTEGER NOT NULL, predecessor_id TEXT, state TEXT NOT NULL DEFAULT 'pending', yield_reason TEXT, start_reason TEXT NOT NULL DEFAULT 'initial', created_at INTEGER NOT NULL, started_at INTEGER, ended_at INTEGER, updated_at INTEGER NOT NULL, FOREIGN KEY (run_id) REFERENCES runs (id), FOREIGN KEY (predecessor_id) REFERENCES turns (id));
CREATE TABLE wake_subscriptions (id TEXT PRIMARY KEY, job_id TEXT NOT NULL, source_kind TEXT NOT NULL, source_ref TEXT, fact_kinds_json TEXT, state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active', 'muted', 'unsubscribed')), mute_until_kind TEXT, mute_until_ref TEXT, created_by TEXT NOT NULL DEFAULT 'agent' CHECK(created_by IN ('agent', 'user', 'system')), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, one_shot INTEGER NOT NULL DEFAULT 0, match_phrase TEXT, FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE);
CREATE INDEX idx_action_configs_project ON action_configs (project_id);
CREATE INDEX idx_action_runs_parent_job ON action_runs (parent_job_id);
CREATE INDEX idx_action_runs_uri_segment ON action_runs (uri_segment);
CREATE INDEX idx_artifacts_job_id ON artifacts (job_id);
CREATE INDEX idx_artifacts_type ON artifacts (artifact_type);
CREATE INDEX IF NOT EXISTS idx_attention_pushes_pending ON attention_pushes (recipient, delivered_event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attention_pushes_supersede ON attention_pushes (recipient, "key") WHERE delivered_event_id IS NULL;
CREATE INDEX idx_checkpoint_cache_job ON checkpoint_command_cache (job_id);
CREATE INDEX idx_checkpoint_cache_lookup ON checkpoint_command_cache (job_id, normalized_command);
CREATE INDEX idx_checkpoint_runs_job ON checkpoint_runs (job_id, ran_at);
CREATE INDEX idx_comments_issue ON comments (issue_id);
CREATE INDEX idx_doc_references_issue ON doc_references (issue_id);
CREATE INDEX idx_event_vibes_session ON event_vibes (session_id);
CREATE INDEX idx_events_change_preview_status ON events (change_preview_status);
CREATE INDEX idx_events_parent_tool_use_id ON events (parent_tool_use_id);
CREATE INDEX idx_events_run_id ON events (run_id);
CREATE INDEX idx_events_run_sequence ON events (run_id, sequence);
CREATE INDEX idx_events_session_id ON events (session_id);
CREATE INDEX idx_events_turn_id ON events (turn_id);
CREATE INDEX IF NOT EXISTS idx_events_type_run ON events (event_type, run_id);
CREATE INDEX idx_trigger_sources_exec ON execution_trigger_sources (triggered_execution_id);
CREATE INDEX idx_trigger_sources_job ON execution_trigger_sources (source_job_id);
CREATE INDEX idx_executions_issue_seq ON executions (issue_id, seq);
CREATE INDEX idx_file_changes_job ON file_changes (job_id);
CREATE INDEX idx_file_changes_path ON file_changes (file_path);
CREATE INDEX IF NOT EXISTS idx_issue_dependencies_depends_on_uri ON issue_dependencies (depends_on_uri);
CREATE INDEX idx_issue_labels_label_id ON issue_labels (label_id);
CREATE INDEX idx_issue_workspaces_updated_at ON issue_workspaces (updated_at);
CREATE INDEX idx_issues_parent_id ON issues (parent_issue_id);
CREATE INDEX idx_issues_parent_job_id ON issues (parent_job_id);
CREATE INDEX idx_issues_project_id ON issues (project_id);
CREATE UNIQUE INDEX idx_issues_project_number ON issues (project_id, number);
CREATE INDEX idx_job_browsers_active ON job_browsers (job_id, status, last_active_at);
CREATE INDEX idx_job_browsers_job_id ON job_browsers (job_id);
CREATE UNIQUE INDEX idx_job_browsers_job_slug ON job_browsers (job_id, slug) WHERE job_id IS NOT NULL;
CREATE INDEX idx_job_browsers_project_id ON job_browsers (project_id);
CREATE UNIQUE INDEX idx_job_browsers_project_slug ON job_browsers (project_id, slug) WHERE project_id IS NOT NULL AND job_id IS NULL;
CREATE INDEX idx_job_terminals_job_id ON job_terminals (job_id);
CREATE UNIQUE INDEX idx_job_terminals_job_slug ON job_terminals (job_id, slug) WHERE job_id IS NOT NULL AND slug IS NOT NULL;
CREATE INDEX idx_job_terminals_project_id ON job_terminals (project_id);
CREATE UNIQUE INDEX idx_job_terminals_project_slug ON job_terminals (project_id, slug) WHERE project_id IS NOT NULL AND job_id IS NULL AND slug IS NOT NULL;
CREATE INDEX idx_job_terminals_run_id ON job_terminals (run_id);
CREATE UNIQUE INDEX idx_jobs_issue_execution_uri_segment ON jobs (issue_id, execution_id, uri_segment) WHERE parent_job_id IS NULL AND issue_id IS NOT NULL AND execution_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE UNIQUE INDEX idx_jobs_parent_uri_segment ON jobs (parent_job_id, uri_segment) WHERE parent_job_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE UNIQUE INDEX idx_memories_job_node_seq ON memories (job_id, node_seq);
CREATE INDEX idx_memories_pending_created ON memories (created_at) WHERE status = 'pending';
CREATE INDEX idx_memories_status_project_created ON memories (status, project_id, created_at);
CREATE INDEX idx_memory_triage_issue_memories_memory ON memory_triage_issue_memories (memory_id);
CREATE INDEX idx_mr_issue ON merge_requests (issue_id) WHERE issue_id IS NOT NULL;
CREATE UNIQUE INDEX idx_mr_job ON merge_requests (job_id);
CREATE INDEX idx_mr_project_status ON merge_requests (project_id, status);
CREATE INDEX idx_message_stream_chunks_stream_kind_index ON message_stream_chunks (stream_id, kind, chunk_index);
CREATE INDEX idx_message_streams_run_status ON message_streams (run_id, status);
CREATE INDEX idx_message_streams_session_status ON message_streams (session_id, status);
CREATE INDEX idx_messages_channel ON messages (channel_type, channel_id, created_at);
CREATE INDEX idx_messages_created_at ON messages (created_at);
CREATE INDEX idx_messages_recipient ON messages (recipient_run_id) WHERE recipient_run_id IS NOT NULL;
CREATE UNIQUE INDEX idx_permission_requests_job_uri_segment ON permission_requests (job_id, uri_segment) WHERE job_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE INDEX idx_permission_requests_run_id ON permission_requests (run_id);
CREATE INDEX idx_permission_requests_status ON permission_requests (status);
CREATE INDEX idx_permission_requests_turn_id ON permission_requests (turn_id);
CREATE INDEX idx_pr_node_port_fires_execution_node ON pr_node_port_fires (execution_id, recipe_node_id);
CREATE UNIQUE INDEX idx_prompts_job_uri_segment ON prompts (job_id, uri_segment) WHERE job_id IS NOT NULL AND uri_segment IS NOT NULL;
CREATE INDEX idx_prompts_run_id ON prompts (run_id);
CREATE INDEX idx_prompts_turn_id ON prompts (turn_id);
CREATE INDEX idx_prompts_uri_segment ON prompts (uri_segment);
CREATE INDEX idx_queued_messages_pending ON queued_messages (job_id, delivered_at);
CREATE INDEX idx_resource_surfacings_session ON resource_surfacings (session_id);
CREATE INDEX idx_resource_surfacings_session_uri ON resource_surfacings (session_id, uri);
CREATE INDEX idx_runs_chat_id ON runs (chat_id);
CREATE INDEX idx_runs_issue_id ON runs (issue_id);
CREATE INDEX idx_runs_job_id ON runs (job_id);
CREATE INDEX idx_runs_project_id ON runs (project_id);
CREATE INDEX IF NOT EXISTS idx_runs_session_id_created_at ON runs (session_id, created_at);
CREATE INDEX idx_search_outbox_status_created ON search_outbox (status, created_at);
CREATE INDEX idx_session_skyline_cache_updated_at ON session_skyline_cache (updated_at);
CREATE INDEX idx_sessions_chat_id ON sessions (chat_id);
CREATE INDEX idx_sessions_job_id ON sessions (job_id);
CREATE INDEX idx_sessions_parent_session_id ON sessions (parent_session_id);
CREATE INDEX idx_sessions_status ON sessions (status);
CREATE INDEX idx_skill_configs_project ON skill_configs (project_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_suppressed_wakes_fact_collapse ON suppressed_wakes (job_id, source_kind, COALESCE (source_ref, ''), COALESCE (fact_kind, '')) WHERE content IS NULL AND delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_suppressed_wakes_pending ON suppressed_wakes (job_id, delivered_at, created_at);
CREATE INDEX idx_todos_job ON todos (job_id);
CREATE INDEX IF NOT EXISTS idx_token_rollup_day ON token_rollup (day);
CREATE INDEX IF NOT EXISTS idx_token_rollup_project_day ON token_rollup (project_id, day);
CREATE INDEX IF NOT EXISTS idx_token_rollup_run ON token_rollup (run_id);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_run ON tool_invocations (run_id);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_shape ON tool_invocations (verb, target_scheme, target_kind);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_ts ON tool_invocations (ts);
CREATE INDEX idx_turns_job_id ON turns (job_id);
CREATE INDEX idx_turns_run_id ON turns (run_id);
CREATE INDEX idx_turns_session_id ON turns (session_id);
CREATE UNIQUE INDEX idx_turns_session_sequence ON turns (session_id, sequence);
CREATE INDEX IF NOT EXISTS idx_wake_subscriptions_job ON wake_subscriptions (job_id, state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wake_subscriptions_unique_scope ON wake_subscriptions (job_id, source_kind, COALESCE (source_ref, ''), COALESCE (fact_kinds_json, ''));
CREATE TRIGGER search_artifacts_delete AFTER DELETE ON artifacts BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'artifacts', OLD.id, 'artifact', 'delete', 'pending', unixepoch ());
END;
CREATE TRIGGER search_artifacts_insert AFTER INSERT ON artifacts WHEN NEW.job_id IS NOT NULL BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'artifacts', NEW.id, 'artifact', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_artifacts_update AFTER UPDATE ON artifacts WHEN OLD.data != NEW.data OR OLD.job_id IS NOT NEW.job_id OR OLD.artifact_type != NEW.artifact_type BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'artifacts', NEW.id, 'artifact', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_comments_delete AFTER DELETE ON comments BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'comments', OLD.id, 'comment', 'delete', 'pending', unixepoch ());
END;
CREATE TRIGGER search_comments_insert AFTER INSERT ON comments BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'comments', NEW.id, 'comment', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_comments_update AFTER UPDATE ON comments WHEN OLD.content != NEW.content OR OLD.issue_id != NEW.issue_id BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'comments', NEW.id, 'comment', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_events_delete AFTER DELETE ON events BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'events', OLD.id, 'event', 'delete', 'pending', unixepoch ());
END;
CREATE TRIGGER search_events_insert AFTER INSERT ON events WHEN NEW.event_type IN ('text', 'tool_result', 'user') AND json_extract (NEW.data, '$.content') IS NOT NULL BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'events', NEW.id, 'event', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_events_update AFTER UPDATE ON events WHEN NEW.event_type IN ('text', 'tool_result', 'user') AND OLD.data != NEW.data AND json_extract (NEW.data, '$.content') IS NOT NULL BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'events', NEW.id, 'event', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_issues_delete AFTER DELETE ON issues BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'issues', OLD.id, 'issue', 'delete', 'pending', unixepoch ());
END;
CREATE TRIGGER search_issues_insert AFTER INSERT ON issues BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_issues_update AFTER UPDATE ON issues WHEN OLD.title != NEW.title OR OLD.description IS NOT NEW.description OR OLD.project_id != NEW.project_id BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'issues', NEW.id, 'issue', 'upsert', 'pending', NEW.updated_at);
END;
CREATE TRIGGER search_messages_delete AFTER DELETE ON messages BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'messages', OLD.id, 'message', 'delete', 'pending', unixepoch ());
END;
CREATE TRIGGER search_messages_insert AFTER INSERT ON messages BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;
CREATE TRIGGER search_messages_update AFTER UPDATE ON messages WHEN OLD.content != NEW.content OR OLD.channel_type != NEW.channel_type OR OLD.channel_id IS NOT NEW.channel_id BEGIN
INSERT INTO search_outbox (id, source_table, source_id, content_type, op, status, created_at) VALUES (lower (hex (randomblob (16))), 'messages', NEW.id, 'message', 'upsert', 'pending', NEW.created_at);
END;
