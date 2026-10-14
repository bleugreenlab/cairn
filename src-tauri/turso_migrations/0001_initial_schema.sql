-- Initial local runtime schema for Turso MVCC.
-- Excludes historical migration metadata and legacy FTS5 virtual tables/triggers.

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    name TEXT NOT NULL,
    key TEXT NOT NULL UNIQUE,
    repo_path TEXT NOT NULL,
    context TEXT DEFAULT '',
    docs_enabled INTEGER DEFAULT 1,
    default_branch TEXT DEFAULT 'main',
    next_issue_number INTEGER DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    ci_commands TEXT,
    setup_commands TEXT,
    terminal_commands TEXT,
    config TEXT
, remote_url TEXT, hidden INTEGER NOT NULL DEFAULT 0, server_id TEXT REFERENCES servers(id));

CREATE TABLE comments (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('user', 'agent')),
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_comments_issue ON comments(issue_id);

CREATE TABLE github_app (
    id TEXT PRIMARY KEY DEFAULT 'default',
    app_id INTEGER,
    app_name TEXT,
    app_slug TEXT,
    private_key TEXT,
    webhook_secret TEXT,
    installation_id INTEGER,
    relay_channel_id TEXT,
    relay_secret TEXT,
    last_event_sync TEXT,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
, relay_public_key TEXT, relay_private_key_encrypted TEXT);

CREATE TABLE ci_logs_cache (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    job_name TEXT NOT NULL,
    log_content TEXT,
    fetched_at TEXT NOT NULL,
    UNIQUE(run_id, job_name)
);

CREATE TABLE webhook_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    action TEXT NOT NULL,
    repo_full_name TEXT NOT NULL,
    pr_number INTEGER,
    payload_summary TEXT NOT NULL,
    processed_at INTEGER NOT NULL
);

CREATE INDEX idx_webhook_events_pr ON webhook_events(repo_full_name, pr_number);

CREATE INDEX idx_webhook_events_processed ON webhook_events(processed_at);

CREATE TABLE doc_references (
    id TEXT PRIMARY KEY,
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    doc_path TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(issue_id, doc_path)
);

CREATE INDEX idx_doc_references_issue ON doc_references(issue_id);

CREATE TABLE IF NOT EXISTS "artifacts" (
    id TEXT PRIMARY KEY,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    data TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    parent_version_id TEXT REFERENCES artifacts(id),
    output_name TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
, seen_at INTEGER);

CREATE INDEX idx_artifacts_job_id ON artifacts(job_id);

CREATE INDEX idx_artifacts_type ON artifacts(artifact_type);

CREATE TABLE chats (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    current_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_chats_project_id ON chats(project_id);

CREATE TABLE IF NOT EXISTS "workspaces" (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    default_model TEXT,
    system_prompt TEXT,
    branch_prefix TEXT,
    max_thinking_tokens INTEGER,
    merge_type TEXT,
    pull_on_merge INTEGER,
    agent_sync_preference TEXT,
    auto_start_jobs INTEGER NOT NULL DEFAULT 0
, timezone TEXT DEFAULT 'system');

CREATE TABLE checkpoint_command_cache (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    command TEXT NOT NULL,
    normalized_command TEXT NOT NULL,
    exit_code INTEGER NOT NULL,
    commit_sha TEXT NOT NULL,
    is_dirty INTEGER NOT NULL DEFAULT 0,
    ran_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_checkpoint_cache_job ON checkpoint_command_cache(job_id);

CREATE INDEX idx_checkpoint_cache_lookup ON checkpoint_command_cache(job_id, normalized_command);

CREATE TABLE IF NOT EXISTS "runs" (
    id TEXT PRIMARY KEY,
    issue_id TEXT REFERENCES issues(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    status TEXT,
    session_id TEXT,
    error_message TEXT,
    started_at INTEGER,
    exited_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    chat_id TEXT REFERENCES chats(id) ON DELETE CASCADE
, backend TEXT, exit_reason TEXT, start_mode TEXT);

CREATE INDEX idx_runs_issue_id ON runs(issue_id);

CREATE INDEX idx_runs_project_id ON runs(project_id);

CREATE INDEX idx_runs_job_id ON runs(job_id);

CREATE INDEX idx_runs_chat_id ON runs(chat_id);

CREATE TABLE IF NOT EXISTS "job_terminals" (
    id TEXT PRIMARY KEY,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    command TEXT NOT NULL,
    title TEXT,
    description TEXT,
    status TEXT NOT NULL,
    exit_code INTEGER,
    created_at INTEGER NOT NULL,
    exited_at INTEGER
, slug TEXT);

CREATE INDEX idx_job_terminals_job_id ON job_terminals(job_id);

CREATE INDEX idx_job_terminals_project_id ON job_terminals(project_id);

CREATE INDEX idx_job_terminals_run_id ON job_terminals(run_id);

CREATE TABLE IF NOT EXISTS "action_configs" (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    command_template TEXT,
    input_schema TEXT,
    output_schema TEXT,
    is_builtin INTEGER NOT NULL DEFAULT 0,
    workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    tool_name TEXT,
    tool_description TEXT,
    CHECK ((workspace_id IS NULL) != (project_id IS NULL))
);

CREATE INDEX idx_action_configs_workspace ON action_configs(workspace_id);

CREATE INDEX idx_action_configs_project ON action_configs(project_id);

CREATE TABLE IF NOT EXISTS "skill_configs" (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    prompt TEXT NOT NULL,
    allowed_tools TEXT,
    model TEXT,
    workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((workspace_id IS NULL) != (project_id IS NULL))
);

CREATE INDEX idx_skill_configs_workspace ON skill_configs(workspace_id);

CREATE INDEX idx_skill_configs_project ON skill_configs(project_id);

CREATE TABLE IF NOT EXISTS "events" (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    session_id TEXT,
    sequence INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    data TEXT NOT NULL,
    parent_tool_use_id TEXT,
    created_at INTEGER NOT NULL
, input_tokens INTEGER, cache_read_tokens INTEGER, cache_create_tokens INTEGER, output_tokens INTEGER, turn_id TEXT REFERENCES turns(id));

CREATE INDEX idx_events_run_id ON events(run_id);

CREATE INDEX idx_events_run_sequence ON events(run_id, sequence);

CREATE INDEX idx_events_session_id ON events(session_id);

CREATE INDEX idx_events_parent_tool_use_id ON events(parent_tool_use_id);

CREATE TABLE IF NOT EXISTS "prompts" (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    questions TEXT NOT NULL,
    response TEXT,
    created_at INTEGER NOT NULL,
    answered_at INTEGER
, turn_id TEXT REFERENCES turns(id));

CREATE INDEX idx_prompts_run_id ON prompts(run_id);

CREATE TABLE IF NOT EXISTS "executions" (
    id TEXT PRIMARY KEY,
    recipe_id TEXT NOT NULL,  -- No longer references recipes(id)
    issue_id TEXT REFERENCES issues(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    snapshot TEXT
, seq INTEGER, initiator_sub TEXT, initiator_auth_mode TEXT, initiator_org_id TEXT, triggered_by TEXT NOT NULL DEFAULT 'manual');

CREATE TABLE IF NOT EXISTS "action_runs" (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    recipe_node_id TEXT NOT NULL,  -- No longer references recipe_nodes(id)
    action_config_id TEXT NOT NULL,
    issue_id TEXT REFERENCES issues(id) ON DELETE SET NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    inputs TEXT,
    output TEXT,
    error_message TEXT,
    started_at INTEGER,
    completed_at INTEGER,
    created_at INTEGER NOT NULL
, parent_job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE);

CREATE TABLE IF NOT EXISTS "condition_evaluations" (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    recipe_node_id TEXT NOT NULL,  -- No longer references recipe_nodes(id)
    result_port TEXT NOT NULL,
    raw_result TEXT,
    error_message TEXT,
    evaluated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "artifact_content" (
    id TEXT PRIMARY KEY,
    artifact_node_id TEXT NOT NULL,  -- No longer references recipe_nodes(id)
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    data TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "jobs" (
    id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT REFERENCES executions(id) ON DELETE CASCADE,
    recipe_node_id TEXT,  -- No longer references recipe_nodes(id)
    parent_job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    worktree_path TEXT,
    branch TEXT,
    base_commit TEXT,
    current_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    agent_config_id TEXT,
    issue_id TEXT REFERENCES issues(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_description TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    parent_tool_use_id TEXT,
    task_index INTEGER,
    started_at INTEGER
, model TEXT, node_name TEXT, base_branch TEXT, current_turn_id TEXT REFERENCES turns(id), manager_id TEXT REFERENCES managers(id), resume_session_id TEXT REFERENCES sessions(id));

CREATE TABLE permission_requests (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    tool_use_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_input TEXT NOT NULL,  -- JSON string
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, allowed, denied
    response TEXT,  -- JSON response (includes message for denied)
    created_at INTEGER NOT NULL,
    responded_at INTEGER, turn_id TEXT REFERENCES turns(id),
    UNIQUE(run_id, tool_use_id)
);

CREATE INDEX idx_permission_requests_run_id ON permission_requests(run_id);

CREATE INDEX idx_permission_requests_status ON permission_requests(status);

CREATE TABLE github_installations (
    id TEXT PRIMARY KEY NOT NULL,
    account_login TEXT NOT NULL UNIQUE,
    account_type TEXT NOT NULL,
    installation_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_github_installations_account ON github_installations(account_login);

CREATE INDEX idx_github_installations_installation_id ON github_installations(installation_id);

CREATE TABLE pending_injections (
    id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,           -- Target Claude session
    injection_type TEXT NOT NULL,       -- "user_message" | "terminal_context" | "system"
    content TEXT NOT NULL,              -- The message to inject
    source_id TEXT,                     -- e.g., terminal session_id for terminal_context
    priority INTEGER NOT NULL DEFAULT 0, -- Higher = inject first
    status TEXT NOT NULL DEFAULT 'pending', -- "pending" | "injected" | "cancelled"
    created_at INTEGER NOT NULL,
    injected_at INTEGER
);

CREATE INDEX idx_pending_injections_session ON pending_injections(session_id, status);

CREATE INDEX idx_pending_injections_status ON pending_injections(status, priority DESC);

CREATE UNIQUE INDEX idx_job_terminals_job_slug ON job_terminals(job_id, slug) WHERE job_id IS NOT NULL AND slug IS NOT NULL;

CREATE UNIQUE INDEX idx_job_terminals_project_slug ON job_terminals(project_id, slug) WHERE project_id IS NOT NULL AND job_id IS NULL AND slug IS NOT NULL;

CREATE INDEX idx_action_runs_parent_job ON action_runs(parent_job_id);

CREATE TABLE file_changes (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    status TEXT NOT NULL,           -- added/modified/deleted/renamed
    additions INTEGER,
    deletions INTEGER,
    previous_path TEXT,             -- for renames
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_file_changes_path ON file_changes(file_path);

CREATE INDEX idx_file_changes_job ON file_changes(job_id);

CREATE INDEX idx_executions_issue_seq ON executions(issue_id, seq);

CREATE TABLE IF NOT EXISTS "memories" (
    id               TEXT PRIMARY KEY NOT NULL,
    project_id       TEXT REFERENCES projects(id) ON DELETE CASCADE,
    content          TEXT NOT NULL,
    confidence       TEXT NOT NULL DEFAULT 'tentative',
    source_issue     TEXT,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    surfaced_count   INTEGER NOT NULL DEFAULT 0,
    last_surfaced_at INTEGER,
    active           INTEGER NOT NULL DEFAULT 1
, scope TEXT NOT NULL DEFAULT 'project', keywords TEXT, source_run_id TEXT);

CREATE TABLE memory_triggers (
    id            INTEGER PRIMARY KEY NOT NULL,
    memory_id     TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
    trigger_index INTEGER NOT NULL,
    json_path     TEXT NOT NULL,
    pattern       TEXT NOT NULL
);

CREATE INDEX idx_memories_active_project ON memories(active, project_id);

CREATE INDEX idx_triggers_memory ON memory_triggers(memory_id);

CREATE TABLE messages (
    id         TEXT PRIMARY KEY NOT NULL,
    -- Channel routing
    channel_type TEXT NOT NULL CHECK (channel_type IN ('project', 'issue', 'direct')),
    channel_id   TEXT,              -- project_id or issue_id; NULL for direct messages
    -- Sender info
    sender_run_id  TEXT,            -- NULL for system messages
    sender_name    TEXT NOT NULL,   -- node name (e.g. "builder-1") or "system"
    -- Recipient (direct messages only)
    recipient_run_id TEXT,          -- target run_id for direct messages
    -- Content
    content    TEXT NOT NULL,
    created_at INTEGER NOT NULL
, recipient_manager_id TEXT REFERENCES managers(id));

CREATE INDEX idx_messages_channel ON messages(channel_type, channel_id, created_at);

CREATE INDEX idx_messages_recipient ON messages(recipient_run_id) WHERE recipient_run_id IS NOT NULL;

CREATE INDEX idx_messages_created_at ON messages(created_at);

CREATE TABLE event_embeddings (
    event_id TEXT PRIMARY KEY NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    embedding BLOB NOT NULL,
    model_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_event_embeddings_created_at ON event_embeddings(created_at);

CREATE TABLE todos (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    todo_id TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'in_progress', 'completed')),
    priority TEXT CHECK(priority IN ('high', 'medium', 'low')),
    active_form TEXT,
    position INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_todos_job ON todos(job_id);

CREATE TABLE IF NOT EXISTS "issues" (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'backlog',
    priority INTEGER,
    completed_at INTEGER,
    dismissed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    model TEXT,
    merged_at INTEGER,
    closed_at INTEGER
, manager_id TEXT REFERENCES managers(id), progress TEXT NOT NULL DEFAULT 'backlog', attention TEXT NOT NULL DEFAULT 'none');

CREATE INDEX idx_issues_project_id ON issues(project_id);

CREATE UNIQUE INDEX idx_issues_project_number ON issues(project_id, number);

CREATE TABLE IF NOT EXISTS issue_dependencies (
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    depends_on_uri TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, depends_on_uri)
);

CREATE INDEX IF NOT EXISTS idx_issue_dependencies_depends_on_uri
    ON issue_dependencies(depends_on_uri);

CREATE TABLE IF NOT EXISTS issue_embeddings (
    issue_id TEXT PRIMARY KEY NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    embedding BLOB NOT NULL,
    model_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS issue_dependencies (
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    depends_on_uri TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, depends_on_uri)
);

CREATE INDEX IF NOT EXISTS idx_issue_dependencies_depends_on_uri
    ON issue_dependencies(depends_on_uri);

CREATE TABLE IF NOT EXISTS issue_embeddings (
    issue_id TEXT PRIMARY KEY NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    embedding BLOB NOT NULL,
    model_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE account (
    user_id TEXT PRIMARY KEY NOT NULL,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    device_id TEXT NOT NULL,
    plan TEXT NOT NULL DEFAULT 'free',
    jwt_encrypted TEXT,
    jwt_expires_at INTEGER,
    org_memberships TEXT,  -- JSON: [{org_id, org_name, role}]
    connected_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "servers" (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    org_id TEXT,
    status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (status IN ('unknown', 'connected', 'offline', 'error')),
    version TEXT,
    error_message TEXT,
    excluded_project_ids TEXT,
    last_seen_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "managers" (
    id                TEXT PRIMARY KEY NOT NULL,
    project_id        TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    branch            TEXT NOT NULL,
    job_id            TEXT REFERENCES jobs(id),
    status            TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'completed')),
    agent_config_id   TEXT,
    model             TEXT,
    parent_manager_id TEXT REFERENCES managers(id),
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
, execution_id TEXT REFERENCES executions(id), home_project_id TEXT REFERENCES projects(id), scope_kind TEXT NOT NULL DEFAULT 'branch' CHECK (scope_kind IN ('branch', 'project', 'workspace')), current_session_id TEXT REFERENCES sessions(id), current_turn_id TEXT REFERENCES turns(id), last_wake_at INTEGER, last_turn_completed_at INTEGER, last_error TEXT);

CREATE TABLE trigger_accumulator_state (
    id TEXT PRIMARY KEY NOT NULL,
    recipe_id TEXT NOT NULL,
    group_key TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    events TEXT NOT NULL DEFAULT '[]',
    event_count INTEGER NOT NULL DEFAULT 0,
    seen_event_ids TEXT NOT NULL DEFAULT '[]',
    first_event_at INTEGER NOT NULL,
    last_event_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_accumulator_unique
    ON trigger_accumulator_state(recipe_id, group_key, scope_key);

CREATE INDEX idx_accumulator_recipe
    ON trigger_accumulator_state(recipe_id);

CREATE TABLE execution_trigger_sources (
    id TEXT NOT NULL PRIMARY KEY,
    source_job_id TEXT NOT NULL REFERENCES jobs(id),
    triggered_execution_id TEXT NOT NULL REFERENCES executions(id),
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_trigger_sources_job ON execution_trigger_sources(source_job_id);

CREATE INDEX idx_trigger_sources_exec ON execution_trigger_sources(triggered_execution_id);

CREATE TABLE turns (
    id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    run_id TEXT,
    job_id TEXT,
    sequence INTEGER NOT NULL,
    predecessor_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    yield_reason TEXT,
    start_reason TEXT NOT NULL DEFAULT 'initial',
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    ended_at INTEGER,
    updated_at INTEGER NOT NULL, manager_id TEXT REFERENCES managers(id),
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (predecessor_id) REFERENCES turns(id)
);

CREATE UNIQUE INDEX idx_turns_session_sequence ON turns(session_id, sequence);

CREATE INDEX idx_turns_session_id ON turns(session_id);

CREATE INDEX idx_turns_run_id ON turns(run_id);

CREATE INDEX idx_turns_job_id ON turns(job_id);

CREATE INDEX idx_events_turn_id ON events(turn_id);

CREATE INDEX idx_prompts_turn_id ON prompts(turn_id);

CREATE INDEX idx_permission_requests_turn_id ON permission_requests(turn_id);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT,
    chat_id TEXT,
    backend TEXT NOT NULL DEFAULT 'claude',
    status TEXT NOT NULL DEFAULT 'open',
    replaced_by_id TEXT,
    terminal_reason TEXT,
    sequence INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    closed_at INTEGER,
    updated_at INTEGER NOT NULL, backend_id TEXT, parent_session_id TEXT REFERENCES sessions(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (chat_id) REFERENCES chats(id),
    FOREIGN KEY (replaced_by_id) REFERENCES sessions(id),
    CHECK ((job_id IS NOT NULL AND chat_id IS NULL) OR (job_id IS NULL AND chat_id IS NOT NULL))
);

CREATE INDEX idx_sessions_job_id ON sessions(job_id);

CREATE INDEX idx_sessions_chat_id ON sessions(chat_id);

CREATE INDEX idx_sessions_status ON sessions(status);

CREATE TABLE effect_outbox (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending', 'running', 'done', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_effect_outbox_state ON effect_outbox(state);

CREATE INDEX idx_effect_outbox_kind_dedupe ON effect_outbox(kind, dedupe_key);

CREATE TABLE merge_requests (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    project_id      TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,

    -- Ownership: exactly one of these is set
    issue_id        TEXT REFERENCES issues(id),
    manager_id      TEXT REFERENCES managers(id),

    -- Authoritative state (Cairn-owned)
    title           TEXT NOT NULL,
    body            TEXT,
    source_branch   TEXT NOT NULL,
    target_branch   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',           -- open | merged | closed
    merge_method    TEXT NOT NULL DEFAULT 'squash',
    additions       INTEGER,
    deletions       INTEGER,
    changed_files   INTEGER,
    commit_count    INTEGER,
    merged_commit   TEXT,
    checks_json     TEXT,
    checks_status   TEXT,
    opened_at       INTEGER NOT NULL,
    merged_at       INTEGER,
    closed_at       INTEGER,
    updated_at      INTEGER NOT NULL,

    -- GitHub sync (all nullable — only populated when GH integration active)
    github_pr_number    INTEGER,
    github_pr_url       TEXT UNIQUE,
    github_state        TEXT,
    github_review       TEXT,
    github_mergeable    TEXT,
    github_fetched_at   INTEGER
);

CREATE UNIQUE INDEX idx_mr_job ON merge_requests(job_id);

CREATE INDEX idx_mr_issue ON merge_requests(issue_id) WHERE issue_id IS NOT NULL;

CREATE INDEX idx_mr_manager ON merge_requests(manager_id) WHERE manager_id IS NOT NULL;

CREATE INDEX idx_mr_project_status ON merge_requests(project_id, status);

CREATE TABLE message_streams (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL,
    session_id TEXT,
    turn_id TEXT,
    backend TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'finalizing', 'finalized', 'aborted')),
    version INTEGER NOT NULL DEFAULT 0,
    content_chars INTEGER NOT NULL DEFAULT 0,
    thinking_chars INTEGER NOT NULL DEFAULT 0,
    chunk_count INTEGER NOT NULL DEFAULT 0,
    final_event_id TEXT,
    abort_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    finalized_at INTEGER,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (final_event_id) REFERENCES events(id) ON DELETE SET NULL
);

CREATE TABLE message_stream_chunks (
    id TEXT PRIMARY KEY NOT NULL,
    stream_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('content', 'thinking')),
    chunk_index INTEGER NOT NULL,
    data TEXT NOT NULL,
    char_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES message_streams(id) ON DELETE CASCADE
);

CREATE INDEX idx_message_streams_run_status
    ON message_streams(run_id, status);

CREATE INDEX idx_message_streams_session_status
    ON message_streams(session_id, status);

CREATE INDEX idx_message_stream_chunks_stream_kind_index
    ON message_stream_chunks(stream_id, kind, chunk_index);

CREATE TABLE manager_scopes (
    id TEXT PRIMARY KEY NOT NULL,
    manager_id TEXT NOT NULL REFERENCES managers(id) ON DELETE CASCADE,
    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('branch', 'project', 'workspace')),
    branch TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_manager_scopes_manager_id ON manager_scopes(manager_id);

CREATE INDEX idx_manager_scopes_project_id ON manager_scopes(project_id);

CREATE TABLE manager_wake_batches (
    id TEXT PRIMARY KEY NOT NULL,
    manager_id TEXT NOT NULL REFERENCES managers(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    outcome TEXT
);

CREATE INDEX idx_manager_wake_batches_manager_id ON manager_wake_batches(manager_id, created_at DESC);

CREATE TABLE manager_mailbox (
    id TEXT PRIMARY KEY NOT NULL,
    manager_id TEXT NOT NULL REFERENCES managers(id) ON DELETE CASCADE,
    cause_type TEXT NOT NULL,
    cause_json TEXT NOT NULL,
    delivery_policy TEXT NOT NULL,
    dedupe_key TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    claimed_at INTEGER,
    processed_at INTEGER,
    superseded_by TEXT REFERENCES manager_mailbox(id),
    source_run_id TEXT REFERENCES runs(id),
    source_issue_id TEXT REFERENCES issues(id),
    source_project_id TEXT REFERENCES projects(id),
    wake_batch_id TEXT REFERENCES manager_wake_batches(id)
);

CREATE INDEX idx_manager_mailbox_pending ON manager_mailbox(manager_id, processed_at, available_at, created_at);

CREATE INDEX idx_manager_mailbox_dedupe ON manager_mailbox(manager_id, dedupe_key);

CREATE INDEX idx_jobs_manager_id ON jobs(manager_id);

CREATE INDEX idx_turns_manager_id ON turns(manager_id);

CREATE INDEX idx_messages_recipient_manager_id ON messages(recipient_manager_id);

CREATE INDEX idx_sessions_parent_session_id ON sessions(parent_session_id);

CREATE TABLE issue_workspaces (
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    execution_id TEXT NOT NULL,
    surface TEXT NOT NULL,
    layout_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    PRIMARY KEY (issue_id, execution_id)
);

CREATE INDEX idx_issue_workspaces_updated_at ON issue_workspaces(updated_at);

CREATE TABLE session_skyline_cache (
    session_id TEXT PRIMARY KEY NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    event_count INTEGER NOT NULL,
    latest_event_created_at INTEGER NOT NULL,
    bars_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_session_skyline_cache_updated_at ON session_skyline_cache(updated_at);
