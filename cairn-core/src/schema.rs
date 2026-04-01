// @generated automatically by Diesel CLI.

diesel::table! {
    account (user_id) {
        user_id -> Text,
        email -> Text,
        name -> Text,
        device_id -> Text,
        plan -> Text,
        jwt_encrypted -> Nullable<Text>,
        jwt_expires_at -> Nullable<Integer>,
        org_memberships -> Nullable<Text>,
        connected_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    action_configs (id) {
        id -> Text,
        name -> Text,
        description -> Text,
        command_template -> Nullable<Text>,
        input_schema -> Nullable<Text>,
        output_schema -> Nullable<Text>,
        is_builtin -> Integer,
        workspace_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        tool_name -> Nullable<Text>,
        tool_description -> Nullable<Text>,
    }
}

diesel::table! {
    action_runs (id) {
        id -> Text,
        execution_id -> Text,
        recipe_node_id -> Text,
        action_config_id -> Text,
        issue_id -> Nullable<Text>,
        project_id -> Text,
        status -> Text,
        inputs -> Nullable<Text>,
        output -> Nullable<Text>,
        error_message -> Nullable<Text>,
        started_at -> Nullable<Integer>,
        completed_at -> Nullable<Integer>,
        created_at -> Integer,
        parent_job_id -> Nullable<Text>,
    }
}

diesel::table! {
    servers (id) {
        id -> Text,
        name -> Text,
        url -> Text,
        org_id -> Nullable<Text>,
        status -> Text,
        version -> Nullable<Text>,
        error_message -> Nullable<Text>,
        excluded_project_ids -> Nullable<Text>,
        last_seen_at -> Nullable<Integer>,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    skill_configs (id) {
        id -> Text,
        name -> Text,
        description -> Text,
        prompt -> Text,
        allowed_tools -> Nullable<Text>,
        model -> Nullable<Text>,
        workspace_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    artifacts (id) {
        id -> Text,
        job_id -> Nullable<Text>,
        artifact_type -> Text,
        schema_version -> Integer,
        data -> Text,
        version -> Integer,
        parent_version_id -> Nullable<Text>,
        output_name -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        seen_at -> Nullable<Integer>,
    }
}

diesel::table! {
    job_terminals (id) {
        id -> Text,
        job_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        run_id -> Nullable<Text>,
        session_id -> Text,
        command -> Text,
        title -> Nullable<Text>,
        description -> Nullable<Text>,
        status -> Text,
        exit_code -> Nullable<Integer>,
        created_at -> Integer,
        exited_at -> Nullable<Integer>,
        slug -> Nullable<Text>,
    }
}

diesel::table! {
    chats (id) {
        id -> Text,
        project_id -> Text,
        current_session_id -> Nullable<Text>,
        status -> Text,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    checkpoint_command_cache (id) {
        id -> Text,
        job_id -> Text,
        command -> Text,
        normalized_command -> Text,
        exit_code -> Integer,
        commit_sha -> Text,
        is_dirty -> Integer,
        ran_at -> Integer,
        created_at -> Integer,
    }
}

diesel::table! {
    ci_logs_cache (id) {
        id -> Integer,
        run_id -> Integer,
        job_name -> Text,
        log_content -> Nullable<Text>,
        fetched_at -> Text,
    }
}

diesel::table! {
    comments (id) {
        id -> Text,
        issue_id -> Text,
        content -> Text,
        source -> Text,
        created_at -> Integer,
    }
}

diesel::table! {
    custom_mcp_servers (id) {
        id -> Text,
        workspace_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        name -> Text,
        server_type -> Text,
        command -> Text,
        args -> Nullable<Text>,
        env -> Nullable<Text>,
        enabled -> Integer,
        discovered_tools -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    doc_references (id) {
        id -> Text,
        issue_id -> Text,
        doc_path -> Text,
        created_at -> Integer,
    }
}

diesel::table! {
    memories (id) {
        id -> Text,
        project_id -> Nullable<Text>,
        content -> Text,
        confidence -> Text,
        source_issue -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        surfaced_count -> Integer,
        last_surfaced_at -> Nullable<Integer>,
        active -> Integer,
        scope -> Text,
        keywords -> Nullable<Text>,
        source_run_id -> Nullable<Text>,
    }
}

diesel::table! {
    memory_triggers (id) {
        id -> Integer,
        memory_id -> Text,
        trigger_index -> Integer,
        json_path -> Text,
        pattern -> Text,
    }
}

diesel::table! {
    message_stream_chunks (id) {
        id -> Text,
        stream_id -> Text,
        kind -> Text,
        chunk_index -> Integer,
        data -> Text,
        char_count -> Integer,
        created_at -> Integer,
    }
}

diesel::table! {
    message_streams (id) {
        id -> Text,
        run_id -> Text,
        session_id -> Nullable<Text>,
        turn_id -> Nullable<Text>,
        backend -> Text,
        sequence -> Integer,
        status -> Text,
        version -> Integer,
        content_chars -> Integer,
        thinking_chars -> Integer,
        chunk_count -> Integer,
        final_event_id -> Nullable<Text>,
        abort_reason -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        finalized_at -> Nullable<Integer>,
    }
}

diesel::table! {
    events (id) {
        id -> Text,
        run_id -> Text,
        session_id -> Nullable<Text>,
        sequence -> Integer,
        timestamp -> Integer,
        event_type -> Text,
        data -> Text,
        parent_tool_use_id -> Nullable<Text>,
        created_at -> Integer,
        input_tokens -> Nullable<Integer>,
        cache_read_tokens -> Nullable<Integer>,
        cache_create_tokens -> Nullable<Integer>,
        output_tokens -> Nullable<Integer>,
        turn_id -> Nullable<Text>,
    }
}

diesel::table! {
    effect_outbox (id) {
        id -> Text,
        kind -> Text,
        dedupe_key -> Text,
        payload_json -> Text,
        state -> Text,
        attempts -> Integer,
        last_error -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    event_embeddings (event_id) {
        event_id -> Text,
        embedding -> Binary,
        model_name -> Text,
        dimensions -> Integer,
        created_at -> Integer,
    }
}

diesel::table! {
    file_changes (id) {
        id -> Text,
        job_id -> Text,
        file_path -> Text,
        status -> Text,
        additions -> Nullable<Integer>,
        deletions -> Nullable<Integer>,
        previous_path -> Nullable<Text>,
        created_at -> Integer,
    }
}
diesel::table! {
    executions (id) {
        id -> Text,
        recipe_id -> Text,
        issue_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        status -> Text,
        started_at -> Integer,
        completed_at -> Nullable<Integer>,
        snapshot -> Nullable<Text>,
        seq -> Nullable<Integer>,
        initiator_sub -> Nullable<Text>,
        initiator_auth_mode -> Nullable<Text>,
        initiator_org_id -> Nullable<Text>,
        triggered_by -> Text,
    }
}

diesel::table! {
    execution_trigger_sources (id) {
        id -> Text,
        source_job_id -> Text,
        triggered_execution_id -> Text,
        created_at -> Integer,
    }
}

diesel::table! {
    github_app (id) {
        id -> Text,
        app_id -> Nullable<Integer>,
        app_name -> Nullable<Text>,
        app_slug -> Nullable<Text>,
        private_key -> Nullable<Text>,
        webhook_secret -> Nullable<Text>,
        installation_id -> Nullable<Integer>,
        relay_channel_id -> Nullable<Text>,
        relay_secret -> Nullable<Text>,
        last_event_sync -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        relay_public_key -> Nullable<Text>,
        relay_private_key_encrypted -> Nullable<Text>,
    }
}

diesel::table! {
    github_installations (id) {
        id -> Text,
        account_login -> Text,
        account_type -> Text,
        installation_id -> Integer,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    issues (id) {
        id -> Text,
        project_id -> Text,
        number -> Integer,
        title -> Text,
        description -> Nullable<Text>,
        status -> Text,
        progress -> Text,
        attention -> Text,
        priority -> Nullable<Integer>,
        completed_at -> Nullable<Integer>,
        dismissed_at -> Nullable<Integer>,
        created_at -> Integer,
        updated_at -> Integer,
        model -> Nullable<Text>,
        merged_at -> Nullable<Integer>,
        closed_at -> Nullable<Integer>,
        manager_id -> Nullable<Text>,
    }
}

diesel::table! {
    managers (id) {
        id -> Text,
        project_id -> Text,
        home_project_id -> Nullable<Text>,
        scope_kind -> Text,
        name -> Text,
        description -> Text,
        branch -> Nullable<Text>,
        job_id -> Nullable<Text>,
        status -> Text,
        current_session_id -> Nullable<Text>,
        current_turn_id -> Nullable<Text>,
        last_wake_at -> Nullable<Integer>,
        last_turn_completed_at -> Nullable<Integer>,
        last_error -> Nullable<Text>,
        agent_config_id -> Nullable<Text>,
        model -> Nullable<Text>,
        parent_manager_id -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        execution_id -> Nullable<Text>,
    }
}

diesel::table! {
    manager_mailbox (id) {
        id -> Text,
        manager_id -> Text,
        cause_type -> Text,
        cause_json -> Text,
        delivery_policy -> Text,
        dedupe_key -> Nullable<Text>,
        priority -> Integer,
        available_at -> Integer,
        created_at -> Integer,
        claimed_at -> Nullable<Integer>,
        processed_at -> Nullable<Integer>,
        superseded_by -> Nullable<Text>,
        source_run_id -> Nullable<Text>,
        source_issue_id -> Nullable<Text>,
        source_project_id -> Nullable<Text>,
        wake_batch_id -> Nullable<Text>,
    }
}

diesel::table! {
    manager_scopes (id) {
        id -> Text,
        manager_id -> Text,
        project_id -> Nullable<Text>,
        scope_kind -> Text,
        branch -> Nullable<Text>,
        created_at -> Integer,
    }
}

diesel::table! {
    manager_wake_batches (id) {
        id -> Text,
        manager_id -> Text,
        created_at -> Integer,
        completed_at -> Nullable<Integer>,
        outcome -> Nullable<Text>,
    }
}

diesel::table! {
    sessions (id) {
        id -> Text,
        job_id -> Nullable<Text>,
        chat_id -> Nullable<Text>,
        backend -> Text,
        status -> Text,
        parent_session_id -> Nullable<Text>,
        replaced_by_id -> Nullable<Text>,
        terminal_reason -> Nullable<Text>,
        sequence -> Integer,
        created_at -> Integer,
        closed_at -> Nullable<Integer>,
        updated_at -> Integer,
        backend_id -> Nullable<Text>,
    }
}

diesel::table! {
    jobs (id) {
        id -> Text,
        execution_id -> Nullable<Text>,
        manager_id -> Nullable<Text>,
        recipe_node_id -> Nullable<Text>,
        parent_job_id -> Nullable<Text>,
        worktree_path -> Nullable<Text>,
        branch -> Nullable<Text>,
        base_commit -> Nullable<Text>,
        current_session_id -> Nullable<Text>,
        resume_session_id -> Nullable<Text>,
        status -> Text,
        agent_config_id -> Nullable<Text>,
        issue_id -> Nullable<Text>,
        project_id -> Text,
        task_description -> Nullable<Text>,
        created_at -> Integer,
        updated_at -> Integer,
        completed_at -> Nullable<Integer>,
        parent_tool_use_id -> Nullable<Text>,
        task_index -> Nullable<Integer>,
        started_at -> Nullable<Integer>,
        model -> Nullable<Text>,
        node_name -> Nullable<Text>,
        base_branch -> Nullable<Text>,
        current_turn_id -> Nullable<Text>,
    }
}

diesel::table! {
    artifact_content (id) {
        id -> Text,
        artifact_node_id -> Text,
        execution_id -> Text,
        job_id -> Nullable<Text>,
        data -> Text,
        version -> Integer,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    condition_evaluations (id) {
        id -> Text,
        execution_id -> Text,
        recipe_node_id -> Text,
        result_port -> Text,
        raw_result -> Nullable<Text>,
        error_message -> Nullable<Text>,
        evaluated_at -> Integer,
    }
}

diesel::table! {
    pending_injections (id) {
        id -> Text,
        session_id -> Text,
        injection_type -> Text,
        content -> Text,
        source_id -> Nullable<Text>,
        priority -> Integer,
        status -> Text,
        created_at -> Integer,
        injected_at -> Nullable<Integer>,
    }
}

diesel::table! {
    permission_requests (id) {
        id -> Text,
        run_id -> Text,
        tool_use_id -> Text,
        tool_name -> Text,
        tool_input -> Text,
        status -> Text,
        response -> Nullable<Text>,
        created_at -> Integer,
        responded_at -> Nullable<Integer>,
        turn_id -> Nullable<Text>,
    }
}

diesel::table! {
    turns (id) {
        id -> Text,
        session_id -> Text,
        run_id -> Nullable<Text>,
        job_id -> Nullable<Text>,
        manager_id -> Nullable<Text>,
        sequence -> Integer,
        predecessor_id -> Nullable<Text>,
        state -> Text,
        yield_reason -> Nullable<Text>,
        start_reason -> Text,
        created_at -> Integer,
        started_at -> Nullable<Integer>,
        ended_at -> Nullable<Integer>,
        updated_at -> Integer,
    }
}

diesel::table! {
    merge_requests (id) {
        id -> Text,
        job_id -> Text,
        project_id -> Text,
        issue_id -> Nullable<Text>,
        manager_id -> Nullable<Text>,
        // Authoritative state
        title -> Text,
        body -> Nullable<Text>,
        source_branch -> Text,
        target_branch -> Text,
        status -> Text,
        merge_method -> Text,
        additions -> Nullable<Integer>,
        deletions -> Nullable<Integer>,
        changed_files -> Nullable<Integer>,
        commit_count -> Nullable<Integer>,
        merged_commit -> Nullable<Text>,
        checks_json -> Nullable<Text>,
        checks_status -> Nullable<Text>,
        opened_at -> Integer,
        merged_at -> Nullable<Integer>,
        closed_at -> Nullable<Integer>,
        updated_at -> Integer,
        // GitHub sync (all nullable)
        github_pr_number -> Nullable<Integer>,
        github_pr_url -> Nullable<Text>,
        github_state -> Nullable<Text>,
        github_review -> Nullable<Text>,
        github_mergeable -> Nullable<Text>,
        github_fetched_at -> Nullable<Integer>,
    }
}

diesel::table! {
    projects (id) {
        id -> Text,
        workspace_id -> Text,
        name -> Text,
        key -> Text,
        repo_path -> Text,
        context -> Nullable<Text>,
        docs_enabled -> Nullable<Integer>,
        default_branch -> Nullable<Text>,
        next_issue_number -> Nullable<Integer>,
        created_at -> Integer,
        updated_at -> Integer,
        ci_commands -> Nullable<Text>,
        setup_commands -> Nullable<Text>,
        terminal_commands -> Nullable<Text>,
        config -> Nullable<Text>,
        remote_url -> Nullable<Text>,

        hidden -> Integer,
        server_id -> Nullable<Text>,
    }
}

diesel::table! {
    prompts (id) {
        id -> Text,
        run_id -> Text,
        questions -> Text,
        response -> Nullable<Text>,
        created_at -> Integer,
        answered_at -> Nullable<Integer>,
        turn_id -> Nullable<Text>,
    }
}

diesel::table! {
    runs (id) {
        id -> Text,
        issue_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        job_id -> Nullable<Text>,
        status -> Nullable<Text>,
        session_id -> Nullable<Text>,
        error_message -> Nullable<Text>,
        started_at -> Nullable<Integer>,
        exited_at -> Nullable<Integer>,
        created_at -> Integer,
        updated_at -> Integer,
        chat_id -> Nullable<Text>,
        backend -> Nullable<Text>,
        exit_reason -> Nullable<Text>,
        start_mode -> Nullable<Text>,
    }
}

diesel::table! {
    trigger_accumulator_state (id) {
        id -> Text,
        recipe_id -> Text,
        group_key -> Text,
        scope_key -> Text,
        events -> Text,
        event_count -> Integer,
        seen_event_ids -> Text,
        first_event_at -> Integer,
        last_event_at -> Integer,
        created_at -> Integer,
    }
}

diesel::table! {
    todos (id) {
        id -> Text,
        job_id -> Text,
        todo_id -> Text,
        content -> Text,
        status -> Text,
        priority -> Nullable<Text>,
        active_form -> Nullable<Text>,
        position -> Integer,
        created_at -> Integer,
        updated_at -> Integer,
    }
}

diesel::table! {
    webhook_events (id) {
        id -> Text,
        event_type -> Text,
        action -> Text,
        repo_full_name -> Text,
        pr_number -> Nullable<Integer>,
        payload_summary -> Text,
        processed_at -> Integer,
    }
}

diesel::table! {
    messages (id) {
        id -> Text,
        channel_type -> Text,
        channel_id -> Nullable<Text>,
        sender_run_id -> Nullable<Text>,
        sender_name -> Text,
        recipient_run_id -> Nullable<Text>,
        recipient_manager_id -> Nullable<Text>,
        content -> Text,
        created_at -> Integer,
    }
}

diesel::table! {
    issue_workspaces (issue_id, execution_id) {
        issue_id -> Text,
        execution_id -> Text,
        surface -> Text,
        layout_json -> Text,
        schema_version -> Integer,
        updated_at -> Integer,
        revision -> Integer,
    }
}

diesel::table! {
    workspaces (id) {
        id -> Text,
        name -> Text,
        created_at -> Integer,
        updated_at -> Integer,
        default_model -> Nullable<Text>,
        system_prompt -> Nullable<Text>,
        branch_prefix -> Nullable<Text>,
        max_thinking_tokens -> Nullable<Integer>,
        merge_type -> Nullable<Text>,
        pull_on_merge -> Nullable<Integer>,
        agent_sync_preference -> Nullable<Text>,
        auto_start_jobs -> Integer,
        timezone -> Nullable<Text>,
    }
}

// Foreign key relationships
diesel::joinable!(memory_triggers -> memories (memory_id));
diesel::joinable!(message_stream_chunks -> message_streams (stream_id));
diesel::joinable!(message_streams -> events (final_event_id));
diesel::joinable!(message_streams -> runs (run_id));
diesel::joinable!(managers -> executions (execution_id));
diesel::joinable!(managers -> jobs (job_id));
diesel::joinable!(managers -> projects (project_id));
diesel::joinable!(manager_mailbox -> managers (manager_id));
diesel::joinable!(manager_mailbox -> manager_wake_batches (wake_batch_id));
diesel::joinable!(manager_scopes -> managers (manager_id));
diesel::joinable!(manager_scopes -> projects (project_id));
diesel::joinable!(manager_wake_batches -> managers (manager_id));
diesel::joinable!(memories -> projects (project_id));
diesel::joinable!(checkpoint_command_cache -> jobs (job_id));
diesel::joinable!(artifacts -> jobs (job_id));
diesel::joinable!(chats -> projects (project_id));
diesel::joinable!(job_terminals -> jobs (job_id));
diesel::joinable!(job_terminals -> projects (project_id));
diesel::joinable!(job_terminals -> runs (run_id));
diesel::joinable!(comments -> issues (issue_id));
diesel::joinable!(doc_references -> issues (issue_id));
diesel::joinable!(event_embeddings -> events (event_id));
diesel::joinable!(events -> runs (run_id));
diesel::joinable!(file_changes -> jobs (job_id));

diesel::joinable!(execution_trigger_sources -> executions (triggered_execution_id));
diesel::joinable!(execution_trigger_sources -> jobs (source_job_id));
diesel::joinable!(executions -> issues (issue_id));
diesel::joinable!(executions -> projects (project_id));
diesel::joinable!(issue_workspaces -> issues (issue_id));
diesel::joinable!(issues -> managers (manager_id));
diesel::joinable!(issues -> projects (project_id));
diesel::joinable!(jobs -> executions (execution_id));

diesel::joinable!(jobs -> issues (issue_id));
diesel::joinable!(jobs -> projects (project_id));
diesel::joinable!(merge_requests -> jobs (job_id));
diesel::joinable!(merge_requests -> projects (project_id));
diesel::joinable!(merge_requests -> issues (issue_id));
diesel::joinable!(merge_requests -> managers (manager_id));
// New joinable relationships for eliminated polymorphic scope pattern
diesel::joinable!(action_configs -> workspaces (workspace_id));
diesel::joinable!(action_configs -> projects (project_id));
diesel::joinable!(skill_configs -> workspaces (workspace_id));
diesel::joinable!(skill_configs -> projects (project_id));
diesel::joinable!(custom_mcp_servers -> workspaces (workspace_id));
diesel::joinable!(custom_mcp_servers -> projects (project_id));
diesel::joinable!(projects -> workspaces (workspace_id));
diesel::joinable!(permission_requests -> runs (run_id));
diesel::joinable!(prompts -> runs (run_id));
diesel::joinable!(turns -> managers (manager_id));
diesel::joinable!(turns -> runs (run_id));
diesel::joinable!(sessions -> jobs (job_id));
diesel::joinable!(sessions -> chats (chat_id));
diesel::joinable!(runs -> chats (chat_id));
diesel::joinable!(runs -> jobs (job_id));
diesel::joinable!(runs -> issues (issue_id));
diesel::joinable!(runs -> projects (project_id));
diesel::joinable!(todos -> jobs (job_id));

diesel::joinable!(artifact_content -> executions (execution_id));
diesel::joinable!(artifact_content -> jobs (job_id));
diesel::joinable!(action_runs -> executions (execution_id));
diesel::joinable!(action_runs -> action_configs (action_config_id));
diesel::joinable!(action_runs -> issues (issue_id));
diesel::joinable!(action_runs -> projects (project_id));
diesel::joinable!(condition_evaluations -> executions (execution_id));

diesel::allow_tables_to_appear_in_same_query!(
    account,
    action_configs,
    action_runs,
    artifact_content,
    effect_outbox,
    artifacts,
    chats,
    checkpoint_command_cache,
    ci_logs_cache,
    condition_evaluations,
    job_terminals,
    comments,
    custom_mcp_servers,
    doc_references,
    event_embeddings,
    events,
    execution_trigger_sources,
    executions,
    file_changes,
    github_app,
    github_installations,
    issue_workspaces,
    issues,
    jobs,
    manager_mailbox,
    manager_scopes,
    manager_wake_batches,
    managers,
    memories,
    message_stream_chunks,
    message_streams,
    memory_triggers,
    messages,
    pending_injections,
    permission_requests,
    merge_requests,
    projects,
    prompts,
    runs,
    sessions,
    servers,
    skill_configs,
    todos,
    trigger_accumulator_state,
    turns,
    webhook_events,
    workspaces,
);
