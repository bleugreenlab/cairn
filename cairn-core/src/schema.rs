// @generated automatically by Diesel CLI.

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
    server_deployments (id) {
        id -> Text,
        name -> Text,
        host -> Text,
        port -> Integer,
        user -> Text,
        ssh_key_path -> Nullable<Text>,
        container_name -> Text,
        api_key -> Text,
        server_port -> Integer,
        status -> Text,
        claude_authenticated -> Integer,
        error_message -> Nullable<Text>,
        created_at -> BigInt,
        updated_at -> BigInt,
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
        claude_session_id -> Nullable<Text>,
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
        priority -> Nullable<Integer>,
        completed_at -> Nullable<Integer>,
        dismissed_at -> Nullable<Integer>,
        created_at -> Integer,
        updated_at -> Integer,
        wait_state -> Nullable<Text>,
        model -> Nullable<Text>,
        skills -> Nullable<Text>,
    }
}

diesel::table! {
    jobs (id) {
        id -> Text,
        execution_id -> Nullable<Text>,
        recipe_node_id -> Nullable<Text>,
        parent_job_id -> Nullable<Text>,
        worktree_path -> Nullable<Text>,
        branch -> Nullable<Text>,
        base_commit -> Nullable<Text>,
        claude_session_id -> Nullable<Text>,
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
    }
}

diesel::table! {
    pr_data (id) {
        id -> Text,
        action_run_id -> Nullable<Text>,
        pr_number -> Integer,
        pr_url -> Text,
        pr_status -> Text,
        // GitHub API fields (formerly in pr_cache)
        title -> Nullable<Text>,
        body -> Nullable<Text>,
        state -> Nullable<Text>,
        is_draft -> Nullable<Integer>,
        review_decision -> Nullable<Text>,
        mergeable -> Nullable<Text>,
        additions -> Nullable<Integer>,
        deletions -> Nullable<Integer>,
        checks_status -> Nullable<Text>,
        checks_json -> Nullable<Text>,
        fetched_at -> Nullable<Integer>,
        // Timestamps
        opened_at -> Nullable<Integer>,
        merged_at -> Nullable<Integer>,
        closed_at -> Nullable<Integer>,
        updated_at -> Integer,
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
        remote_api_key -> Nullable<Text>,
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
    }
}

diesel::table! {
    runs (id) {
        id -> Text,
        issue_id -> Nullable<Text>,
        project_id -> Nullable<Text>,
        job_id -> Nullable<Text>,
        status -> Nullable<Text>,
        claude_session_id -> Nullable<Text>,
        error_message -> Nullable<Text>,
        started_at -> Nullable<Integer>,
        completed_at -> Nullable<Integer>,
        created_at -> Integer,
        updated_at -> Integer,
        todos -> Nullable<Text>,
        chat_id -> Nullable<Text>,
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
        content -> Text,
        created_at -> Integer,
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
diesel::joinable!(memories -> projects (project_id));
diesel::joinable!(checkpoint_command_cache -> jobs (job_id));
diesel::joinable!(artifacts -> jobs (job_id));
diesel::joinable!(chats -> projects (project_id));
diesel::joinable!(job_terminals -> jobs (job_id));
diesel::joinable!(job_terminals -> projects (project_id));
diesel::joinable!(job_terminals -> runs (run_id));
diesel::joinable!(comments -> issues (issue_id));
diesel::joinable!(doc_references -> issues (issue_id));
diesel::joinable!(events -> runs (run_id));
diesel::joinable!(file_changes -> jobs (job_id));

diesel::joinable!(executions -> issues (issue_id));
diesel::joinable!(executions -> projects (project_id));
diesel::joinable!(issues -> projects (project_id));
diesel::joinable!(jobs -> executions (execution_id));

diesel::joinable!(jobs -> issues (issue_id));
diesel::joinable!(jobs -> projects (project_id));
diesel::joinable!(pr_data -> action_runs (action_run_id));
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
diesel::joinable!(runs -> chats (chat_id));
diesel::joinable!(runs -> jobs (job_id));
diesel::joinable!(runs -> issues (issue_id));
diesel::joinable!(runs -> projects (project_id));

diesel::joinable!(artifact_content -> executions (execution_id));
diesel::joinable!(artifact_content -> jobs (job_id));
diesel::joinable!(action_runs -> executions (execution_id));
diesel::joinable!(action_runs -> action_configs (action_config_id));
diesel::joinable!(action_runs -> issues (issue_id));
diesel::joinable!(action_runs -> projects (project_id));
diesel::joinable!(condition_evaluations -> executions (execution_id));

diesel::allow_tables_to_appear_in_same_query!(
    action_configs,
    action_runs,
    artifact_content,
    artifacts,
    chats,
    checkpoint_command_cache,
    ci_logs_cache,
    condition_evaluations,
    job_terminals,
    comments,
    custom_mcp_servers,
    doc_references,
    events,
    file_changes,
    executions,
    github_app,
    github_installations,
    issues,
    jobs,
    memories,
    memory_triggers,
    messages,
    pending_injections,
    permission_requests,
    pr_data,
    projects,
    prompts,
    runs,
    server_deployments,
    skill_configs,
    webhook_events,
    workspaces,
);
