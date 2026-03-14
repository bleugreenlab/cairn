//! Agent-related MCP handlers.
//!
//! Handles: task (sub-agent spawning)

use crate::mcp::types::{McpCallbackRequest, TaskPayload};
use crate::models::{AgentConfig, RunStatus};
use crate::orchestrator::session::start_claude_session;
use crate::orchestrator::Orchestrator;
use crate::schema::{jobs, projects, runs};
use crate::{
    config::agents as config_agents,
    diesel_models::{NewJob, NewRun},
};
use diesel::prelude::*;
use std::path::PathBuf;
use tokio::sync::broadcast;
use uuid::Uuid;

// ============================================================================
// Handler
// ============================================================================

/// Handle task tool call - spawns a sub-agent to handle a delegated task
pub async fn handle_task(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let services = &orch.services;
    let handler_entry_id = Uuid::new_v4();
    log::info!(
        "[DEBUG-HANDLER] handle_task ENTERED handler_id={} run_id={:?}",
        handler_entry_id,
        request.run_id
    );

    let payload: TaskPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            log::error!(
                "[DEBUG-HANDLER] Invalid payload handler_id={}: {}",
                handler_entry_id,
                e
            );
            return format!("Invalid payload: {}", e);
        }
    };

    log::info!(
        "[DEBUG-HANDLER] task for handler_id={} cwd={}, subagent_type={}, description={}, prompt_len={}, parent_tool_use_id={:?}, task_index={:?}",
        handler_entry_id,
        request.cwd,
        payload.subagent_type,
        payload.description,
        payload.prompt.len(),
        request.tool_use_id,
        payload.task_index
    );

    // Look up the parent run by worktree path
    let parent_ctx = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Failed to lock database: {}", e),
        };

        match super::lookup_run(&mut conn, request) {
            Ok(ctx) => ctx,
            Err(e) => return e,
        }
    };

    // Get project path for file-based config lookup
    let project_path: Option<PathBuf> = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Failed to lock database: {}", e),
        };

        projects::table
            .find(&parent_ctx.project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(PathBuf::from)
    };

    // Load the agent config from files
    let config_dir = orch.config_dir.clone();
    let agent_config: AgentConfig = {
        // Look up agent by name first (Claude sends the agent name), then by ID
        let file_agent = match config_agents::get_agent(
            &config_dir,
            &payload.subagent_type,
            project_path.as_deref(),
        ) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
                // Try searching by name in all agents
                let agents = config_agents::list_agents(&config_dir, project_path.as_deref())
                    .unwrap_or_default();
                let mut found = None;
                for result in agents {
                    if let crate::config::ConfigResult::Ok(agent) = result {
                        if agent.name == payload.subagent_type {
                            found = Some(agent);
                            break;
                        }
                    }
                }
                match found {
                    Some(agent) => agent,
                    None => return format!("Agent config not found: {}", payload.subagent_type),
                }
            }
            Err(e) => return format!("Failed to load agent config: {}", e),
        };

        // Convert FileAgent to AgentConfig
        AgentConfig {
            id: file_agent.id,
            name: file_agent.name,
            description: file_agent.description,
            prompt: file_agent.prompt,
            tools: file_agent.tools,
            model: file_agent.model,
            workspace_id: if file_agent.is_project_scoped {
                None
            } else {
                Some("workspace".to_string())
            },
            project_id: if file_agent.is_project_scoped {
                Some(parent_ctx.project_id.clone())
            } else {
                None
            },
            created_at: 0,
            updated_at: 0,
            disallowed_tools: file_agent.disallowed_tools,
            skills: file_agent.skills,
            permission_mode: file_agent.permission_mode,
        }
    };

    // Create a new timeline node and run for the sub-agent
    log::info!(
        "[DEBUG-HANDLER] Creating job and run handler_id={}",
        handler_entry_id
    );
    let (run_id, node_id, session_id) = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Failed to lock database: {}", e),
        };

        let diesel_conn = &mut *conn;
        let now = chrono::Utc::now().timestamp() as i32;

        // Create a timeline node for the agent with pre-generated session_id
        let node_id = Uuid::new_v4().to_string();
        let session_id = Uuid::new_v4().to_string();
        log::info!(
            "[DEBUG-HANDLER] Created node_id={} session_id={} handler_id={}",
            node_id,
            session_id,
            handler_entry_id
        );

        // Extract model from agent config
        let model_str = agent_config.model.as_ref().map(|m| m.to_string());

        // Task-spawned sub-agents don't have recipe_node_id (they're ad-hoc)
        // They inherit parent's execution_id for proper tree nesting in UI
        let new_node = NewJob {
            id: &node_id,
            execution_id: parent_ctx.execution_id.as_deref(),
            recipe_node_id: None,
            parent_job_id: Some(&parent_ctx.job_id),
            worktree_path: Some(&request.cwd), // Use parent's working directory
            branch: None,
            base_commit: None,
            claude_session_id: Some(&session_id),
            status: "pending",
            agent_config_id: Some(&agent_config.id),
            issue_id: parent_ctx.issue_id.as_deref(),
            project_id: &parent_ctx.project_id,
            task_description: Some(&payload.description),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: request.tool_use_id.as_deref(),
            task_index: payload.task_index,
            started_at: None,
            model: model_str.as_deref(),
            node_name: None, // Task-spawned sub-agents don't have recipe node names
        };

        log::info!(
            "[DEBUG-HANDLER] Inserting job into database handler_id={} node_id={}",
            handler_entry_id,
            node_id
        );
        if let Err(e) = diesel::insert_into(jobs::table)
            .values(&new_node)
            .execute(diesel_conn)
        {
            log::error!(
                "[DEBUG-HANDLER] Failed to insert job handler_id={}: {}",
                handler_entry_id,
                e
            );
            return format!("Failed to create agent node: {}", e);
        }
        log::info!(
            "[DEBUG-HANDLER] Job inserted successfully handler_id={} node_id={}",
            handler_entry_id,
            node_id
        );

        // Create a run for this agent
        let run_id = Uuid::new_v4().to_string();
        log::info!(
            "[DEBUG-HANDLER] Created run_id={} for node_id={} handler_id={}",
            run_id,
            node_id,
            handler_entry_id
        );
        let run_status = RunStatus::Pending.to_string();
        let new_run = NewRun {
            id: &run_id,
            issue_id: parent_ctx.issue_id.as_deref(),
            project_id: Some(&parent_ctx.project_id), // Always set project_id
            job_id: Some(&node_id),
            chat_id: None,
            status: Some(&run_status),
            claude_session_id: Some(&session_id), // Pre-generated session_id
            error_message: None,
            started_at: None,
            completed_at: None,
            created_at: now,
            updated_at: now,
            todos: None,
        };

        log::info!(
            "[DEBUG-HANDLER] Inserting run into database handler_id={} run_id={}",
            handler_entry_id,
            run_id
        );
        if let Err(e) = diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(diesel_conn)
        {
            log::error!(
                "[DEBUG-HANDLER] Failed to insert run handler_id={}: {}",
                handler_entry_id,
                e
            );
            return format!("Failed to create agent run: {}", e);
        }
        log::info!(
            "[DEBUG-HANDLER] Run inserted successfully handler_id={} run_id={} node_id={}",
            handler_entry_id,
            run_id,
            node_id
        );

        (run_id, node_id, session_id)
    };

    log::info!(
        "[DEBUG-HANDLER] Created job and run: handler_id={} job_id={} run_id={} session_id={}",
        handler_entry_id,
        node_id,
        run_id,
        session_id
    );

    // Emit db-change events for the newly created node and run
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "insert"}),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "insert"}),
    );

    // Build the initial prompt for the agent
    // The prompt field contains the full task description
    let initial_message = payload.prompt.clone();

    // Create default output schema for task-spawned agents
    // This ensures every sub-agent has a "return" tool to submit results
    let output_schema = crate::models::OutputSchemaInfo {
        schema: crate::models::OutputSchema::Preset("return".to_string()),
        tool_name: None, // Defaults to "return"
        description: Some("Submit the task result".to_string()),
    };

    // Start the Claude session for the agent
    // Permission handling is controlled by agent_config.permission_mode
    let job_model = agent_config.model.clone(); // Pass job.model (from agent config)
    if let Err(e) = start_claude_session(
        orch,
        &run_id,
        &initial_message, // Pass task as prompt (agent's system prompt will be appended)
        &request.cwd,
        Some(&session_id),      // Pre-generated session_id for new session
        None,                   // Not resuming an existing session
        job_model,              // Pass model from agent config (stored in job.model)
        Some(&initial_message), // Also pass as initial_user_message for transcript display
        Some(&agent_config),
        Some(&output_schema), // Provide return schema for sub-agent
        false,                // is_job_level - task/child run, agent config wins
        parent_ctx.execution_id.as_deref(), // Pass execution_id for snapshot-based skill loading
    ) {
        return format!("Failed to start agent session: {}", e);
    }

    // Wait for the agent run to complete using broadcast channel
    let run_id_clone = run_id.clone();
    let node_id_clone = node_id.clone();
    let orch_clone = orch.clone();

    // Subscribe to run completions broadcast
    let mut rx = orch.run_completions.subscribe();

    // Wait for completion with inactivity timeout (5 minutes of no events)
    let result = {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let inactivity_timeout_secs = 300; // 5 minutes

        let timeout_check = async {
            loop {
                interval.tick().await;

                let mut conn = match orch_clone.db.conn.lock() {
                    Ok(c) => c,
                    Err(_) => break,
                };

                // Get the most recent event timestamp for this run (Unix timestamp in seconds)
                use crate::schema::events;
                let last_event_time: Option<i32> = events::table
                    .filter(events::run_id.eq(&run_id_clone))
                    .select(events::created_at)
                    .order(events::created_at.desc())
                    .first(&mut *conn)
                    .ok();

                if let Some(last_time) = last_event_time {
                    let now = chrono::Utc::now().timestamp() as i32;
                    let elapsed = now - last_time;

                    if elapsed > inactivity_timeout_secs {
                        // No activity for 5 minutes, timeout
                        break;
                    }
                }
            }
        };

        let completion_wait = async {
            loop {
                match rx.recv().await {
                    Ok(completed_run_id) if completed_run_id == run_id => return Ok(()),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Err(()),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        };

        tokio::select! {
            res = completion_wait => Ok(res),
            _ = timeout_check => Err(()),
        }
    };

    match result {
        Ok(Ok(())) => {
            // Run completed, fetch final status and output
            let mut conn = match orch.db.conn.lock() {
                Ok(c) => c,
                Err(_) => return "Agent completed but failed to fetch results".to_string(),
            };

            use crate::schema::{artifacts, events, runs};

            // Get run status
            let final_status: Option<String> = runs::table
                .find(&run_id)
                .select(runs::status)
                .first(&mut *conn)
                .ok()
                .flatten();

            match final_status.as_deref() {
                Some("completed") => {
                    // All task-spawned agents use artifacts table with "return" schema
                    // The artifact data contains { "content": "..." }
                    let artifact_data: Option<String> = artifacts::table
                        .filter(artifacts::job_id.eq(&node_id_clone))
                        .order(artifacts::version.desc())
                        .select(artifacts::data)
                        .first(&mut *conn)
                        .ok();

                    if let Some(data_json) = artifact_data {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&data_json) {
                            data.get("content")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Task completed.")
                                .to_string()
                        } else {
                            "Task completed.".to_string()
                        }
                    } else {
                        // No artifact - try to get the last assistant message as fallback
                        let last_assistant_content: Option<String> = events::table
                            .filter(events::run_id.eq(&run_id))
                            .filter(events::event_type.eq("assistant"))
                            .order(events::sequence.desc())
                            .select(events::data)
                            .first::<String>(&mut *conn)
                            .ok()
                            .and_then(|data_json| {
                                serde_json::from_str::<crate::claude::stream::TranscriptEvent>(
                                    &data_json,
                                )
                                .ok()
                                .and_then(|transcript| transcript.content)
                            });

                        last_assistant_content.unwrap_or_else(|| "Task completed.".to_string())
                    }
                }
                Some("failed") => format!(
                    "Agent '{}' failed.\n\nTask: {}\n\nThe agent encountered an error.",
                    agent_config.name, payload.description
                ),
                _ => format!(
                    "Agent '{}' finished with unknown status.\n\nTask: {}",
                    agent_config.name, payload.description
                ),
            }
        }
        Ok(Err(_)) => format!("Agent '{}' completion signal failed", agent_config.name),
        Err(_) => format!(
            "Agent '{}' timed out due to inactivity (no events for 5 minutes).\n\nTask: {}\n\nThe agent may still be running. Check the timeline.",
            agent_config.name, payload.description
        ),
    }
}
