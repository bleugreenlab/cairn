//! Agent-related MCP handlers.
//!
//! Handles: task (sub-agent spawning)

use crate::mcp::types::{McpCallbackRequest, TaskPayload, TaskSessionMode};
use crate::models::TurnState;
use crate::models::{
    AgentConfig, DelegatedOutputContract, DelegatedSessionMode, DelegatedSessionStrategy,
    DelegatedStatus, ExecutionSnapshot, Model, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType,
};
use crate::node_segments::visible_node_segment;
use crate::orchestrator::Orchestrator;
#[cfg(test)]
use crate::schema::runs;
use crate::schema::{executions, jobs, projects};
use crate::{
    config::{
        agents as config_agents,
        presets::{load_effective_presets, resolve_runtime_selection, PresetsConfig},
    },
    diesel_models::NewExecution,
    effects::types::WorkflowEffect,
    execution::delegation::{
        create_or_reuse_task_packet, latest_run_for_job, refresh_packet_state,
        CreateDelegatedPacketInput,
    },
};
use cairn_common::protocol::CallbackResponse;
use diesel::prelude::*;
use std::path::PathBuf;
use tokio::sync::broadcast;
use uuid::Uuid;

const INLINE_TASK_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(45);
const DEFERRED_TASK_PARENT_SUSPEND_GRACE: std::time::Duration =
    std::time::Duration::from_millis(75);

#[cfg(test)]
fn transition_spawned_job_to_running(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    crate::transitions::transition_job(
        &mut conn,
        &*orch.services.emitter,
        job_id,
        crate::models::JobStatus::Running,
        &orch.trigger_events,
    )
    .map_err(|e| format!("Failed to transition child job to running: {}", e))?;

    Ok(())
}

#[cfg(test)]
fn find_existing_child_invocation(
    orch: &Orchestrator,
    parent_job_id: &str,
    parent_tool_use_id: Option<&str>,
    task_index: Option<i32>,
) -> Result<Option<(String, String)>, String> {
    let Some(parent_tool_use_id) = parent_tool_use_id else {
        return Ok(None);
    };

    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let existing_job_id: Option<String> = jobs::table
        .filter(jobs::parent_job_id.eq(parent_job_id))
        .filter(jobs::parent_tool_use_id.eq(parent_tool_use_id))
        .filter(jobs::task_index.eq(task_index))
        .select(jobs::id)
        .order(jobs::created_at.asc())
        .first(&mut *conn)
        .optional()
        .map_err(|e| format!("Failed to query existing child job: {}", e))?;

    let Some(job_id) = existing_job_id else {
        return Ok(None);
    };

    let run_id: String = runs::table
        .filter(runs::job_id.eq(&job_id))
        .select(runs::id)
        .order(runs::created_at.desc())
        .first(&mut *conn)
        .map_err(|e| format!("Failed to query existing child run: {}", e))?;

    Ok(Some((job_id, run_id)))
}

fn build_task_callback_response(
    conn: &mut diesel::sqlite::SqliteConnection,
    parent_ctx: &super::RunContext,
    run_id: &str,
    node_id: &str,
    agent_config: &AgentConfig,
    task_description: &str,
    expected_artifact_type: &str,
) -> CallbackResponse {
    use crate::schema::runs;

    let final_status: Option<String> = runs::table
        .find(run_id)
        .select(runs::status)
        .first(conn)
        .ok()
        .flatten();

    match final_status.as_deref() {
        Some("complete") | Some("completed") | Some("exited") => {
            let result =
                latest_nonempty_artifact_content(conn, node_id, Some(expected_artifact_type))
                    .or_else(|| latest_nonempty_artifact_content(conn, node_id, None))
                    .or_else(|| latest_nonempty_assistant_content(conn, run_id))
                    .unwrap_or_else(|| "Task completed.".to_string());

            let artifact_uri = compute_artifact_uri(conn, parent_ctx, node_id);

            CallbackResponse {
                result,
                artifact_uri,
            }
        }
        Some("failed") | Some("crashed") => CallbackResponse {
            result: format!(
                "Agent '{}' failed.\n\nTask: {}\n\nThe agent encountered an error.",
                agent_config.name, task_description
            ),
            artifact_uri: None,
        },
        _ => CallbackResponse {
            result: format!(
                "Agent '{}' finished with unknown status.\n\nTask: {}",
                agent_config.name, task_description
            ),
            artifact_uri: None,
        },
    }
}

fn normalize_result_text(text: String) -> Option<String> {
    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

fn parse_nonempty_artifact_content(data_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(data_json)
        .ok()
        .and_then(|data| {
            data.get("content")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .and_then(normalize_result_text)
}

fn parse_nonempty_assistant_content(data_json: &str) -> Option<String> {
    serde_json::from_str::<crate::agent_process::stream::TranscriptEvent>(data_json)
        .ok()
        .and_then(|event| event.content)
        .and_then(normalize_result_text)
}

fn latest_nonempty_artifact_content(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
    artifact_type: Option<&str>,
) -> Option<String> {
    use crate::schema::artifacts;

    let mut query = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .into_boxed();
    if let Some(artifact_type) = artifact_type {
        query = query.filter(artifacts::artifact_type.eq(artifact_type));
    }

    query
        .order(artifacts::version.desc())
        .limit(10)
        .select(artifacts::data)
        .load::<String>(conn)
        .ok()
        .and_then(|rows| {
            rows.into_iter()
                .filter_map(|data_json| parse_nonempty_artifact_content(&data_json))
                .next()
        })
}

fn latest_nonempty_assistant_content(
    conn: &mut diesel::sqlite::SqliteConnection,
    run_id: &str,
) -> Option<String> {
    use crate::schema::events;

    events::table
        .filter(events::run_id.eq(run_id))
        .filter(events::event_type.eq("assistant"))
        .order(events::sequence.desc())
        .limit(20)
        .select(events::data)
        .load::<String>(conn)
        .ok()
        .and_then(|rows| {
            rows.into_iter()
                .filter_map(|data_json| parse_nonempty_assistant_content(&data_json))
                .next()
        })
}

fn ensure_task_execution_context(
    orch: &Orchestrator,
    parent_ctx: &super::RunContext,
) -> Result<String, String> {
    if let Some(execution_id) = &parent_ctx.execution_id {
        return Ok(execution_id.clone());
    }

    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let existing_execution_id: Option<String> = jobs::table
        .find(&parent_ctx.job_id)
        .select(jobs::execution_id)
        .first(&mut *conn)
        .map_err(|e| format!("Failed to load parent job execution context: {}", e))?;
    if let Some(execution_id) = existing_execution_id {
        return Ok(execution_id);
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let project_path: Option<PathBuf> = projects::table
        .find(&parent_ctx.project_id)
        .select(projects::repo_path)
        .first::<String>(&mut *conn)
        .ok()
        .map(PathBuf::from);
    let presets = load_effective_presets(&orch.config_dir, project_path.as_deref());

    let snapshot = ExecutionSnapshot {
        recipe: RecipeSnapshot {
            id: format!("delegation-{}", parent_ctx.job_id),
            name: "Delegated Work".to_string(),
            description: Some("Synthetic execution for task delegation".to_string()),
            trigger: RecipeTrigger::Manual,
            nodes: vec![],
            edges: vec![],
        },
        agents: std::collections::HashMap::new(),
        skills: std::collections::HashMap::new(),
        tools: std::collections::HashMap::new(),
        trigger_context: TriggerContext {
            issue_id: parent_ctx.issue_id.clone(),
            project_id: parent_ctx.project_id.clone(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
        },
        presets: Some((&presets).into()),
        delegated_packets: vec![],
        created_at: now as i64,
    };
    let snapshot_json = snapshot.to_json()?;

    let seq = match parent_ctx.issue_id.as_deref() {
        Some(issue_id) => executions::table
            .filter(executions::issue_id.eq(issue_id))
            .select(diesel::dsl::max(executions::seq))
            .first::<Option<i32>>(&mut *conn)
            .ok()
            .flatten()
            .map(|value| value + 1)
            .or(Some(1)),
        None => None,
    };

    let execution_id = Uuid::new_v4().to_string();
    diesel::insert_into(executions::table)
        .values(NewExecution {
            id: &execution_id,
            recipe_id: &snapshot.recipe.id,
            issue_id: parent_ctx.issue_id.as_deref(),
            project_id: if parent_ctx.issue_id.is_none() {
                Some(parent_ctx.project_id.as_str())
            } else {
                None
            },
            status: "running",
            started_at: now,
            completed_at: None,
            snapshot: Some(&snapshot_json),
            seq,
            initiator_sub: None,
            initiator_auth_mode: None,
            initiator_org_id: None,
            triggered_by: "manual",
        })
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to create synthetic delegation execution: {}", e))?;

    diesel::update(jobs::table.find(&parent_ctx.job_id))
        .set(jobs::execution_id.eq(Some(execution_id.as_str())))
        .execute(&mut *conn)
        .map_err(|e| {
            format!(
                "Failed to attach synthetic delegation execution to parent job: {}",
                e
            )
        })?;

    Ok(execution_id)
}

fn load_execution_presets(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    config_dir: &std::path::Path,
    project_path: Option<&std::path::Path>,
) -> Result<PresetsConfig, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load execution: {}", e))?
        .flatten();

    if let Some(json) = snapshot_json {
        let snapshot: ExecutionSnapshot = serde_json::from_str(&json)
            .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;
        if let Some(presets) = snapshot.presets.as_ref() {
            return Ok(PresetsConfig::from(presets));
        }
    }

    Ok(load_effective_presets(config_dir, project_path))
}

fn persist_task_packet(
    orch: &Orchestrator,
    execution_id: &str,
    parent_ctx: &super::RunContext,
    payload: &TaskPayload,
    agent_config: &AgentConfig,
    cwd: &str,
    parent_tool_use_id: Option<&str>,
) -> Result<crate::models::DelegatedWorkPacket, String> {
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("Failed to lock database: {}", e))?;

    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(&mut *conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;

    let parent_turn_id: Option<String> = jobs::table
        .find(&parent_ctx.job_id)
        .select(jobs::current_turn_id)
        .first(&mut *conn)
        .ok()
        .flatten();
    let parent_backend = jobs::table
        .find(&parent_ctx.job_id)
        .select(jobs::model)
        .first::<Option<String>>(&mut *conn)
        .ok()
        .flatten()
        .as_deref()
        .and_then(crate::backends::backend_for_model);
    let session = match payload.session_mode()? {
        TaskSessionMode::New => DelegatedSessionStrategy {
            mode: DelegatedSessionMode::New,
        },
        TaskSessionMode::Fork => DelegatedSessionStrategy {
            mode: DelegatedSessionMode::Fork,
        },
    };

    let packet = create_or_reuse_task_packet(
        &mut snapshot,
        CreateDelegatedPacketInput {
            parent_job_id: &parent_ctx.job_id,
            parent_turn_id: parent_turn_id.as_deref(),
            parent_tool_use_id,
            title: &payload.description,
            problem_statement: &payload.prompt,
            agent_config_id: &agent_config.id,
            cwd,
            filesystem_scope: agent_config.filesystem_scope.clone(),
            approval_policy: agent_config.approval_policy.clone(),
            acceptance: vec!["Return the delegated result with the return tool".to_string()],
            output_contract: DelegatedOutputContract {
                schema_type: "return".to_string(),
                tool_name: None,
                description: Some("Submit the task result".to_string()),
            },
            session,
            task_index: payload.task_index,
            tier_override: payload.tier.as_deref(),
            backend_preference: payload.backend_preference.as_deref().or(parent_backend),
        },
    );

    let snapshot_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize execution snapshot: {}", e))?;
    diesel::update(executions::table.find(execution_id))
        .set(crate::diesel_models::UpdateExecutionChangeset {
            snapshot: Some(snapshot_json),
            ..Default::default()
        })
        .execute(&mut *conn)
        .map_err(|e| format!("Failed to persist delegated packet: {}", e))?;

    Ok(packet)
}

fn lookup_packet_run(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    packet_id: &str,
) -> Result<Option<(crate::models::DelegatedWorkPacket, String, String)>, String> {
    let packet = match refresh_packet_state(conn, execution_id, packet_id)? {
        Some(packet) => packet,
        None => return Ok(None),
    };
    let Some(job_id) = packet.result_artifact_job_id.clone() else {
        return Ok(None);
    };
    let Some(run_id) = latest_run_for_job(conn, &job_id)? else {
        return Ok(None);
    };
    Ok(Some((packet, job_id, run_id)))
}

async fn wait_for_packet_run_materialization(
    orch: &Orchestrator,
    execution_id: &str,
    packet_id: &str,
) -> Result<(crate::models::DelegatedWorkPacket, String, String), String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        let lookup = {
            let mut conn = orch
                .db
                .conn
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            lookup_packet_run(&mut conn, execution_id, packet_id)
        }?;

        if let Some(result) = lookup {
            return Ok(result);
        }

        if tokio::time::Instant::now() >= deadline {
            return Err("Delegated task did not materialize a child job".to_string());
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn dispatch_ready_agent_jobs(
    orch: &Orchestrator,
    ready_jobs: Vec<crate::models::Job>,
) -> Result<(), String> {
    if ready_jobs.is_empty() {
        return Ok(());
    }

    let executor = orch.executor.get().ok_or_else(|| {
        format!(
            "No executor configured for {} ready delegated jobs",
            ready_jobs.len()
        )
    })?;

    executor
        .execute(orch, WorkflowEffect::StartAgentJobs(ready_jobs))
        .await
        .map(|_| ())
}

// ============================================================================
// Handler
// ============================================================================

/// Handle task tool call - spawns a sub-agent to handle a delegated task
pub async fn handle_task(orch: &Orchestrator, request: &McpCallbackRequest) -> CallbackResponse {
    let payload: TaskPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return CallbackResponse {
                result: format!("Invalid payload: {}", e),
                artifact_uri: None,
            };
        }
    };
    log::info!(
        "[DEBUG-TASK-HANDLER] parsed task payload run_id={:?} tool_use_id={:?} session={:?} raw_payload={}",
        request.run_id,
        request.tool_use_id,
        payload.session,
        request.payload
    );
    if let Err(e) = payload.session_mode() {
        return CallbackResponse {
            result: e,
            artifact_uri: None,
        };
    }

    macro_rules! err {
        ($msg:expr) => {
            return CallbackResponse {
                result: $msg,
                artifact_uri: None,
            }
        };
    }

    let parent_ctx = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => err!(format!("Failed to lock database: {}", e)),
        };
        match super::lookup_run(&mut conn, request) {
            Ok(ctx) => ctx,
            Err(e) => err!(e),
        }
    };

    let project_path: Option<PathBuf> = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => err!(format!("Failed to lock database: {}", e)),
        };
        projects::table
            .find(&parent_ctx.project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(PathBuf::from)
    };

    let execution_id = match ensure_task_execution_context(orch, &parent_ctx) {
        Ok(execution_id) => execution_id,
        Err(e) => err!(e),
    };

    let config_dir = orch.config_dir.clone();
    let (agent_config, _resolved_model, _resolved_backend): (AgentConfig, Model, String) = {
        let file_agent = match config_agents::get_agent(
            &config_dir,
            &payload.subagent_type,
            project_path.as_deref(),
        ) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
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
                    None => err!(format!("Agent config not found: {}", payload.subagent_type)),
                }
            }
            Err(e) => err!(format!("Failed to load agent config: {}", e)),
        };

        let parent_backend = {
            let mut conn = match orch.db.conn.lock() {
                Ok(c) => c,
                Err(e) => err!(format!("Failed to lock database: {}", e)),
            };
            jobs::table
                .find(&parent_ctx.job_id)
                .select(jobs::model)
                .first::<Option<String>>(&mut *conn)
                .ok()
                .flatten()
                .as_deref()
                .and_then(crate::backends::backend_for_model)
                .map(str::to_string)
        };
        let presets = {
            let mut conn = match orch.db.conn.lock() {
                Ok(c) => c,
                Err(e) => err!(format!("Failed to lock database: {}", e)),
            };
            match load_execution_presets(
                &mut conn,
                &execution_id,
                &config_dir,
                project_path.as_deref(),
            ) {
                Ok(p) => p,
                Err(e) => err!(e),
            }
        };
        let authored_tier = payload
            .tier
            .as_deref()
            .or(file_agent.tier.as_ref().map(Model::as_str));
        let authored_backend = payload
            .backend_preference
            .as_deref()
            .or(parent_backend.as_deref())
            .or(file_agent.backend_preference.as_deref());
        let (resolved_model, resolved_backend, _resolved_extras) =
            match resolve_runtime_selection(authored_tier, authored_backend, &presets) {
                Ok(result) => result,
                Err(e) => err!(e),
            };

        (
            AgentConfig {
                id: file_agent.id,
                name: file_agent.name,
                description: file_agent.description,
                prompt: file_agent.prompt,
                tools: file_agent.tools,
                tier: payload
                    .tier
                    .as_ref()
                    .map(|selection| Model::new(selection))
                    .or(file_agent.tier),
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
                approval_policy: file_agent.approval_policy,
                filesystem_scope: file_agent.filesystem_scope,
                backend_preference: payload
                    .backend_preference
                    .clone()
                    .or(parent_backend)
                    .or(file_agent.backend_preference),
            },
            resolved_model,
            resolved_backend,
        )
    };

    let packet = match persist_task_packet(
        orch,
        &execution_id,
        &parent_ctx,
        &payload,
        &agent_config,
        &request.cwd,
        request.tool_use_id.as_deref(),
    ) {
        Ok(packet) => packet,
        Err(e) => err!(e),
    };

    let packet_result = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => err!(format!("Failed to lock database: {}", e)),
        };
        lookup_packet_run(&mut conn, &execution_id, &packet.id)
    };
    if let Ok(Some((packet_state, job_id, run_id))) = packet_result {
        if matches!(
            packet_state.status,
            DelegatedStatus::Completed | DelegatedStatus::Failed
        ) {
            let mut conn = match orch.db.conn.lock() {
                Ok(c) => c,
                Err(e) => err!(format!("Failed to lock database: {}", e)),
            };
            return build_task_callback_response(
                &mut conn,
                &parent_ctx,
                &run_id,
                &job_id,
                &agent_config,
                &payload.description,
                &packet_state.output_contract.schema_type,
            );
        }
    }

    let ready_jobs =
        match crate::execution::advancement::advance_execution_with_actions(orch, &execution_id)
            .await
        {
            Ok(jobs) => jobs,
            Err(e) => err!(format!("Failed to advance delegated execution: {}", e)),
        };
    if let Err(e) = dispatch_ready_agent_jobs(orch, ready_jobs).await {
        err!(format!("Failed to dispatch delegated job: {}", e));
    }

    let (packet_state, job_id, run_id) =
        match wait_for_packet_run_materialization(orch, &execution_id, &packet.id).await {
            Ok(result) => result,
            Err(e) => err!(e),
        };

    if matches!(
        packet_state.status,
        DelegatedStatus::Completed | DelegatedStatus::Failed
    ) {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => err!(format!("Failed to lock database: {}", e)),
        };
        return build_task_callback_response(
            &mut conn,
            &parent_ctx,
            &run_id,
            &job_id,
            &agent_config,
            &payload.description,
            &packet_state.output_contract.schema_type,
        );
    }

    wait_for_task_completion(
        orch,
        &parent_ctx,
        &packet_state,
        run_id,
        job_id,
        agent_config,
        payload.description,
        packet_state.output_contract.schema_type.clone(),
    )
    .await
}

fn prepare_parent_for_delegated_wait(
    orch: &Orchestrator,
    parent_ctx: &super::RunContext,
    packet: &crate::models::DelegatedWorkPacket,
) -> Result<(), String> {
    if let Some(pred_turn_id) = packet.parent_turn_id.as_deref() {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let turn_state: Option<String> = crate::schema::turns::table
            .find(pred_turn_id)
            .select(crate::schema::turns::state)
            .first::<String>(&mut *conn)
            .optional()
            .map_err(|e| format!("Failed to load parent turn: {}", e))?;

        if matches!(turn_state.as_deref(), Some("running")) {
            crate::transitions::yield_turn(
                &mut conn,
                pred_turn_id,
                crate::models::TurnYieldReason::DependencyWait,
                &*orch.services.emitter,
            )
            .map_err(|e| format!("Failed to yield parent turn: {}", e))?;
        }

        let session_id: Option<String> = jobs::table
            .find(&parent_ctx.job_id)
            .select(jobs::current_session_id)
            .first::<Option<String>>(&mut *conn)
            .map_err(|e| format!("Failed to load parent session: {}", e))?;
        if let Some(session_id) = session_id.as_deref() {
            crate::turns::queries::ensure_successor_turn(
                &mut conn,
                &uuid::Uuid::new_v4().to_string(),
                session_id,
                &parent_ctx.job_id,
                pred_turn_id,
                crate::models::TurnStartReason::DependencyUnblock,
                &*orch.services.emitter,
            )
            .map_err(|e| format!("Failed to create dependency successor turn: {}", e))?;
        }

        if let Some(issue_id) = parent_ctx.issue_id.as_deref() {
            crate::transitions::recompute_issue_status(&mut conn, issue_id);
        }
    }

    Ok(())
}

#[cfg(test)]
fn suspend_parent_for_delegated_wait(
    orch: &Orchestrator,
    parent_ctx: &super::RunContext,
    packet: &crate::models::DelegatedWorkPacket,
) -> Result<(), String> {
    prepare_parent_for_delegated_wait(orch, parent_ctx, packet)?;
    crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
        orch,
        &parent_ctx.run_id,
        "delegated_wait_suspended",
    )
}

fn finish_deferred_parent_suspend_for_delegated_wait(
    orch: &Orchestrator,
    parent_run_id: &str,
    parent_job_id: &str,
    child_job_id: &str,
) -> Result<(), String> {
    crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
        orch,
        parent_run_id,
        "delegated_wait_suspended",
    )?;
    if let Err(error) = resume_suspended_parent_after_task_completion(orch, child_job_id) {
        log::warn!(
            "Post-suspend resume check failed for parent job {} via child {}: {}",
            parent_job_id,
            child_job_id,
            error
        );
    }
    Ok(())
}

fn schedule_deferred_parent_suspend_for_delegated_wait(
    orch: Orchestrator,
    parent_run_id: String,
    parent_job_id: String,
    child_job_id: String,
) {
    tokio::spawn(async move {
        tokio::time::sleep(DEFERRED_TASK_PARENT_SUSPEND_GRACE).await;

        let worker_orch = orch.clone();
        let worker_parent_run_id = parent_run_id.clone();
        let worker_parent_job_id = parent_job_id.clone();
        let worker_child_job_id = child_job_id.clone();
        match tokio::task::spawn_blocking(move || {
            finish_deferred_parent_suspend_for_delegated_wait(
                &worker_orch,
                &worker_parent_run_id,
                &worker_parent_job_id,
                &worker_child_job_id,
            )
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::warn!(
                    "Deferred suspend worker failed for parent job {} run {}: {}",
                    parent_job_id,
                    parent_run_id,
                    error
                );
            }
            Err(join_error) => {
                log::warn!(
                    "Deferred suspend worker panicked for parent job {} run {}: {}",
                    parent_job_id,
                    parent_run_id,
                    join_error
                );
            }
        }
    });
}

fn delegated_result_text(
    conn: &mut diesel::sqlite::SqliteConnection,
    packet: &crate::models::DelegatedWorkPacket,
) -> Result<String, String> {
    use crate::execution::delegation::latest_run_for_job;

    let Some(job_id) = packet.result_artifact_job_id.as_deref() else {
        return Ok(format!(
            "Delegated task '{}' has no materialized result.",
            packet.title
        ));
    };

    if matches!(
        packet.status,
        DelegatedStatus::Failed | DelegatedStatus::Cancelled
    ) {
        return Ok(format!("Delegated task '{}' failed.", packet.title));
    }

    if let Some(content) = latest_nonempty_artifact_content(conn, job_id, None) {
        return Ok(content);
    }

    let child_run_id = latest_run_for_job(conn, job_id)?;
    if let Some(child_run_id) = child_run_id.as_deref() {
        if let Some(content) = latest_nonempty_assistant_content(conn, child_run_id) {
            return Ok(content);
        }
    }

    Ok("Task completed.".to_string())
}

fn format_delegated_resume_message(
    packets: &[(crate::models::DelegatedWorkPacket, String)],
) -> String {
    if packets.len() == 1 {
        let (packet, result) = &packets[0];
        return format!("Delegated task '{}' result:\n\n{}", packet.title, result);
    }

    packets
        .iter()
        .enumerate()
        .map(|(i, (packet, result))| format!("## Task {}: {}\n\n{}", i + 1, packet.title, result))
        .collect::<Vec<_>>()
        .join("\n\n---\n\n")
}

pub fn resume_suspended_parent_after_task_completion(
    orch: &Orchestrator,
    child_job_id: &str,
) -> Result<(), String> {
    use crate::execution::delegation::refresh_packet_state;

    let resume = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let execution_id: Option<String> = jobs::table
            .find(child_job_id)
            .select(jobs::execution_id)
            .first::<Option<String>>(&mut *conn)
            .optional()
            .map_err(|e| format!("Failed to load child execution: {}", e))?
            .flatten();
        let Some(execution_id) = execution_id else {
            return Ok(());
        };

        let snapshot_json: Option<String> = executions::table
            .find(&execution_id)
            .select(executions::snapshot)
            .first(&mut *conn)
            .map_err(|e| format!("Failed to load execution snapshot: {}", e))?;
        let Some(snapshot_json) = snapshot_json else {
            return Ok(());
        };
        let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
            .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;
        let Some(packet) = snapshot
            .delegated_packets
            .iter()
            .find(|packet| packet.result_artifact_job_id.as_deref() == Some(child_job_id))
            .cloned()
        else {
            return Ok(());
        };

        let refreshed_packet = refresh_packet_state(&mut conn, &execution_id, &packet.id)?
            .ok_or_else(|| format!("Delegated packet disappeared: {}", packet.id))?;
        if !matches!(
            refreshed_packet.status,
            DelegatedStatus::Completed | DelegatedStatus::Failed | DelegatedStatus::Cancelled
        ) {
            return Ok(());
        }

        let sibling_ids: Vec<String> = snapshot
            .delegated_packets
            .iter()
            .filter(|candidate| {
                candidate.parent_job_id == refreshed_packet.parent_job_id
                    && if let Some(parent_tool_use_id) =
                        refreshed_packet.parent_tool_use_id.as_deref()
                    {
                        candidate.parent_tool_use_id.as_deref() == Some(parent_tool_use_id)
                    } else {
                        candidate.id == refreshed_packet.id
                    }
            })
            .map(|candidate| candidate.id.clone())
            .collect();

        let mut grouped_packets = Vec::with_capacity(sibling_ids.len());
        for packet_id in sibling_ids {
            let packet = refresh_packet_state(&mut conn, &execution_id, &packet_id)?
                .ok_or_else(|| format!("Delegated packet disappeared: {}", packet_id))?;
            if !matches!(
                packet.status,
                DelegatedStatus::Completed | DelegatedStatus::Failed | DelegatedStatus::Cancelled
            ) {
                return Ok(());
            }
            grouped_packets.push(packet);
        }

        let Some(parent_turn_id) = refreshed_packet.parent_turn_id.as_deref() else {
            return Ok(());
        };
        let Some(successor_turn) =
            crate::turns::queries::get_successor_turn(&mut conn, parent_turn_id)?
        else {
            return Ok(());
        };
        if successor_turn.state != TurnState::Pending {
            return Ok(());
        }

        let current_turn_id: Option<String> = jobs::table
            .find(&refreshed_packet.parent_job_id)
            .select(jobs::current_turn_id)
            .first::<Option<String>>(&mut *conn)
            .map_err(|e| format!("Failed to load parent turn pointer: {}", e))?;
        if current_turn_id.as_deref() != Some(successor_turn.id.as_str()) {
            return Ok(());
        }

        grouped_packets.sort_by_key(|packet| (packet.task_index, packet.created_at));
        let mut rendered_packets = Vec::with_capacity(grouped_packets.len());
        for packet in grouped_packets {
            let result = delegated_result_text(&mut conn, &packet)?;
            rendered_packets.push((packet, result));
        }

        (
            refreshed_packet.parent_job_id,
            format_delegated_resume_message(&rendered_packets),
        )
    };
    crate::execution::jobs::continue_job_impl(orch, &resume.0, Some(&resume.1), None).map(|_| ())
}

async fn wait_for_task_completion(
    orch: &Orchestrator,
    parent_ctx: &super::RunContext,
    packet: &crate::models::DelegatedWorkPacket,
    run_id: String,
    node_id: String,
    agent_config: AgentConfig,
    task_description: String,
    expected_artifact_type: String,
) -> CallbackResponse {
    let mut rx = orch.run_completions.subscribe();

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

    let result = tokio::time::timeout(INLINE_TASK_WAIT_BUDGET, completion_wait).await;

    match result {
        Ok(Ok(())) => {
            let mut conn = match orch.db.conn.lock() {
                Ok(c) => c,
                Err(_) => {
                    return CallbackResponse {
                        result: "Agent completed but failed to fetch results".to_string(),
                        artifact_uri: None,
                    }
                }
            };

            build_task_callback_response(
                &mut conn,
                parent_ctx,
                &run_id,
                &node_id,
                &agent_config,
                &task_description,
                &expected_artifact_type,
            )
        }
        Ok(Err(_)) => CallbackResponse {
            result: format!("Agent '{}' completion signal failed", agent_config.name),
            artifact_uri: None,
        },
        Err(_) => {
            if let Err(error) = prepare_parent_for_delegated_wait(orch, parent_ctx, packet) {
                log::warn!(
                    "Failed to prepare durable wait for parent job {}: {}",
                    parent_ctx.job_id,
                    error
                );
            } else {
                schedule_deferred_parent_suspend_for_delegated_wait(
                    orch.clone(),
                    parent_ctx.run_id.clone(),
                    parent_ctx.job_id.clone(),
                    node_id.clone(),
                );
            }
            CallbackResponse {
                result: format!(
                    "Delegated task '{}' suspended; the parent run will resume from the real child result.",
                    task_description
                ),
                artifact_uri: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_task_execution_context, find_existing_child_invocation,
        format_delegated_resume_message, resume_suspended_parent_after_task_completion,
        schedule_deferred_parent_suspend_for_delegated_wait, suspend_parent_for_delegated_wait,
        transition_spawned_job_to_running, wait_for_packet_run_materialization,
    };
    use crate::agent_process::process::{RunHandle, RunLifecycle, RunOccupancy};
    use crate::db::DbState;
    use crate::diesel_models::{NewExecution, NewJob, NewRun};
    use crate::models::{
        DelegatedOutputContract, DelegatedSessionMode, DelegatedSessionStrategy,
        DelegatedStatus, DelegatedWorkPacket, ExecutionSnapshot, RecipeSnapshot, RecipeTrigger,
        TriggerContext, TriggerType,
    };
    use crate::orchestrator::Orchestrator;
    use crate::schema::{executions, jobs, runs};
    use crate::services::testing::{MockChildProcess, MockProcessSpawner, TestServicesBuilder};
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use diesel::prelude::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn test_orchestrator(conn: diesel::sqlite::SqliteConnection) -> Orchestrator {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));

        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(crate::agent_process::process::AgentProcessState::default()),
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    fn test_orchestrator_with_process(
        conn: diesel::sqlite::SqliteConnection,
        process: MockProcessSpawner,
    ) -> Orchestrator {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().with_process(process).build());
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));

        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(crate::agent_process::process::AgentProcessState::default()),
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    fn register_active_process(
        orch: &Orchestrator,
        run_id: &str,
        session_id: &str,
        job_id: &str,
        turn_id: &str,
    ) -> Arc<Mutex<Option<Box<dyn crate::services::ChildProcess>>>> {
        let child = Arc::new(Mutex::new(Some(Box::new(MockChildProcess::with_stdout(
            999_999,
            vec![],
        ))
            as Box<dyn crate::services::ChildProcess>)));
        let stdin = Arc::new(Mutex::new(None));
        let mut handle = RunHandle::new(
            child.clone(),
            stdin,
            Some(session_id.to_string()),
            Some(job_id.to_string()),
        );
        handle.lifecycle = RunLifecycle::Live;
        handle.begin_turn(turn_id);
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register(run_id.to_string(), handle);
        child
    }

    #[test]
    fn spawned_child_job_transitions_from_ready_to_running() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let job_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        let new_job = NewJob {
            id: &job_id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: Some("/tmp/test-repo"),
            branch: None,
            base_commit: None,
            current_session_id: Some("session-1"),
            resume_session_id: None,
            status: "ready",
            agent_config_id: Some("Explore"),
            issue_id: None,
            project_id: &project_id,
            task_description: Some("Test child"),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: Some("tool-1"),
            task_index: Some(0),
            started_at: None,
            model: None,
            node_name: Some("Explore"),
            base_branch: None,
            current_turn_id: None,
        };

        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        transition_spawned_job_to_running(&orch, &job_id).unwrap();

        let mut conn = orch.db.conn.lock().unwrap();
        let (status, started_at): (String, Option<i32>) = jobs::table
            .find(&job_id)
            .select((jobs::status, jobs::started_at))
            .first(&mut *conn)
            .unwrap();

        assert_eq!(status, "running");
        assert!(started_at.is_some());
    }

    #[test]
    fn task_payload_fork_round_trip_preserves_delegated_packet_session() {
        let payload = crate::mcp::types::TaskPayload {
            description: "Explore".to_string(),
            prompt: "Inspect the code".to_string(),
            subagent_type: "Explore".to_string(),
            tier: None,
            backend_preference: None,
            run_in_background: None,
            session: Some(crate::mcp::types::TaskSessionMode::Fork),
            task_index: Some(0),
        };

        let session = match payload.session_mode().unwrap() {
            crate::mcp::types::TaskSessionMode::New => DelegatedSessionStrategy {
                mode: DelegatedSessionMode::New,
            },
            crate::mcp::types::TaskSessionMode::Fork => DelegatedSessionStrategy {
                mode: DelegatedSessionMode::Fork,
            },
        };

        let mut snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Test Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: None,
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
        );
        let packet = crate::execution::delegation::create_or_reuse_task_packet(
            &mut snapshot,
            crate::execution::delegation::CreateDelegatedPacketInput {
                parent_job_id: "parent-job",
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1"),
                title: &payload.description,
                problem_statement: &payload.prompt,
                agent_config_id: &payload.subagent_type,
                cwd: "/tmp/test-repo",
                filesystem_scope: None,
                approval_policy: None,
                acceptance: vec!["Return the delegated result with the return tool".to_string()],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: Some("Submit the task result".to_string()),
                },
                session,
                task_index: payload.task_index,
                tier_override: payload.tier.as_deref(),
                backend_preference: payload.backend_preference.as_deref(),
            },
        );

        assert_eq!(packet.session.mode, DelegatedSessionMode::Fork);
        assert_eq!(snapshot.delegated_packets[0].session.mode, DelegatedSessionMode::Fork);
    }

    #[test]
    fn ensure_task_execution_context_persists_synthetic_execution_on_parent_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let parent_job_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        let new_job = NewJob {
            id: &parent_job_id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: Some("/tmp/test-repo"),
            branch: None,
            base_commit: None,
            current_session_id: Some("session-1"),
            resume_session_id: None,
            status: "running",
            agent_config_id: Some("Manager"),
            issue_id: None,
            project_id: &project_id,
            task_description: Some("Parent"),
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: Some("Manager"),
            base_branch: None,
            current_turn_id: None,
        };

        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        let parent_ctx = crate::mcp::handlers::RunContext {
            run_id: "run-1".to_string(),
            job_id: parent_job_id.clone(),
            execution_id: None,
            exec_seq: None,
            issue_id: None,
            issue_number: None,
            project_id: project_id.clone(),
            project_key: "TASK".to_string(),
            job_type: "agent".to_string(),
            job_name: Some("Manager".to_string()),
        };

        let execution_id = ensure_task_execution_context(&orch, &parent_ctx).unwrap();

        let mut conn = orch.db.conn.lock().unwrap();
        let persisted_execution_id: Option<String> = jobs::table
            .find(&parent_job_id)
            .select(jobs::execution_id)
            .first(&mut *conn)
            .unwrap();
        let execution_exists = executions::table
            .find(&execution_id)
            .select(executions::id)
            .first::<String>(&mut *conn)
            .unwrap();

        let reused_execution_id = ensure_task_execution_context(&orch, &parent_ctx).unwrap();
        let execution_count: i64 = executions::table.count().get_result(&mut *conn).unwrap();

        assert_eq!(
            persisted_execution_id.as_deref(),
            Some(execution_id.as_str())
        );
        assert_eq!(execution_exists, execution_id);
        assert_eq!(reused_execution_id, execution_id);
        assert_eq!(execution_count, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_packet_run_materialization_waits_for_run_creation() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let parent_job_id = Uuid::new_v4().to_string();
        let child_job_id = Uuid::new_v4().to_string();
        let execution_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        for (job_id, execution_ref, parent_ref, status, node_name) in [
            (&parent_job_id, None, None, "running", Some("Manager")),
            (
                &child_job_id,
                None,
                Some(parent_job_id.as_str()),
                "running",
                Some("Explore"),
            ),
        ] {
            let new_job = NewJob {
                id: job_id,
                execution_id: execution_ref,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: parent_ref,
                worktree_path: Some("/tmp/test-repo"),
                branch: None,
                base_commit: None,
                current_session_id: Some("session-1"),
                resume_session_id: None,
                status,
                agent_config_id: Some("Explore"),
                issue_id: None,
                project_id: &project_id,
                task_description: Some("Delegated task"),
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: Some("tool-1"),
                task_index: Some(0),
                started_at: Some(now),
                model: None,
                node_name,
                base_branch: None,
                current_turn_id: None,
            };
            diesel::insert_into(jobs::table)
                .values(&new_job)
                .execute(&mut conn)
                .unwrap();
        }

        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: project_id.clone(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![DelegatedWorkPacket {
                id: "packet-1".to_string(),
                parent_job_id: parent_job_id.clone(),
                parent_turn_id: None,
                parent_tool_use_id: Some("tool-1".to_string()),
                origin: crate::models::DelegationOrigin::TaskTool,
                title: "Explore".to_string(),
                problem_statement: "Inspect the code".to_string(),
                agent_config_id: "Explore".to_string(),
                ownership: crate::models::DelegatedOwnershipScope {
                    cwd: "/tmp/test-repo".to_string(),
                    filesystem_scope: None,
                    approval_policy: None,
                },
                session: DelegatedSessionStrategy::default(),
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                status: DelegatedStatus::Materialized,
                materialized_node_ids: vec!["delegated-packet-1-agent".to_string()],
                result_artifact_job_id: Some(child_job_id.clone()),
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                created_at: now as i64,
            }],
            created_at: now as i64,
        };
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();
        diesel::insert_into(executions::table)
            .values(NewExecution {
                id: &execution_id,
                recipe_id: "recipe-1",
                issue_id: None,
                project_id: Some(&project_id),
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: None,
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        let db = orch.db.clone();
        let project_id_clone = project_id.clone();
        let child_job_id_clone = child_job_id.clone();
        let run_id = Uuid::new_v4().to_string();
        let run_id_for_task = run_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let mut conn = db.conn.lock().unwrap();
            let new_run = NewRun {
                id: &run_id_for_task,
                issue_id: None,
                project_id: Some(&project_id_clone),
                job_id: Some(&child_job_id_clone),
                chat_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some("fresh"),
            };
            diesel::insert_into(runs::table)
                .values(&new_run)
                .execute(&mut *conn)
                .unwrap();
        });

        let (_, materialized_job_id, materialized_run_id) =
            wait_for_packet_run_materialization(&orch, &execution_id, "packet-1")
                .await
                .unwrap();

        assert_eq!(materialized_job_id, child_job_id);
        assert_eq!(materialized_run_id, run_id);
    }

    #[test]
    fn format_delegated_resume_message_matches_batch_layout() {
        let packets = vec![
            (
                DelegatedWorkPacket {
                    id: "packet-1".to_string(),
                    parent_job_id: "parent".to_string(),
                    parent_turn_id: None,
                    parent_tool_use_id: Some("batch-1".to_string()),
                    origin: crate::models::DelegationOrigin::TaskTool,
                    title: "First task".to_string(),
                    problem_statement: "One".to_string(),
                    agent_config_id: "Explore".to_string(),
                    ownership: crate::models::DelegatedOwnershipScope {
                        cwd: "/tmp/test-repo".to_string(),
                        filesystem_scope: None,
                        approval_policy: None,
                    },
                    acceptance: vec![],
                    output_contract: DelegatedOutputContract {
                        schema_type: "return".to_string(),
                        tool_name: None,
                        description: None,
                    },
                    session: DelegatedSessionStrategy::default(),
                    status: DelegatedStatus::Completed,
                    materialized_node_ids: vec![],
                    result_artifact_job_id: Some("job-1".to_string()),
                    task_index: Some(0),
                    tier_override: None,
                    backend_preference: None,
                    created_at: 0,
                },
                "Result one".to_string(),
            ),
            (
                DelegatedWorkPacket {
                    id: "packet-2".to_string(),
                    parent_job_id: "parent".to_string(),
                    parent_turn_id: None,
                    parent_tool_use_id: Some("batch-1".to_string()),
                    origin: crate::models::DelegationOrigin::TaskTool,
                    title: "Second task".to_string(),
                    problem_statement: "Two".to_string(),
                    agent_config_id: "Explore".to_string(),
                    ownership: crate::models::DelegatedOwnershipScope {
                        cwd: "/tmp/test-repo".to_string(),
                        filesystem_scope: None,
                        approval_policy: None,
                    },
                    acceptance: vec![],
                    output_contract: DelegatedOutputContract {
                        schema_type: "return".to_string(),
                        tool_name: None,
                        description: None,
                    },
                    session: DelegatedSessionStrategy::default(),
                    status: DelegatedStatus::Completed,
                    materialized_node_ids: vec![],
                    result_artifact_job_id: Some("job-2".to_string()),
                    task_index: Some(1),
                    tier_override: None,
                    backend_preference: None,
                    created_at: 1,
                },
                "Result two".to_string(),
            ),
        ];

        let rendered = format_delegated_resume_message(&packets);
        assert_eq!(
            rendered,
            "## Task 1: First task\n\nResult one\n\n---\n\n## Task 2: Second task\n\nResult two"
        );
    }

    #[test]
    fn duplicate_child_lookup_reuses_existing_invocation() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let parent_job_id = Uuid::new_v4().to_string();
        let child_job_id = Uuid::new_v4().to_string();
        let older_run_id = Uuid::new_v4().to_string();
        let newer_run_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        for (id, status, parent_job_id_ref, tool_use_id, task_index) in [
            (&parent_job_id, "running", None, None, None),
            (
                &child_job_id,
                "running",
                Some(parent_job_id.as_str()),
                Some("tool-1"),
                Some(3),
            ),
        ] {
            let new_job = NewJob {
                id,
                execution_id: None,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: parent_job_id_ref,
                worktree_path: Some("/tmp/test-repo"),
                branch: None,
                base_commit: None,
                current_session_id: Some("session-1"),
                resume_session_id: None,
                status,
                agent_config_id: Some("Explore"),
                issue_id: None,
                project_id: &project_id,
                task_description: Some("Test child"),
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: tool_use_id,
                task_index,
                started_at: if status == "running" { Some(now) } else { None },
                model: None,
                node_name: Some("Explore"),
                base_branch: None,
                current_turn_id: None,
            };
            diesel::insert_into(jobs::table)
                .values(&new_job)
                .execute(&mut conn)
                .unwrap();
        }

        for (id, created_at) in [(&older_run_id, now - 5), (&newer_run_id, now)] {
            let new_run = NewRun {
                id,
                issue_id: None,
                project_id: Some(&project_id),
                job_id: Some(&child_job_id),
                chat_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(created_at),
                exited_at: None,
                created_at,
                updated_at: created_at,
                backend: None,
                exit_reason: None,
                start_mode: Some("fresh"),
            };
            diesel::insert_into(runs::table)
                .values(&new_run)
                .execute(&mut conn)
                .unwrap();
        }

        let orch = test_orchestrator(conn);
        let existing =
            find_existing_child_invocation(&orch, &parent_job_id, Some("tool-1"), Some(3))
                .unwrap()
                .unwrap();
        assert_eq!(existing.0, child_job_id);
        assert_eq!(existing.1, newer_run_id);

        let none =
            find_existing_child_invocation(&orch, &parent_job_id, Some("tool-1"), Some(4)).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn suspend_parent_for_delegated_wait_creates_dependency_successor_turn() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let issue_id = create_test_issue(&mut conn, &project_id, "Delegated wait");
        let parent_job_id = Uuid::new_v4().to_string();
        let parent_run_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &parent_job_id,
                execution_id: None,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: None,
                worktree_path: Some("/tmp/test-repo"),
                branch: None,
                base_commit: None,
                current_session_id: Some("session-1"),
                resume_session_id: None,
                status: "running",
                agent_config_id: None,
                issue_id: Some(&issue_id),
                project_id: &project_id,
                task_description: Some("Parent"),
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(now),
                model: None,
                node_name: Some("Manager"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();

        diesel::insert_into(runs::table)
            .values(NewRun {
                id: &parent_run_id,
                issue_id: Some(&issue_id),
                project_id: Some(&project_id),
                job_id: Some(&parent_job_id),
                chat_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some("fresh"),
            })
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        let predecessor = crate::turns::queries::create_initial_turn(
            &mut *orch.db.conn.lock().unwrap(),
            "turn-1",
            "session-1",
            &parent_job_id,
            &*orch.services.emitter,
        )
        .unwrap();
        crate::transitions::start_turn(
            &mut *orch.db.conn.lock().unwrap(),
            &predecessor.id,
            &parent_run_id,
            &*orch.services.emitter,
        )
        .unwrap();
        let child_handle = register_active_process(
            &orch,
            &parent_run_id,
            "session-1",
            &parent_job_id,
            &predecessor.id,
        );

        let parent_ctx = crate::mcp::handlers::RunContext {
            run_id: parent_run_id.clone(),
            job_id: parent_job_id.clone(),
            execution_id: None,
            exec_seq: Some(1),
            issue_id: Some(issue_id.clone()),
            issue_number: Some(1),
            project_id: project_id.clone(),
            project_key: "TASK".to_string(),
            job_type: "implementation".to_string(),
            job_name: Some("Manager".to_string()),
        };
        let packet = DelegatedWorkPacket {
            id: "packet-1".to_string(),
            parent_job_id: parent_job_id.clone(),
            parent_turn_id: Some(predecessor.id.clone()),
            parent_tool_use_id: None,
            origin: crate::models::DelegationOrigin::TaskTool,
            title: "Task".to_string(),
            problem_statement: "Do the work".to_string(),
            agent_config_id: "Explore".to_string(),
            ownership: crate::models::DelegatedOwnershipScope {
                cwd: "/tmp/test-repo".to_string(),
                filesystem_scope: None,
                approval_policy: None,
            },
            session: DelegatedSessionStrategy::default(),
            acceptance: vec![],
            output_contract: DelegatedOutputContract {
                schema_type: "return".to_string(),
                tool_name: None,
                description: None,
            },
            status: DelegatedStatus::Materialized,
            materialized_node_ids: vec![],
            result_artifact_job_id: None,
            task_index: None,
            tier_override: None,
            backend_preference: None,
            created_at: now as i64,
        };

        suspend_parent_for_delegated_wait(&orch, &parent_ctx, &packet).unwrap();

        let mut conn = orch.db.conn.lock().unwrap();
        let current_turn_id: Option<String> = jobs::table
            .find(&parent_job_id)
            .select(jobs::current_turn_id)
            .first(&mut *conn)
            .unwrap();
        let successor = crate::turns::queries::get_successor_turn(&mut conn, &predecessor.id)
            .unwrap()
            .unwrap();
        let run_status: Option<String> = runs::table
            .find(&parent_run_id)
            .select(runs::status)
            .first(&mut *conn)
            .unwrap();

        assert_eq!(current_turn_id.as_deref(), Some(successor.id.as_str()));
        assert_eq!(
            successor.start_reason,
            crate::models::TurnStartReason::DependencyUnblock
        );
        assert_eq!(successor.state, crate::models::TurnState::Pending);
        assert_eq!(run_status.as_deref(), Some("live"));
        assert!(matches!(
            orch.process_state.get_occupancy(&parent_run_id),
            Some(RunOccupancy::Idle)
        ));
        assert_eq!(
            orch.process_state.find_process_by_session("session-1"),
            Some(parent_run_id.clone())
        );
        assert!(child_handle
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deferred_parent_suspend_waits_until_after_callback_returns() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let issue_id = create_test_issue(&mut conn, &project_id, "Delegated wait");
        let parent_job_id = Uuid::new_v4().to_string();
        let parent_run_id = Uuid::new_v4().to_string();
        let child_job_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &parent_job_id,
                execution_id: None,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: None,
                worktree_path: Some("/tmp/test-repo"),
                branch: None,
                base_commit: None,
                current_session_id: Some("session-1"),
                resume_session_id: None,
                status: "running",
                agent_config_id: None,
                issue_id: Some(&issue_id),
                project_id: &project_id,
                task_description: Some("Parent"),
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(now),
                model: None,
                node_name: Some("Manager"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();

        diesel::insert_into(runs::table)
            .values(NewRun {
                id: &parent_run_id,
                issue_id: Some(&issue_id),
                project_id: Some(&project_id),
                job_id: Some(&parent_job_id),
                chat_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some("fresh"),
            })
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        let predecessor = crate::turns::queries::create_initial_turn(
            &mut *orch.db.conn.lock().unwrap(),
            "turn-1",
            "session-1",
            &parent_job_id,
            &*orch.services.emitter,
        )
        .unwrap();
        crate::transitions::start_turn(
            &mut *orch.db.conn.lock().unwrap(),
            &predecessor.id,
            &parent_run_id,
            &*orch.services.emitter,
        )
        .unwrap();
        let child_handle = register_active_process(
            &orch,
            &parent_run_id,
            "session-1",
            &parent_job_id,
            &predecessor.id,
        );

        let parent_ctx = crate::mcp::handlers::RunContext {
            run_id: parent_run_id.clone(),
            job_id: parent_job_id.clone(),
            execution_id: None,
            exec_seq: Some(1),
            issue_id: Some(issue_id.clone()),
            issue_number: Some(1),
            project_id: project_id.clone(),
            project_key: "TASK".to_string(),
            job_type: "implementation".to_string(),
            job_name: Some("Manager".to_string()),
        };
        let packet = DelegatedWorkPacket {
            id: "packet-1".to_string(),
            parent_job_id: parent_job_id.clone(),
            parent_turn_id: Some(predecessor.id.clone()),
            parent_tool_use_id: None,
            origin: crate::models::DelegationOrigin::TaskTool,
            title: "Task".to_string(),
            problem_statement: "Do the work".to_string(),
            agent_config_id: "Explore".to_string(),
            ownership: crate::models::DelegatedOwnershipScope {
                cwd: "/tmp/test-repo".to_string(),
                filesystem_scope: None,
                approval_policy: None,
            },
            session: DelegatedSessionStrategy::default(),
            acceptance: vec![],
            output_contract: DelegatedOutputContract {
                schema_type: "return".to_string(),
                tool_name: None,
                description: None,
            },
            status: DelegatedStatus::Materialized,
            materialized_node_ids: vec![],
            result_artifact_job_id: Some(child_job_id.clone()),
            task_index: None,
            tier_override: None,
            backend_preference: None,
            created_at: now as i64,
        };

        super::prepare_parent_for_delegated_wait(&orch, &parent_ctx, &packet).unwrap();
        schedule_deferred_parent_suspend_for_delegated_wait(
            orch.clone(),
            parent_run_id.clone(),
            parent_job_id.clone(),
            child_job_id.clone(),
        );

        {
            let mut conn = orch.db.conn.lock().unwrap();
            let run_status: Option<String> = runs::table
                .find(&parent_run_id)
                .select(runs::status)
                .first(&mut *conn)
                .unwrap();
            assert_eq!(run_status.as_deref(), Some("live"));
        }

        tokio::time::sleep(
            super::DEFERRED_TASK_PARENT_SUSPEND_GRACE + std::time::Duration::from_millis(150),
        )
        .await;

        let mut conn = orch.db.conn.lock().unwrap();
        let run_status: Option<String> = runs::table
            .find(&parent_run_id)
            .select(runs::status)
            .first(&mut *conn)
            .unwrap();
        assert_eq!(run_status.as_deref(), Some("live"));
        assert!(matches!(
            orch.process_state.get_occupancy(&parent_run_id),
            Some(RunOccupancy::Idle)
        ));
        assert_eq!(
            orch.process_state.find_process_by_session("session-1"),
            Some(parent_run_id.clone())
        );
        assert!(child_handle
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none());
    }

    #[test]
    fn completed_child_resume_path_recovers_once_suspend_establishes_successor() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TASK");
        let issue_id = create_test_issue(&mut conn, &project_id, "Delegated resume");
        let parent_job_id = Uuid::new_v4().to_string();
        let parent_run_id = Uuid::new_v4().to_string();
        let child_job_id = Uuid::new_v4().to_string();
        let execution_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &parent_job_id,
                execution_id: None,
                manager_id: None,
                recipe_node_id: None,
                parent_job_id: None,
                worktree_path: Some("/tmp/test-repo"),
                branch: None,
                base_commit: None,
                current_session_id: Some("session-1"),
                resume_session_id: None,
                status: "running",
                agent_config_id: None,
                issue_id: Some(&issue_id),
                project_id: &project_id,
                task_description: Some("Parent"),
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(now),
                model: None,
                node_name: Some("Manager"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(runs::table)
            .values(NewRun {
                id: &parent_run_id,
                issue_id: Some(&issue_id),
                project_id: Some(&project_id),
                job_id: Some(&parent_job_id),
                chat_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some("fresh"),
            })
            .execute(&mut conn)
            .unwrap();
        let predecessor = crate::turns::queries::create_initial_turn(
            &mut conn,
            "turn-1",
            "session-1",
            &parent_job_id,
            &crate::services::testing::CapturingEmitter::new(),
        )
        .unwrap();
        crate::transitions::start_turn(
            &mut conn,
            &predecessor.id,
            &parent_run_id,
            &crate::services::testing::CapturingEmitter::new(),
        )
        .unwrap();

        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Delegation".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some(issue_id.clone()),
                project_id: project_id.clone(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![DelegatedWorkPacket {
                id: "packet-1".to_string(),
                parent_job_id: parent_job_id.clone(),
                parent_turn_id: Some(predecessor.id.clone()),
                parent_tool_use_id: None,
                origin: crate::models::DelegationOrigin::TaskTool,
                title: "Task".to_string(),
                problem_statement: "Do the work".to_string(),
                agent_config_id: "Explore".to_string(),
                ownership: crate::models::DelegatedOwnershipScope {
                    cwd: "/tmp/test-repo".to_string(),
                    filesystem_scope: None,
                    approval_policy: None,
                },
                session: DelegatedSessionStrategy::default(),
                acceptance: vec![],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: None,
                },
                status: DelegatedStatus::Materialized,
                materialized_node_ids: vec!["delegated-packet-1-agent".to_string()],
                result_artifact_job_id: Some(child_job_id.clone()),
                task_index: None,
                tier_override: None,
                backend_preference: None,
                created_at: now as i64,
            }],
            created_at: now as i64,
        };
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();
        diesel::insert_into(executions::table)
            .values(NewExecution {
                id: &execution_id,
                recipe_id: "recipe-1",
                issue_id: Some(&issue_id),
                project_id: None,
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: Some(1),
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(jobs::table)
            .values(NewJob {
                id: &child_job_id,
                execution_id: Some(&execution_id),
                manager_id: None,
                recipe_node_id: Some("delegated-packet-1-agent"),
                parent_job_id: Some(&parent_job_id),
                worktree_path: Some("/tmp/test-repo"),
                branch: None,
                base_commit: None,
                current_session_id: None,
                resume_session_id: None,
                status: "complete",
                agent_config_id: None,
                issue_id: Some(&issue_id),
                project_id: &project_id,
                task_description: Some("Child"),
                created_at: now,
                updated_at: now,
                completed_at: Some(now),
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(now),
                model: None,
                node_name: Some("Explore"),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(&mut conn)
            .unwrap();

        let mut mock_process = MockProcessSpawner::new();
        mock_process
            .expect_spawn()
            .returning(|_| Err("mock: not a real process".to_string()));
        let orch = test_orchestrator_with_process(conn, mock_process);

        let parent_ctx = crate::mcp::handlers::RunContext {
            run_id: parent_run_id.clone(),
            job_id: parent_job_id.clone(),
            execution_id: None,
            exec_seq: Some(1),
            issue_id: Some(issue_id.clone()),
            issue_number: Some(1),
            project_id: project_id.clone(),
            project_key: "TASK".to_string(),
            job_type: "implementation".to_string(),
            job_name: Some("Manager".to_string()),
        };
        let packet = DelegatedWorkPacket {
            id: "packet-1".to_string(),
            parent_job_id: parent_job_id.clone(),
            parent_turn_id: Some(predecessor.id.clone()),
            parent_tool_use_id: None,
            origin: crate::models::DelegationOrigin::TaskTool,
            title: "Task".to_string(),
            problem_statement: "Do the work".to_string(),
            agent_config_id: "Explore".to_string(),
            ownership: crate::models::DelegatedOwnershipScope {
                cwd: "/tmp/test-repo".to_string(),
                filesystem_scope: None,
                approval_policy: None,
            },
            session: DelegatedSessionStrategy::default(),
            acceptance: vec![],
            output_contract: DelegatedOutputContract {
                schema_type: "return".to_string(),
                tool_name: None,
                description: None,
            },
            status: DelegatedStatus::Materialized,
            materialized_node_ids: vec![],
            result_artifact_job_id: Some(child_job_id.clone()),
            task_index: None,
            tier_override: None,
            backend_preference: None,
            created_at: now as i64,
        };
        let result_before_suspend =
            resume_suspended_parent_after_task_completion(&orch, &child_job_id);
        assert!(
            result_before_suspend.is_ok(),
            "expected pre-suspend resume attempt to no-op, got {:?}",
            result_before_suspend
        );

        suspend_parent_for_delegated_wait(&orch, &parent_ctx, &packet).unwrap();

        let result = resume_suspended_parent_after_task_completion(&orch, &child_job_id);
        assert!(
            result.is_err(),
            "expected post-suspend resume path to reach continue_job_impl and fail under test harness, got {:?}",
            result
        );
    }
}

/// Compute the cairn:// artifact URI for a task node, matching the display name logic
/// used by issue_resources.rs (siblings ordered by created_at, -N suffix for duplicates).
///
/// Returns None if required context (issue, execution, parent node name) is unavailable.
fn compute_artifact_uri(
    conn: &mut diesel::SqliteConnection,
    parent_ctx: &super::RunContext,
    task_node_id: &str,
) -> Option<String> {
    let issue_id = parent_ctx.issue_id.as_deref()?;
    let issue_number = parent_ctx.issue_number?;
    let execution_id = parent_ctx.execution_id.as_deref()?;
    let parent_node_name = parent_ctx.job_name.as_deref()?;

    // exec_num: 1-based position in executions ordered by started_at (matches issue_resources.rs)
    let exec_ids: Vec<String> = executions::table
        .filter(executions::issue_id.eq(issue_id))
        .order(executions::started_at.asc())
        .select(executions::id)
        .load(conn)
        .ok()?;
    let exec_num = exec_ids.iter().position(|id| id == execution_id)? + 1;

    let parent_recipe_node_id: Option<String> = jobs::table
        .find(&parent_ctx.job_id)
        .select(jobs::recipe_node_id)
        .first(conn)
        .ok()?;
    let task_display_name: Option<String> = jobs::table
        .find(task_node_id)
        .select(jobs::node_name)
        .first(conn)
        .ok()
        .flatten();
    let parent_node_segment =
        visible_node_segment(parent_recipe_node_id.as_deref(), Some(parent_node_name))?;
    let task_display_name = task_display_name.unwrap_or_else(|| "task".to_string());

    Some(format!(
        "cairn://{}/{}/{}/{}/task/{}/artifact",
        parent_ctx.project_key, issue_number, exec_num, parent_node_segment, task_display_name,
    ))
}
