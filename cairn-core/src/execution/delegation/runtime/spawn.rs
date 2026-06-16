use crate::config::{
    agents as config_agents,
    presets::{load_effective_presets, resolve_runtime_selection},
};
use crate::execution::delegation::{DelegatedTaskPayload, SpawnTaskPacketsInput};
use crate::models::{AgentConfig, DelegatedStatus, Model};
use crate::orchestrator::Orchestrator;
use cairn_common::protocol::CallbackResponse;
use std::path::PathBuf;
use tokio::sync::broadcast;

use super::common::{
    dispatch_ready_agent_jobs, ensure_task_execution_context, lookup_run_context,
    persist_task_packet, project_repo_path, select_optional_text,
    wait_for_packet_run_materialization, ParentRunContext,
};
use super::results::{build_task_callback_response, compute_artifact_uri};
use super::resume::{
    prepare_parent_for_delegated_wait, schedule_deferred_parent_suspend_for_delegated_wait,
};

const INLINE_TASK_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(45);

/// Resolve the effective agent config for a delegated task payload.
async fn resolve_task_agent_config(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    _execution_id: &str,
    project_path: Option<&std::path::Path>,
    payload: &DelegatedTaskPayload,
) -> Result<AgentConfig, String> {
    let config_dir = orch.config_dir.clone();
    let file_agent =
        match config_agents::get_agent(&config_dir, &payload.subagent_type, project_path) {
            Ok(Some(agent)) => agent,
            Ok(None) => {
                let agents =
                    config_agents::list_agents(&config_dir, project_path).unwrap_or_default();
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
                    None => {
                        return Err(format!("Agent config not found: {}", payload.subagent_type))
                    }
                }
            }
            Err(e) => return Err(format!("Failed to load agent config: {}", e)),
        };

    let parent_backend = select_optional_text(
        &orch.db.local,
        "SELECT model FROM jobs WHERE id = ?1",
        &parent_ctx.job_id,
    )
    .await
    .ok()
    .flatten()
    .as_deref()
    .and_then(crate::backends::backend_for_model)
    .map(str::to_string);
    let presets = load_effective_presets(&config_dir, project_path);
    let authored_tier = payload
        .tier
        .as_deref()
        .or(file_agent.tier.as_ref().map(Model::as_str));
    let authored_backend = payload
        .backend_preference
        .as_deref()
        .or(parent_backend.as_deref())
        .or(file_agent.backend_preference.as_deref());
    // Resolve-early: real resolution (loud) at materialization fills the concrete
    // atomic selection + extras, so the child session never re-resolves a tier.
    let (selection, extras) = resolve_runtime_selection(authored_tier, authored_backend, &presets)?;

    Ok(AgentConfig {
        id: file_agent.id,
        name: file_agent.name,
        description: file_agent.description,
        prompt: file_agent.prompt,
        tools: file_agent.tools,
        tier: payload.tier.as_ref().map(Model::new).or(file_agent.tier),
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
        fence: file_agent.fence,
        backend_preference: payload
            .backend_preference
            .clone()
            .or(parent_backend)
            .or(file_agent.backend_preference),
        selection: Some(selection),
        extras: Some(extras),
    })
}

/// A delegated task whose child job and run have materialized.
struct MaterializedTask {
    packet: crate::models::DelegatedWorkPacket,
    job_id: String,
    run_id: String,
    agent_config: AgentConfig,
    description: String,
    schema_type: String,
}

/// Canonical delegated-task spawn pipeline.
///
/// Persists one packet per payload under a shared `group_id` (so multiple tasks
/// in a single request resume together as a batch), advances and dispatches the
/// delegated execution once, then either returns spawned task URIs immediately
/// (`background`) or waits inline for completion — durably suspending the parent
/// turn once if the inline budget expires, to resume from the real results.
pub async fn spawn_task_packets(
    orch: &Orchestrator,
    input: SpawnTaskPacketsInput<'_>,
) -> CallbackResponse {
    macro_rules! err {
        ($msg:expr) => {
            return CallbackResponse {
                result: $msg,
                ..Default::default()
            }
        };
    }

    if input.payloads.is_empty() {
        err!("No tasks provided".to_string());
    }

    let parent_ctx = match lookup_run_context(&orch.db.local, input.run_id, input.cwd).await {
        Ok(ctx) => ctx,
        Err(e) => err!(e),
    };
    let project_path = project_repo_path(&orch.db.local, &parent_ctx.project_id)
        .await
        .map(PathBuf::from);
    let execution_id = match ensure_task_execution_context(orch, &parent_ctx).await {
        Ok(execution_id) => execution_id,
        Err(e) => err!(e),
    };

    let multiple = input.payloads.len() > 1;
    let mut prepared: Vec<(crate::models::DelegatedWorkPacket, AgentConfig, String)> =
        Vec::with_capacity(input.payloads.len());
    for (idx, payload) in input.payloads.iter().enumerate() {
        let mut payload = payload.clone();
        if multiple && payload.task_index.is_none() {
            payload.task_index = Some(idx as i32);
        }
        let agent_config = match resolve_task_agent_config(
            orch,
            &parent_ctx,
            &execution_id,
            project_path.as_deref(),
            &payload,
        )
        .await
        {
            Ok(config) => config,
            Err(e) => err!(e),
        };
        let packet = match persist_task_packet(
            orch,
            &execution_id,
            &parent_ctx,
            &payload,
            &agent_config,
            input.cwd,
            // The originating tool-use id is the durable resume-group key and the
            // value the transcript queries child jobs by; fall back to the
            // synthetic group id only when no tool-use id was forwarded.
            Some(input.parent_tool_use_id.unwrap_or(input.group_id)),
        )
        .await
        {
            Ok(packet) => packet,
            Err(e) => err!(e),
        };
        prepared.push((packet, agent_config, payload.description.clone()));
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

    let mut materialized: Vec<MaterializedTask> = Vec::with_capacity(prepared.len());
    for (packet, agent_config, description) in prepared {
        let (packet_state, job_id, run_id) =
            match wait_for_packet_run_materialization(orch, &execution_id, &packet.id).await {
                Ok(result) => result,
                Err(e) => err!(e),
            };
        let schema_type = packet_state.output_contract.schema_type.clone();
        materialized.push(MaterializedTask {
            packet: packet_state,
            job_id,
            run_id,
            agent_config,
            description,
            schema_type,
        });
    }

    if input.background {
        return background_task_response(orch, &parent_ctx, &materialized).await;
    }

    wait_for_tasks_completion(orch, &parent_ctx, materialized).await
}

/// Fire-and-forget response for `background` spawns: return task URIs without
/// yielding the parent turn. The child still runs; its result is later readable
/// via the task artifact URI and the node `tasks` collection.
async fn background_task_response(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    let mut lines = Vec::with_capacity(materialized.len());
    let mut last_uri = None;
    for task in materialized {
        let uri = compute_artifact_uri(&orch.db.local, parent_ctx, &task.job_id).await;
        if uri.is_some() {
            last_uri = uri.clone();
        }
        lines.push(format!(
            "- {} ({})",
            uri.unwrap_or_else(|| "<artifact pending>".to_string()),
            task.description
        ));
    }
    let artifact_uri = if materialized.len() == 1 {
        last_uri
    } else {
        None
    };
    CallbackResponse {
        result: format!(
            "Spawned {} background task(s); results will appear at:\n{}",
            materialized.len(),
            lines.join("\n")
        ),
        artifact_uri,
        ..Default::default()
    }
}

/// Wait inline for all delegated runs to finish; if the budget expires, durably
/// suspend the parent once so it resumes from the real combined child results.
async fn wait_for_tasks_completion(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: Vec<MaterializedTask>,
) -> CallbackResponse {
    let pending: std::collections::HashSet<String> = materialized
        .iter()
        .filter(|task| {
            !matches!(
                task.packet.status,
                DelegatedStatus::Completed | DelegatedStatus::Failed
            )
        })
        .map(|task| task.run_id.clone())
        .collect();

    if !pending.is_empty() {
        let mut rx = orch.run_completions.subscribe();
        let completion_wait = async {
            let mut remaining = pending.clone();
            while !remaining.is_empty() {
                match rx.recv().await {
                    Ok(run_id) => {
                        remaining.remove(&run_id);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Err(()),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
            Ok(())
        };
        match tokio::time::timeout(INLINE_TASK_WAIT_BUDGET, completion_wait).await {
            Ok(Ok(())) => {}
            _ => return suspend_parent_for_tasks(orch, parent_ctx, &materialized),
        }
    }

    build_tasks_response(orch, parent_ctx, &materialized).await
}

/// Yield the parent turn once and schedule the durable suspend for a task batch.
/// The real combined results are delivered on resume via
/// `resume_suspended_parent_after_task_completion`.
fn suspend_parent_for_tasks(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    let first = &materialized[0];
    if let Err(error) = prepare_parent_for_delegated_wait(orch, parent_ctx, &first.packet) {
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
            first.job_id.clone(),
        );
    }
    let summary = if materialized.len() == 1 {
        format!("Delegated task '{}'", materialized[0].description)
    } else {
        format!("{} delegated tasks", materialized.len())
    };
    CallbackResponse {
        result: format!(
            "{} suspended; the parent run will resume from the real child result(s).",
            summary
        ),
        ..Default::default()
    }
}

/// Build the combined inline response once all delegated runs are terminal.
async fn build_tasks_response(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    if materialized.len() == 1 {
        let task = &materialized[0];
        return build_task_callback_response(
            &orch.db.local,
            parent_ctx,
            &task.run_id,
            &task.job_id,
            &task.agent_config,
            &task.description,
            &task.schema_type,
        )
        .await;
    }

    let mut sections = Vec::with_capacity(materialized.len());
    for (index, task) in materialized.iter().enumerate() {
        let response = build_task_callback_response(
            &orch.db.local,
            parent_ctx,
            &task.run_id,
            &task.job_id,
            &task.agent_config,
            &task.description,
            &task.schema_type,
        )
        .await;
        sections.push(format!(
            "## Task {}: {}\n\n{}",
            index + 1,
            task.description,
            response.result
        ));
    }
    CallbackResponse {
        result: sections.join("\n\n---\n\n"),
        ..Default::default()
    }
}
