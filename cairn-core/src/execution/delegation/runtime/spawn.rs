use crate::agent_process::process::SuspendKind;
use crate::config::{
    agents as config_agents,
    presets::{load_effective_presets, resolve_runtime_selection},
};
use crate::execution::delegation::{
    DelegatedTaskPayload, SpawnCallPacketsInput, SpawnTaskPacketsInput, SpawnWorkflowPacketsInput,
};
use crate::execution::jobs::{
    prepare_call_run, prepare_workflow_run, start_call_run, start_workflow_run, CreateCallRunInput,
    CreateWorkflowRunInput,
};
use crate::models::{AgentConfig, DelegatedStatus, Model};
use crate::orchestrator::Orchestrator;
use cairn_common::protocol::CallbackResponse;
use std::path::PathBuf;
use tokio::sync::broadcast;

use super::common::{
    dispatch_ready_agent_jobs, ensure_task_execution_context, lookup_run_context,
    persist_call_packet, persist_task_packet, project_repo_path, select_optional_text,
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
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id).await?;
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
        &db,
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

    // Reject a malformed inline schema or unknown preset now, before persisting
    // any packet, so the caller sees the error at task-append time rather than
    // when the child writes its result.
    for payload in input.payloads {
        if let Err(e) = crate::execution::delegation::resolve_delegated_output_contract(
            payload.output_schema.as_ref(),
        ) {
            err!(e);
        }
    }

    // Resolve the parent run's owning database (a team run lives in its replica).
    // The cwd fallback (run_id None) stays on the private DB.
    let db = match input.run_id {
        Some(run_id) => crate::execution::routing::owning_db_for_run(&orch.db, run_id)
            .await
            .unwrap_or_else(|_| orch.db.local.clone()),
        None => orch.db.local.clone(),
    };
    let parent_ctx = match lookup_run_context(&db, input.run_id, input.cwd).await {
        Ok(ctx) => ctx,
        Err(e) => err!(e),
    };
    let project_path = project_repo_path(&db, &parent_ctx.project_id)
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
            input.background,
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
        let schema_type = packet_state.output_contract.artifact_name();
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

    // Owned-loop backends (OpenRouter) never inline-wait on delegated tasks:
    // there is no warm process, so the parent resumes by cold-restarting when the
    // child results land. Suspend the turn now instead of blocking the HTTP
    // thread on the completion wait.
    let owns_turn_loop = input
        .run_id
        .map(|run_id| orch.process_state.run_owns_turn_loop(run_id))
        .unwrap_or(false);
    if owns_turn_loop {
        return suspend_parent_for_owned_loop(orch, &parent_ctx, &materialized);
    }

    wait_for_tasks_completion(orch, &parent_ctx, materialized).await
}

/// Canonical ephemeral-call spawn pipeline (CAIRN-2481).
///
/// Each call is created directly as a node-less job+session+run (no packet ->
/// node expansion, no DAG advancement), then wrapped in a pre-materialized
/// `CallTool` packet so the SAME task wait/suspend/resume tail runs it: inline
/// wait, durable suspend on budget, or fire-and-forget `background`. The packet
/// is persisted BEFORE the session starts, so a fast-finishing call always finds
/// a packet to resume its parent through.
pub async fn spawn_call_packets(
    orch: &Orchestrator,
    input: SpawnCallPacketsInput<'_>,
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
        err!("No calls provided".to_string());
    }

    // Reject a malformed inline schema / unknown preset now, before creating any
    // run, so the caller sees the error at call-append time.
    for payload in input.payloads {
        if let Err(e) = crate::execution::delegation::resolve_delegated_output_contract(
            payload.output_schema.as_ref(),
        ) {
            err!(e);
        }
    }

    let db = match input.run_id {
        Some(run_id) => crate::execution::routing::owning_db_for_run(&orch.db, run_id)
            .await
            .unwrap_or_else(|_| orch.db.local.clone()),
        None => orch.db.local.clone(),
    };
    let parent_ctx = match lookup_run_context(&db, input.run_id, input.cwd).await {
        Ok(ctx) => ctx,
        Err(e) => err!(e),
    };
    let execution_id = match ensure_task_execution_context(orch, &parent_ctx).await {
        Ok(execution_id) => execution_id,
        Err(e) => err!(e),
    };

    // The originating tool-use id is the durable resume-group key; fall back to
    // the synthetic group id when none was forwarded.
    let parent_tool_use_id = input.parent_tool_use_id.unwrap_or(input.group_id);
    let multiple = input.payloads.len() > 1;
    let mut materialized: Vec<MaterializedTask> = Vec::with_capacity(input.payloads.len());

    // Journaled replay (CAIRN-2498) is scoped to a workflow harness's background
    // `agent()` calls: only there does a restart re-run the SAME run_id with the
    // same dispatch ordinals, which is the sole reader of the journal. The
    // detection is gated on `background` so an ordinary inline call never pays
    // for it and its response shape stays bit-for-bit unchanged.
    let parent_is_workflow = input.background
        && select_optional_text(
            &db,
            "SELECT agent_config_id FROM jobs WHERE id = ?1",
            &parent_ctx.job_id,
        )
        .await
        .ok()
        .flatten()
        .as_deref()
            == Some("workflow");
    // The journal is per-machine private state, so it always reads/writes the
    // private DB even when the workflow run itself lives in a team replica.
    let journal_db = orch.db.local.clone();
    // Bodies of journal cache hits, replayed inline instead of spawning a call.
    let mut memo_bodies: Vec<String> = Vec::new();

    for (idx, payload) in input.payloads.iter().enumerate() {
        let task_index = payload
            .task_index
            .or(if multiple { Some(idx as i32) } else { None });

        // Journal check: a workflow-parented call at a known ordinal short-
        // circuits on a prompt-hash match (memo hit, no spawn), and otherwise
        // records the journal key so the live call is journaled on completion.
        let journal_link = if parent_is_workflow {
            if let Some(ordinal) = payload.ordinal {
                let prompt_hash = crate::workflow_journal::hash_prompt(&payload.prompt);
                match crate::workflow_journal::get_entry(&journal_db, &parent_ctx.run_id, ordinal)
                    .await
                {
                    Ok(Some(entry)) if entry.prompt_hash == prompt_hash => {
                        // Cache hit: replay the memoized body (or the failure
                        // sentinel) without spawning a call.
                        memo_bodies.push(
                            entry
                                .result_json
                                .unwrap_or_else(|| CALL_FAILED_SENTINEL.to_string()),
                        );
                        continue;
                    }
                    // Cache miss or prompt mismatch: run live, overwrite on
                    // completion.
                    _ => Some(crate::workflow_journal::CallLink {
                        workflow_run_id: parent_ctx.run_id.clone(),
                        ordinal,
                        prompt_hash,
                    }),
                }
            } else {
                None
            }
        } else {
            None
        };

        // Already validated above; unwrap the resolved contract.
        let contract = match crate::execution::delegation::resolve_delegated_output_contract(
            payload.output_schema.as_ref(),
        ) {
            Ok(contract) => contract,
            Err(e) => err!(e),
        };

        // 1. Create the run's rows + transcript seed (no session yet).
        let prepared = match prepare_call_run(
            orch,
            CreateCallRunInput {
                parent_job_id: parent_ctx.job_id.clone(),
                execution_id: Some(execution_id.clone()),
                subagent_type: payload.subagent_type.clone(),
                description: payload.description.clone(),
                prompt: payload.prompt.clone(),
                tier: payload.tier.clone(),
                backend_preference: payload.backend_preference.clone(),
                output_contract: contract.clone(),
                worktree: payload.worktree,
                label: payload.label.clone(),
                phase: payload.phase.clone(),
                parent_tool_use_id: Some(parent_tool_use_id.to_string()),
                task_index,
                workflow_journal_link: journal_link,
            },
        ) {
            Ok(prepared) => prepared,
            Err(e) => err!(e),
        };

        // 2. Persist the call packet BEFORE the session starts (close the
        //    fast-finish resume race). A `none`-mode call has no worktree, so the
        //    packet's cwd falls back to the caller's cwd for display.
        let cwd_for_packet = prepared
            .worktree_path
            .clone()
            .unwrap_or_else(|| input.cwd.to_string());
        let packet = match persist_call_packet(
            orch,
            &execution_id,
            &parent_ctx,
            &prepared.agent_config.id,
            &payload.description,
            &payload.prompt,
            &cwd_for_packet,
            contract.clone(),
            &prepared.job_id,
            Some(parent_tool_use_id),
            payload.tier.as_deref(),
            task_index,
            input.background,
        )
        .await
        {
            Ok(packet) => packet,
            Err(e) => err!(e),
        };

        // 3. Start the backend session now that the packet exists.
        if let Err(e) = start_call_run(orch, &prepared) {
            err!(e);
        }

        let schema_type = contract.artifact_name();
        materialized.push(MaterializedTask {
            packet,
            job_id: prepared.job_id,
            run_id: prepared.run_id,
            agent_config: prepared.agent_config,
            description: payload.description.clone(),
            schema_type,
        });
    }

    if input.background {
        // Ordinary (non-workflow) callers never journal, so their response is
        // built by the unchanged path — bit-for-bit identical. Only a workflow
        // caller with at least one memo hit takes the memo-aware response.
        if memo_bodies.is_empty() {
            return background_task_response(orch, &parent_ctx, &materialized).await;
        }
        return background_call_response_with_memos(orch, &parent_ctx, &materialized, &memo_bodies)
            .await;
    }

    let owns_turn_loop = input
        .run_id
        .map(|run_id| orch.process_state.run_owns_turn_loop(run_id))
        .unwrap_or(false);
    if owns_turn_loop {
        return suspend_parent_for_owned_loop(orch, &parent_ctx, &materialized);
    }

    wait_for_tasks_completion(orch, &parent_ctx, materialized).await
}

/// The failure sentinel a journaled call failure replays as, mirroring
/// `resources::node::CALL_WAIT_FAILED` and the harness client's constant. A
/// memo hit whose stored result is a failure carries this body so `agent()`
/// resolves to `null`.
const CALL_FAILED_SENTINEL: &str = "__CAIRN_CALL_FAILED__";

/// One-line prefix marking a journal-replayed result in the calls-append
/// response. The harness detects this before parsing a call URI and reads the
/// memoized body directly, so a memo hit never spawns or polls a call. The body
/// after the prefix is a JSON-encoded string (the raw artifact JSON or the
/// failure sentinel), single-lined so a multi-line artifact never breaks
/// parsing. Kept in lockstep with `packages/harness/src/client.ts`.
const CALL_MEMO_RESULT_PREFIX: &str = "__CAIRN_MEMO_RESULT__";

/// Background calls-append response for a workflow caller with journal hits:
/// each memo hit emits a `__CAIRN_MEMO_RESULT__` line carrying the memoized
/// body, each live call the usual spawned-URI line. Reached only from a
/// workflow parent, so an ordinary caller's response is never reshaped.
async fn background_call_response_with_memos(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
    memo_bodies: &[String],
) -> CallbackResponse {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let mut lines = Vec::with_capacity(memo_bodies.len() + materialized.len());
    for body in memo_bodies {
        let encoded = serde_json::to_string(body).unwrap_or_else(|_| "\"\"".to_string());
        lines.push(format!("{CALL_MEMO_RESULT_PREFIX} {encoded}"));
    }
    for task in materialized {
        let uri = compute_artifact_uri(&db, parent_ctx, &task.job_id).await;
        lines.push(format!(
            "- {} ({})",
            uri.unwrap_or_else(|| "<artifact pending>".to_string()),
            task.description
        ));
    }
    CallbackResponse {
        result: format!(
            "Spawned {} background call(s), {} replayed from journal:\n{}",
            materialized.len(),
            memo_bodies.len(),
            lines.join("\n")
        ),
        ..Default::default()
    }
}

/// Owned-loop (OpenRouter) variant of `suspend_parent_for_tasks`: prepare the
/// parent's pending successor turn so completion-resume works, then request a
/// structured suspend on the parent run. Unlike the warm-process path it does
/// NOT schedule the deferred durable suspend (which interrupts the process) —
/// the owned loop finalizes the run itself after this returns.
fn suspend_parent_for_owned_loop(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    let first = &materialized[0];
    if let Err(error) = prepare_parent_for_delegated_wait(orch, parent_ctx, &first.packet) {
        log::warn!(
            "Failed to prepare owned-loop delegated wait for parent job {}: {}",
            parent_ctx.job_id,
            error
        );
    }
    orch.process_state
        .request_suspend(&parent_ctx.run_id, SuspendKind::DelegatedTask);
    let summary = if materialized.len() == 1 {
        format!("Delegated task '{}'", materialized[0].description)
    } else {
        format!("{} delegated tasks", materialized.len())
    };
    CallbackResponse {
        result: format!(
            "{} suspended; the run will resume from the real child result(s).",
            summary
        ),
        ..Default::default()
    }
}

/// Fire-and-forget response for `background` spawns: return task URIs without
/// yielding the parent turn. The child still runs; its result is later readable
/// via the task artifact URI and the node `tasks` collection.
async fn background_task_response(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let mut lines = Vec::with_capacity(materialized.len());
    let mut last_uri = None;
    for task in materialized {
        let uri = compute_artifact_uri(&db, parent_ctx, &task.job_id).await;
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

/// A minimal placeholder agent config for a workflow's `MaterializedTask`. The
/// suspend/resume tail reads only `name` from it (in the failure-branch message);
/// a workflow's success result is rendered from its output artifact, not this.
fn workflow_placeholder_agent_config(workflow_id: &str) -> AgentConfig {
    AgentConfig {
        id: "workflow".to_string(),
        name: format!("workflow {workflow_id}"),
        description: String::new(),
        prompt: String::new(),
        tools: vec![],
        tier: None,
        workspace_id: Some("workspace".to_string()),
        project_id: None,
        created_at: 0,
        updated_at: 0,
        disallowed_tools: None,
        skills: None,
        fence: None,
        backend_preference: None,
        selection: None,
        extras: None,
    }
}

/// Canonical workflow-invocation spawn pipeline (CAIRN-2487).
///
/// A workflow `run` target is delegation-shaped like a call, but its process is a
/// supervised `bun <script>` instead of an agent session. It creates the
/// node-less workflow run, wraps it in a pre-materialized `CallTool` packet, and
/// runs the SAME wait/suspend/resume tail: the caller suspends after the inline
/// budget and resumes with the workflow's output artifact when the process
/// finalizes. `background` returns the workflow URI immediately and wakes the
/// caller on completion.
pub async fn spawn_workflow_packets(
    orch: &Orchestrator,
    input: SpawnWorkflowPacketsInput<'_>,
) -> CallbackResponse {
    macro_rules! err {
        ($msg:expr) => {
            return CallbackResponse {
                result: $msg,
                ..Default::default()
            }
        };
    }

    // Reject a malformed inline schema / unknown preset now, before creating the
    // run, so the caller sees the error at invocation time.
    let contract = match crate::execution::delegation::resolve_delegated_output_contract(
        input.output_schema.as_ref(),
    ) {
        Ok(contract) => contract,
        Err(e) => err!(e),
    };

    let db = match input.run_id {
        Some(run_id) => crate::execution::routing::owning_db_for_run(&orch.db, run_id)
            .await
            .unwrap_or_else(|_| orch.db.local.clone()),
        None => orch.db.local.clone(),
    };
    let parent_ctx = match lookup_run_context(&db, input.run_id, input.cwd).await {
        Ok(ctx) => ctx,
        Err(e) => err!(e),
    };
    let execution_id = match ensure_task_execution_context(orch, &parent_ctx).await {
        Ok(execution_id) => execution_id,
        Err(e) => err!(e),
    };

    let parent_tool_use_id = input.parent_tool_use_id.unwrap_or(input.group_id);
    let description = format!("Workflow: {}", input.workflow_id);

    // 1. Create the node-less workflow run's rows + turn (no process yet).
    let prepared = match prepare_workflow_run(
        orch,
        CreateWorkflowRunInput {
            parent_job_id: parent_ctx.job_id.clone(),
            execution_id: Some(execution_id.clone()),
            workflow_id: input.workflow_id.to_string(),
            script_path: input.script_path.clone(),
            description: description.clone(),
            args_json: input.args_json.clone(),
            output_contract: contract.clone(),
            worktree: input.worktree,
            label: None,
            phase: None,
            parent_tool_use_id: Some(parent_tool_use_id.to_string()),
            task_index: None,
        },
    ) {
        Ok(prepared) => prepared,
        Err(e) => err!(e),
    };

    // 2. Persist the call packet BEFORE the process starts (close the fast-finish
    //    resume race); `result_artifact_job_id` = the workflow job so completion
    //    resume finds this packet by it.
    let cwd_for_packet = prepared.working_dir.clone();
    let packet = match persist_call_packet(
        orch,
        &execution_id,
        &parent_ctx,
        "workflow",
        &description,
        &input.args_json,
        &cwd_for_packet,
        contract.clone(),
        &prepared.job_id,
        Some(parent_tool_use_id),
        None,
        None,
        input.background,
    )
    .await
    {
        Ok(packet) => packet,
        Err(e) => err!(e),
    };

    // 3. Spawn the supervised bun process now that the packet exists.
    if let Err(e) = start_workflow_run(orch, &prepared) {
        err!(e);
    }

    let materialized = vec![MaterializedTask {
        packet,
        job_id: prepared.job_id.clone(),
        run_id: prepared.run_id.clone(),
        agent_config: workflow_placeholder_agent_config(input.workflow_id),
        description,
        schema_type: contract.artifact_name(),
    }];

    if input.background {
        return background_task_response(orch, &parent_ctx, &materialized).await;
    }
    let owns_turn_loop = input
        .run_id
        .map(|run_id| orch.process_state.run_owns_turn_loop(run_id))
        .unwrap_or(false);
    if owns_turn_loop {
        return suspend_parent_for_owned_loop(orch, &parent_ctx, &materialized);
    }
    wait_for_tasks_completion(orch, &parent_ctx, materialized).await
}

/// Build the combined inline response once all delegated runs are terminal.
async fn build_tasks_response(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    if materialized.len() == 1 {
        let task = &materialized[0];
        return build_task_callback_response(
            &db,
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
            &db,
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

#[cfg(test)]
mod journal_replay_tests {
    use super::*;
    use crate::db::DbState;
    use crate::execution::delegation::DelegatedCallPayload;
    use crate::execution::jobs::CallWorktree;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open(tempfile::tempdir().unwrap().keep().join("replay.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    /// Seed a live workflow node run (`j-wf` / `wf-run`, agent_config_id
    /// `workflow`) that a harness `agent()` call resolves against.
    async fn seed_workflow_run(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-wf','e','i','p','running','flow','Flow','workflow',1,1)",
            "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('sess','j-wf',1,1)",
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('wf-run','j-wf','i','live',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    async fn job_count(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT COUNT(*) FROM jobs", ()).await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|r| r.i64(0))
                    .transpose()?
                    .unwrap_or(0))
            })
        })
        .await
        .unwrap()
    }

    fn call_payload(prompt: &str, ordinal: i64) -> DelegatedCallPayload {
        DelegatedCallPayload {
            description: "vote".to_string(),
            prompt: prompt.to_string(),
            subagent_type: "Explore".to_string(),
            tier: None,
            backend_preference: None,
            output_schema: None,
            worktree: CallWorktree::None,
            label: None,
            phase: None,
            task_index: None,
            ordinal: Some(ordinal),
        }
    }

    /// A workflow `agent()` call whose (run_id, ordinal) hits the journal with a
    /// matching prompt replays the memoized body inline and spawns NO new call
    /// job — the core restart-durability guarantee.
    #[tokio::test]
    async fn journal_hit_replays_inline_and_spawns_no_call_job() {
        let (orch, db) = orch_with_db().await;
        seed_workflow_run(&db).await;
        let prompt = "score this candidate";
        // Pre-seed the journal as though this call already resolved on a prior run.
        crate::workflow_journal::store_entry(
            &db,
            "wf-run",
            0,
            &crate::workflow_journal::hash_prompt(prompt),
            Some(r#"{"score":42}"#),
            crate::workflow_journal::JournalStatus::Success,
        )
        .await
        .unwrap();

        let before = job_count(&db).await;
        let payloads = vec![call_payload(prompt, 0)];
        let response = spawn_call_packets(
            &orch,
            SpawnCallPacketsInput {
                run_id: Some("wf-run"),
                cwd: "/tmp/p",
                payloads: &payloads,
                group_id: "grp",
                parent_tool_use_id: Some("tu"),
                background: true,
            },
        )
        .await;
        let after = job_count(&db).await;

        assert_eq!(after, before, "a journal hit must not spawn a new call job");
        assert!(
            response.result.contains("__CAIRN_MEMO_RESULT__"),
            "memo-hit response should carry the replay marker: {}",
            response.result
        );
        assert!(
            response.result.contains("score"),
            "memo-hit response should carry the memoized body: {}",
            response.result
        );
    }

    /// A prompt that differs from the journaled prompt at the same ordinal is a
    /// cache miss: the memo is NOT replayed, so the response is not a memo hit
    /// (the call runs live — it materializes a real job here).
    #[tokio::test]
    async fn changed_prompt_is_a_cache_miss_not_a_replay() {
        let (orch, db) = orch_with_db().await;
        seed_workflow_run(&db).await;
        crate::workflow_journal::store_entry(
            &db,
            "wf-run",
            0,
            &crate::workflow_journal::hash_prompt("the original prompt"),
            Some(r#"{"score":42}"#),
            crate::workflow_journal::JournalStatus::Success,
        )
        .await
        .unwrap();

        let payloads = vec![call_payload("a DIFFERENT prompt at ordinal 0", 0)];
        let response = spawn_call_packets(
            &orch,
            SpawnCallPacketsInput {
                run_id: Some("wf-run"),
                cwd: "/tmp/p",
                payloads: &payloads,
                group_id: "grp",
                parent_tool_use_id: Some("tu"),
                background: true,
            },
        )
        .await;

        // A mismatch never replays: the response is not a memo hit. (The live
        // spawn itself needs an `Explore` agent config that this bare test env
        // lacks, so the call surfaces a config error rather than a memo line —
        // the point under test is that no journal replay occurred.)
        assert!(
            !response.result.contains("__CAIRN_MEMO_RESULT__"),
            "a prompt mismatch must be a cache miss, not a replay: {}",
            response.result
        );
    }
}
