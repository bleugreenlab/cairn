use crate::config::{
    agents as config_agents,
    presets::{load_effective_presets, resolve_runtime_selection, spawned_task_backend},
};
use crate::execution::delegation::{
    DelegatedTaskPayload, SpawnCallPacketsInput, SpawnTaskPacketsInput, SpawnWorkflowPacketsInput,
};
use crate::execution::jobs::{
    prepare_call_run, prepare_workflow_run, start_call_run, start_workflow_run, CreateCallRunInput,
    CreateWorkflowRunInput,
};
use crate::models::{AgentConfig, Model};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::protocol::CallbackResponse;
use cairn_models::turn::SuspendKind;
use serde::Serialize;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast;

use super::common::{
    dispatch_ready_agent_jobs, ensure_task_execution_context, latest_run_for_job,
    link_or_retire_unmaterialized_packet, lookup_run_context, persist_call_packet,
    persist_task_packet, project_repo_path, refresh_packet_state, select_optional_text,
    ParentRunContext,
};
use super::results::{build_task_callback_response, task_artifact_uri, task_has_settled};
use super::resume::{
    prepare_parent_for_delegated_wait, schedule_deferred_parent_suspend_for_delegated_wait,
};

use cairn_common::callback_contract::INLINE_DURABLE_WAIT_BUDGET;

/// What a spawning agent reads for a delegated task that outlived its inline
/// budget, after the summary naming what was spawned.
///
/// Like every suspension marker this says what actually happened -- the call is
/// continuing -- and never implies anyone declined it
/// (`crate::mcp::handlers::suspension_markers` pins that property). The
/// transcript also reads it: a row whose result is a hand-off is still in
/// flight, not settled (`src/components/chat/suspensionHandoff.ts`).
pub(crate) const DELEGATED_TASKS_SUSPENDED_SUFFIX: &str =
    "suspended; the run will resume from the real child result(s).";

/// [`DELEGATED_TASKS_SUSPENDED_SUFFIX`] for the MCP-hosted spawn path, which
/// names the parent explicitly because the child runs are agents of their own.
pub(crate) const DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX: &str =
    "suspended; the parent run will resume from the real child result(s).";

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

    let policy = crate::config::contextual_packages::load_contextual_packages(project_path);
    let selection = policy.selection(
        crate::config::contextual_packages::ContextualPackageKind::Agent,
        &file_agent.id,
        &file_agent.bundles,
    );
    if !policy.is_selected(
        crate::config::contextual_packages::ContextualPackageKind::Agent,
        &file_agent.id,
        &file_agent.bundles,
    ) {
        let reason = if matches!(
            selection,
            crate::config::contextual_packages::ContextualPackageSelection::ExplicitlyDisabled
        ) {
            "disabled"
        } else {
            "not enabled"
        };
        return Err(format!(
            "Agent '{}' is {reason} for project {}",
            file_agent.name, parent_ctx.project_key
        ));
    }

    let presets = load_effective_presets(&config_dir, project_path);
    let authored_tier = payload
        .tier
        .as_deref()
        .or(file_agent.tier.as_ref().map(Model::as_str));
    let authored_backend = spawned_task_backend(
        payload.backend_preference.as_deref(),
        file_agent.backend_preference.as_deref(),
    );
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
        icon: file_agent.icon,
        backend_preference: payload
            .backend_preference
            .clone()
            .or(file_agent.backend_preference),
        selection: Some(selection),
        extras: Some(extras),
    })
}

/// A delegated child whose job exists. Its run may not exist yet: a task is
/// created when its job is, and starting it is a separate, later fact.
struct MaterializedTask {
    packet: crate::models::DelegatedWorkPacket,
    job_id: String,
    /// The child's run when the spawn already knew it. Results resolve the run
    /// again when they render, so a child that starts later still reports.
    run_id: Option<String>,
    agent_config: AgentConfig,
    description: String,
    schema_type: String,
    /// How far the child had got when the spawn answered. Set by the task
    /// pipeline; calls and workflows create their run synchronously.
    launch: Option<TaskLaunch>,
}

/// How far a created child that has no run yet has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StartStage {
    /// The job exists and is still `pending`: advancement has not claimed it
    /// and handed it to a runner.
    Queued,
    /// The job was claimed (`running`) and handed to a runner, which prepares
    /// its branch under the project's store lock and then its session. The run
    /// row is inserted only at the end of that preparation.
    Preparing,
}

impl StartStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Preparing => "preparing",
        }
    }
}

/// Where a spawned child stood at the moment its spawn answered.
///
/// Creating the child job and starting it are separate facts, so a task can be
/// created and not yet started. That is a state to report, not a failure: the
/// child will start, and whoever spawned it is woken by its completion either
/// way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum TaskLaunch {
    /// The child has a run.
    #[serde(rename_all = "camelCase")]
    Started {
        run_id: String,
        /// Where the run stands in session admission while it is still queued
        /// for a slot.
        #[serde(skip_serializing_if = "Option::is_none")]
        admission: Option<String>,
    },
    /// The child job exists but has no run yet.
    #[serde(rename_all = "camelCase")]
    WaitingToStart {
        /// When the current stage began: the job's creation while it is
        /// queued, its claim while it is preparing.
        since_unix: i64,
        stage: StartStage,
        /// What the stage is waiting behind, when that is known.
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

impl TaskLaunch {
    /// One-line rendering of a waiting child (`None` once it has started).
    pub(crate) fn waiting_note(&self) -> Option<String> {
        let Self::WaitingToStart {
            since_unix,
            stage,
            detail,
        } = self
        else {
            return None;
        };
        let since = chrono::DateTime::from_timestamp(*since_unix, 0)
            .map(|at| at.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_else(|| since_unix.to_string());
        let mut note = format!("waiting to start ({}, since {since})", stage.as_str());
        if let Some(detail) = detail {
            note.push_str(": ");
            note.push_str(detail);
        }
        Some(note)
    }
}

/// A task the spawn created, in payload order.
#[derive(Debug, Clone)]
pub(crate) struct SpawnedTask {
    /// The payload this task was spawned from.
    pub(crate) index: usize,
    pub(crate) job_id: String,
    pub(crate) task_uri: Option<String>,
    pub(crate) description: String,
    pub(crate) launch: TaskLaunch,
}

/// What a task spawn did, per payload.
///
/// Every payload is either `spawned` (its child job exists) or `refused` (no
/// child will run for it), so a caller never has to read a created task out of
/// prose or mistake one for a failure.
#[derive(Debug, Default)]
pub(crate) struct TaskSpawnOutcome {
    /// The blocking text: inline results, the suspension hand-off, or the
    /// background list. `None` when nothing was spawned.
    pub(crate) response: Option<CallbackResponse>,
    pub(crate) spawned: Vec<SpawnedTask>,
    /// Payload index and what failed for it.
    pub(crate) refused: Vec<(usize, String)>,
}

impl TaskSpawnOutcome {
    fn refuse_all(count: usize, error: impl Into<String>) -> Self {
        let error = error.into();
        Self {
            refused: (0..count).map(|index| (index, error.clone())).collect(),
            ..Default::default()
        }
    }

    /// One payload failed before anything was created, so none of the batch is.
    fn refuse_batch(count: usize, culprit: usize, error: String) -> Self {
        Self {
            refused: (0..count)
                .map(|index| {
                    if index == culprit {
                        (index, error.clone())
                    } else {
                        (
                            index,
                            format!(
                                "not spawned: another task in the same append batch was refused, \
                                 and a batch spawns all of its tasks or none ({error})"
                            ),
                        )
                    }
                })
                .collect(),
            ..Default::default()
        }
    }
}

/// What went wrong between persisting a batch's packets and handing their
/// children to a runner.
///
/// Only [`StartProblem::Dispatch`] proves a child will not start: it names the
/// jobs this call's own advancement claimed and its own dispatch then failed to
/// hand over. Packets are persisted and advanced independently, so a concurrent
/// spawn's advancement can claim and dispatch this call's child, and an
/// advancement runs cascading effects before it returns; an advancement error
/// therefore says nothing about whether a child it created was handed off.
enum StartProblem {
    /// This call's advancement failed. Children that exist are reported as they
    /// are, never failed.
    Advancement(String),
    /// This call's advancement returned `undispatched` claimed jobs and handing
    /// them to the runner failed.
    Dispatch {
        error: String,
        undispatched: std::collections::HashSet<String>,
    },
    /// The batch was never advanced.
    NotAdvanced(String),
}

/// A packet's state once advancement and dispatch have had their say.
enum PacketLaunch {
    Created {
        packet: crate::models::DelegatedWorkPacket,
        job_id: String,
        launch: TaskLaunch,
    },
    Refused(String),
}

/// Canonical delegated-task spawn pipeline.
///
/// Persists one packet per payload under a shared `group_id` (so multiple tasks
/// in a single request resume together as a batch), advances and dispatches the
/// delegated execution once, then either returns spawned task URIs immediately
/// (`background`) or waits inline for completion — durably suspending the parent
/// turn once if the inline budget expires, to resume from the real results.
pub(crate) async fn spawn_task_packets(
    orch: &Orchestrator,
    input: SpawnTaskPacketsInput<'_>,
) -> TaskSpawnOutcome {
    spawn_task_packets_within(orch, input, INLINE_DURABLE_WAIT_BUDGET).await
}

/// [`spawn_task_packets`] with an explicit inline budget: how long a blocking
/// spawn waits for its children before parking the parent. It is the only wait
/// in the pipeline; a child that has not started is reported as created and
/// waiting, never timed out into a failure.
pub(crate) async fn spawn_task_packets_within(
    orch: &Orchestrator,
    input: SpawnTaskPacketsInput<'_>,
    inline_budget: Duration,
) -> TaskSpawnOutcome {
    let count = input.payloads.len();
    if count == 0 {
        return TaskSpawnOutcome {
            response: Some(CallbackResponse {
                result: "No tasks provided".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
    }

    // Reject a malformed inline schema or unknown preset now, before persisting
    // any packet, so the caller sees the error at task-append time rather than
    // when the child writes its result.
    for (index, payload) in input.payloads.iter().enumerate() {
        if let Err(e) = crate::execution::delegation::resolve_delegated_output_contract(
            payload.output_schema.as_ref(),
        ) {
            return TaskSpawnOutcome::refuse_batch(count, index, e);
        }
    }

    // Resolve the parent run's owning database (a team run lives in its replica).
    let Some(run_id) = input.run_id else {
        return TaskSpawnOutcome::refuse_all(
            count,
            "Authenticated delegated request is missing its run ID",
        );
    };
    let db = match crate::execution::routing::owning_db_for_run(&orch.db, run_id).await {
        Ok(db) => db,
        Err(error) => return TaskSpawnOutcome::refuse_all(count, error.to_string()),
    };
    let parent_ctx = match lookup_run_context(&db, run_id).await {
        Ok(ctx) => ctx,
        Err(e) => return TaskSpawnOutcome::refuse_all(count, e),
    };
    let project_path = project_repo_path(&db, &parent_ctx.project_id)
        .await
        .map(PathBuf::from);
    let execution_id = match ensure_task_execution_context(orch, &parent_ctx).await {
        Ok(execution_id) => execution_id,
        Err(e) => return TaskSpawnOutcome::refuse_all(count, e),
    };
    let execution_db =
        match crate::execution::routing::owning_db_for_execution(&orch.db, &execution_id).await {
            Ok(db) => db,
            Err(e) => return TaskSpawnOutcome::refuse_all(count, e.to_string()),
        };

    // Resolve every agent config before persisting any packet, so an unknown or
    // disabled agent refuses the batch without leaving a packet behind.
    let multiple = count > 1;
    let mut resolved = Vec::with_capacity(count);
    for (index, payload) in input.payloads.iter().enumerate() {
        let mut payload = payload.clone();
        if multiple && payload.task_index.is_none() {
            payload.task_index = Some(index as i32);
        }
        match resolve_task_agent_config(
            orch,
            &parent_ctx,
            &execution_id,
            project_path.as_deref(),
            &payload,
        )
        .await
        {
            Ok(config) => resolved.push((payload, config)),
            Err(e) => return TaskSpawnOutcome::refuse_batch(count, index, e),
        }
    }

    let mut prepared: Vec<(crate::models::DelegatedWorkPacket, AgentConfig, String)> =
        Vec::with_capacity(count);
    let mut persist_failure: Option<(usize, String)> = None;
    for (index, (payload, agent_config)) in resolved.into_iter().enumerate() {
        #[cfg(test)]
        if let Some(error) = injected_persist_fault(orch, run_id, &execution_id, index).await {
            persist_failure = Some((index, error));
            break;
        }
        match persist_task_packet(
            orch,
            &execution_id,
            &parent_ctx,
            &payload,
            &agent_config,
            input.cwd,
            input.parent_tool_use_id,
            input.background,
        )
        .await
        {
            Ok(packet) => prepared.push((packet, agent_config, payload.description.clone())),
            Err(e) => {
                persist_failure = Some((index, e));
                break;
            }
        }
    }

    // A batch that could not persist all its packets is not advanced. Packets
    // it did write are still classified one by one below: each runs on its own
    // execution lock, so a concurrent spawn's advancement may already have
    // turned one into a real child, and that child is reported and waited on
    // like any other. Only a packet with no child is retired and refused.
    let start_problem = if let Some((failed, error)) = &persist_failure {
        Some(StartProblem::NotAdvanced(format!(
            "the batch was not advanced because task {failed} could not be persisted: {error}"
        )))
    } else {
        match crate::execution::advancement::advance_execution_with_actions(orch, &execution_id)
            .await
        {
            Ok(ready_jobs) => {
                let handed: std::collections::HashSet<String> =
                    ready_jobs.iter().map(|job| job.id.clone()).collect();
                dispatch_ready_agent_jobs(orch, ready_jobs)
                    .await
                    .err()
                    .map(|error| StartProblem::Dispatch {
                        error,
                        undispatched: handed,
                    })
            }
            Err(e) => Some(StartProblem::Advancement(e.to_string())),
        }
    };

    let mut materialized: Vec<MaterializedTask> = Vec::with_capacity(prepared.len());
    let mut materialized_indices = Vec::with_capacity(prepared.len());
    let mut refused = Vec::new();
    for (index, (packet, agent_config, description)) in prepared.into_iter().enumerate() {
        match classify_packet_launch(
            orch,
            &execution_db,
            &execution_id,
            &packet.id,
            &description,
            start_problem.as_ref(),
        )
        .await
        {
            PacketLaunch::Created {
                packet,
                job_id,
                launch,
            } => {
                let schema_type = packet.output_contract.artifact_name();
                let run_id = match &launch {
                    TaskLaunch::Started { run_id, .. } => Some(run_id.clone()),
                    TaskLaunch::WaitingToStart { .. } => None,
                };
                materialized.push(MaterializedTask {
                    packet,
                    job_id,
                    run_id,
                    agent_config,
                    description,
                    schema_type,
                    launch: Some(launch),
                });
                materialized_indices.push(index);
            }
            PacketLaunch::Refused(error) => refused.push((index, error)),
        }
    }
    if let Some((failed, error)) = persist_failure {
        refused.push((failed, error.clone()));
        refused.extend((failed + 1..count).map(|index| {
            (
                index,
                format!(
                    "not spawned: task {failed} in the same append batch could not be persisted, \
                     so the batch stopped there ({error})"
                ),
            )
        }));
    }

    if materialized.is_empty() {
        return TaskSpawnOutcome {
            response: None,
            spawned: Vec::new(),
            refused,
        };
    }

    let mut spawned = Vec::with_capacity(materialized.len());
    for (task, index) in materialized.iter().zip(materialized_indices) {
        spawned.push(SpawnedTask {
            index,
            job_id: task.job_id.clone(),
            task_uri: crate::jobs::queries::home_uri_for_job(&execution_db, &task.job_id)
                .await
                .ok()
                .flatten(),
            description: task.description.clone(),
            launch: task
                .launch
                .clone()
                .expect("the task pipeline records every launch"),
        });
    }

    let response = if input.background {
        background_task_response(orch, &parent_ctx, &materialized).await
    } else if input
        .run_id
        .map(|run_id| orch.process_state.run_owns_turn_loop(run_id))
        .unwrap_or(false)
    {
        // Owned-loop backends (OpenRouter) never inline-wait on delegated tasks:
        // there is no warm process, so the parent resumes by cold-restarting
        // when the child results land. Suspend the turn now instead of blocking
        // the HTTP thread on the completion wait.
        suspend_parent_for_owned_loop(orch, &parent_ctx, &materialized)
    } else {
        wait_for_tasks_completion(
            orch,
            &parent_ctx,
            &execution_db,
            materialized,
            inline_budget,
        )
        .await
    };
    TaskSpawnOutcome {
        response: Some(response),
        spawned,
        refused,
    }
}

/// Classify one packet after advancement and dispatch: created (started or
/// waiting to start) or refused, with what failed.
///
/// A created child this call positively knows will not start — one its own
/// dispatch failed to hand over, still without a run — is failed through
/// [`mark_job_start_failed`], the same path every start failure takes, so it is
/// never left pending and any waiter on it is answered. Every other child that
/// exists is reported as created, whatever went wrong around it.
///
/// [`mark_job_start_failed`]: crate::execution::jobs::mark_job_start_failed
async fn classify_packet_launch(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
    description: &str,
    start_problem: Option<&StartProblem>,
) -> PacketLaunch {
    let job_id = match link_or_retire_unmaterialized_packet(orch, db, execution_id, packet_id).await
    {
        Ok(Some(job_id)) => job_id,
        Ok(None) => {
            let cause = match start_problem {
                Some(StartProblem::NotAdvanced(why)) => why.clone(),
                Some(StartProblem::Advancement(error)) => {
                    format!("advancement did not materialize it: {error}")
                }
                _ => "advancement did not materialize it".to_string(),
            };
            return PacketLaunch::Refused(format!(
                "no child job was created for task '{description}' (packet {packet_id}): {cause}"
            ));
        }
        Err(error) => {
            return PacketLaunch::Refused(format!(
                "could not settle task '{description}' (packet {packet_id}): {error}"
            ))
        }
    };

    let job_owned = job_id.clone();
    let facts = db
        .read(|conn| {
            let job_id = job_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, started_at, created_at FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok((row.text(0)?, row.opt_i64(1)?, row.i64(2)?)))
                    .transpose()
            })
        })
        .await;
    let (status, started_at, created_at) = match facts {
        Ok(Some(facts)) => facts,
        Ok(None) => {
            return PacketLaunch::Refused(format!(
                "child job {job_id} for task '{description}' (packet {packet_id}) disappeared"
            ))
        }
        Err(error) => {
            return PacketLaunch::Refused(format!(
                "could not read child job {job_id} for task '{description}': {error}"
            ))
        }
    };
    let run_id = latest_run_for_job(db, &job_id).await.ok().flatten();

    if matches!(status.as_str(), "failed" | "cancelled") {
        // A child this young that is already terminal never started: its
        // handover failed (see `hand_over_claimed_jobs`) or it was cancelled.
        let reason = match &run_id {
            Some(run_id) => {
                select_optional_text(db, "SELECT error_message FROM runs WHERE id = ?1", run_id)
                    .await
                    .ok()
                    .flatten()
            }
            None => None,
        }
        .unwrap_or_else(|| format!("the job is {status}"));
        let uri = crate::jobs::queries::home_uri_for_job(db, &job_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| job_id.clone());
        return PacketLaunch::Refused(format!(
            "child job {uri} was created but {status} before it started: {reason}"
        ));
    }

    let launch = if let Some(run_id) = run_id {
        TaskLaunch::Started {
            admission: crate::execution::jobs::describe_job_launch(orch, &job_id).await,
            run_id,
        }
    } else if let Some(StartProblem::Dispatch {
        error,
        undispatched,
    }) = start_problem.filter(|_| matches!(status.as_str(), "pending" | "running"))
    {
        if undispatched.contains(&job_id) {
            crate::execution::jobs::mark_job_start_failed(
                orch,
                db,
                &job_id,
                &format!("delegated job could not be dispatched: {error}"),
                true,
            )
            .await;
            let uri = crate::jobs::queries::home_uri_for_job(db, &job_id)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| job_id.clone());
            return PacketLaunch::Refused(format!(
                "child job {uri} was created but could not be dispatched: {error}; it was marked failed"
            ));
        }
        waiting_launch(orch, &job_id, &status, started_at, created_at, None)
    } else {
        let problem_detail = match start_problem {
            Some(StartProblem::Advancement(error)) => Some(format!(
                "advancement reported an error after creating it: {error}"
            )),
            _ => None,
        };
        waiting_launch(
            orch,
            &job_id,
            &status,
            started_at,
            created_at,
            problem_detail,
        )
    };

    match refresh_packet_state(db, execution_id, packet_id).await {
        Ok(Some(packet)) => PacketLaunch::Created {
            packet,
            job_id,
            launch,
        },
        Ok(None) => PacketLaunch::Refused(format!(
            "packet {packet_id} for task '{description}' disappeared from its execution"
        )),
        Err(error) => PacketLaunch::Refused(format!(
            "could not read back task '{description}' (packet {packet_id}): {error}"
        )),
    }
}

/// The launch of a created child that has no run yet.
fn waiting_launch(
    orch: &Orchestrator,
    job_id: &str,
    status: &str,
    started_at: Option<i64>,
    created_at: i64,
    problem_detail: Option<String>,
) -> TaskLaunch {
    if status == "pending" {
        TaskLaunch::WaitingToStart {
            since_unix: created_at,
            stage: StartStage::Queued,
            detail: problem_detail,
        }
    } else {
        TaskLaunch::WaitingToStart {
            since_unix: started_at.unwrap_or(created_at),
            stage: StartStage::Preparing,
            detail: problem_detail.or_else(|| preparation_wait_detail(orch, job_id)),
        }
    }
}

/// Test-only fault hook: fail persisting packet `index` of a spawn on `run_id`,
/// optionally after running a concurrent advancement and dispatch first — the
/// interleaving in which another spawn materializes this batch's earlier packets
/// before the failure.
#[cfg(test)]
static PERSIST_FAULTS: std::sync::Mutex<Vec<(String, usize, bool)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn injected_persist_fault(
    orch: &Orchestrator,
    run_id: &str,
    execution_id: &str,
    index: usize,
) -> Option<String> {
    let advance_first = {
        let mut faults = PERSIST_FAULTS.lock().unwrap();
        let position = faults
            .iter()
            .position(|(run, at, _)| run == run_id && *at == index)?;
        faults.remove(position).2
    };
    if advance_first {
        let ready =
            crate::execution::advancement::advance_execution_with_actions(orch, execution_id)
                .await
                .unwrap_or_default();
        let _ = dispatch_ready_agent_jobs(orch, ready).await;
    }
    Some("injected persistence failure".to_string())
}

/// Where a preparing child's branch preparation stands in the project store
/// lock's queue, in the words its node page uses. `None` unless it is actually
/// queued there.
fn preparation_wait_detail(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let (status, position, since_ms) = orch.branch_preparation_wait(job_id)?;
    let since = chrono::DateTime::from_timestamp(since_ms / 1_000, 0)
        .map(|at| at.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| (since_ms / 1_000).to_string());
    Some(crate::orchestrator::describe_preparation_wait(
        &status, position, &since,
    ))
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

    let Some(run_id) = input.run_id else {
        err!("Authenticated delegated request is missing its run ID".to_string());
    };
    let db = match crate::execution::routing::owning_db_for_run(&orch.db, run_id).await {
        Ok(db) => db,
        Err(error) => err!(error.to_string()),
    };
    let parent_ctx = match lookup_run_context(&db, run_id).await {
        Ok(ctx) => ctx,
        Err(e) => err!(e),
    };
    let execution_id = match ensure_task_execution_context(orch, &parent_ctx).await {
        Ok(execution_id) => execution_id,
        Err(e) => err!(e),
    };

    let parent_tool_use_id = input.parent_tool_use_id;
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
                branch_policy: payload.branch_policy,
                label: payload.label.clone(),
                phase: payload.phase.clone(),
                parent_tool_use_id: parent_tool_use_id.map(str::to_string),
                task_index,
                workflow_journal_link: journal_link,
            },
        ) {
            Ok(prepared) => prepared,
            Err(e) => err!(e),
        };

        // 2. Persist the call packet BEFORE the session starts (close the
        //    fast-finish resume race). Packet cwd is non-authoritative; run ID
        //    carries identity and session start recreates the scratch residence.
        let cwd_for_packet = "";
        let packet = match persist_call_packet(
            orch,
            &execution_id,
            &parent_ctx,
            &prepared.agent_config.id,
            &payload.description,
            &payload.prompt,
            cwd_for_packet,
            contract.clone(),
            &prepared.job_id,
            parent_tool_use_id,
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
            run_id: Some(prepared.run_id),
            agent_config: prepared.agent_config,
            description: payload.description.clone(),
            schema_type,
            launch: None,
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

    wait_for_tasks_completion(
        orch,
        &parent_ctx,
        &db,
        materialized,
        INLINE_DURABLE_WAIT_BUDGET,
    )
    .await
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
        let uri = task_artifact_uri(&db, &task.job_id).await;
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

/// Owned-loop (OpenRouter) variant of `suspend_parent_for_tasks`: yield the
/// parent on its dependency anchor so completion-resume works, then request a
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
        result: format!("{summary} {DELEGATED_TASKS_SUSPENDED_SUFFIX}"),
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
        let uri = task_artifact_uri(&db, &task.job_id).await;
        if uri.is_some() {
            last_uri = uri.clone();
        }
        let mut line = format!(
            "- {} ({})",
            uri.unwrap_or_else(|| "<artifact pending>".to_string()),
            task.description
        );
        if let Some(note) = task.launch.as_ref().and_then(TaskLaunch::waiting_note) {
            line.push_str(" — ");
            line.push_str(&note);
        }
        lines.push(line);
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

/// How often an inline wait re-reads its children with no completion broadcast.
///
/// The broadcast is only a hint that something settled. A child can become
/// terminal on a path that sends none — cancelled before it ever started, or a
/// start failure whose run row could not be written — and an inline wait must
/// still see it inside its budget rather than park on a child that is already
/// done.
const INLINE_WAIT_RECHECK: Duration = Duration::from_secs(1);

/// Wait inline until every delegated child has settled; if the budget expires,
/// durably suspend the parent once so it resumes from the real combined child
/// results.
///
/// Settledness is read from each child's job (with its latest run as the
/// fallback), never from a set of run ids, so a child that had no run when the
/// spawn answered is waited on exactly like one that did.
async fn wait_for_tasks_completion(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    db: &LocalDb,
    materialized: Vec<MaterializedTask>,
    budget: Duration,
) -> CallbackResponse {
    // Subscribe before the first read so a completion between the read and the
    // wait is not missed.
    let mut rx = orch.run_completions.subscribe();
    let all_settled = async {
        loop {
            let mut settled = true;
            for task in &materialized {
                if !task_has_settled(db, &task.job_id).await {
                    settled = false;
                    break;
                }
            }
            if settled {
                return true;
            }
            match tokio::time::timeout(INLINE_WAIT_RECHECK, rx.recv()).await {
                Ok(Err(broadcast::error::RecvError::Closed)) => return false,
                Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) | Err(_) => {}
            }
        }
    };
    match tokio::time::timeout(budget, all_settled).await {
        Ok(true) => build_tasks_response(orch, parent_ctx, &materialized).await,
        _ => suspend_parent_for_tasks(orch, parent_ctx, &materialized).await,
    }
}

/// Yield the parent turn once and schedule the durable suspend for a task batch.
/// The real combined results are delivered on resume via
/// `resume_suspended_parent_after_task_completion`.
async fn suspend_parent_for_tasks(
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
            first
                .packet
                .parent_turn_id
                .clone()
                .expect("prepared dependency wait has a parent turn"),
        );
    }
    // Name the address each task was actually minted at. A task's slug is
    // derived from its description rather than being the description
    // (`derive_unique_task_slug` takes the first two words), so a caller holding
    // only what it wrote cannot predict it — and could not address the task it
    // had just spawned. The marker stays last: it is the hand-off the agent and
    // the transcript both key on.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let mut homes = Vec::with_capacity(materialized.len());
    for task in materialized {
        if let Ok(Some(home)) = crate::jobs::queries::home_uri_for_job(&db, &task.job_id).await {
            homes.push(home);
        }
    }
    let at = if homes.is_empty() {
        String::new()
    } else {
        format!(" ({})", homes.join(", "))
    };
    let summary = if materialized.len() == 1 {
        format!("Delegated task '{}'{at}", materialized[0].description)
    } else {
        format!("{} delegated tasks{at}", materialized.len())
    };
    CallbackResponse {
        result: format!("{summary} {DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX}"),
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
        icon: None,
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

    let Some(run_id) = input.run_id else {
        err!("Authenticated delegated request is missing its run ID".to_string());
    };
    let db = match crate::execution::routing::owning_db_for_run(&orch.db, run_id).await {
        Ok(db) => db,
        Err(error) => err!(error.to_string()),
    };
    let parent_ctx = match lookup_run_context(&db, run_id).await {
        Ok(ctx) => ctx,
        Err(e) => err!(e),
    };
    let execution_id = match ensure_task_execution_context(orch, &parent_ctx).await {
        Ok(execution_id) => execution_id,
        Err(e) => err!(e),
    };

    let parent_tool_use_id = input.parent_tool_use_id;
    let description = format!("Workflow: {}", input.workflow_id);

    // 1. Create the node-less workflow run's rows + turn (no process yet).
    let prepared = match prepare_workflow_run(
        orch,
        CreateWorkflowRunInput {
            parent_job_id: Some(parent_ctx.job_id.clone()),
            project_id: None,
            issue_id: None,
            base_branch: None,
            execution_id: Some(execution_id.clone()),
            workflow_id: input.workflow_id.to_string(),
            script_path: input.script_path.clone(),
            description: description.clone(),
            args_json: input.args_json.clone(),
            output_contract: contract.clone(),
            branch_policy: input.branch_policy,
            label: None,
            phase: None,
            parent_tool_use_id: parent_tool_use_id.map(str::to_string),
            task_index: None,
        },
    ) {
        Ok(prepared) => prepared,
        Err(e) => err!(e),
    };

    // 2. Persist the call packet BEFORE the process starts (close the fast-finish
    //    resume race); `result_artifact_job_id` = the workflow job so completion
    //    resume finds this packet by it.
    let cwd_for_packet = "";
    let packet = match persist_call_packet(
        orch,
        &execution_id,
        &parent_ctx,
        "workflow",
        &description,
        &input.args_json,
        cwd_for_packet,
        contract.clone(),
        &prepared.job_id,
        parent_tool_use_id,
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
    if let Err(e) = start_workflow_run(orch, &prepared).await {
        err!(e);
    }

    let materialized = vec![MaterializedTask {
        packet,
        job_id: prepared.job_id.clone(),
        run_id: Some(prepared.run_id.clone()),
        agent_config: workflow_placeholder_agent_config(input.workflow_id),
        description,
        schema_type: contract.artifact_name(),
        launch: None,
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
    wait_for_tasks_completion(
        orch,
        &parent_ctx,
        &db,
        materialized,
        INLINE_DURABLE_WAIT_BUDGET,
    )
    .await
}

/// Build the combined inline response once all delegated children have settled.
///
/// Each child's run is resolved now rather than taken from the spawn, so a
/// child that started after the spawn answered — or never started at all —
/// renders from what actually happened.
async fn build_tasks_response(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    materialized: &[MaterializedTask],
) -> CallbackResponse {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let mut run_ids = Vec::with_capacity(materialized.len());
    for task in materialized {
        run_ids.push(
            latest_run_for_job(&db, &task.job_id)
                .await
                .ok()
                .flatten()
                .or_else(|| task.run_id.clone()),
        );
    }
    if materialized.len() == 1 {
        let task = &materialized[0];
        return build_task_callback_response(
            &db,
            run_ids[0].as_deref(),
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
            run_ids[index].as_deref(),
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

/// A task whose child job exists but has not started, and a task whose child
/// fails before it ever had a run: both are reported for what they are, and a
/// parent waiting on them is answered.
#[cfg(test)]
mod unstarted_child_tests {
    use super::*;
    use crate::agent_process::process::RunHandle;
    use crate::effects::executor::EffectExecutor;
    use crate::effects::types::{EffectResult, WorkflowEffect};
    use crate::execution::delegation::runtime::resume::background_completion_tests::{
        seed, test_orchestrator,
    };
    use crate::execution::delegation::DelegatedTaskSessionMode;
    use std::sync::{Arc, Mutex};

    const ANCHOR: &str = "T1";
    const CHILD_ARTIFACT: &str = "the late child's explored answer";

    /// Records each start request and starts nothing, so a created child job
    /// stays queued exactly as it does behind a busy runner.
    #[derive(Default)]
    struct HoldingExecutor {
        started: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl EffectExecutor for HoldingExecutor {
        async fn execute(
            &self,
            _orch: &Orchestrator,
            effect: WorkflowEffect,
        ) -> Result<Option<EffectResult>, String> {
            if let WorkflowEffect::StartAgentJobs(jobs) = effect {
                self.started
                    .lock()
                    .unwrap()
                    .extend(jobs.into_iter().map(|job| job.id));
            }
            Ok(None)
        }
    }

    /// An issue-node parent (`j-parent`, run `run-parent`) mid-turn on `T1`,
    /// an `Explore` agent, and an executor that never starts a child.
    async fn parent_fixture(db_name: &str) -> (Orchestrator, Arc<HoldingExecutor>) {
        let executor = Arc::new(HoldingExecutor::default());
        let orch = parent_fixture_with(db_name, executor.clone()).await;
        (orch, executor)
    }

    /// Refuses every start, as a runner that cannot take work would.
    struct RefusingExecutor;

    #[async_trait::async_trait]
    impl EffectExecutor for RefusingExecutor {
        async fn execute(
            &self,
            _orch: &Orchestrator,
            _effect: WorkflowEffect,
        ) -> Result<Option<EffectResult>, String> {
            Err("the runner refused the start".to_string())
        }
    }

    /// Starts nothing, and answers every executed action as complete — which
    /// feeds a cascading `AdvanceDag` back into the effect loop.
    #[derive(Default)]
    struct NestingExecutor {
        actions: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl EffectExecutor for NestingExecutor {
        async fn execute(
            &self,
            _orch: &Orchestrator,
            effect: WorkflowEffect,
        ) -> Result<Option<EffectResult>, String> {
            match effect {
                WorkflowEffect::ExecuteAction { execution_id, .. } => {
                    *self.actions.lock().unwrap() += 1;
                    Ok(Some(EffectResult::ActionComplete { execution_id }))
                }
                _ => Ok(None),
            }
        }
    }

    /// Add a ready action node, fed by a trigger, with the given config.
    async fn add_ready_action_node(orch: &Orchestrator, config: Option<serde_json::Value>) {
        let db = orch.db.local.clone();
        let mut snapshot: serde_json::Value = serde_json::from_str(
            &db.query_opt_text("SELECT snapshot FROM executions WHERE id = 'e'", ())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let mut action = serde_json::json!({
            "id": "ready-action",
            "nodeType": "action",
            "name": "ready action",
            "position": { "x": 0.0, "y": 0.0 }
        });
        if let Some(config) = config {
            action["actionConfig"] = config;
        }
        let nodes = snapshot["recipe"]["nodes"].as_array_mut().unwrap();
        nodes.push(serde_json::json!({
            "id": "ready-trigger",
            "nodeType": "trigger",
            "name": "ready trigger",
            "position": { "x": 0.0, "y": 0.0 }
        }));
        nodes.push(action);
        snapshot["recipe"]["edges"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "id": "ready-edge",
                "edgeType": "control",
                "sourceNodeId": "ready-trigger",
                "sourceHandle": "control-out",
                "targetNodeId": "ready-action",
                "targetHandle": "control-in"
            }));
        db.execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = 'e'",
            (snapshot.to_string(),),
        )
        .await
        .unwrap();
    }

    /// Add a ready action node whose missing config makes advancement fail
    /// after it has already claimed the ready delegated jobs.
    async fn break_advancement_after_its_claim(orch: &Orchestrator) {
        add_ready_action_node(orch, None).await;
    }

    async fn parent_fixture_with(db_name: &str, executor: Arc<dyn EffectExecutor>) -> Orchestrator {
        let db = crate::storage::migrated_test_db(db_name).await;
        seed(&db, &[]).await;
        db.execute_script(
            "INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('session-parent','j-parent','codex','native-thread','open',1,1,1);
             INSERT INTO runs(id,job_id,issue_id,project_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('run-parent','j-parent','i','p','session-parent','live','resume',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
               VALUES('T1','session-parent','run-parent','j-parent',1,'running','initial',1,1,1);
             UPDATE jobs
                SET current_session_id='session-parent', current_turn_id='T1',
                    agent_config_id='Builder', model='gpt-5.6-sol'
              WHERE id='j-parent';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        std::fs::write(
            orch.config_dir.join("agents").join("explore.md"),
            "---\nname: Explore\ndescription: Reads code\n---\n\nExplore the code.\n",
        )
        .unwrap();
        assert!(orch.set_executor(executor).is_ok());
        orch
    }

    /// Register the parent's live codex process so it can be parked and
    /// resumed through the real durable-wait path.
    fn register_parent_process(
        orch: &Orchestrator,
    ) -> (
        Arc<Mutex<Vec<serde_json::Value>>>,
        Arc<Mutex<Option<String>>>,
    ) {
        let (stdin, requests, provider_turn) =
            crate::backends::codex::scripted_delegated_wait_stdin_for_test(
                "native-thread",
                "provider-parent",
            );
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(stdin))),
            Some("session-parent".into()),
            Some("j-parent".into()),
        );
        handle.backend = Some("codex".into());
        handle.model = Some("gpt-5.6-sol".into());
        handle.begin_turn(ANCHOR);
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-parent".into(), handle);
        orch.process_state
            .set_current_turn_id("run-parent", Some(ANCHOR));
        (requests, provider_turn)
    }

    fn explore_payload(description: &str) -> DelegatedTaskPayload {
        DelegatedTaskPayload {
            description: description.to_string(),
            prompt: "Trace how executors are placed.".to_string(),
            subagent_type: "Explore".to_string(),
            tier: None,
            backend_preference: None,
            session: DelegatedTaskSessionMode::New,
            task_index: None,
            output_schema: None,
        }
    }

    async fn spawn(
        orch: &Orchestrator,
        payloads: &[DelegatedTaskPayload],
        budget: Duration,
    ) -> TaskSpawnOutcome {
        spawn_task_packets_within(
            orch,
            SpawnTaskPacketsInput {
                run_id: Some("run-parent"),
                cwd: "/tmp",
                payloads,
                parent_tool_use_id: Some("tool-write"),
                background: false,
            },
            budget,
        )
        .await
    }

    async fn text(orch: &Orchestrator, sql: &str) -> Option<String> {
        orch.db
            .local
            .query_opt_text(sql.to_string(), ())
            .await
            .unwrap()
    }

    async fn count(orch: &Orchestrator, sql: &str) -> i64 {
        orch.db
            .local
            .query_opt_i64(sql.to_string(), ())
            .await
            .unwrap()
            .unwrap_or(0)
    }

    async fn successor_count(orch: &Orchestrator) -> i64 {
        count(
            orch,
            "SELECT COUNT(*) FROM turns WHERE predecessor_id = 'T1' AND start_reason = 'dependency_unblock'",
        )
        .await
    }

    /// The tool result the wake delivered to the spawning call. Delivery is
    /// stored after the successor turn is claimed, so wait for it.
    async fn delivered_tool_result(orch: &Orchestrator) -> Option<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(result) = stored_tool_result(orch).await {
                return Some(result);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn stored_tool_result(orch: &Orchestrator) -> Option<String> {
        let data = text(
            orch,
            "SELECT data FROM events WHERE run_id='run-parent' AND event_type='tool_result' \
             AND json_extract(data,'$.toolUseId')='tool-write'",
        )
        .await?;
        let event: serde_json::Value = serde_json::from_str(&data).unwrap();
        event["toolResult"].as_str().map(str::to_string)
    }

    /// Wait until the deferred park has interrupted the parent's provider turn
    /// and its post-park resume check has run.
    async fn await_park(requests: &Arc<Mutex<Vec<serde_json::Value>>>) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request["method"] == "turn/interrupt")
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the parent was never parked: {:?}",
                requests.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    async fn await_successor(orch: &Orchestrator) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while successor_count(orch).await == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the parked parent was never woken"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Let a duplicate wake, if there were one, land before counting.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    /// Spawn one blocking task whose child stays queued, then return the
    /// parent parked on it and the child's job id.
    async fn spawn_and_park(
        db_name: &str,
    ) -> (Orchestrator, String, Arc<Mutex<Vec<serde_json::Value>>>) {
        let (orch, executor) = parent_fixture(db_name).await;
        let (requests, provider_turn) = register_parent_process(&orch);

        let outcome = spawn(
            &orch,
            &[explore_payload("map the fleet")],
            Duration::from_millis(200),
        )
        .await;

        assert!(outcome.refused.is_empty(), "{:?}", outcome.refused);
        assert_eq!(outcome.spawned.len(), 1);
        let task = &outcome.spawned[0];
        assert_eq!(task.index, 0);
        assert!(
            matches!(
                task.launch,
                TaskLaunch::WaitingToStart {
                    stage: StartStage::Preparing,
                    ..
                }
            ),
            "{:?}",
            task.launch
        );
        assert!(
            task.task_uri
                .as_deref()
                .is_some_and(|uri| uri.starts_with("cairn://p/prj/7/1/builder/task/")),
            "{:?}",
            task.task_uri
        );
        assert_eq!(
            *executor.started.lock().unwrap(),
            vec![task.job_id.clone()],
            "the child was created and handed to the runner"
        );
        let response = outcome.response.expect("a spawned task has blocking text");
        assert!(
            response
                .result
                .ends_with(DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX),
            "{}",
            response.result
        );
        assert_eq!(
            text(
                &orch,
                "SELECT state || '/' || yield_reason FROM turns WHERE id = 'T1'"
            )
            .await
            .as_deref(),
            Some("yielded/dependency_wait")
        );

        await_park(&requests).await;
        *provider_turn.lock().unwrap() = None;
        assert_eq!(successor_count(&orch).await, 0, "nothing has settled yet");
        (orch, task.job_id.clone(), requests)
    }

    /// The child is created but not started: the spawn reports it as created
    /// and waiting, the parent parks, and when the child is placed late and
    /// completes, the parent is woken exactly once with its result.
    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_spawn_with_unstarted_child_reports_it_created_and_parks_then_wakes() {
        let (orch, child, requests) = spawn_and_park("unstarted-child-wake.db").await;

        // Placed late: the child gets its run, works, writes its result, and
        // completes through the warm path a `return` takes.
        let child_owned = child.clone();
        orch.db
            .local
            .write(|conn| {
                let child = child_owned.clone();
                Box::pin(async move {
                    for sql in [
                        "INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at) VALUES('session-child',?1,'claude','open',1,2,2)",
                        "INSERT INTO runs(id,job_id,issue_id,project_id,session_id,status,start_mode,created_at,updated_at) VALUES('run-child',?1,'i','p','session-child','live','fresh',2,2)",
                        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at) VALUES('child-turn','session-child','run-child',?1,1,'running','initial',2,2,2)",
                        "UPDATE jobs SET status='running', started_at=2, current_session_id='session-child', current_turn_id='child-turn' WHERE id=?1",
                    ] {
                        conn.execute(sql, (child.as_str(),)).await?;
                    }
                    conn.execute(
                        "INSERT INTO artifacts(id,job_id,artifact_type,output_name,data,version,created_at,updated_at) VALUES('child-result',?1,'return','return',?2,1,3,3)",
                        (child.as_str(), serde_json::json!({ "content": CHILD_ARTIFACT }).to_string()),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        {
            let mut handle = RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(
                    cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
                ))),
                Some("session-child".into()),
                Some(child.clone()),
            );
            handle.begin_turn("child-turn");
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .register("run-child".into(), handle);
        }
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));
        let warm_orch = orch.clone();
        assert!(tokio::task::spawn_blocking(move || {
            crate::orchestrator::lifecycle::transition_to_warm_state(&warm_orch, "run-child", None)
        })
        .await
        .unwrap());

        await_successor(&orch).await;
        assert_eq!(
            successor_count(&orch).await,
            1,
            "the parent is woken exactly once"
        );
        let delivered = delivered_tool_result(&orch)
            .await
            .expect("the wake delivers the child's result to the spawning call");
        assert!(delivered.contains(CHILD_ARTIFACT), "{delivered}");
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request["method"] == "turn/start")
                .count(),
            1
        );
    }

    /// A child that fails before it ever had a run wakes the parent parked on
    /// it, with the failure.
    #[tokio::test(flavor = "multi_thread")]
    async fn start_failure_of_a_never_started_child_wakes_its_parked_parent() {
        let (orch, child, _requests) = spawn_and_park("unstarted-child-start-failure.db").await;
        assert_eq!(
            count(
                &orch,
                &format!("SELECT COUNT(*) FROM runs WHERE job_id = '{child}'")
            )
            .await,
            0,
            "the child never had a run"
        );

        crate::execution::jobs::mark_job_start_failed(
            &orch,
            &orch.db.local,
            &child,
            "branch preparation failed: store unavailable",
            true,
        )
        .await;

        await_successor(&orch).await;
        assert_eq!(successor_count(&orch).await, 1);
        let delivered = delivered_tool_result(&orch)
            .await
            .expect("the wake delivers the failure to the spawning call");
        assert!(delivered.contains("failed"), "{delivered}");
    }

    /// An inline wait answers inside its budget for a child that fails before
    /// it ever had a run, instead of parking on a child that is already done.
    #[tokio::test(flavor = "multi_thread")]
    async fn inline_wait_answers_for_a_child_that_fails_before_it_had_a_run() {
        let (orch, _executor) = parent_fixture("unstarted-child-inline.db").await;
        let budget = Duration::from_secs(20);
        let started = std::time::Instant::now();
        let spawn_orch = orch.clone();
        let spawning = tokio::spawn(async move {
            spawn(&spawn_orch, &[explore_payload("map the fleet")], budget).await
        });

        let child = loop {
            if let Some(child) = text(
                &orch,
                "SELECT id FROM jobs WHERE parent_job_id = 'j-parent'",
            )
            .await
            {
                break child;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "no child created"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        // Let the spawn reach its wait before the child fails.
        tokio::time::sleep(Duration::from_millis(300)).await;
        crate::execution::jobs::mark_job_start_failed(
            &orch,
            &orch.db.local,
            &child,
            "branch preparation failed: store unavailable",
            true,
        )
        .await;

        let outcome = spawning.await.unwrap();
        assert!(
            started.elapsed() < budget,
            "the inline wait ran out its budget"
        );
        let response = outcome.response.expect("blocking text").result;
        assert!(response.contains("failed"), "{response}");
        assert!(
            !response.contains(DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX),
            "answered inline, not parked: {response}"
        );
        assert_eq!(
            text(&orch, "SELECT state FROM turns WHERE id = 'T1'")
                .await
                .as_deref(),
            Some("running")
        );
    }

    /// An unknown agent creates nothing: every task in the batch is refused,
    /// no packet is written, and no job exists.
    #[tokio::test]
    async fn an_unknown_agent_refuses_the_batch_and_creates_nothing() {
        let (orch, executor) = parent_fixture("unstarted-child-unknown.db").await;
        let mut ghost = explore_payload("ghost");
        ghost.subagent_type = "Nonexistent".to_string();

        let outcome = spawn(
            &orch,
            &[explore_payload("real"), ghost],
            Duration::from_millis(200),
        )
        .await;

        assert!(outcome.response.is_none());
        assert!(outcome.spawned.is_empty());
        assert_eq!(outcome.refused.len(), 2);
        assert!(
            outcome.refused[1]
                .1
                .contains("Agent config not found: Nonexistent"),
            "{:?}",
            outcome.refused
        );
        assert!(
            outcome.refused[0].1.contains("not spawned"),
            "{:?}",
            outcome.refused
        );
        assert_eq!(
            count(
                &orch,
                "SELECT json_array_length(snapshot, '$.delegatedPackets') FROM executions WHERE id = 'e'"
            )
            .await,
            0
        );
        assert_eq!(
            count(
                &orch,
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'"
            )
            .await,
            0
        );
        assert!(executor.started.lock().unwrap().is_empty());
    }

    /// Classification after a partial advancement: a packet whose job exists
    /// but was never dispatched is failed through the start-failure path, a
    /// packet whose job exists but was never linked is linked, and a packet with
    /// no job is refused as such and retired so no later advancement creates it.
    #[tokio::test]
    async fn a_partial_advancement_classifies_each_packet_by_whether_its_job_exists() {
        // A real child created by advancement and held unstarted: the packet
        // whose job exists but was never dispatched.
        let (orch, _executor) = parent_fixture("partial-advancement.db").await;
        let created = spawn_task_packets_within(
            &orch,
            SpawnTaskPacketsInput {
                run_id: Some("run-parent"),
                cwd: "/tmp",
                payloads: &[explore_payload("Explore")],
                parent_tool_use_id: Some("tool-write"),
                background: true,
            },
            Duration::from_millis(200),
        )
        .await;
        let child = created.spawned[0].job_id.clone();
        let db = orch.db.local.clone();
        let mut snapshot: serde_json::Value = serde_json::from_str(
            &db.query_opt_text("SELECT snapshot FROM executions WHERE id = 'e'", ())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let packets = snapshot["delegatedPackets"].as_array_mut().unwrap();
        let packet_a = packets[0]["id"].as_str().unwrap().to_string();
        let mut lost = packets[0].clone();
        lost["id"] = "pkt-b".into();
        lost["title"] = "lost".into();
        lost["status"] = "pending".into();
        lost.as_object_mut().unwrap().remove("resultArtifactJobId");
        lost.as_object_mut().unwrap().remove("materializedNodeIds");
        let mut unlinked = lost.clone();
        unlinked["id"] = "pkt-c".into();
        unlinked["title"] = "unlinked".into();
        unlinked["status"] = "materialized".into();
        unlinked["materializedNodeIds"] = serde_json::json!([
            "delegated-pkt-c-trigger",
            "delegated-pkt-c-context",
            "delegated-pkt-c-agent"
        ]);
        packets.push(lost);
        packets.push(unlinked);
        db.execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = 'e'",
            (snapshot.to_string(),),
        )
        .await
        .unwrap();
        db.execute_script(
            "INSERT INTO jobs(id,execution_id,project_id,issue_id,status,uri_segment,node_name,parent_job_id,recipe_node_id,created_at,updated_at)
               VALUES('j-c','e','p','i','running','j-c','Explore','j-parent','delegated-pkt-c-agent',5,5);
             INSERT INTO runs(id,job_id,issue_id,project_id,status,created_at,updated_at)
               VALUES('run-c','j-c','i','p','starting',6,6);",
        )
        .await
        .unwrap();
        let problem = StartProblem::Advancement("snapshot write failed".to_string());
        let problem = &problem;
        let classify = |packet: String, title: &'static str| {
            let orch = orch.clone();
            async move {
                classify_packet_launch(&orch, &orch.db.local, "e", &packet, title, Some(problem))
                    .await
            }
        };

        // The child was already handed to a runner (the holding executor) and is
        // waiting in preparation. A later advancement error proves nothing about
        // that handoff, so the child is reported as created, never failed.
        match classify(packet_a, "Explore").await {
            PacketLaunch::Created { job_id, launch, .. } => {
                assert_eq!(job_id, child);
                assert!(
                    matches!(
                        &launch,
                        TaskLaunch::WaitingToStart {
                            stage: StartStage::Preparing,
                            detail: Some(detail),
                            ..
                        } if detail.contains("advancement reported an error after creating it")
                    ),
                    "{launch:?}"
                );
            }
            PacketLaunch::Refused(error) => panic!("a handed-off child was refused: {error}"),
        }
        assert_eq!(
            text(
                &orch,
                &format!("SELECT status FROM jobs WHERE id = '{child}'")
            )
            .await
            .as_deref(),
            Some("running"),
            "a child its runner may still start is not failed"
        );

        match classify("pkt-b".to_string(), "lost").await {
            PacketLaunch::Refused(error) => assert_eq!(
                error,
                "no child job was created for task 'lost' (packet pkt-b): advancement did not \
                 materialize it: snapshot write failed"
            ),
            PacketLaunch::Created { launch, .. } => panic!("refused expected, got {launch:?}"),
        }
        assert_eq!(
            text(
                &orch,
                "SELECT json_extract(snapshot, '$.delegatedPackets[1].status') FROM executions WHERE id = 'e'"
            )
            .await
            .as_deref(),
            Some("failed"),
            "a refused packet is retired, not left for a later advancement to expand"
        );

        match classify("pkt-c".to_string(), "unlinked").await {
            PacketLaunch::Created { job_id, launch, .. } => {
                assert_eq!(job_id, "j-c");
                assert_eq!(
                    launch,
                    TaskLaunch::Started {
                        run_id: "run-c".to_string(),
                        admission: None,
                    },
                    "a child that already has a run is started, not failed"
                );
            }
            PacketLaunch::Refused(error) => panic!("created expected: {error}"),
        }
        assert_eq!(
            text(
                &orch,
                "SELECT json_extract(snapshot, '$.delegatedPackets[2].resultArtifactJobId') FROM executions WHERE id = 'e'"
            )
            .await
            .as_deref(),
            Some("j-c")
        );
    }

    /// A dispatch failure fails only the children this call's own dispatch
    /// failed to hand over; a sibling handed off another way keeps waiting.
    #[tokio::test]
    async fn a_dispatch_failure_fails_only_the_children_it_failed_to_hand_over() {
        let (orch, _executor) = parent_fixture("dispatch-failure.db").await;
        let created = spawn_task_packets_within(
            &orch,
            SpawnTaskPacketsInput {
                run_id: Some("run-parent"),
                cwd: "/tmp",
                payloads: &[explore_payload("first"), explore_payload("second")],
                parent_tool_use_id: Some("tool-write"),
                background: true,
            },
            Duration::from_millis(200),
        )
        .await;
        assert_eq!(created.spawned.len(), 2, "{:?}", created.refused);
        let packets: serde_json::Value = serde_json::from_str(
            &text(&orch, "SELECT snapshot FROM executions WHERE id = 'e'")
                .await
                .unwrap(),
        )
        .unwrap();
        let packet_of = |job: &str| {
            packets["delegatedPackets"]
                .as_array()
                .unwrap()
                .iter()
                .find(|packet| packet["resultArtifactJobId"] == job)
                .unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let (stranded, handed) = (
            created.spawned[0].job_id.clone(),
            created.spawned[1].job_id.clone(),
        );
        let problem = StartProblem::Dispatch {
            error: "No executor configured for 1 ready delegated jobs".to_string(),
            undispatched: [stranded.clone()].into_iter().collect(),
        };

        match classify_packet_launch(
            &orch,
            &orch.db.local,
            "e",
            &packet_of(&stranded),
            "first",
            Some(&problem),
        )
        .await
        {
            PacketLaunch::Refused(error) => assert!(
                error.contains("was created but could not be dispatched")
                    && error.contains("it was marked failed"),
                "{error}"
            ),
            PacketLaunch::Created { launch, .. } => panic!("refused expected, got {launch:?}"),
        }
        assert_eq!(
            text(
                &orch,
                &format!("SELECT status FROM jobs WHERE id = '{stranded}'")
            )
            .await
            .as_deref(),
            Some("failed"),
            "a child nothing will start is not left pending"
        );

        match classify_packet_launch(
            &orch,
            &orch.db.local,
            "e",
            &packet_of(&handed),
            "second",
            Some(&problem),
        )
        .await
        {
            PacketLaunch::Created { launch, .. } => assert!(
                matches!(launch, TaskLaunch::WaitingToStart { .. }),
                "{launch:?}"
            ),
            PacketLaunch::Refused(error) => panic!("a handed-off sibling was refused: {error}"),
        }
        assert_ne!(
            text(
                &orch,
                &format!("SELECT status FROM jobs WHERE id = '{handed}'")
            )
            .await
            .as_deref(),
            Some("failed"),
            "a sibling this dispatch did not own is not failed"
        );
    }

    async fn spawn_background(orch: &Orchestrator, description: &str) -> TaskSpawnOutcome {
        spawn_task_packets_within(
            orch,
            SpawnTaskPacketsInput {
                run_id: Some("run-parent"),
                cwd: "/tmp",
                payloads: &[explore_payload(description)],
                parent_tool_use_id: Some("tool-write"),
                background: true,
            },
            Duration::from_millis(200),
        )
        .await
    }

    /// Advancement claims the child, then fails before returning it. The claim
    /// is not stranded: it is handed to the runner, and the spawn reports the
    /// child as created and waiting, with the advancement error.
    #[tokio::test]
    async fn a_claim_stranded_by_a_failed_advancement_is_handed_to_its_runner() {
        let (orch, executor) = parent_fixture("stranded-claim-handed.db").await;
        break_advancement_after_its_claim(&orch).await;

        let outcome = spawn_background(&orch, "map the fleet").await;

        assert!(outcome.refused.is_empty(), "{:?}", outcome.refused);
        let task = &outcome.spawned[0];
        assert!(
            matches!(
                &task.launch,
                TaskLaunch::WaitingToStart {
                    stage: StartStage::Preparing,
                    detail: Some(detail),
                    ..
                } if detail.contains("advancement reported an error after creating it")
                    && detail.contains("Action node has no valid config")
            ),
            "{:?}",
            task.launch
        );
        assert_eq!(
            *executor.started.lock().unwrap(),
            vec![task.job_id.clone()],
            "the claimed child was handed to the runner despite the advancement error"
        );
    }

    /// The same stranded claim, when handing it to the runner fails too: the
    /// child is failed rather than left `running` with no run, and the spawn
    /// reports why.
    #[tokio::test]
    async fn a_stranded_claim_whose_handover_fails_is_failed_and_reported() {
        let orch =
            parent_fixture_with("stranded-claim-refused.db", Arc::new(RefusingExecutor)).await;
        break_advancement_after_its_claim(&orch).await;

        let outcome = spawn_background(&orch, "map the fleet").await;

        assert!(outcome.spawned.is_empty(), "{:?}", outcome.spawned);
        assert!(outcome.response.is_none());
        let (index, error) = &outcome.refused[0];
        assert_eq!(*index, 0);
        assert!(
            error.contains("was created but failed before it started")
                && error.contains("handing it to a runner failed: the runner refused the start"),
            "{error}"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT status FROM jobs WHERE parent_job_id = 'j-parent'"
            )
            .await
            .as_deref(),
            Some("failed")
        );
    }

    /// Advancement's DAG reduction waits for the execution snapshot lock that
    /// packet persistence and settlement hold, so a packet can never be seen
    /// materialized while its job is still being created.
    #[tokio::test(flavor = "multi_thread")]
    async fn advancement_reduction_waits_for_the_execution_snapshot_lock() {
        let (orch, _executor) = parent_fixture("reduction-lock.db").await;
        let parent = ParentRunContext {
            run_id: "run-parent".into(),
            job_id: "j-parent".into(),
            execution_id: Some("e".into()),
            project_id: "p".into(),
            project_key: "prj".into(),
        };
        let payload = explore_payload("map the fleet");
        let agent = resolve_task_agent_config(&orch, &parent, "e", None, &payload)
            .await
            .unwrap();
        persist_task_packet(&orch, "e", &parent, &payload, &agent, "/tmp", None, true)
            .await
            .unwrap();

        let lock = orch.execution_lock("e");
        let held = lock.lock().await;
        let advancing = {
            let orch = orch.clone();
            tokio::spawn(async move {
                crate::execution::advancement::advance_execution_with_actions(&orch, "e").await
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        let jobs = "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'";
        assert_eq!(
            count(&orch, jobs).await,
            0,
            "expansion ran under a held lock"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT json_extract(snapshot, '$.delegatedPackets[0].status') FROM executions WHERE id = 'e'"
            )
            .await
            .as_deref(),
            Some("pending")
        );

        drop(held);
        advancing.await.unwrap().unwrap();
        assert_eq!(count(&orch, jobs).await, 1);
    }

    /// Persist one background `Explore` packet for `j-parent` without advancing.
    async fn persist_pending_packet(orch: &Orchestrator) {
        let parent = ParentRunContext {
            run_id: "run-parent".into(),
            job_id: "j-parent".into(),
            execution_id: Some("e".into()),
            project_id: "p".into(),
            project_key: "prj".into(),
        };
        let payload = explore_payload("map the fleet");
        let agent = resolve_task_agent_config(orch, &parent, "e", None, &payload)
            .await
            .unwrap();
        persist_task_packet(orch, "e", &parent, &payload, &agent, "/tmp", None, true)
            .await
            .unwrap();
    }

    fn cascading_advance() -> Vec<WorkflowEffect> {
        vec![WorkflowEffect::AdvanceDag {
            execution_id: "e".to_string(),
            outbox_entry_id: None,
        }]
    }

    /// A cascading `AdvanceDag` in the effect loop that claims a child and then
    /// fails does not strand it: the loop's error carries the claim and the host
    /// hands it to the runner.
    #[tokio::test]
    async fn a_cascading_reduction_that_fails_after_its_claim_hands_the_child_over() {
        let (orch, executor) = parent_fixture("cascading-claim.db").await;
        persist_pending_packet(&orch).await;
        break_advancement_after_its_claim(&orch).await;

        let error = crate::effects::run::execute_effects(&orch, cascading_advance())
            .await
            .expect_err("the broken action node fails the cascading reduction");
        assert!(error.contains("Action node has no valid config"), "{error}");

        let child = text(
            &orch,
            "SELECT id FROM jobs WHERE parent_job_id = 'j-parent'",
        )
        .await
        .expect("the reduction created and claimed the child");
        assert_eq!(
            *executor.started.lock().unwrap(),
            vec![child.clone()],
            "the claimed child was handed to the runner"
        );
        assert_eq!(
            text(
                &orch,
                &format!("SELECT status FROM jobs WHERE id = '{child}'")
            )
            .await
            .as_deref(),
            Some("running")
        );
    }

    /// A cascading `AdvanceDag` takes the same execution snapshot lock as
    /// top-level advancement and packet writers.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cascading_reduction_waits_for_the_execution_snapshot_lock() {
        let (orch, _executor) = parent_fixture("cascading-lock.db").await;
        persist_pending_packet(&orch).await;

        let lock = orch.execution_lock("e");
        let held = lock.lock().await;
        let running = {
            let orch = orch.clone();
            tokio::spawn(async move {
                crate::effects::run::execute_effects(&orch, cascading_advance()).await
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        let jobs = "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'";
        assert_eq!(
            count(&orch, jobs).await,
            0,
            "expansion ran under a held lock"
        );
        assert_eq!(
            text(
                &orch,
                "SELECT json_extract(snapshot, '$.delegatedPackets[0].status') FROM executions WHERE id = 'e'"
            )
            .await
            .as_deref(),
            Some("pending")
        );

        drop(held);
        running.await.unwrap().unwrap();
        assert_eq!(count(&orch, jobs).await, 1);
    }

    /// The lock now wraps every reduction for every caller, so drive the nested
    /// path: an advancement whose effect loop executes an action, whose
    /// completion cascades into a second reduction, while several spawns persist
    /// packets into the same snapshot. It must finish within a bound — a
    /// self-deadlock fails here instead of hanging — and lose no packet.
    #[tokio::test(flavor = "multi_thread")]
    async fn nested_reductions_and_concurrent_spawns_neither_deadlock_nor_lose_packets() {
        let executor = Arc::new(NestingExecutor::default());
        let orch = parent_fixture_with("nested-reductions.db", executor.clone()).await;
        add_ready_action_node(
            &orch,
            Some(serde_json::json!({ "action": "noop", "actionParams": {} })),
        )
        .await;

        let mut work = Vec::new();
        {
            let orch = orch.clone();
            work.push(tokio::spawn(async move {
                crate::execution::advancement::advance_execution_with_actions(&orch, "e")
                    .await
                    .map(|_| 0)
            }));
        }
        for spawner in 0..4 {
            let orch = orch.clone();
            work.push(tokio::spawn(async move {
                let payloads = [
                    explore_payload(&format!("spawner {spawner} first")),
                    explore_payload(&format!("spawner {spawner} second")),
                ];
                let outcome = spawn_task_packets_within(
                    &orch,
                    SpawnTaskPacketsInput {
                        run_id: Some("run-parent"),
                        cwd: "/tmp",
                        payloads: &payloads,
                        parent_tool_use_id: None,
                        background: true,
                    },
                    Duration::from_millis(200),
                )
                .await;
                if outcome.refused.is_empty() {
                    Ok(outcome.spawned.len())
                } else {
                    Err(format!("{:?}", outcome.refused))
                }
            }));
        }

        let finished = tokio::time::timeout(Duration::from_secs(120), async {
            let mut spawned = 0;
            for task in work {
                spawned += task.await.unwrap().expect("no reduction or spawn failed");
            }
            spawned
        })
        .await
        .expect("nested reductions and concurrent spawns deadlocked");

        assert_eq!(finished, 8, "every task of every spawn was created");
        assert!(
            *executor.actions.lock().unwrap() >= 1,
            "the action ran, so a cascading reduction followed it"
        );
        let snapshot: serde_json::Value = serde_json::from_str(
            &text(&orch, "SELECT snapshot FROM executions WHERE id = 'e'")
                .await
                .unwrap(),
        )
        .unwrap();
        let packets = snapshot["delegatedPackets"].as_array().unwrap();
        assert_eq!(packets.len(), 8, "no persisted packet was dropped");
        assert!(
            packets
                .iter()
                .all(|packet| packet["resultArtifactJobId"].is_string()),
            "every packet is linked to its job: {packets:?}"
        );
        assert_eq!(
            count(
                &orch,
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'"
            )
            .await,
            8
        );
    }

    /// A batch whose second packet cannot be persisted, after a concurrent
    /// advancement has already turned its first packet into a real child:
    /// that child is reported as applied and waited on to completion, and only
    /// the uncreated task is refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_persistence_failure_keeps_an_already_created_sibling() {
        let (orch, _executor) = parent_fixture("persist-failure.db").await;
        super::PERSIST_FAULTS
            .lock()
            .unwrap()
            .push(("run-parent".to_string(), 1, true));
        let started = std::time::Instant::now();
        let spawning = {
            let orch = orch.clone();
            tokio::spawn(async move {
                spawn(
                    &orch,
                    &[explore_payload("first"), explore_payload("second")],
                    Duration::from_secs(20),
                )
                .await
            })
        };

        let child = loop {
            if let Some(child) = text(
                &orch,
                "SELECT id FROM jobs WHERE parent_job_id = 'j-parent'",
            )
            .await
            {
                break child;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "no child created"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        let child_owned = child.clone();
        orch.db
            .local
            .write(|conn| {
                let child = child_owned.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO artifacts(id,job_id,artifact_type,output_name,data,version,created_at,updated_at) VALUES('first-result',?1,'return','return',?2,1,3,3)",
                        (child.as_str(), serde_json::json!({ "content": CHILD_ARTIFACT }).to_string()),
                    )
                    .await?;
                    conn.execute("UPDATE jobs SET status = 'complete' WHERE id = ?1", (child.as_str(),))
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let outcome = spawning.await.unwrap();
        assert_eq!(outcome.spawned.len(), 1, "{:?}", outcome.refused);
        assert_eq!(outcome.spawned[0].index, 0);
        assert_eq!(outcome.spawned[0].job_id, child);
        assert!(outcome.spawned[0].task_uri.is_some());
        assert_eq!(
            outcome.refused,
            vec![(1, "injected persistence failure".to_string())]
        );
        let response = outcome
            .response
            .expect("the created child is waited on")
            .result;
        assert!(response.contains(CHILD_ARTIFACT), "{response}");
        assert_eq!(
            count(
                &orch,
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'"
            )
            .await,
            1,
            "the task that could not be persisted created nothing"
        );
    }

    async fn write_tasks(
        orch: &Orchestrator,
        tool_use_id: &str,
        task: serde_json::Value,
    ) -> serde_json::Value {
        let request = crate::mcp::types::McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: Some("run-parent".to_string()),
            tool: "write".to_string(),
            tool_use_id: Some(tool_use_id.to_string()),
            payload: serde_json::json!({
                "changes": [{
                    "target": "cairn://p/prj/7/1/builder/tasks",
                    "mode": "append",
                    "payload": task
                }]
            }),
        };
        let result = crate::mcp::handlers::write::handle_write(orch, &request).await;
        serde_json::Deserializer::from_str(&result)
            .into_iter::<serde_json::Value>()
            .next()
            .unwrap_or_else(|| panic!("no report: {result}"))
            .unwrap_or_else(|error| panic!("{error}: {result}"))
    }

    /// A background task whose child is created but not started is an applied
    /// change carrying its URI and launch state; a redelivery of the same write
    /// call replays that report and creates nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn background_spawn_report_lists_waiting_task_and_redelivery_replays_it() {
        let (orch, _executor) = parent_fixture("background-waiting-report.db").await;
        let task = serde_json::json!({
            "subagentType": "Explore",
            "description": "map the fleet",
            "prompt": "Trace how executors are placed.",
            "runInBackground": true
        });

        let report = write_tasks(&orch, "tool-bg", task.clone()).await;
        assert!(report.get("failures").is_none(), "{report}");
        let applied = &report["applied"][0];
        assert_eq!(applied["index"], 0, "{report}");
        assert_eq!(applied["kind"], "resource");
        assert_eq!(
            applied["data"]["launch"]["state"], "waiting_to_start",
            "{report}"
        );
        assert_eq!(applied["data"]["launch"]["stage"], "preparing", "{report}");
        let task_uri = applied["data"]["taskUri"]
            .as_str()
            .expect("task URI")
            .to_string();
        assert!(
            task_uri.starts_with("cairn://p/prj/7/1/builder/task/"),
            "{task_uri}"
        );
        assert!(
            applied["summary"].as_str().unwrap().starts_with(&format!(
                "Spawned task {task_uri}, waiting to start (preparing"
            )),
            "{report}"
        );
        let blocking = report["blocking"]["text"].as_str().unwrap();
        assert!(
            blocking.starts_with("Spawned 1 background task(s)")
                && blocking.contains("waiting to start"),
            "{blocking}"
        );
        let jobs = "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'";
        assert_eq!(count(&orch, jobs).await, 1);
        // The ledger row a parked parent's resumed tool result is completed
        // from, and a redelivery replays, names the task this call created.
        let ledger = text(
            &orch,
            "SELECT report FROM write_replay_ledger WHERE run_id = 'run-parent' AND tool_use_id = 'tool-bg'",
        )
        .await
        .expect("the write is recorded for replay");
        assert!(ledger.contains(&task_uri), "{ledger}");

        let replay = write_tasks(&orch, "tool-bg", task).await;
        assert!(replay.get("replayed").is_some(), "{replay}");
        assert_eq!(replay["applied"], report["applied"]);
        assert_eq!(count(&orch, jobs).await, 1, "a redelivery creates nothing");
    }

    /// A task append that creates no child is a failure of that append, not
    /// blocking text, and leaves no packet or job behind.
    #[tokio::test]
    async fn a_task_append_that_creates_no_child_is_a_failure() {
        let (orch, _executor) = parent_fixture("append-no-child.db").await;
        let report = write_tasks(
            &orch,
            "tool-ghost",
            serde_json::json!({
                "subagentType": "Nonexistent",
                "description": "ghost",
                "prompt": "Do not spawn"
            }),
        )
        .await;
        assert_eq!(report["applied"], serde_json::json!([]), "{report}");
        assert!(report.get("blocking").is_none(), "{report}");
        assert_eq!(report["failures"][0]["index"], 0);
        assert!(
            report["failures"][0]["error"]
                .as_str()
                .unwrap()
                .contains("Agent config not found: Nonexistent"),
            "{report}"
        );
        assert_eq!(
            count(
                &orch,
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = 'j-parent'"
            )
            .await,
            0
        );
        assert_eq!(
            count(
                &orch,
                "SELECT json_array_length(snapshot, '$.delegatedPackets') FROM executions WHERE id = 'e'"
            )
            .await,
            0
        );
    }

    #[test]
    fn launch_states_serialize_with_a_state_tag() {
        let waiting = TaskLaunch::WaitingToStart {
            since_unix: 1_792_263_908,
            stage: StartStage::Preparing,
            detail: Some(
                "the project store lock that branch preparation needs is held by 'x'".into(),
            ),
        };
        let wire = serde_json::to_value(&waiting).unwrap();
        assert_eq!(wire["state"], "waiting_to_start");
        assert_eq!(wire["stage"], "preparing");
        assert_eq!(wire["sinceUnix"], 1_792_263_908);
        assert_eq!(
            waiting.waiting_note().as_deref(),
            Some("waiting to start (preparing, since 2026-10-17T19:05:08Z): the project store lock that branch preparation needs is held by 'x'")
        );
        let started = TaskLaunch::Started {
            run_id: "r".into(),
            admission: None,
        };
        assert_eq!(
            serde_json::to_value(&started).unwrap(),
            serde_json::json!({ "state": "started", "runId": "r" })
        );
        assert!(started.waiting_note().is_none());
    }
}

#[cfg(test)]
mod journal_replay_tests {
    use super::*;
    use crate::db::DbState;
    use crate::execution::delegation::DelegatedCallPayload;
    use crate::execution::jobs::CallBranchPolicy;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, RowExt, SearchIndex};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("replay.db"))
                .await
                .unwrap(),
        );
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
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
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
            branch_policy: CallBranchPolicy::None,
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
                payloads: &payloads,
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

    #[tokio::test]
    async fn blocking_write_envelope_preserves_journal_replay_without_spawning() {
        let (orch, db) = orch_with_db().await;
        seed_workflow_run(&db).await;
        let prompt = "replay through the write handler";
        let before = job_count(&db).await;
        for (ordinal, status, stored, expected) in [
            (
                0,
                crate::workflow_journal::JournalStatus::Success,
                Some(r#"{"failures":[{"error":"artifact data, not a host failure"}]}"#),
                r#"{"failures":[{"error":"artifact data, not a host failure"}]}"#,
            ),
            (
                1,
                crate::workflow_journal::JournalStatus::Failure,
                None,
                CALL_FAILED_SENTINEL,
            ),
        ] {
            crate::workflow_journal::store_entry(
                &db,
                "wf-run",
                ordinal,
                &crate::workflow_journal::hash_prompt(prompt),
                stored,
                status,
            )
            .await
            .unwrap();
            let request = crate::mcp::types::McpCallbackRequest {
                thread_id: None,
                cwd: "/tmp/p".to_string(),
                run_id: Some("wf-run".to_string()),
                tool: "write".to_string(),
                tool_use_id: None,
                payload: serde_json::json!({
                    "changes": [{
                        "target": "cairn://p/prj/7/1/flow/calls",
                        "mode": "append",
                        "payload": { "prompt": prompt, "background": true, "ordinal": ordinal }
                    }]
                }),
            };
            let result = crate::mcp::handlers::write::handle_write(&orch, &request).await;
            let report: serde_json::Value = serde_json::from_str(&result).unwrap();
            assert!(report.get("failures").is_none(), "{result}");
            let text = report["blocking"]["text"]
                .as_str()
                .expect("host blocking marker");
            let encoded = text
                .lines()
                .find_map(|line| line.strip_prefix(CALL_MEMO_RESULT_PREFIX))
                .expect("journal replay marker");
            assert_eq!(
                serde_json::from_str::<String>(encoded.trim()).unwrap(),
                expected
            );
            assert_eq!(job_count(&db).await, before, "a replay must not spawn");
        }
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
                payloads: &payloads,
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
