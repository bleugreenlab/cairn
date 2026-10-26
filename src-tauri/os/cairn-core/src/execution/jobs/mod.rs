//! Job lifecycle functions — start, continue, complete, create child task.
//!
//! All business logic lives here. Host layers (Tauri, cairn-server) provide thin
//! wrappers that handle framework-specific concerns (async spawning, process start).
//!
//! ## Key functions
//!
//! - [`prepare_job`] — branch-coordinate and DB preparation, returns [`PreparedJob`] for session spawn.
//! - [`continue_job_impl`] — sends follow-up message to a running/warm job.
//! - [`on_job_complete_impl`] — DAG advancement after a job finishes.
//! - [`create_child_task`] — user-initiated sub-agent under a running job.

use crate::agent_process::stream::TranscriptEvent;
use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_runtime_selection,
    spawned_task_backend, LaunchSelectionOverride,
};
use crate::config::{self, agents as config_agents, ConfigResult};
use crate::db_records::{db_job_from_row, DbJob, DbRecipeNode, JOB_COLUMNS};
use crate::execution::advancement::format_resolved_inputs;
use crate::execution::step_behavior::resolve_node_behavior;
use crate::models::{
    AgentConfig, AgentSnapshot, ExecutionSnapshot, Job, JobStatus, Model, OutputSchema,
    OutputSchemaInfo, Run, RunStatus, Session, SessionStatus, TurnStartReason, TurnState,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use crate::transcripts::stream_store::{insert_event, insert_event_stamping_pushes, EventInsert};
use cairn_common::ids;
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod calls;
mod child_tasks;
mod config_loading;
mod goal_intent;
mod inputs;
mod lifecycle;
mod ordinary_admission;
mod persistence;
mod private_lane;
pub(crate) use private_lane::{
    describe_job_launch, describe_lane, describe_shortfall, private_lane_budgets,
    promote_private_lanes_blocking, spawn_produced_first_output, spawn_reports_first_output,
    withdraw_orphaned_ordinary_admissions, PrivateLaneLedger,
};
/// Drive queued private admission deterministically without a scheduler task.
#[cfg(any(test, feature = "test-utils"))]
pub fn promote_private_lanes_for_test(orch: &Orchestrator) {
    private_lane::promote_private_lanes_blocking(orch);
}

mod team_admission_lifecycle;
#[cfg(test)]
pub(crate) use persistence::prepare_session_for_test;
mod slash_commands;
mod snapshots;
mod status;
mod turns;
mod workflow;

pub(crate) use calls::{
    heal_settled_call_admissions, on_call_run_finalized, prepare_call_run, promote_call_lanes,
    start_call_run,
};
pub(crate) use ordinary_admission::{
    promote_private_lane, recover_on_startup as recover_ordinary_admissions_on_startup,
};
pub(crate) use team_admission_lifecycle::journal_and_retry_terminal;
// The per-call Restart action reaches this via
// `cairn_core::internal::execution::jobs::restart_call`.
pub use calls::restart_call;
// Startup call recovery (CAIRN-2548): the always-on hosts call this via
// `cairn_core::internal::...` through the Orchestrator method.
pub(crate) use calls::fail_orphaned_calls_on_startup;
pub use child_tasks::create_child_task;
pub(crate) use goal_intent::{goal_intent_for_job, goal_intent_for_run, record_goal_intent};
pub use goal_intent::{job_goal_state, set_job_goal_intent, GoalDisplay, GoalIntent, JobGoalState};
pub(crate) use inputs::{
    find_downstream_artifact_schema_conn, find_downstream_artifact_schema_with_snapshot_conn,
    is_long_running_node_conn, node_ships_a_pr_conn, resolve_ctx_self_schemas_conn,
    resolve_ctx_self_schemas_with_snapshot, resolve_instruction_prompt_conn,
};
pub(crate) use lifecycle::{
    continue_automatic_retry, continue_job_launch_locked_for_operator_compact,
    continue_job_launch_locked_for_watchdog, OperatorBoundary, WatchdogContinuation,
};
// The branch-mode coordinate decision. Production reaches it only through
// `prepare_job`; the delegation edge's regression imports it to run the mode it
// emits all the way to the commit a delegated task starts from.
pub use lifecycle::{
    compose_launch_inputs, continue_job_impl, continue_job_or_enqueue, on_job_complete_impl,
    prepare_job, preview_job_digest, resume_job_from_digest, DigestPreview, ResumeContext,
};
#[cfg(any(test, feature = "test-utils"))]
pub use lifecycle::{
    continue_job_or_enqueue_with_before_resume_for_test, in_flight_launch_for_test,
    reconcile_stale_active_turn_for_continue_for_test,
};
pub(crate) use lifecycle::{
    current_session_refuses_turns, recover_unresolvable_session_from_digest, GoalReactivation,
    GoalReconciliation, JobRunnable,
};
#[cfg(test)]
pub(crate) use lifecycle::{select_job_coordinate, CoordinateRequest, ParentCoordinate};
pub(crate) use persistence::load_job_conn;
pub(crate) use slash_commands::resolve_skill_slash_command;
pub(crate) use snapshots::queued_user_transcript_event;
pub(crate) use turns::{admit_codex_goal_turn, CodexGoalTurnAdmission};
// Delegation reaches the host-execution invariant here: a job that arrives at
// delegation with no execution gets one carrying its OWN agent, not an empty
// agents map.
pub(crate) use snapshots::{ensure_host_agent_snapshot, SnapshotFreshness};
// The agent-snapshot editor establishes a session job's host through this before
// the job's first turn, so a thread is configurable before it has ever run.
pub use snapshots::ensure_job_agent_snapshot;
pub(crate) use snapshots::store_blocking_write_result_with_resolution;
pub use snapshots::{store_tool_result_event_with_resolution, store_tool_result_event_with_turn};
// Exercised end-to-end by the `synthetic_continuation_event` integration test,
// which pins the stored event type that keeps a Cairn-synthesized resume out of
// every user-attributed surface (CAIRN-3175).
#[cfg(any(test, feature = "test-utils"))]
pub use snapshots::store_continuation_event_with_turn;
pub use snapshots::store_launch_event_with_turn;
pub(crate) use workflow::{
    delete_workflow_run_row, prepare_workflow_run, redispatch_crashed_workflows,
    start_workflow_run, CreateWorkflowRunInput,
};
// The header Restart action reaches this from the host crates via
// `cairn_core::internal::execution::jobs::restart_workflow`.
pub use workflow::restart_workflow;
// The standalone (UI-driven, caller-less) workflow launch reaches this from the
// host crates via `cairn_core::internal::execution::jobs::launch_standalone_workflow`.
pub use workflow::{launch_standalone_workflow, LaunchedWorkflow};
// The canonical, routing-aware turn-start. Host job-start paths call this
// instead of hand-rolling the turns UPDATE against the private DB (CAIRN-2206).
pub(crate) use turns::start_turn;
pub(crate) use turns::{
    abandon_pending_retry_if_head_matches, claim_retry_successor_if_head_matches,
    claim_retry_turn_start, consecutive_retry_turn_count,
};

use config_loading::*;
use inputs::*;
use persistence::*;
use snapshots::*;
use status::*;
use turns::*;

// The canonical run projection and row mapper live in `runs::queries`. The job
// persistence path (`load_run`, `create_run`) reuses them instead of keeping a
// duplicate column list, so the `runs` projection has one source of truth.
use crate::runs::queries::{run_from_row, RUN_COLUMNS};

/// Mark the latest run for a job crashed and fail the job after a failed start.
///
/// Both a direct start and a start promoted out of ordinary admission end here,
/// so a launch that cannot begin is terminalized the same way whichever path
/// attempted it. A preparation failure may precede any run row; one is then
/// inserted so the failure stays visible on the job.
pub async fn mark_job_start_failed(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    job_id: &str,
    error: &str,
    preparation_failed: bool,
) {
    let exit_reason = if preparation_failed {
        "job_preparation_failed"
    } else {
        "session_start_failed"
    };
    let job_id_owned = job_id.to_string();
    let latest_run_id: Option<String> = owning_db
        .read(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .ok()
        .flatten();

    let latest_run_id = match latest_run_id {
        Some(run_id) => Some(run_id),
        None => {
            let run_id = ids::mint_child(job_id);
            let now = chrono::Utc::now().timestamp();
            let inserted = owning_db
                .write(|conn| {
                    let run_id = run_id.clone();
                    let job_id = job_id.to_string();
                    let error = error.to_string();
                    Box::pin(async move {
                        conn.execute(
                            "INSERT INTO runs (id, job_id, status, error_message, exit_reason, created_at, updated_at, start_mode)
                             VALUES (?1, ?2, 'crashed', ?3, ?4, ?5, ?5, 'fresh')",
                            params![run_id.as_str(), job_id.as_str(), error.as_str(), exit_reason, now],
                        )
                        .await?;
                        Ok(())
                    })
                })
                .await;
            match inserted {
                Ok(()) => Some(run_id),
                Err(insert_error) => {
                    log::error!(
                        "Failed to persist preparation error for job {job_id}: {insert_error}"
                    );
                    None
                }
            }
        }
    };

    if let Some(run_id) = latest_run_id.clone() {
        let _ = crate::transitions::transition_run(
            owning_db,
            &run_id,
            RunStatus::Crashed,
            &*orch.services.emitter,
        )
        .await;
        let error_owned = error.to_string();
        let _ = owning_db
            .write(|conn| {
                let run_id = run_id.clone();
                let error_owned = error_owned.clone();
                Box::pin(async move {
                    conn.execute(
                        "UPDATE runs SET error_message = ?1, exit_reason = ?2 WHERE id = ?3",
                        params![error_owned, exit_reason, run_id],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await;
    }

    if let Err(e) = crate::execution::advancement::mark_job_failed(orch, job_id, error) {
        log::error!("Failed to transition job {} to failed: {}", job_id, e);
    }

    match crate::jobs::queries::get_job(owning_db, job_id).await {
        Ok(job) => {
            let _ = orch
                .services
                .emitter
                .emit("db-change", crate::notify::job_db_change(&job, "update"));
        }
        Err(e) => {
            log::warn!("Failed to reload job {} for db-change: {}", job_id, e);
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "jobs", "action": "update"}),
            );
        }
    }

    wake_waiters_after_start_failure(orch, job_id, latest_run_id.as_deref());
}

/// Answer whoever is waiting on a job that just failed to start.
///
/// A delegated parent is normally woken by its child's run finalizing, which a
/// child that failed to start never does, so a parent parked on it would sit
/// there until the boot heal. The completion broadcast tells an inline waiter
/// to re-read its children; the resume gate wakes a parked one. The gate is
/// idempotent and self-gating, so for a job no delegated packet names — or a
/// sibling batch still in flight — it is a no-op.
///
/// The gate runs on its own thread, after every piece of the failure's
/// bookkeeping above. It may launch the parent's next turn, and that launch must
/// neither re-enter the admission path a promoted start failure arrives from
/// nor make that path's bookkeeping wait on it.
fn wake_waiters_after_start_failure(orch: &Orchestrator, job_id: &str, run_id: Option<&str>) {
    if let Some(run_id) = run_id {
        let _ = orch.run_completions.send(run_id.to_string());
    }
    let orch = orch.clone();
    let job_id = job_id.to_string();
    let thread_job_id = job_id.clone();
    let spawned = std::thread::Builder::new()
        .name("start-failure-wake".to_string())
        .spawn(move || {
            if let Err(error) =
                crate::execution::delegation::resume_suspended_parent_after_task_completion(
                    &orch,
                    &thread_job_id,
                )
            {
                log::warn!(
                    "Failed to wake the parent of job {thread_job_id} after its start failed: {error}"
                );
            }
        });
    if let Err(error) = spawned {
        log::error!("Could not wake the parent of job {job_id} after its start failed: {error}");
    }
}

// ============================================================================
// Public types
// ============================================================================

/// Input for creating a user-initiated child task.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChildTaskInput {
    pub(crate) parent_job_id: String,
    pub(crate) description: String,
    pub(crate) prompt: String,
    pub(crate) subagent_type: String,
    #[serde(alias = "model")]
    pub(crate) tier: Option<String>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub(crate) backend_preference: Option<String>,
}

/// The single backend handoff for a prepared ordinary cold launch.
///
/// Initial transport starts, delegated children, and cold continuations route
/// through this seam before they reach the backend. Admission promotion can
/// therefore fence and recheck one operation instead of wrapping individual
/// backend call sites. Keeping the prepared launch claim borrowed here also
/// guarantees it spans process registration on the direct path.
#[allow(clippy::too_many_arguments)]
pub fn start_ordinary_session(
    orch: &Orchestrator,
    run_id: &str,
    prompt: &str,
    session_start: crate::backends::SessionStart,
    job_model: Option<Model>,
    agent_config: Option<&AgentConfig>,
    artifact_schema_info: Option<&OutputSchemaInfo>,
    execution_id: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<(), String> {
    crate::orchestrator::session::start_agent_session(
        orch,
        run_id,
        prompt,
        session_start,
        job_model,
        None,
        agent_config,
        artifact_schema_info,
        false,
        false,
        execution_id,
        identity_override,
    )
}

/// Start a prepared ordinary session, or queue it behind durable admission.
/// Returns `true` when a process already holds the prompt and `false` while the
/// launch waits in admission's queue.
pub fn start_prepared_job_session(
    orch: &Orchestrator,
    mut prepared: PreparedJob,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<bool, String> {
    ordinary_admission::enqueue_or_start(orch, &mut prepared, identity_override)
}

fn start_ordinary_session_now(
    orch: &Orchestrator,
    prepared: &PreparedJob,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<(), String> {
    start_ordinary_session(
        orch,
        &prepared.run_id,
        &prepared.prompt,
        prepared.session_start.clone(),
        prepared.job_model.clone(),
        prepared.agent_config.as_ref(),
        prepared.artifact_schema_info.as_ref(),
        prepared.execution_id.as_deref(),
        identity_override,
    )
}

/// Result of creating a child task.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChildTaskResult {
    pub(crate) job_id: String,
    run_id: String,
}

/// Durable branch policy for an ephemeral call (CAIRN-2481).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CallBranchPolicy {
    /// Inherit the caller's branch and logical head.
    #[default]
    Inherit,
    /// Keep the call branchless; its process still resides in job scratch.
    None,
}

/// Input for creating an ephemeral agent-call run.
pub(crate) struct CreateCallRunInput {
    pub(crate) parent_job_id: String,
    /// The parent's task-execution id (the snapshot that carries the call
    /// packet); falls back to the parent job's execution_id when absent.
    pub(crate) execution_id: Option<String>,
    pub(crate) subagent_type: String,
    pub(crate) description: String,
    pub(crate) prompt: String,
    pub(crate) tier: Option<String>,
    pub(crate) backend_preference: Option<String>,
    pub(crate) output_contract: crate::models::DelegatedOutputContract,
    pub(crate) branch_policy: CallBranchPolicy,
    pub(crate) label: Option<String>,
    pub(crate) phase: Option<String>,
    pub(crate) parent_tool_use_id: Option<String>,
    pub(crate) task_index: Option<i32>,
    /// When set (a workflow-parented call that missed the journal), the call's
    /// run is linked to this journal key so its result is journaled on
    /// completion (CAIRN-2498). `None` for an ordinary call.
    pub(crate) workflow_journal_link: Option<crate::workflow_journal::CallLink>,
}

/// A prepared ephemeral call run: all DB rows and the transcript seed exist, but
/// the backend session has not started yet (the caller persists the call packet
/// first, then calls [`start_call_run`]).
///
/// `Clone` because the admission seam clones a queued call onto its VecDeque
/// while the spawn path still reads the borrowed original after `start_call_run`.
#[derive(Clone, Serialize, Deserialize)]
pub struct PreparedCallRun {
    pub(crate) job_id: String,
    pub(crate) run_id: String,
    session_id: String,
    pub(crate) agent_config: AgentConfig,
    selected_model: Option<Model>,
    prompt: String,
    output_schema: OutputSchemaInfo,
    execution_id: Option<String>,
}

/// Everything needed by the host layer to spawn a Claude process for a job.
///
/// Returned by [`prepare_job`] after branch preparation, DB work, run creation,
/// and initial user-event storage are complete.
pub struct PreparedJob {
    pub job_id: String,
    pub project_id: String,
    pub run_id: String,
    pub session_id: String,
    pub session_start: crate::backends::SessionStart,
    pub prompt: String,
    pub job_model: Option<Model>,
    pub agent_config: Option<AgentConfig>,
    pub artifact_schema_info: Option<OutputSchemaInfo>,
    pub execution_id: Option<String>,
    pub turn_id: String,
    pub launch_event_id: String,
    pub start_reason: TurnStartReason,
    /// Who this launch serves. An operator launch is admitted as urgent, ahead
    /// of background work; see `ordinary_admission`.
    pub origin: crate::storage::LaunchOrigin,
    /// Ownership of this job's launch, held from admission until the spawned
    /// process registers. The host layer must keep it alive across the spawn and
    /// drop it only once the process is registered or the start has failed;
    /// dropping it early re-opens the job to a concurrent resume that would
    /// launch a second process against this same session (CAIRN-3283).
    pub launch_claim: Option<crate::orchestrator::JobLaunchClaim>,
}

fn run_start_mode(session_start: &crate::backends::SessionStart) -> &'static str {
    match session_start {
        crate::backends::SessionStart::New { .. } => "fresh",
        crate::backends::SessionStart::Resume { .. } => "resume",
        crate::backends::SessionStart::Fork { .. } => "fork",
    }
}

fn resolve_continue_session_start(
    session: &crate::models::Session,
) -> Result<crate::backends::SessionStart, String> {
    if let Some(backend_id) = session.backend_id.clone() {
        return Ok(crate::backends::SessionStart::Resume {
            session_id: session.id.clone(),
            backend_id,
        });
    }

    Err(format!(
        "Session {} has no confirmed backend resume handle; cannot continue an unstarted or failed startup",
        &session.id[..session.id.len().min(8)]
    ))
}

fn run_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to start database runtime: {}", e))?
            .block_on(future)
    })
    .join()
    .map_err(|payload| {
        format!(
            "Database task panicked: {}",
            crate::storage::panic_message(&*payload)
        )
    })?
}

/// [`run_db`] for a future that BORROWS its inputs.
///
/// The scoped thread keeps the borrow alive across the join, which a `'static`
/// spawn cannot, so a synchronous caller holding `&Orchestrator` can drive async
/// database work without cloning the world. The thread itself is there for the
/// same reason as in [`run_db`]: a fresh current-thread runtime must not be
/// built inside an existing one.
fn run_db_borrowed<T>(future: impl Future<Output = Result<T, String>> + Send) -> Result<T, String>
where
    T: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to start database runtime: {}", e))?
                    .block_on(future)
            })
            .join()
            .map_err(|payload| {
                format!(
                    "Database task panicked: {}",
                    crate::storage::panic_message(&*payload)
                )
            })?
    })
}

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

#[cfg(test)]
pub(crate) use inputs::is_long_running_node;
