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
    LaunchSelectionOverride,
};
use crate::config::{self, agents as config_agents, ConfigResult};
use crate::db_records::{db_job_from_row, DbJob, DbRecipeEdge, DbRecipeNode, JOB_COLUMNS};
use crate::execution::advancement::{format_resolved_inputs, ResolvedInput};
use crate::execution::dag::{recipe_edge_to_db, recipe_node_to_db};
use crate::execution::step_behavior::resolve_node_behavior;
use crate::models::{
    AgentConfig, AgentSnapshot, ExecutionSnapshot, Job, JobStatus, Model, OutputSchema,
    OutputSchemaInfo, RecipeNode, Run, RunStatus, Session, SessionStatus, TurnStartReason,
    TurnState,
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

mod call_admission;
mod calls;
mod child_tasks;
mod config_loading;
mod inputs;
mod lifecycle;
mod persistence;
mod slash_commands;
mod snapshots;
mod status;
mod turns;
mod workflow;

pub(crate) use call_admission::CallAdmission;
pub(crate) use calls::{on_call_run_finalized, prepare_call_run, start_call_run};
// The per-call Restart action reaches this via
// `cairn_core::internal::execution::jobs::restart_call`.
pub use calls::restart_call;
// Startup call recovery (CAIRN-2548): the always-on hosts call this via
// `cairn_core::internal::...` through the Orchestrator method.
pub(crate) use calls::fail_orphaned_calls_on_startup;
pub use child_tasks::create_child_task;
pub(crate) use inputs::{
    find_downstream_artifact_schema_conn, find_downstream_artifact_schema_with_snapshot_conn,
    is_long_running_node, is_long_running_node_conn, node_ships_a_pr_conn,
    resolve_ctx_self_schemas_conn, resolve_ctx_self_schemas_with_snapshot,
    resolve_instruction_prompt_conn,
};
pub(crate) use lifecycle::continue_automatic_retry;
// The branch-mode coordinate decision. Production reaches it only through
// `prepare_job`; the delegation edge's regression imports it to run the mode it
// emits all the way to the commit a delegated task starts from.
pub use lifecycle::{
    continue_job_impl, continue_job_or_enqueue, on_job_complete_impl, prepare_job,
    resume_job_from_digest, ResumeContext,
};
#[cfg(any(test, feature = "test-utils"))]
pub use lifecycle::{in_flight_launch_for_test, reconcile_stale_active_turn_for_continue_for_test};
#[cfg(test)]
pub(crate) use lifecycle::{select_job_coordinate, CoordinateRequest, ParentCoordinate};
pub(crate) use slash_commands::resolve_skill_slash_command;
pub use snapshots::store_tool_result_event_with_turn;
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
#[derive(Clone)]
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
    pub run_id: String,
    pub session_id: String,
    pub session_start: crate::backends::SessionStart,
    pub prompt: String,
    pub job_model: Option<Model>,
    pub agent_config: Option<AgentConfig>,
    pub artifact_schema_info: Option<OutputSchemaInfo>,
    pub execution_id: Option<String>,
    pub turn_id: String,
    /// Ownership of this job's launch, held from admission until the spawned
    /// process registers. The host layer must keep it alive across the spawn and
    /// drop it only once the process is registered or the start has failed;
    /// dropping it early re-opens the job to a concurrent resume that would
    /// launch a second process against this same session (CAIRN-3283).
    pub launch_claim: crate::orchestrator::JobLaunchClaim,
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
    .map_err(|_| "Database task panicked".to_string())?
}

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}
