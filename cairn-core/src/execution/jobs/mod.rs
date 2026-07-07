//! Job lifecycle functions — start, continue, complete, create child task.
//!
//! All business logic lives here. Host layers (Tauri, cairn-server) provide thin
//! wrappers that handle framework-specific concerns (async spawning, process start).
//!
//! ## Key functions
//!
//! - [`prepare_job`] — DB work + worktree setup, returns [`PreparedJob`] for session spawn.
//! - [`continue_job_impl`] — sends follow-up message to a running/warm job.
//! - [`on_job_complete_impl`] — DAG advancement after a job finishes.
//! - [`create_child_task`] — user-initiated sub-agent under a running job.

use crate::agent_process::stream::TranscriptEvent;
use crate::config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_runtime_selection,
    LaunchSelectionOverride,
};
use crate::config::project_settings::load_project_settings;
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
use crate::transcripts::stream_store::{
    get_next_sequence, insert_event, insert_event_stamping_pushes, EventInsert,
};
use cairn_common::ids;
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

mod calls;
mod child_tasks;
mod config_loading;
mod inputs;
mod lifecycle;
mod persistence;
pub(crate) mod setup_progress;
mod slash_commands;
mod snapshots;
mod status;
mod turns;
mod workflow;
mod worktrees;

pub(crate) use calls::{prepare_call_run, start_call_run};
// The per-call Restart action reaches this via
// `cairn_core::internal::execution::jobs::restart_call`.
pub use calls::restart_call;
pub use child_tasks::create_child_task;
pub(crate) use inputs::{
    find_downstream_artifact_schema_conn, find_downstream_artifact_schema_with_snapshot_conn,
    is_long_running_node, resolve_ctx_self_schemas_conn, resolve_ctx_self_schemas_with_snapshot,
    resolve_instruction_prompt_conn,
};
#[cfg(any(test, feature = "test-utils"))]
pub use lifecycle::reconcile_stale_active_turn_for_continue_for_test;
pub use lifecycle::{continue_job_impl, on_job_complete_impl, prepare_job, ResumeContext};
pub use slash_commands::resolve_skill_slash_command;
pub use snapshots::store_tool_result_event_with_turn;
pub(crate) use workflow::{
    delete_workflow_run_row, prepare_workflow_run, redispatch_crashed_workflows,
    start_workflow_run, CreateWorkflowRunInput,
};
// The header Restart action reaches this from the host crates via
// `cairn_core::internal::execution::jobs::restart_workflow`.
pub use workflow::restart_workflow;
// The canonical, routing-aware turn-start. Host job-start paths call this
// instead of hand-rolling the turns UPDATE against the private DB (CAIRN-2206).
pub use turns::start_turn;
pub(crate) use worktrees::prepare_worktree_for_job;

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
    pub parent_job_id: String,
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    #[serde(alias = "model")]
    pub tier: Option<String>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
}

/// Result of creating a child task.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChildTaskResult {
    pub job_id: String,
    pub run_id: String,
}

/// Worktree binding for an ephemeral call (CAIRN-2481).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CallWorktree {
    /// Run in the caller's inherited worktree; mutating calls are first-class.
    #[default]
    Inherit,
    /// Run in a fresh scratch dir with no project-tree binding (a pure
    /// prompt->JSON worker, e.g. web-research calls).
    None,
}

/// Input for creating an ephemeral agent-call run.
pub struct CreateCallRunInput {
    pub parent_job_id: String,
    /// The parent's task-execution id (the snapshot that carries the call
    /// packet); falls back to the parent job's execution_id when absent.
    pub execution_id: Option<String>,
    pub subagent_type: String,
    pub description: String,
    pub prompt: String,
    pub tier: Option<String>,
    pub backend_preference: Option<String>,
    pub output_contract: crate::models::DelegatedOutputContract,
    pub worktree: CallWorktree,
    pub label: Option<String>,
    pub phase: Option<String>,
    pub parent_tool_use_id: Option<String>,
    pub task_index: Option<i32>,
    /// When set (a workflow-parented call that missed the journal), the call's
    /// run is linked to this journal key so its result is journaled on
    /// completion (CAIRN-2498). `None` for an ordinary call.
    pub workflow_journal_link: Option<crate::workflow_journal::CallLink>,
}

/// A prepared ephemeral call run: all DB rows and the transcript seed exist, but
/// the backend session has not started yet (the caller persists the call packet
/// first, then calls [`start_call_run`]).
pub struct PreparedCallRun {
    pub job_id: String,
    pub run_id: String,
    pub session_id: String,
    pub agent_config: AgentConfig,
    pub selected_model: Option<Model>,
    pub working_dir: String,
    pub prompt: String,
    pub output_schema: OutputSchemaInfo,
    pub execution_id: Option<String>,
    pub worktree_path: Option<String>,
}

/// Everything needed by the host layer to spawn a Claude process for a job.
///
/// Returned by [`prepare_job`] after all DB work, worktree setup, run creation,
/// and initial user-event storage are complete.
pub const SETUP_CANCELLED_ERROR: &str = "__cairn_setup_cancelled__";

pub struct PreparedJob {
    pub run_id: String,
    pub session_id: String,
    pub session_start: crate::backends::SessionStart,
    pub prompt: String,
    pub worktree_path: String,
    pub job_model: Option<Model>,
    pub agent_config: Option<AgentConfig>,
    pub artifact_schema_info: Option<OutputSchemaInfo>,
    pub execution_id: Option<String>,
    pub turn_id: String,
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
