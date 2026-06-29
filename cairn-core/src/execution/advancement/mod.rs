//! DAG advancement logic for recipe execution.
//!
//! Core execution engine that advances the recipe DAG, resolves inputs from artifacts,
//! and determines which jobs are ready to run.
//!
//! ## Architecture
//!
//! `advance_execution_with_actions` is the canonical async entry point. It calls
//! `reduce_dag` (synchronous DB advancement → typed effects) then processes effects
//! through `execute_effects` with the `LegacyExecutor`. All callers — event listeners,
//! direct async calls, scheduler — go through this function, ensuring `reduce_dag` is
//! the single source of truth for DAG advancement.

use crate::db_records::{db_job_from_row, DbJob, DbRecipeEdge, DbRecipeNode, JOB_COLUMNS};
use crate::models::{DelegatedSessionMode, ExecutionSnapshot, Job, JobStatus};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use turso::params;

mod actions;
mod core;
mod dependents;
mod inputs;
mod job_creation;
mod persistence;
mod readiness;
mod recompute;
mod restart;
mod snapshot_edit;
mod snapshots;
mod status;

pub use actions::{
    advance_execution_with_actions, block_job, create_action_run, mark_action_run_failed,
    mark_job_failed, rearm_blocked_checkpoints, rerun_checkpoint_job,
    wake_upstream_after_checkpoint_failure,
};
pub use core::advance_execution_impl;
pub use dependents::release_dependent_executions;
pub use inputs::{format_resolved_inputs, ResolvedInput};
pub use job_creation::create_jobs_for_new_nodes;
pub use readiness::{find_ready_action_nodes, find_ready_condition_nodes, is_action_node_ready};
pub use recompute::{
    recompute_execution_jobs, recompute_execution_jobs_conn, recompute_job,
    recompute_job_status_conn, JobStatusChange,
};
pub use restart::{restart_node, RestartNodeOutcome};
pub use snapshot_edit::{reconcile_removed_nodes, RemovedNodesReconcile};
pub use snapshots::load_nodes_from_execution;

pub(crate) use job_creation::{create_jobs_for_execution, create_jobs_for_new_nodes_conn};
pub(crate) use persistence::{load_job, load_project_repo_path, run_advancement_db};
pub(crate) use recompute::force_fail_job_turn_conn;
pub(crate) use snapshot_edit::reconcile_removed_nodes_conn;
pub(crate) use snapshots::{load_execution_snapshot, update_execution_snapshot};

use persistence::*;
use readiness::*;
use snapshots::*;
use status::*;
