//! Recipe DAG readiness, job claims, state projection, and restart.
//!
//! `advance_execution_with_actions` reduces the DAG and drives the resulting
//! effects. The host receives ready agent jobs and external workloads.

use crate::db_records::{db_job_from_row, DbJob, DbRecipeEdge, DbRecipeNode, JOB_COLUMNS};
use crate::models::{DelegatedSessionMode, ExecutionSnapshot, Job, JobStatus};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;

use crate::runtime::{WorkflowOwner, WorkflowRuntime};

mod actions;
mod core;
mod inputs;
mod job_creation;
mod persistence;
mod readiness;
pub mod recompute;
mod restart;
mod snapshot_edit;
mod snapshots;
mod status;

pub use actions::{
    advance_execution_with_actions, block_job, create_action_run, mark_action_run_failed,
    mark_job_failed, rearm_blocked_checkpoints, reevaluable_condition, reevaluate_condition,
    rerun_checkpoint_job, wake_upstream_after_checkpoint_failure,
};
pub use core::advance_execution_impl;
pub use inputs::{format_resolved_inputs, ResolvedInput};
pub use job_creation::{
    create_jobs_for_execution, create_jobs_for_execution_conn, create_jobs_for_new_nodes,
    create_jobs_for_new_nodes_conn,
};
pub use persistence::{load_job, load_project_repo_path, run_advancement_db};
pub use readiness::{
    find_ready_action_nodes, find_ready_condition_nodes, is_action_node_ready, issue_settled,
};
pub use recompute::{
    heal_stuck_delegated_children, issue_is_terminal_conn, recompute_execution_jobs,
    recompute_execution_jobs_conn, recompute_job, recompute_job_status_conn,
    rederive_after_reopen_conn, reduce_delegated_child_job, ChangeCause, JobStatusChange,
    RecomputeOutcome,
};
pub use restart::{restart_node, RestartNodeOutcome};
pub use snapshot_edit::{
    cancel_job_conn, reconcile_removed_nodes, reconcile_removed_nodes_conn, RemovedNodesReconcile,
};
pub use snapshots::{
    load_execution_snapshot, load_nodes_from_execution, update_execution_snapshot,
};

use persistence::*;
use readiness::*;
use recompute::force_fail_job_turn_conn;
use snapshots::*;
use status::*;
