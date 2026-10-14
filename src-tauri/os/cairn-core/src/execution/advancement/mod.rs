//! Application entry points for the durable engine in `cairn-workflow`.
//!
//! Host-dependent operations construct a `WorkflowRuntime`; database-only
//! operations are re-exported directly from the engine.

use crate::models::{Job, JobStatus};
use crate::orchestrator::Orchestrator;

mod actions;
mod dependents;
mod recompute;
pub use actions::{
    advance_execution_with_actions, block_job, mark_job_failed, reevaluable_condition,
    reevaluate_condition, rerun_checkpoint_job,
};
pub(crate) use cairn_workflow::advancement::{
    cancel_job_conn, create_jobs_for_execution, format_resolved_inputs, issue_settled,
    run_advancement_db,
};
pub use cairn_workflow::advancement::{
    create_jobs_for_new_nodes, find_ready_action_nodes, find_ready_condition_nodes,
    is_action_node_ready, recompute_execution_jobs_conn, recompute_job_status_conn,
    reconcile_removed_nodes, restart_node, JobStatusChange, RecomputeOutcome,
    RemovedNodesReconcile, RestartNodeOutcome,
};
pub(crate) use dependents::release_dependent_executions;
pub use recompute::{
    heal_stuck_delegated_children, recompute_execution_jobs, recompute_job,
    reduce_delegated_child_job,
};
