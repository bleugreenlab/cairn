//! Host-agnostic PR lifecycle actions (merge / close / refresh), keyed by
//! `job_id`.
//!
//! The node-level `/pr` artifact URI resolves to a job; a `merge_requests` row
//! linked to that job *is* the PR. These functions are the single
//! implementation of merge/close/refresh, called both from the desktop Tauri
//! commands (via the action_run -> job_id indirection) and directly from the
//! `write` dispatcher when a driver patches the `/pr` artifact with a reserved
//! `action` key.

mod conflict;
mod context;
mod create_pr;
mod merge;
mod refresh;
mod resolution;
mod store_merge;

#[cfg(test)]
mod test_support;

pub(crate) use conflict::{
    conflict_recovery_hint, format_conflicted_commits, source_conflict_report,
};
pub use context::{
    query_mr_context_for_job, resolve_merge_mr_context_for_job, resolve_mr_context_for_job,
    try_resolve_mr_context_for_job, MergeMrContext, MrContext, PrNodeResolution,
};
pub(crate) use create_pr::sync_create_pr_artifact_for_job;
pub use merge::{merge_pr_for_job, reconcile_after_merge};
pub use refresh::{close_pr_for_job, refresh_pr_for_job, render_live_pr_section};
pub use resolution::{advance_producing_execution_after_pr_resolution, resolve_pr_node};
