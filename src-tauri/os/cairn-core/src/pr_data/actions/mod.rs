//! Host-agnostic PR lifecycle actions (merge / close / refresh), keyed by
//! `job_id`.
//!
//! The node-level `/pr` artifact URI resolves to a job; a `merge_requests` row
//! linked to that job *is* the PR. These functions are the single
//! implementation of merge/close/refresh, called both from the desktop Tauri
//! commands (via the action_run -> job_id indirection) and directly from the
//! `write` dispatcher when a driver patches the `/pr` artifact with a reserved
//! `action` key.

mod config_preflight;
mod conflict;
mod context;
mod create_pr;
mod landing_gate;
mod merge;
mod overrides;
mod pin_bump;
pub(crate) mod refresh;
mod resolution;
mod store_merge;
mod verdict_gate;

#[cfg(test)]
mod test_support;

pub(crate) use conflict::{
    conflict_recovery_hint, format_conflicted_commits, source_conflict_report,
};
pub use context::{
    query_mr_context_for_job, resolve_merge_mr_context_for_job, resolve_mr_context_for_job,
    try_resolve_mr_context_for_job, MergeMrContext, MrContext, PrNodeResolution,
};
pub(crate) use create_pr::{publish_pr_branch_for_job, sync_create_pr_artifact_for_job};
pub use landing_gate::LandingGateSkip;
pub use merge::{
    merge_pr_for_job, parse_merge_precondition, reconcile_after_merge, CheckoutReconciliation,
    MergePrecondition,
};
pub use overrides::{
    parse_operator_overrides, refuse_operator_overrides_on_write, OperatorOverrides,
};
pub use refresh::{
    close_pr_for_job, refresh_pr_for_job, refresh_pr_for_job_live, render_live_pr_section,
};
pub use resolution::{
    advance_producing_execution_after_pr_resolution, ensure_unjournaled_merge_observation,
    latest_resolution_attribution, resolve_pr_node, PrResolutionAttribution,
};
pub use verdict_gate::{VerdictWaiver, NO_VERDICT};
