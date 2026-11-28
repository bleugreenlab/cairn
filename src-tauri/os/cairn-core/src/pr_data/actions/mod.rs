//! Host-agnostic PR lifecycle actions (merge / close / refresh), keyed by
//! `job_id`.
//!
//! The node-level `/pr` artifact URI resolves to a job; a `merge_requests` row
//! linked to that job *is* the PR. These functions are the single
//! implementation of merge/close/refresh, called both from the desktop Tauri
//! commands (via the action_run -> job_id indirection) and directly from the
//! `write` dispatcher when a driver patches the `/pr` artifact with a reserved
//! `action` key.

mod child_landing;
mod config_preflight;
mod conflict;
mod context;
mod create_pr;
mod default_landing;
mod human_approval;
mod landing_record;
mod merge;
pub(crate) mod merge_metrics;
mod pin_bump;
pub(crate) mod refresh;
mod resolution;
mod store_merge;

#[cfg(test)]
mod seal_tests;
#[cfg(test)]
mod test_support;

pub(crate) use conflict::{
    conflict_recovery_hint, format_conflicted_commits, immutable_store_read,
    source_conflict_report, unknown_conflict_note, SourceConflicts, StoreView,
};
pub use context::{
    query_mr_context_for_job, resolve_merge_mr_context_for_job, resolve_mr_context_for_job,
    try_resolve_mr_context_for_job, MergeMrContext, MrContext, PrNodeResolution,
};
#[cfg(feature = "test-utils")]
pub use create_pr::prose_edit_seam;
pub(crate) use create_pr::{
    complete_deferred_pr_sync, ensure_pr_artifact_writable, publish_pr_branch_for_job,
    sync_create_pr_artifact_for_job, DeferredPrSync, DeferredPrSyncOutcome, GatedAct,
    DEFERRED_PR_SYNC_PENDING,
};
pub use default_landing::reconcile_approved_merges;
pub(crate) use default_landing::{
    discard_request_branch, land_request_commit, retire_request_branch, RequestLandingError,
};
pub(crate) use landing_record::request_publication_pending;
pub use landing_record::{
    reconcile_pending_landings, settle_github_after_landing, GitHubSettlement, LandingRecovery,
};
pub(crate) use merge::merge_response_run;
pub use merge::{
    merge_pr_for_job, merge_pr_for_job_preserving_run, parse_merge_precondition,
    reconcile_after_merge, CheckoutReconciliation, MergePrecondition,
};
pub use refresh::{
    close_pr_for_job, refresh_pr_for_job, refresh_pr_for_job_live, render_live_pr_section,
};
#[cfg(test)]
pub(crate) use resolution::resolve_pr_node_landed;
pub use resolution::{
    advance_producing_execution_after_pr_resolution, latest_resolution_attribution,
    resolution_actor_label, resolve_pr_node, Landed, PrResolutionAttribution,
};
