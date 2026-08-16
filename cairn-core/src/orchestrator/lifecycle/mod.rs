//! Session lifecycle management
//!
//! This module handles cleanup, finalization, and stopping of backend sessions,
//! including cascading stops to child runs.
//!
//! ## Warm Process Retention
//!
//! When a turn completes successfully, processes can be transitioned to "warm" state
//! instead of being terminated. Warm processes:
//! - Keep their stdin open for follow-up messages
//! - Retain their MCP authentication token
//! - Preserve Claude's conversation cache
//! - Can be reused for continuation without spawning a new process
//!
//! The implementation is split into cohesive submodules behind this facade:
//! - [`common`] — turn/run state-transition primitives and id-keyed lookups
//! - [`stop`] — stop/kill/suspend paths
//! - [`review_push`] — review-push creation and turn-end emitters
//! - [`finalize`] — warm transition, run finalization, memory-review completion

mod common;
mod finalize;
mod review_push;
mod stop;
mod watchdog_recovery;

pub(crate) use common::set_exit_reason;
pub use finalize::{fail_run, finalize_run, transition_to_warm_state};
pub(crate) use finalize::{
    finalize_run_for_recovery, memory_review_turn_ended, report_recovery_launch_failure,
};
pub(crate) use review_push::detach_onto_runtime;
pub use review_push::{
    create_review_push_for_pr_open, evaluate_review_readiness, rearm_one_bounded_failed_review,
    rearm_review_checks_on_startup,
};
#[cfg(test)]
pub(crate) use stop::USER_STOP_TOOL_RESULT;
pub use stop::{
    kill_session, kill_session_with_reason, live_run_id_for_job, stop_active_turn_for_run,
    stop_agents_for_host_shutdown, stop_call, stop_job, stop_session, stop_workflow,
    suspend_run_for_durable_wait, suspend_run_for_durable_wait_after_handoff,
    suspend_run_for_durable_wait_after_handoff_then, HostShutdownStops, ParkSlots,
    RUNNER_SHUTDOWN_EXIT_REASON, SUSPEND_HANDOFF_GRACE,
};
pub(crate) use stop::{PROVIDER_SILENCE_RECOVERY_EXIT_REASON, WATCHDOG_ARM_FAILED_EXIT_REASON};
pub(crate) use watchdog_recovery::{
    recover_provider_watchdog, ProviderWatchdogRecovery, ALREADY_TERMINAL_RECONCILED_REASON,
};

#[cfg(test)]
pub(crate) use finalize::finish_memory_review_if_due;
#[cfg(test)]
pub(crate) use finalize::handle_session_crash;
#[cfg(test)]
pub(crate) use review_push::{record_bounded_rearm_lookup_failure, review_artifact_ref};
#[cfg(test)]
pub(crate) use stop::{stop_session_internal, InterruptFailurePolicy};

#[cfg(test)]
mod memory_review_tests;
#[cfg(test)]
mod watchdog_recovery_tests;

/// CAIRN-3104: the decide-and-record half of the crashed-resume digest-reseed
/// fallback, exercised against a migrated database. These cover what the pure
/// predicate cannot — that the widened `runs` query reads the right columns,
/// that the notice lands in the transcript, and that the per-session claim holds
/// across a duplicate finalize.
#[cfg(test)]
mod reseed_fallback_tests;

/// CAIRN-1582: a run that completes via the terminal-tool warm transition
/// (instead of `finalize_run`) must still carry the full completion contract —
/// wake a suspended delegated parent and signal the internal completion
/// broadcast. CAIRN-1576 routed terminal-tool completion through
/// `transition_to_warm_state`, and that path had dropped both, leaving batch
/// parents hung forever.
#[cfg(test)]
mod warm_completion_tests;

/// CAIRN-1882: the work-turn idle edge is the single creator of a review push.
/// These cover the canonical scenarios from `docs/attention-redesign.md`: a
/// reviewable work-turn idle pushes exactly one `review:{issue}` to each watcher
/// (never to the producing node), a memory-review turn end does not, an idle with
/// no reviewable output does not, and successive idles supersede to one
/// undelivered row.
#[cfg(test)]
mod review_push_tests;

#[cfg(test)]
mod detach_tests;
