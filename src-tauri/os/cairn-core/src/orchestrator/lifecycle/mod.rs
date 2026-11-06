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
//! - [`unanswered_calls`] — answering the tool calls an interrupted turn left open
//! - [`review_push`] — review-push creation and turn-end emitters
//! - [`finalize`] — warm transition, run finalization, memory-review completion

mod backend_turn;
mod child_attention;
mod child_stall;
mod child_unresumed;
mod common;
mod finalize;
mod process_lifetime;
#[cfg(test)]
mod process_lifetime_tests;
mod review_push;
mod stop;
mod unanswered_calls;
mod unowned_running;
#[cfg(test)]
mod unowned_running_tests;
mod watchdog_recovery;

pub(crate) use backend_turn::{
    apply_claude_artifact_handoff, apply_claude_turn_completion, apply_codex_turn_completion,
    apply_owned_turn_outcome, was_host_interrupted, ClaudeTurnContext, CodexTurnContext,
};

pub use crate::workflow_host::rearm_failed_resolutions_on_startup;
pub(crate) use child_stall::{render_stall_push_body, sweep_stalled_children};
pub(crate) use child_unresumed::{render_unresumed_push_body, sweep_unresumed_children};
pub(crate) use common::set_exit_reason;
pub(crate) use finalize::{
    current_session_closed, finalize_run_for_operator_compact, finalize_run_for_recovery,
    interrupt_run_for_provider_depletion, memory_review_turn_ended, record_run_failure_if_active,
    release_and_stop_process, settle_provider_turn_failure, stop_processes_for_sessions,
};
pub use finalize::{fail_run, finalize_run, transition_to_warm_state, warm_provider_held_run};
pub(crate) use process_lifetime::{
    launch_still_wanted, retire_idle_provider, sweep_process_lifetimes,
};
pub(crate) use review_push::{
    arm_turn_end_successor, arm_turn_end_successor_now, spawn_review_wave_unless_in_flight,
};
pub use review_push::{
    create_review_push_for_pr_open, evaluate_review_readiness, rearm_one_bounded_failed_review,
    rearm_review_checks_on_startup,
};
pub(crate) use stop::{
    find_descendant_job_ids, settle_live_turn, TurnSettlement, OPERATOR_COMPACT_EXIT_REASON,
    PROVIDER_SILENCE_RECOVERY_EXIT_REASON, WATCHDOG_ARM_FAILED_EXIT_REASON,
};
pub use stop::{
    kill_session, kill_session_with_reason, live_run_id_for_job, operator_stop_job,
    operator_stop_run, stop_active_turn_for_run, stop_agents_for_host_shutdown, stop_call,
    stop_job, stop_session, stop_workflow, suspend_run_for_durable_wait,
    suspend_run_for_durable_wait_after_handoff, suspend_run_for_durable_wait_after_handoff_then,
    HostShutdownStops, ParkSlots, RUNNER_SHUTDOWN_EXIT_REASON, SUSPEND_HANDOFF_GRACE,
};
#[cfg(test)]
pub(crate) use unanswered_calls::{
    pending_tool_result_ids, restart_answer, CallEffect, LedgerRow, USER_STOP_TOOL_RESULT,
};
pub(crate) use unanswered_calls::{
    restart_answered_calls_note, settle_unanswered_calls, InterruptCause,
};
pub(crate) use unowned_running::{render_unowned_push_body, sweep_unowned_running};
pub(crate) use watchdog_recovery::notify_watchdog_successor_started;
pub(crate) use watchdog_recovery::sweep_stranded_watchdog_jobs;
pub(crate) use watchdog_recovery::{
    recover_provider_watchdog, ProviderWatchdogRecovery, ALREADY_TERMINAL_RECONCILED_REASON,
};

#[cfg(test)]
pub(crate) use finalize::finish_memory_review_if_due;
#[cfg(test)]
pub(crate) use finalize::{handle_session_crash, run_digest_reseed_fallback};
#[cfg(test)]
pub(crate) use review_push::{record_bounded_rearm_lookup_failure, review_artifact_ref};
#[cfg(test)]
pub(crate) use stop::{stop_session_internal, StopReason};

/// CAIRN-4609: the parent-facing stalled-child fact — which shapes produce it,
/// which quiet-but-healthy shapes must not, and that repeated deaths update one
/// notice rather than accumulating wakes.
#[cfg(test)]
mod child_stall_tests;
/// cairn/4835: the parent-facing fact for a child whose one restart resume did
/// not take — which row shapes produce it, which must not, and that one episode
/// is one wake however many ticks and boots observe it.
#[cfg(test)]
mod child_unresumed_tests;
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
mod thread_task_completion_tests;
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
