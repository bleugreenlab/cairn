//! The `codex-contract` suite: Cairn's own Codex client code against the REAL
//! pinned `codex app-server`, with a hermetic fake Responses provider.
//!
//! Built only with `--features codex-contract`, and run by
//! `bun run codex:contract`, which stages the pinned package and names it in
//! `CAIRN_CODEX_CONTRACT_ROOT`. Without that variable every test FAILS rather
//! than skipping, so the lane can never read green without evidence.
//!
//! One `#[test]` per surface, so the lane's per-test result has one row per
//! case. Every `negative_*` test runs a case against the real binary behind
//! `break_proxy.ts`, rewritten to break exactly that case's surface, and
//! requires the case to fail naming itself: a gate that cannot go red fails
//! its own lane. See docs/backends.md, "Managed Codex CLI".

mod cases;
mod env;
mod fake_responses;
mod surface;

use env::{CaseResult, ContractEnv, Surface};

fn env() -> ContractEnv {
    ContractEnv::new().unwrap_or_else(|error| panic!("{error}"))
}

fn pass(result: CaseResult) {
    if let Err(failure) = result {
        panic!("{failure}");
    }
}

/// `case` must fail, and say it was `case` that failed.
fn red(case: &str, result: CaseResult) {
    match result {
        Ok(()) => panic!(
            "gate negative: `{case}` passed against a Codex broken for it, so the gate could not catch that break"
        ),
        Err(failure) => {
            assert!(
                !failure.setup,
                "gate negative: `{case}` never reached its assertions, so this proves nothing: {failure}"
            );
            assert_eq!(
                failure.case, case,
                "gate negative: the failure came from another case: {failure}"
            );
        }
    }
}

macro_rules! contract {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            pass(cases::$case(&env()));
        }
    )*};
}

contract!(
    managed_install_of_pinned_package,
    handshake_and_thread_start,
    normal_turn_streams_and_completes,
    turn_interrupt_exact,
    interrupt_after_completion_is_success,
    interrupt_mismatch_retries_found_id,
    goal_set_pause_reactivate_clear,
    objectiveless_goal_set_on_goalless_thread,
    resume_unknown_thread_falls_back,
    error_notification_fatal,
    error_notification_capacity,
    mcp_tool_call_items,
    no_egress_at_startup,
    no_ignored_config_warnings,
    protocol_surface,
    protocol_surface_exemptions_are_narrow,
);

#[test]
fn error_notification_will_retry() {
    pass(cases::error_notification_will_retry(&env().retries(1)));
}

#[test]
fn refresh_request_does_not_hold_thread_start() {
    pass(cases::refresh_request_does_not_hold_thread_start(
        &env().chatgpt_provider(),
    ));
}

#[test]
fn refresh_request_is_sent_with_expired_token() {
    pass(cases::refresh_request_is_sent_with_expired_token(
        &env().chatgpt_provider(),
    ));
}

macro_rules! negative {
    ($($test:ident: $case:ident under $surface:ident $(with $adjust:ident $(($arg:expr))?)?;)*) => {$(
        #[test]
        fn $test() {
            let env = env()$(.$adjust($($arg)?))?.broken(Surface::$surface);
            red(stringify!($case), cases::$case(&env));
        }
    )*};
}

negative! {
    negative_no_active_turn_text: interrupt_after_completion_is_success under NoActiveTurnText;
    negative_interrupt_mismatch_text: interrupt_mismatch_retries_found_id under InterruptMismatchText;
    negative_missing_rollout_text: resume_unknown_thread_falls_back under MissingRolloutText;
    negative_goal_status_renamed: goal_set_pause_reactivate_clear under GoalStatusRenamed;
    negative_will_retry_dropped: error_notification_will_retry under WillRetryDropped with retries(1);
    negative_capacity_sentence: error_notification_capacity under CapacitySentence;
    negative_turn_status_renamed: error_notification_fatal under TurnStatusRenamed;
    negative_mcp_item_renamed: mcp_tool_call_items under McpItemRenamed;
    negative_unhandled_server_request: mcp_tool_call_items under UnhandledServerRequest;
    negative_thread_start_param_rejected: handshake_and_thread_start under ThreadStartParamRejected;
    negative_egress: no_egress_at_startup under Egress;
    negative_config_warning: no_ignored_config_warnings under ConfigWarning;
    negative_thread_start_awaits_server_request: refresh_request_does_not_hold_thread_start under ThreadStartAwaitsServerRequest with chatgpt_provider;
    negative_refresh_never_sent: refresh_request_is_sent_with_expired_token under RefreshNeverSent with chatgpt_provider;
}

#[test]
fn negative_protocol_surface() {
    red(
        "protocol_surface",
        cases::protocol_surface_with_a_phantom_read(&env()),
    );
}
