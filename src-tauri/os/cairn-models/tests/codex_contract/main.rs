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
mod pool_probes;
mod proc_tree;
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

macro_rules! probe {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            pass(pool_probes::$case(&env()));
        }
    )*};
}

probe!(
    pool_resume_after_unsubscribe_while_loaded,
    pool_interrupt_during_hung_mcp_call,
    pool_tool_call_thread_id_comes_from_codex,
    pool_thread_brings_its_own_mcp_server,
    pool_goal_on_a_shared_server,
    pool_active_goal_paused_before_resume_after_server_loss,
    pool_load,
);

#[cfg(unix)]
probe!(pool_shim_lifetime);

#[cfg(unix)]
probe!(pool_agent_cannot_spawn_under_the_server);

#[test]
#[ignore = "long: waits out Codex's thread unload, polling for up to 40 minutes; run alone with --run-ignored only"]
fn pool_resume_after_unload() {
    pass(pool_probes::pool_resume_after_unload(&env()));
}

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

#[test]
fn project_config_dot_codex() {
    pass(cases::project_config_isolation(&env(), "dot-codex"));
}
#[test]
fn project_config_bare() {
    pass(cases::project_config_isolation(&env(), "bare"));
}
#[test]
fn project_config_without_git() {
    pass(cases::project_config_isolation(&env(), "no-git"));
}
#[test]
fn project_config_parent() {
    pass(cases::project_config_isolation(&env(), "parent"));
}

#[test]
fn project_config_shared_cwds() {
    pass(cases::project_config_shared_cwds(&env()));
}

#[test]
fn project_config_parent_bare() {
    pass(cases::project_config_isolation(&env(), "parent-bare"));
}

// Removing only the runtime distrust reproduces the vulnerable production path.
#[test]
fn negative_project_config_dot_codex() {
    red(
        "project_config_isolation",
        cases::project_config_isolation(&env().broken(Surface::ProjectTrustRemoved), "dot-codex"),
    );
}
#[test]
fn negative_project_config_without_git() {
    red(
        "project_config_isolation",
        cases::project_config_isolation(&env().broken(Surface::ProjectTrustRemoved), "no-git"),
    );
}
#[test]
fn negative_project_config_parent() {
    red(
        "project_config_isolation",
        cases::project_config_isolation(&env().broken(Surface::ProjectTrustRemoved), "parent"),
    );
}
#[test]
fn project_config_bare_without_hardening() {
    pass(cases::project_config_isolation(
        &env().broken(Surface::ProjectTrustRemoved),
        "bare",
    ));
}
#[test]
fn project_config_parent_bare_without_hardening() {
    pass(cases::project_config_isolation(
        &env().broken(Surface::ProjectTrustRemoved),
        "parent-bare",
    ));
}

#[cfg(unix)]
#[test]
fn project_config_symlink() {
    pass(cases::project_config_isolation(&env(), "symlink"));
}
#[test]
fn negative_project_config_shared_thread_trust() {
    red(
        "project_config_shared_cwds",
        cases::project_config_shared_cwds(&env().broken(Surface::ThreadProjectTrustRemoved)),
    );
}
