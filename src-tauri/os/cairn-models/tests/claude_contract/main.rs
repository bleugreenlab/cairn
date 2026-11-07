//! The `claude-contract` suite: Cairn's own Claude client code against the
//! REAL pinned Claude Code binary, with a hermetic fake Messages API.
//!
//! Built only with `--features claude-contract`, and run by
//! `bun run claude:contract`, which stages the pinned binary and names its
//! root in `CAIRN_CLAUDE_CONTRACT_ROOT`. Without that variable every test
//! FAILS rather than skipping, so the lane can never read green without
//! evidence.
//!
//! One `#[test]` per surface, so the lane's per-test result has one row per
//! case. Every `negative_*` test runs a case against the real binary behind
//! `break_relay.ts`, rewritten to break exactly that case's surface (or, for
//! the updater, with the update switches stripped from the launch), and
//! requires the case to fail naming itself: a gate that cannot go red fails
//! its own lane. See docs/backends.md, "Managed CLIs".

#![cfg(unix)]

mod cases;
mod env;
mod fake_messages;

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
            "gate negative: `{case}` passed against a Claude Code broken for it, so the gate could not catch that break"
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
            eprintln!("gate negative held: {failure}");
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
    managed_install_of_pinned_binary,
    init_reports_pinned_version_session_and_model,
    init_declares_cairn_core_verbs,
    tools_empty_offers_only_mcp_tools,
    setting_sources_user_loads_no_project_or_operator_config,
    normal_turn_streams_and_completes,
    result_is_error_is_authoritative,
    api_error_429_is_rolling_window,
    api_retry_401_then_pushed_token_rescues_turn,
    rate_limit_event_shape_parses,
    rate_limit_rejected_is_blocking,
    interrupt_is_acknowledged_and_session_survives,
    set_permission_mode_and_set_model_acknowledged,
    mcp_tool_round_trip,
    structured_output_survives_tools_empty,
    resume_existing_session,
    fork_session_creates_new_id,
    resume_transcript_from_an_older_cli,
    resume_unknown_is_session_unresolvable,
    session_id_in_use_is_classified,
    credential_by_descriptor_and_operator_home_never_loads,
    no_unhandled_cli_control_requests,
    update_is_refused_under_launch_env,
    no_egress,
);

macro_rules! negative {
    ($($test:ident: $case:ident under $surface:ident;)*) => {$(
        #[test]
        fn $test() {
            red(stringify!($case), cases::$case(&env().broken(Surface::$surface)));
        }
    )*};
}

negative! {
    negative_claude_version_renamed: init_reports_pinned_version_session_and_model under ClaudeVersionRenamed;
    negative_core_verbs_dropped: init_declares_cairn_core_verbs under CoreVerbsDropped;
    negative_builtin_tool_offered: tools_empty_offers_only_mcp_tools under BuiltinToolOffered;
    negative_is_error_dropped: result_is_error_is_authoritative under IsErrorDropped;
    negative_usage_field_renamed: normal_turn_streams_and_completes under UsageFieldRenamed;
    negative_rate_limit_reset_renamed: rate_limit_event_shape_parses under RateLimitResetRenamed;
    negative_rate_limit_status_renamed: rate_limit_rejected_is_blocking under RateLimitStatusRenamed;
    negative_control_response_reshaped: interrupt_is_acknowledged_and_session_survives under ControlResponseReshaped;
    negative_tool_result_renamed: mcp_tool_round_trip under ToolResultRenamed;
    negative_resume_miss_text: resume_unknown_is_session_unresolvable under ResumeMissText;
    negative_session_in_use_text: session_id_in_use_is_classified under SessionInUseText;
    negative_unsolicited_control_request: no_unhandled_cli_control_requests under UnsolicitedControlRequest;
    negative_egress: no_egress under Egress;
}

/// Not a relay surface: the launch runs without `DISABLE_AUTOUPDATER` and
/// `DISABLE_UPDATES`. Measured on 2.1.311 (macOS and Linux): `claude update`
/// then tries `downloads.claude.ai` for its `latest` channel, where under the
/// hardening it answers "Updates are disabled" and connects nowhere.
#[test]
fn negative_updater_enabled() {
    red(
        "update_is_refused_under_launch_env",
        cases::update_is_refused_under_launch_env(&env().updater_enabled()),
    );
}

#[test]
fn planted_cwd_settings() {
    pass(cases::planted_cwd_config_isolation(
        &env(),
        ".claude/settings.json",
    ));
}
#[test]
fn planted_cwd_local_settings() {
    pass(cases::planted_cwd_config_isolation(
        &env(),
        ".claude/settings.local.json",
    ));
}
#[test]
fn planted_cwd_mcp() {
    pass(cases::planted_cwd_config_isolation(&env(), ".mcp.json"));
}
