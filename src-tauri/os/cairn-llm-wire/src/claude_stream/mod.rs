//! Claude CLI stream-json: the line protocol `claude --output-format stream-json
//! --input-format stream-json` speaks on stdout and stdin.
//!
//! Everything here is a fact about what the CLI emits or accepts: the typed
//! event per stdout line, the readers that pull text, tool calls, and usage out
//! of those events, the rules for when a turn has ended and what an early EOF
//! proves, and the stdin messages that drive a running process. What Cairn
//! does with those facts (the persisted transcript row, watchdog leases, host
//! callbacks) belongs to the session runtime that consumes this module.

mod content;
mod event;
mod evidence;
mod input;
mod tool_input;

#[cfg(test)]
mod tests;

pub use content::{
    extract_assistant_content, extract_user_content, AssistantContent, UserContent, UserContentKind,
};
pub use event::{
    extract_tool_start, message_start_usage, parse_event, parse_thinking_tokens_estimate,
    ClaudeEvent, ClaudeMessage, ContentBlock, ControlResponseInner, DeltaContent,
    MessageContentInner, OutputTokensDetails, RateLimitInfo, StreamEventInner, Usage,
};
pub use evidence::{
    result_is_rate_limited, stderr_reports_session_id_in_use, stderr_reports_unknown_session,
    terminal_provider_depletion, terms_acceptance_refusal, ProviderDepletion, StreamEvidence,
};
pub use input::{
    build_message_content, send_control_response, send_interrupt_request, send_set_model_request,
    send_set_permission_mode_request, send_update_environment_variables, send_user_message,
};
pub use tool_input::{InputDefect, RejectedToolInput};
