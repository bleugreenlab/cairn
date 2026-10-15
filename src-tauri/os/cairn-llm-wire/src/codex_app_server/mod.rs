//! `codex app-server`: the JSON-RPC protocol Codex speaks over stdio.
//!
//! Everything here is a fact about what the server sends or accepts: the typed
//! reading of each method-keyed notification, the rule for when a provider turn
//! has ended, token usage normalized from Codex's counters, readers for the
//! items a turn produces, the goal payloads, and the `UserInput` a turn starts
//! with. Spawning the server and correlating requests is transport
//! (`cairn_models::codex::app_server`); persisting what a turn did belongs to the
//! session runtime.

pub mod goal;
mod input;
mod items;
mod notification;
mod usage;

#[cfg(test)]
mod tests;

pub use input::build_codex_input;
pub use items::{
    extract_app_server_delta, extract_command_execution, extract_mcp_result,
    extract_raw_response_message_text, summarize_command_result, summarize_file_change_result,
};
pub use notification::{
    is_selected_model_capacity_error, normalize_capacity_error, notification_turn_id, Notification,
    ProviderTurnStatus,
};
pub use usage::{
    add_usage, context_usage, u64_pointer_as_i64, u64_pointer_as_u32, CodexContextUsage,
    CodexTurnUsageAccumulator, CodexUsageBreakdown,
};
