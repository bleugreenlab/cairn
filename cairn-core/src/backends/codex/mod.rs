//! Codex CLI backend implementation.
//!
//! Communicates with `codex app-server` over stdio using JSON-RPC.
//! Cairn starts or resumes a Codex thread, starts turns against that thread,
//! and translates app-server notifications into Cairn transcript events.

pub mod app_server;
mod auth;
mod backend_impl;
mod config;
mod events;
mod models;
mod permissions;
mod protocol;
mod runtime;
mod thread_params;
pub mod usage;
mod version;

pub use auth::refresh_codex_oauth_tokens_for_current_account;
pub use usage::{collect_codex_usage_snapshot, consume_codex_usage_reset};

pub(super) const CODEX_BACKEND_NAME: &str = "Codex";

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct CodexModelProviderConfig {
    pub name: &'static str,
    pub base_url: &'static str,
    pub env_key: &'static str,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct CodexAppServerProfile {
    pub backend_name: &'static str,
    pub backend_key: &'static str,
    pub model_provider: Option<CodexModelProviderConfig>,
    pub api_key_env: Option<(&'static str, String)>,
    pub require_codex_auth: bool,
}

#[allow(dead_code)]
pub(crate) fn start_app_server_session(
    config: crate::backends::SessionConfig,
    orch: &crate::orchestrator::Orchestrator,
    profile: CodexAppServerProfile,
) -> Result<(), String> {
    backend_impl::start_app_server_session(config, orch, profile)
}

#[allow(dead_code)]
pub(crate) fn send_app_server_user_message(
    stdin: &mut dyn crate::agent_process::process::BackendStdin,
    content: &str,
) -> Result<(), String> {
    if let Some(app_stdin) = stdin.as_any_mut().downcast_mut::<protocol::CodexStdin>() {
        app_stdin.send_turn(content)
    } else {
        Err("Codex app-server stdin unavailable".to_string())
    }
}

#[allow(dead_code)]
pub(crate) fn interrupt_app_server(
    stdin: &mut dyn crate::agent_process::process::BackendStdin,
) -> Result<(), String> {
    if let Some(app_stdin) = stdin.as_any_mut().downcast_mut::<protocol::CodexStdin>() {
        app_stdin.interrupt()
    } else {
        Err("Codex app-server stdin unavailable".to_string())
    }
}

pub struct CodexBackend;

fn json_string(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(serde_json::Value::as_str).map(str::to_owned)
}
