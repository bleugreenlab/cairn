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
mod version;

pub use auth::refresh_codex_oauth_tokens_for_current_account;

pub(super) const CODEX_BACKEND_NAME: &str = "Codex";

pub struct CodexBackend;

fn json_string(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(serde_json::Value::as_str).map(str::to_owned)
}
