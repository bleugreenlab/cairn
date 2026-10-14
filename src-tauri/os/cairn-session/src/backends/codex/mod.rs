//! Codex app-server protocol and process client.
pub mod protocol;

pub mod events;
pub mod runtime;
pub const CODEX_BACKEND_NAME: &str = "Codex";

pub fn json_string(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(serde_json::Value::as_str).map(str::to_owned)
}

pub mod pool;
