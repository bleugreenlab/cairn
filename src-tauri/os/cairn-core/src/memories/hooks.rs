//! Hook settings file generation for the channel-message pull and slash commands.
//!
//! Writes a standalone settings JSON file that gets passed to Claude CLI
//! via `--settings`, keeping hooks scoped to Cairn runs only.

/// Write a settings JSON file with memory hook configuration.
///
/// Returns the path to the written file. The file is written to
/// `~/.cairn/hook-settings.json` and reused across runs.
///
/// `mcp_callback_port` is the port the MCP callback server listens on.
pub(crate) fn hook_settings_file(
    mcp_callback_port: u16,
) -> cairn_models::interface::session::PlacedFile {
    // The configured Cairn home, not a hardcoded `~/.cairn`: a dev instance keeps
    // its hook settings in its own home rather than writing into the production
    // one it is otherwise isolated from.
    let cairn_dir = cairn_common::paths::cairn_home();

    let hook_command = format!(
        "curl -sf -X POST http://127.0.0.1:{}/api/hooks -H 'Content-Type: application/json' -d @- 2>/dev/null || true",
        mcp_callback_port
    );

    let hook_entry = serde_json::json!([{
        "hooks": [{
            "type": "command",
            "command": hook_command
        }]
    }]);

    let settings = serde_json::json!({
        "hooks": {
            "PostToolUse": hook_entry,
            "PostToolUseFailure": hook_entry,
            "UserPromptSubmit": hook_entry,
        }
    });

    cairn_models::interface::session::PlacedFile {
        dir: cairn_dir.to_string_lossy().into_owned(),
        name: "hook-settings.json".to_string(),
        contents: serde_json::to_string_pretty(&settings).expect("a JSON value always serializes"),
    }
}
