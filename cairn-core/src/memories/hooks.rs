//! Hook settings file generation for the channel-message pull and slash commands.
//!
//! Writes a standalone settings JSON file that gets passed to Claude CLI
//! via `--settings`, keeping hooks scoped to Cairn runs only.

use std::path::PathBuf;

/// Write a settings JSON file with memory hook configuration.
///
/// Returns the path to the written file. The file is written to
/// `~/.cairn/hook-settings.json` and reused across runs.
///
/// `mcp_callback_port` is the port the MCP callback server listens on.
pub fn write_hook_settings_file(mcp_callback_port: u16) -> Result<PathBuf, String> {
    let home = dirs::home_dir().ok_or("Could not find home directory")?;
    let cairn_dir = home.join(".cairn");
    let settings_path = cairn_dir.join("hook-settings.json");

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

    std::fs::create_dir_all(&cairn_dir)
        .map_err(|e| format!("Failed to create .cairn directory: {}", e))?;

    std::fs::write(
        &settings_path,
        serde_json::to_string_pretty(&settings)
            .map_err(|e| format!("Failed to serialize hook settings: {}", e))?,
    )
    .map_err(|e| format!("Failed to write hook settings: {}", e))?;

    Ok(settings_path)
}
