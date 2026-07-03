use serde_json::Value;
use std::path::PathBuf;

/// Candidate locations for Codex's bundled model catalog inside the vendored
/// `openai/codex` checkout under `~/.cairn/resources`. The path moves between
/// Codex releases (e.g. it relocated from `core/` to `models-manager/` in
/// 0.133.0), so we probe known locations in newest-first order rather than
/// pinning a single path that silently breaks the apply_patch override on
/// upgrade.
const CAIRN_CODEX_MODELS_RESOURCES: &[&str] = &[
    // Codex >= 0.133.0
    ".cairn/resources/codex/codex-rs/models-manager/models.json",
    // Codex < 0.133.0
    ".cairn/resources/codex/codex-rs/core/models.json",
];

/// The Cairn-owned `CODEX_HOME`. Every Codex process Cairn spawns must point
/// here so Codex's config layering resolves `[mcp_servers]` from Cairn's
/// generated `config.toml` (cairn-only) rather than the user's native
/// `~/.codex/config.toml`. This redirection is the sole mechanism that isolates
/// spawned agents to a single MCP server: Codex deep-merges `[mcp_servers]`
/// across layers, so a `-c mcp_servers=...` CLI override cannot remove native
/// servers — only a clean CODEX_HOME does.
pub(super) fn cairn_codex_home() -> PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    home.join(".cairn").join("codex")
}

/// Write a stable Codex config with Cairn MCP server.
/// Uses a single shared CODEX_HOME (~/.cairn/codex/) so Codex's thread/state DB
/// persists across runs, enabling cross-run resume.
///
/// Run-specific values (CAIRN_RUN_ID, CAIRN_MCP_SECRET, CAIRN_HOME_URI) are
/// inherited from the process env via `env_vars` rather than baked into the
/// config, so the config is safe to share across parallel runs.
pub(super) fn write_codex_config(
    mcp_binary_path: &str,
    callback_port: u16,
    mcp_config_json: &str,
) -> Result<String, String> {
    write_codex_config_for_provider(mcp_binary_path, callback_port, mcp_config_json, None)
}

pub(crate) fn write_codex_config_for_provider(
    mcp_binary_path: &str,
    callback_port: u16,
    mcp_config_json: &str,
    provider: Option<&super::CodexModelProviderConfig>,
) -> Result<String, String> {
    let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
    let codex_home = cairn_codex_home();

    std::fs::create_dir_all(&codex_home)
        .map_err(|e| format!("Failed to create codex home: {}", e))?;

    let callback_url = format!("http://127.0.0.1:{}/api/mcp", callback_port);
    let model_catalog_path = write_codex_model_catalog_override(&home, &codex_home)?;

    // Extract the cairn-cmd args from the shared Claude MCP config JSON (same
    // args for cairn-cmd regardless of backend).
    let mcp_args: Vec<String> = serde_json::from_str::<Value>(mcp_config_json)
        .ok()
        .and_then(|config| {
            config
                .pointer("/mcpServers/cairn/args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
        })
        .unwrap_or_default();

    // Format args as TOML array
    let args_toml = if mcp_args.is_empty() {
        "[]".to_string()
    } else {
        let quoted: Vec<String> = mcp_args
            .iter()
            .map(|a| format!("\"{}\"", a.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect();
        format!("[{}]", quoted.join(", "))
    };
    let model_catalog_toml = model_catalog_path.as_ref().map_or(String::new(), |path| {
        format!(
            "model_catalog_json = \"{}\"\n",
            path.to_string_lossy()
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
        )
    });

    let provider_toml = render_model_provider_toml(provider);
    let config_toml = render_codex_config_toml(
        mcp_binary_path,
        &callback_url,
        &args_toml,
        &model_catalog_toml,
        &provider_toml,
    );

    let config_path = codex_home.join("config.toml");
    std::fs::write(&config_path, config_toml)
        .map_err(|e| format!("Failed to write codex config: {}", e))?;

    // Auth is written by the caller (start_session) from Cairn-managed identity

    log::info!("Wrote Codex config to {:?}", config_path);
    Ok(codex_home.to_string_lossy().to_string())
}

fn render_model_provider_toml(provider: Option<&super::CodexModelProviderConfig>) -> String {
    let Some(provider) = provider else {
        return String::new();
    };
    format!(
        "model_provider = \"{name}\"\n\n[model_providers.{name}]\nname = \"{name}\"\nbase_url = \"{base_url}\"\nenv_key = \"{env_key}\"\n\n",
        name = provider.name,
        base_url = provider.base_url.replace('\\', "\\\\").replace('"', "\\\""),
        env_key = provider.env_key,
    )
}

/// Render the Codex `config.toml` body. Spawned agents must expose exactly one
/// MCP server (cairn): the only `[mcp_servers.*]` block emitted here is cairn, and
/// CODEX_HOME is redirected to `~/.cairn/codex/` so the user's native
/// `~/.codex` `[mcp_servers.*]` are never loaded. External MCP reaches the agent
/// only through the cairn gateway, not as native Codex servers.
fn render_codex_config_toml(
    mcp_binary_path: &str,
    callback_url: &str,
    args_toml: &str,
    model_catalog_toml: &str,
    provider_toml: &str,
) -> String {
    format!(
        r#"# Auto-generated by Cairn
include_apply_patch_tool = false
{provider_toml}{model_catalog_toml}[features]
apply_patch_freeform = false
# Disable Codex's ChatGPT Apps/Connectors subsystem. It is on by default for
# ChatGPT-authed sessions and would expose external connector tools
# (mcp__codex_apps__github, gmail, linear, stripe, ...) that bypass the cairn
# gateway. This is separate from [mcp_servers] and is NOT governed by
# CODEX_HOME; the feature flag is the only lever that removes it.
apps = false
# Disable Codex's native shell/exec tools so the agent's only path to the shell
# is `mcp__cairn__run` (gateway-fenced, commit-aware). The native shell pulls
# agents out of the Cairn workflow: it runs apply_patch (which never commits)
# and ad-hoc shell commands that fight Codex's own sandbox fence instead of
# Cairn's and frequently wedge. `shell_tool = false` makes Codex emit no shell
# runtime at all (ConfigShellToolType::Disabled); `unified_exec = false` removes
# the PTY exec tool for defense in depth.
shell_tool = false
unified_exec = false

[mcp_servers.cairn]
command = "{mcp_binary}"
args = {args_toml}
env = {{ CAIRN_CALLBACK_URL = "{callback_url}" }}
env_vars = ["CAIRN_RUN_ID", "CAIRN_MCP_SECRET", "CAIRN_HOME_URI", "CAIRN_HOME"]
# Raise Codex's per-server MCP tool-call timeout far above any legal Cairn
# callback. The host owns execution: `run` items are capped per-item (<=600s,
# returning partial output on expiry) and blocking `write` appends to a node's
# tasks/questions collection await a sub-agent task or user answer with no
# host-side deadline. cairn-cmd sizes its HTTP callback ceiling just under this
# (see UNBOUNDED_CALLBACK_TIMEOUT in cairn-cmd); Codex's default per-call
# timeout would abandon the call above cairn-cmd and leave the agent with no
# output while the host keeps running. 7 days is an effective "never" while
# still bounding a truly wedged socket. This is a static, run-independent value,
# so it is safe to bake into the shared config (unlike CAIRN_RUN_ID, which is
# inherited per-process via env_vars).
tool_timeout_sec = 604800
"#,
        mcp_binary = mcp_binary_path.replace('\\', "\\\\").replace('"', "\\\""),
        callback_url = callback_url,
        args_toml = args_toml,
        model_catalog_toml = model_catalog_toml,
        provider_toml = provider_toml,
    )
}

pub(super) fn write_codex_model_catalog_override(
    home: &std::path::Path,
    codex_home: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, String> {
    let Some(source_path) = CAIRN_CODEX_MODELS_RESOURCES
        .iter()
        .map(|rel| home.join(rel))
        .find(|path| path.exists())
    else {
        log::warn!(
            "Codex models catalog not found in any known location ({:?}); native apply_patch may remain available",
            CAIRN_CODEX_MODELS_RESOURCES
        );
        return Ok(None);
    };

    let source = std::fs::read_to_string(&source_path).map_err(|e| {
        format!(
            "Failed to read Codex models catalog {:?}: {}",
            source_path, e
        )
    })?;
    let rewritten = disable_apply_patch_in_model_catalog(&source)?;
    let catalog_path = codex_home.join("model_catalog.json");
    std::fs::write(&catalog_path, rewritten)
        .map_err(|e| format!("Failed to write Codex model catalog override: {}", e))?;
    Ok(Some(catalog_path))
}

pub(super) fn disable_apply_patch_in_model_catalog(catalog_json: &str) -> Result<String, String> {
    let mut catalog: Value = serde_json::from_str(catalog_json)
        .map_err(|e| format!("Failed to parse Codex models catalog: {}", e))?;
    let models = catalog
        .get_mut("models")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "Codex models catalog missing models array".to_string())?;

    for model in models.iter_mut() {
        let Some(model_obj) = model.as_object_mut() else {
            return Err("Codex models catalog contains non-object model entry".to_string());
        };
        model_obj.insert("apply_patch_tool_type".to_string(), Value::Null);
    }

    serde_json::to_string(&catalog)
        .map_err(|e| format!("Failed to serialize Codex model catalog override: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codex_config_has_exactly_one_mcp_server() {
        // A spawned Codex agent must expose only the cairn MCP server. Assert the
        // generated config defines exactly one [mcp_servers.*] block and that it
        // is cairn.
        let config = render_codex_config_toml(
            "/path/to/cairn-cmd",
            "http://127.0.0.1:3847/api/mcp",
            "[\"mcp\"]",
            "",
            "",
        );
        let server_blocks: Vec<&str> = config
            .lines()
            .filter(|line| line.trim_start().starts_with("[mcp_servers."))
            .collect();
        assert_eq!(
            server_blocks,
            vec!["[mcp_servers.cairn]"],
            "expected exactly one mcp_servers block (cairn), got {:?}",
            server_blocks
        );
    }

    #[test]
    fn test_codex_config_disables_apps_connectors() {
        // Codex's ChatGPT Apps/Connectors subsystem is on by default and exposes
        // external connector tools (mcp__codex_apps__*) outside the cairn
        // gateway. The generated config must disable it under [features].
        let config = render_codex_config_toml(
            "/path/to/cairn-cmd",
            "http://127.0.0.1:3847/api/mcp",
            "[\"mcp\"]",
            "",
            "",
        );
        assert!(
            config.contains("[features]"),
            "config should have a [features] table"
        );
        assert!(
            config.lines().any(|line| line.trim() == "apps = false"),
            "config must set `apps = false` to disable Codex connectors, got:\n{}",
            config
        );
    }

    #[test]
    fn test_codex_config_disables_native_shell_tools() {
        // The agent's only shell path must be `mcp__cairn__run`. Codex's native
        // shell/exec tools (shell_tool, unified_exec) bypass Cairn's fence and
        // commit flow, so the generated config must turn them off.
        let config = render_codex_config_toml(
            "/path/to/cairn-cmd",
            "http://127.0.0.1:3847/api/mcp",
            "[\"mcp\"]",
            "",
            "",
        );
        for flag in ["shell_tool = false", "unified_exec = false"] {
            assert!(
                config.lines().any(|line| line.trim() == flag),
                "config must set `{}` to disable native shell, got:\n{}",
                flag,
                config
            );
        }
    }

    #[test]
    fn test_codex_config_sets_generous_cairn_tool_timeout() {
        // The host owns execution; Codex's per-server tool timeout must sit far
        // above any legal Cairn callback (blocking task/question appends await
        // an unbounded external event) so it never abandons cairn-cmd mid-call.
        let config = render_codex_config_toml(
            "/path/to/cairn-cmd",
            "http://127.0.0.1:3847/api/mcp",
            "[\"mcp\"]",
            "",
            "",
        );
        assert!(
            config
                .lines()
                .any(|line| line.trim() == "tool_timeout_sec = 604800"),
            "config must set a generous tool_timeout_sec on the cairn MCP server, got:\n{}",
            config
        );
    }
}
