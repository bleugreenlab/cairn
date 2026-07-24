use serde_json::Value;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static CONFIG_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(not(target_os = "windows"))]
fn replace_file(source: &std::path::Path, destination: &std::path::Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(target_os = "windows")]
fn replace_file(source: &std::path::Path, destination: &std::path::Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_REPLACE_EXISTING};

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn atomic_write(path: &std::path::Path, contents: &[u8]) -> Result<(), String> {
    let sequence = CONFIG_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Invalid Codex config path: {path:?}"))?;
    let temporary_path = path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let result = (|| {
        let mut temporary = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|error| format!("Failed to create temporary Codex config: {error}"))?;
        temporary
            .write_all(contents)
            .map_err(|error| format!("Failed to write temporary Codex config: {error}"))?;
        temporary
            .sync_all()
            .map_err(|error| format!("Failed to sync temporary Codex config: {error}"))?;
        replace_file(&temporary_path, path)
            .map_err(|error| format!("Failed to atomically replace Codex config: {error}"))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary_path);
    }
    result
}

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
    let codex_home = cairn_codex_home();

    std::fs::create_dir_all(&codex_home)
        .map_err(|e| format!("Failed to create codex home: {}", e))?;

    let callback_url = format!("http://127.0.0.1:{}/api/mcp", callback_port);
    let model_catalog_path = write_codex_model_catalog_override(&codex_home)?;

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
    atomic_write(&config_path, config_toml.as_bytes())?;

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
# Disable Codex's native multi-agent/collab tools (spawn_agent, wait,
# send_message). Cairn's task delegation (cairn:~/tasks -> child jobs with
# their own transcripts) is the canonical path; native collab threads run
# inside the same app-server process and their events would multiplex into
# the parent session's transcript untagged. `multi_agent` is the current
# feature key; `collab` is its legacy alias, set for older Codex builds.
multi_agent = false
collab = false
multi_agent_v2 = false

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

fn write_codex_model_catalog_override(
    codex_home: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, String> {
    let cache_path = codex_home.join("models_cache.json");
    let source = std::fs::read_to_string(&cache_path).map_err(|error| {
        format!(
            "Codex did not produce a model catalog for hardening at {:?}: {}",
            cache_path, error
        )
    })?;
    let rewritten = harden_codex_model_catalog(&source)?;
    let catalog_path = codex_home.join("model_catalog.json");
    std::fs::write(&catalog_path, rewritten)
        .map_err(|e| format!("Failed to write Codex model catalog override: {}", e))?;
    Ok(Some(catalog_path))
}

/// Harden Codex's bundled model catalog before Cairn hands it to a spawned
/// agent. Two model-level capabilities must be neutralized on every model entry
/// because a model capability overrides Cairn's `[features]` config, not the
/// other way around:
///
/// - `apply_patch_tool_type = null` removes native `apply_patch`, so file edits
///   flow through `mcp__cairn__run` (gateway-fenced, commit-aware) instead of
///   Codex's own patch tool, which never commits.
/// - `multi_agent_version = "disabled"` removes the model's native
///   multi-agent/collab capability at the higher-precedence model layer:
///   Codex resolves the turn's collaboration runtime as
///   `model_info.multi_agent_version.unwrap_or_else(|| config.multi_agent_version_from_features())`
///   (codex `core/src/session/mod.rs`). A model entry that carries
///   `"multi_agent_version": "v1"`/`"v2"` is therefore a `Some(_)` that
///   short-circuits the fallback and registers native `spawn_agent`/collab
///   tools even though Cairn's `multi_agent`/`collab`/`multi_agent_v2` feature
///   flags are all false. Explicitly disabling the model capability closes that
///   path directly; the feature flags remain the lower-precedence defense for
///   models without the field.
///
/// All other model metadata is preserved unchanged.
fn harden_codex_model_catalog(catalog_json: &str) -> Result<String, String> {
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
        model_obj.insert(
            "multi_agent_version".to_string(),
            Value::String("disabled".to_string()),
        );
    }

    serde_json::to_string(&catalog)
        .map_err(|e| format!("Failed to serialize Codex model catalog override: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
    fn test_codex_config_disables_native_multi_agent() {
        // Native collab threads share the parent app-server process, so their
        // events would be persisted into the parent transcript untagged.
        // Delegation must instead go through Cairn child task jobs.
        let config = render_codex_config_toml(
            "/path/to/cairn-cmd",
            "http://127.0.0.1:3847/api/mcp",
            "[\"mcp\"]",
            "",
            "",
        );
        for flag in [
            "multi_agent = false",
            "collab = false",
            "multi_agent_v2 = false",
        ] {
            assert!(
                config.lines().any(|line| line.trim() == flag),
                "config must set `{}` to disable native multi-agent tools, got:\n{}",
                flag,
                config
            );
        }
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

    #[test]
    fn harden_model_catalog_disables_multi_agent_version_on_every_model() {
        // The bug in CAIRN-2987 was a model catalog entry carrying an explicit
        // multi_agent_version, which Codex resolves ahead of Cairn's disabled
        // feature flags. The transform must force that field to disabled on every
        // model regardless of whether it started as "v1", "v2", null, or absent,
        // so no model can expose the native collaboration runtime.
        let source = r#"{
            "models": [
                {"slug": "gpt-5.5", "multi_agent_version": "v1", "apply_patch_tool_type": "freeform", "context_window": 272000},
                {"slug": "gpt-5-codex", "multi_agent_version": "v2", "apply_patch_tool_type": "function"},
                {"slug": "legacy", "multi_agent_version": null},
                {"slug": "bare"}
            ]
        }"#;

        let rewritten = harden_codex_model_catalog(source).expect("transform should succeed");
        let parsed: Value = serde_json::from_str(&rewritten).expect("output must be valid JSON");
        let models = parsed["models"].as_array().expect("models array");
        assert_eq!(models.len(), 4, "no models may be dropped");

        for model in models {
            let obj = model.as_object().expect("model object");
            assert!(
                obj.contains_key("multi_agent_version"),
                "every model must carry an explicit multi_agent_version key"
            );
            assert_eq!(
                obj["multi_agent_version"],
                Value::String("disabled".to_string()),
                "multi_agent_version must explicitly disable the model capability, got: {}",
                model
            );
            // The existing apply_patch neutralization must also remain intact.
            assert_eq!(
                obj["apply_patch_tool_type"],
                Value::Null,
                "apply_patch_tool_type must remain null, got: {}",
                model
            );
        }
    }

    #[test]
    fn harden_model_catalog_preserves_unrelated_model_metadata() {
        // Only the two capability fields are rewritten; all other metadata must
        // survive the transform untouched.
        let source = r#"{
            "models": [
                {"slug": "gpt-5.5", "multi_agent_version": "v2", "apply_patch_tool_type": "freeform", "context_window": 272000, "display_name": "GPT-5.5"}
            ]
        }"#;

        let rewritten = harden_codex_model_catalog(source).expect("transform should succeed");
        let parsed: Value = serde_json::from_str(&rewritten).expect("output must be valid JSON");
        let model = &parsed["models"][0];
        assert_eq!(model["slug"], Value::from("gpt-5.5"));
        assert_eq!(model["context_window"], Value::from(272000));
        assert_eq!(model["display_name"], Value::from("GPT-5.5"));
        assert_eq!(
            model["multi_agent_version"],
            Value::String("disabled".to_string())
        );
        assert_eq!(model["apply_patch_tool_type"], Value::Null);
    }

    #[test]
    fn harden_model_catalog_rejects_malformed_catalog() {
        // A catalog without a models array or with a non-object entry indicates
        // an upstream format change we must surface loudly rather than silently
        // ship an un-hardened catalog.
        assert!(harden_codex_model_catalog("{\"models\": {}}").is_err());
        assert!(harden_codex_model_catalog("{\"models\": [\"nope\"]}").is_err());
        assert!(harden_codex_model_catalog("not json").is_err());
    }

    #[test]
    fn writes_hardened_catalog_from_installed_runtime_cache() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("models_cache.json"),
            r#"{"client_version":"9.8.7","fetched_at":"now","models":[{"slug":"current","multi_agent_version":"v2","apply_patch_tool_type":"freeform"}]}"#,
        )
        .unwrap();
        let path = write_codex_model_catalog_override(directory.path())
            .expect("runtime cache should be hardenable")
            .expect("catalog path");
        let parsed: Value = serde_json::from_str(
            &std::fs::read_to_string(path).expect("written catalog should be readable"),
        )
        .expect("written catalog should be valid JSON");
        assert_eq!(parsed["client_version"], "9.8.7");
        assert_eq!(parsed["fetched_at"], "now");
        assert_eq!(parsed["models"][0]["slug"], "current");
        assert_eq!(parsed["models"][0]["multi_agent_version"], "disabled");
        assert_eq!(parsed["models"][0]["apply_patch_tool_type"], Value::Null);
    }

    #[test]
    fn concurrent_atomic_config_writes_leave_one_parseable_cairn_server() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = Arc::new(directory.path().join("config.toml"));
        let writers = (0..16)
            .map(|index| {
                let config_path = Arc::clone(&config_path);
                std::thread::spawn(move || {
                    let config = render_codex_config_toml(
                        &format!("/path/to/cairn-cmd-{index}"),
                        "http://127.0.0.1:3849/api/mcp",
                        "[\"mcp\"]",
                        "",
                        "",
                    );
                    atomic_write(&config_path, config.as_bytes()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }

        let config = std::fs::read_to_string(config_path.as_ref()).unwrap();
        let parsed: toml::Value = toml::from_str(&config).expect("config must be complete TOML");
        let servers = parsed["mcp_servers"]
            .as_table()
            .expect("mcp_servers must be a table");
        assert_eq!(servers.len(), 1);
        assert!(servers.contains_key("cairn"));
    }
}
