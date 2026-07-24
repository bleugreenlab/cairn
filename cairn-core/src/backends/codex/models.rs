use crate::backends::{DiscoveredModel, DiscoveredReasoningEffort};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

struct DiscoveryHome(std::path::PathBuf);

impl Drop for DiscoveryHome {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct DiscoveryChild(std::process::Child);

impl Drop for DiscoveryChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexModelListResult {
    data: Vec<CodexModelListEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexModelListEntry {
    id: String,
    model: String,
    #[allow(dead_code)]
    display_name: String,
    description: Option<String>,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    is_default: bool,
    default_reasoning_effort: Option<String>,
    #[serde(default)]
    supported_reasoning_efforts: Vec<CodexReasoningEffortEntry>,
    #[serde(default, alias = "modelContextWindow")]
    context_window: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexReasoningEffortEntry {
    reasoning_effort: String,
    description: Option<String>,
}

pub(super) fn discover_codex_models(
    login_params: Option<Value>,
    extra_env: &HashMap<String, String>,
) -> Result<Vec<DiscoveredModel>, String> {
    let codex_path = crate::env::find_binary("codex")?;
    let codex_home = super::config::cairn_codex_home();
    // Discovery needs the installed runtime's live catalog, not the hardened
    // static catalog used by agent sessions. Give it an isolated CODEX_HOME so
    // an earlier session's model_catalog_json cannot freeze model/list and the
    // user's native MCP configuration is never loaded.
    let discovery_home = DiscoveryHome(std::env::temp_dir().join(format!(
        "cairn-codex-model-discovery-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    )));
    std::fs::create_dir_all(&discovery_home.0)
        .map_err(|error| format!("Failed to create Codex discovery home: {error}"))?;
    let auth_source = codex_home.join("auth.json");
    if auth_source.exists() {
        std::fs::copy(&auth_source, discovery_home.0.join("auth.json"))
            .map_err(|error| format!("Failed to stage Codex discovery auth: {error}"))?;
    }
    std::fs::write(
        discovery_home.0.join("config.toml"),
        "[features]\napps = false\nshell_tool = false\nunified_exec = false\nmulti_agent = false\ncollab = false\nmulti_agent_v2 = false\n",
    )
    .map_err(|error| format!("Failed to write Codex discovery config: {error}"))?;
    let mut child = DiscoveryChild(
        Command::new(&codex_path)
            .arg("app-server")
            .env("CODEX_HOME", &discovery_home.0)
            .envs(extra_env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to start Codex app-server: {}", e))?,
    );

    let mut stdin = child
        .0
        .stdin
        .take()
        .ok_or_else(|| "Failed to capture Codex app-server stdin".to_string())?;
    let stdout = child
        .0
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture Codex app-server stdout".to_string())?;
    let mut reader = BufReader::new(stdout);

    let mut messages = vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "cairn",
                    "title": "Cairn",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        }),
    ];
    if let Some(login_params) = login_params {
        messages.push(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "account/login/start",
            "params": login_params
        }));
    }
    messages.push(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "model/list",
        "params": {}
    }));
    for message in messages {
        writeln!(stdin, "{}", message)
            .map_err(|e| format!("Failed to write to Codex app-server stdin: {}", e))?;
    }
    stdin
        .flush()
        .map_err(|e| format!("Failed to flush Codex app-server stdin: {}", e))?;

    let mut line = String::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("Failed reading Codex app-server output: {}", e))?;
        if read == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line.trim())
            .map_err(|e| format!("Failed to parse Codex model/list response: {}", e))?;
        if value.get("id").and_then(Value::as_i64) != Some(2) {
            continue;
        }
        let result: CodexModelListResult = serde_json::from_value(
            value
                .get("result")
                .cloned()
                .ok_or_else(|| "Codex model/list response missing result".to_string())?,
        )
        .map_err(|e| format!("Failed to decode Codex model/list result: {}", e))?;
        let _ = child.0.kill();
        let _ = child.0.wait();
        let discovered_cache = discovery_home.0.join("models_cache.json");
        if !discovered_cache.exists() {
            return Err("Codex model/list did not produce models_cache.json".to_string());
        }
        std::fs::create_dir_all(&codex_home)
            .map_err(|error| format!("Failed to create Cairn Codex home: {error}"))?;
        std::fs::copy(&discovered_cache, codex_home.join("models_cache.json"))
            .map_err(|error| format!("Failed to publish Codex model cache: {error}"))?;
        let mut models: Vec<DiscoveredModel> = result
            .data
            .into_iter()
            .map(|model| DiscoveredModel {
                id: model.id,
                display_name: model.model.clone(),
                model: model.model,
                description: model.description,
                hidden: model.hidden,
                is_default: model.is_default,
                default_reasoning_effort: model.default_reasoning_effort,
                supported_reasoning_efforts: model
                    .supported_reasoning_efforts
                    .into_iter()
                    .map(|effort| DiscoveredReasoningEffort {
                        reasoning_effort: effort.reasoning_effort,
                        description: effort.description,
                    })
                    .collect(),
                context_window: model.context_window,
                canonical_slug: None,
                pricing: None,
                supported_parameters: Vec::new(),
                router: false,
                architecture_modality: None,
            })
            .collect();
        merge_fallback_models(&mut models);
        return Ok(models);
    }

    let stderr = child
        .0
        .stderr
        .take()
        .and_then(|mut stderr| {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut stderr, &mut buf).ok()?;
            Some(buf)
        })
        .unwrap_or_default();
    let _ = child.0.kill();
    let _ = child.0.wait();
    Err(if stderr.trim().is_empty() {
        "Codex app-server exited before returning model/list".to_string()
    } else {
        format!(
            "Codex app-server exited before returning model/list: {}",
            stderr.trim()
        )
    })
}

/// Refresh the published Codex model cache for a session start, tolerating a
/// transient discovery failure when a cache from a prior successful run is
/// already on disk.
///
/// Session start (including a `continue_job` resume) calls model discovery only
/// for its side effect: `discover_codex_models` publishes
/// `~/.cairn/codex/models_cache.json`, which `write_codex_config` then hardens
/// into the session's catalog. The freshly enumerated model list is discarded.
/// A live `model/list` refresh keeps that catalog current, but the app-server
/// can answer `model/list` without writing its cache, or the spawn can fail
/// transiently (offline, a flaky process). Failing the whole job in that case is
/// failing closed on a non-fatal hiccup — the previously published cache is a
/// valid basis for the session. So we fall back to it and only surface the
/// discovery error when no cache has ever been published.
pub(super) fn refresh_codex_model_cache(
    login_params: Option<Value>,
    extra_env: &HashMap<String, String>,
) -> Result<(), String> {
    let discovery = discover_codex_models(login_params, extra_env);
    let published_cache_exists = super::config::cairn_codex_home()
        .join("models_cache.json")
        .exists();
    resolve_cache_refresh(discovery, published_cache_exists)
}

/// Decide a session-start cache refresh: a successful discovery always wins;
/// otherwise a previously published cache lets the session proceed on it, and
/// only a missing cache turns the discovery error fatal.
fn resolve_cache_refresh(
    discovery: Result<Vec<DiscoveredModel>, String>,
    published_cache_exists: bool,
) -> Result<(), String> {
    match discovery {
        Ok(_) => Ok(()),
        Err(error) if published_cache_exists => {
            log::warn!(
                "Codex model discovery failed ({error}); reusing the previously published \
                 models_cache.json for this session"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn reasoning_effort(name: &str, description: &str) -> DiscoveredReasoningEffort {
    DiscoveredReasoningEffort {
        reasoning_effort: name.to_string(),
        description: Some(description.to_string()),
    }
}

fn fallback_model(
    slug: &str,
    default_effort: &str,
    supported_reasoning_efforts: Vec<DiscoveredReasoningEffort>,
) -> DiscoveredModel {
    DiscoveredModel {
        id: slug.to_string(),
        model: slug.to_string(),
        display_name: slug.to_string(),
        description: None,
        hidden: false,
        // Never claim the default flag: Codex's own `model/list` owns which
        // model is default, and Cairn pins the tier presets independently.
        is_default: false,
        default_reasoning_effort: Some(default_effort.to_string()),
        supported_reasoning_efforts,
        context_window: Some(372_000),
        canonical_slug: None,
        pricing: None,
        supported_parameters: Vec::new(),
        router: false,
        architecture_modality: None,
    }
}

/// Codex model slugs Cairn knows are selectable (they run via `codex -m <slug>`
/// and back the default tier presets) but that the pinned `codex` CLI's
/// `model/list` does not yet enumerate. Metadata mirrors Codex's own
/// `models-manager/models.json`. Deduped against discovery by slug, so once the
/// CLI reports them natively its entry wins and these are dropped.
fn fallback_codex_models() -> Vec<DiscoveredModel> {
    let base = || {
        vec![
            reasoning_effort("low", "Fast responses with lighter reasoning"),
            reasoning_effort(
                "medium",
                "Balances speed and reasoning depth for everyday tasks",
            ),
            reasoning_effort("high", "Greater reasoning depth for complex problems"),
            reasoning_effort("xhigh", "Extra high reasoning depth for complex problems"),
            reasoning_effort("max", "Maximum reasoning depth for the hardest problems"),
        ]
    };
    let with_ultra = || {
        let mut efforts = base();
        efforts.push(reasoning_effort(
            "ultra",
            "Maximum reasoning with automatic task delegation",
        ));
        efforts
    };
    vec![
        fallback_model("gpt-5.6-sol", "low", with_ultra()),
        fallback_model("gpt-5.6-terra", "medium", with_ultra()),
        fallback_model("gpt-5.6-luna", "medium", base()),
    ]
}

/// Prepend known-selectable Codex models the CLI hasn't enumerated yet, skipping
/// any slug already present so a native discovery of the same model wins.
fn merge_fallback_models(discovered: &mut Vec<DiscoveredModel>) {
    let present: std::collections::HashSet<String> =
        discovered.iter().map(|m| m.model.clone()).collect();
    let mut missing: Vec<DiscoveredModel> = fallback_codex_models()
        .into_iter()
        .filter(|m| !present.contains(&m.model))
        .collect();
    if missing.is_empty() {
        return;
    }
    missing.append(discovered);
    *discovered = missing;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_home_guard_removes_staged_credentials_on_error() {
        let path = std::env::temp_dir().join(format!(
            "cairn-codex-discovery-cleanup-test-{}",
            uuid::Uuid::new_v4()
        ));
        let forced_error = (|| -> Result<(), String> {
            let home = DiscoveryHome(path.clone());
            std::fs::create_dir_all(&home.0).map_err(|error| error.to_string())?;
            std::fs::write(home.0.join("auth.json"), "secret")
                .map_err(|error| error.to_string())?;
            Err("forced discovery failure".to_string())
        })();
        assert!(forced_error.is_err());
        assert!(
            !path.exists(),
            "credential-bearing discovery home survived error"
        );
    }

    #[test]
    fn fallback_covers_the_three_5_6_models() {
        let models = fallback_codex_models();
        let slugs: Vec<&str> = models.iter().map(|m| m.model.as_str()).collect();
        assert_eq!(slugs, ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]);

        // Every fallback is visible and carries selectable reasoning efforts, so
        // the effort control renders (an empty list hides it in the UI).
        for model in &models {
            assert!(!model.hidden);
            assert!(!model.is_default);
            assert!(!model.supported_reasoning_efforts.is_empty());
        }

        // Sol/terra expose `ultra`; luna does not (matches models.json).
        let has_ultra = |m: &DiscoveredModel| {
            m.supported_reasoning_efforts
                .iter()
                .any(|e| e.reasoning_effort == "ultra")
        };
        assert!(has_ultra(&models[0]));
        assert!(has_ultra(&models[1]));
        assert!(!has_ultra(&models[2]));
    }

    #[test]
    fn merge_prepends_all_when_discovery_lacks_them() {
        let mut discovered = vec![fallback_model("gpt-5.5", "high", vec![])];
        merge_fallback_models(&mut discovered);
        let slugs: Vec<&str> = discovered.iter().map(|m| m.model.as_str()).collect();
        assert_eq!(
            slugs,
            ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5"]
        );
    }

    #[test]
    fn cache_refresh_succeeds_when_discovery_succeeds() {
        // A fresh discovery is always authoritative, whether or not a prior cache
        // exists.
        let ok = || Ok(vec![fallback_model("gpt-5.6-sol", "low", vec![])]);
        assert!(resolve_cache_refresh(ok(), true).is_ok());
        assert!(resolve_cache_refresh(ok(), false).is_ok());
    }

    #[test]
    fn cache_refresh_falls_back_to_published_cache_on_transient_failure() {
        // The regression (CAIRN-3078): a transient discovery failure fails the
        // whole job even though a valid cache from a prior run is on disk. When
        // the published cache exists, the session must proceed on it.
        let result = resolve_cache_refresh(
            Err("Codex model/list did not produce models_cache.json".to_string()),
            true,
        );
        assert!(
            result.is_ok(),
            "a transient discovery failure must not fail closed when a published cache exists"
        );
    }

    #[test]
    fn cache_refresh_fails_closed_when_no_cache_was_ever_published() {
        // With no cache to fall back to (a genuine first-run-offline case) the
        // discovery error must still surface — there is nothing to harden.
        let result = resolve_cache_refresh(Err("model/list unavailable".to_string()), false);
        assert_eq!(result, Err("model/list unavailable".to_string()));
    }

    #[test]
    fn merge_skips_slugs_discovery_already_reports() {
        // Once the CLI enumerates a 5.6 model, discovery's entry is authoritative
        // and the fallback for that slug is dropped (no duplicate).
        let mut discovered = vec![fallback_model("gpt-5.6-sol", "high", vec![])];
        merge_fallback_models(&mut discovered);
        let slugs: Vec<&str> = discovered.iter().map(|m| m.model.as_str()).collect();
        assert_eq!(slugs, ["gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.6-sol"]);
        assert_eq!(slugs.iter().filter(|s| **s == "gpt-5.6-sol").count(), 1);
    }
}
