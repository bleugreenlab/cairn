//! Codex model discovery: the installed CLI's live `model/list`, and the
//! model cache a session's hardened catalog is built from.

use crate::types::catalog::{DiscoveredModel, DiscoveredReasoningEffort};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
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

/// Ask the Codex CLI at `codex_path` for its models. A cache Codex writes
/// while answering is published into `codex_home` for
/// [`super::home::write_codex_config`] to harden.
pub fn discover_codex_models(
    codex_path: &str,
    codex_home: &Path,
    login_params: Option<Value>,
    extra_env: &HashMap<String, String>,
) -> Result<Vec<DiscoveredModel>, String> {
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
        Command::new(codex_path)
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

    send_message(
        &mut stdin,
        &json!({
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
    )?;
    send_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        }),
    )?;
    if let Some(login_params) = login_params {
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "account/login/start",
                "params": login_params
            }),
        )?;
        // model/list sent before the login lands answers from the CLI's
        // bundled inventory and never fetches the account catalog, which is
        // what writes models_cache.json. A home without auth.json would then
        // have no catalog for sessions to harden.
        let Some(response) = read_response(&mut reader, 3)? else {
            return Err(exited_before(&mut child, "account/login/start"));
        };
        if let Some(error) = response.get("error") {
            return Err(format!("Codex rejected the discovery login: {error}"));
        }
    }
    send_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "model/list",
            "params": {}
        }),
    )?;

    let Some(response) = read_response(&mut reader, 2)? else {
        return Err(exited_before(&mut child, "model/list"));
    };
    let result: CodexModelListResult = serde_json::from_value(
        response
            .get("result")
            .cloned()
            .ok_or_else(|| "Codex model/list response missing result".to_string())?,
    )
    .map_err(|e| format!("Failed to decode Codex model/list result: {}", e))?;
    let _ = child.0.kill();
    let _ = child.0.wait();
    finish_model_discovery(result, &discovery_home.0, codex_home)
}

fn send_message(stdin: &mut impl Write, message: &Value) -> Result<(), String> {
    writeln!(stdin, "{}", message)
        .and_then(|()| stdin.flush())
        .map_err(|e| format!("Failed to write to Codex app-server stdin: {}", e))
}

/// Read app-server output up to the response for request `id`, skipping
/// notifications and other responses. `None` when the app-server exits first.
fn read_response(reader: &mut impl BufRead, id: i64) -> Result<Option<Value>, String> {
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("Failed reading Codex app-server output: {}", e))?;
        if read == 0 {
            return Ok(None);
        }
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line.trim())
            .map_err(|e| format!("Failed to parse Codex app-server output: {}", e))?;
        if value.get("id").and_then(Value::as_i64) == Some(id) {
            return Ok(Some(value));
        }
    }
}

fn exited_before(child: &mut DiscoveryChild, awaiting: &str) -> String {
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
    if stderr.trim().is_empty() {
        format!("Codex app-server exited before answering {awaiting}")
    } else {
        format!(
            "Codex app-server exited before answering {awaiting}: {}",
            stderr.trim()
        )
    }
}

/// Keep model enumeration usable independently of Codex's optional cache write.
fn finish_model_discovery(
    result: CodexModelListResult,
    discovery_home: &Path,
    codex_home: &Path,
) -> Result<Vec<DiscoveredModel>, String> {
    let discovered_cache = discovery_home.join("models_cache.json");
    // model/list can return the CLI's bundled inventory without writing a
    // cache. That inventory still belongs in the provider picker. Publish a
    // session catalog only when discovery actually supplies one, preserving a
    // previously published catalog otherwise.
    if discovered_cache.exists() {
        std::fs::create_dir_all(codex_home)
            .map_err(|error| format!("Failed to create Cairn Codex home: {error}"))?;
        std::fs::copy(&discovered_cache, codex_home.join("models_cache.json"))
            .map_err(|error| format!("Failed to publish Codex model cache: {error}"))?;
    }
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
            serving_account_ids: Vec::new(),
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            modalities: None,
            serves: None,
        })
        .collect();
    merge_fallback_models(&mut models);
    Ok(models)
}

/// Refresh the published Codex model cache for a session start, tolerating a
/// transient discovery failure when a cache from a prior successful run is
/// already on disk.
///
/// Session start (including a `continue_job` resume) calls model discovery only
/// for its side effect: `discover_codex_models` publishes
/// `<codex_home>/models_cache.json`, which `write_codex_config` then hardens
/// into the session's catalog. The freshly enumerated model list is discarded.
/// A live `model/list` refresh keeps that catalog current, but the app-server
/// can answer `model/list` without writing its cache, or the spawn can fail
/// transiently (offline, a flaky process). Failing the whole job in that case is
/// failing closed on a non-fatal hiccup — the previously published cache is a
/// valid basis for the session. So we fall back to it and only surface the
/// discovery error when no cache has ever been published.
pub fn refresh_codex_model_cache(
    codex_path: &str,
    codex_home: &Path,
    login_params: Option<Value>,
    extra_env: &HashMap<String, String>,
) -> Result<(), String> {
    let discovery = discover_codex_models(codex_path, codex_home, login_params, extra_env);
    let published_cache_exists = codex_home.join("models_cache.json").exists();
    resolve_cache_refresh(discovery, published_cache_exists)
}

/// Session startup requires a published catalog for hardening, even when
/// model/list succeeded without writing one. A prior catalog remains usable
/// when refreshing it fails.
fn resolve_cache_refresh(
    discovery: Result<Vec<DiscoveredModel>, String>,
    published_cache_exists: bool,
) -> Result<(), String> {
    match discovery {
        Ok(_) if published_cache_exists => Ok(()),
        Ok(_) => Err("Codex model/list did not produce models_cache.json and no published session catalog exists".to_string()),
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
        serving_account_ids: Vec::new(),
        pricing: None,
        supported_parameters: Vec::new(),
        router: false,
        modalities: None,
        serves: None,
    }
}

/// Codex model slugs Cairn knows are selectable (they run via `codex -m <slug>`)
/// but that the pinned `codex` CLI's `model/list` does not yet enumerate.
/// Metadata mirrors Codex's own
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
        DiscoveredModel {
            // Codex's session window differs from the public API window.
            context_window: Some(272_000),
            ..fallback_model("gpt-6-astra", "medium", with_ultra())
        },
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
    fn model_list_without_cache_still_offers_astra() {
        let home = tempfile::tempdir().unwrap();
        let published = home.path().join("published");
        // The installed CLI successfully answers model/list but does not
        // necessarily write models_cache.json into its isolated discovery home.
        let result = serde_json::from_value(json!({"data": []})).unwrap();
        let models = finish_model_discovery(result, home.path(), &published).unwrap();
        let astra = models
            .iter()
            .find(|model| model.model == "gpt-6-astra")
            .unwrap();
        assert!(!astra.hidden);
        assert!(!published.join("models_cache.json").exists());
        assert!(resolve_cache_refresh(Ok(models), false).is_err());
    }

    #[test]
    fn cacheless_model_list_keeps_native_metadata_and_published_session_catalog() {
        let home = tempfile::tempdir().unwrap();
        let published = tempfile::tempdir().unwrap();
        let cache = published.path().join("models_cache.json");
        let retained = r#"{"models":[{"slug":"gpt-5.5"}]}"#;
        std::fs::write(&cache, retained).unwrap();
        let result = serde_json::from_value(json!({"data": [{
            "id": "gpt-6-astra", "model": "gpt-6-astra", "displayName": "GPT-6 Astra",
            "isDefault": true, "defaultReasoningEffort": "high", "contextWindow": 1_050_000,
            "supportedReasoningEfforts": [{"reasoningEffort": "high"}]
        }]}))
        .unwrap();
        let models = finish_model_discovery(result, home.path(), published.path()).unwrap();
        let astra: Vec<_> = models
            .iter()
            .filter(|model| model.model == "gpt-6-astra")
            .collect();
        assert_eq!(astra.len(), 1);
        assert!(astra[0].is_default);
        assert_eq!(astra[0].default_reasoning_effort.as_deref(), Some("high"));
        assert_eq!(astra[0].context_window, Some(1_050_000));
        assert_eq!(std::fs::read_to_string(cache).unwrap(), retained);
        assert!(resolve_cache_refresh(Ok(models), true).is_ok());
    }

    #[test]
    fn model_list_publishes_a_cache_when_codex_writes_one() {
        let home = tempfile::tempdir().unwrap();
        let published = home.path().join("published");
        let cache = r#"{"models":[{"slug":"gpt-6-astra"}]}"#;
        std::fs::write(home.path().join("models_cache.json"), cache).unwrap();
        let models = finish_model_discovery(
            serde_json::from_value(json!({"data": []})).unwrap(),
            home.path(),
            &published,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(published.join("models_cache.json")).unwrap(),
            cache
        );
        assert!(resolve_cache_refresh(Ok(models), true).is_ok());
    }

    /// A stand-in `codex app-server` that behaves like codex-cli 0.153: a
    /// login, whatever its params, lands asynchronously, writing
    /// `models_cache.json` only when it does, while `model/list` answers at
    /// once from whatever has landed so far. `FAKE_LOGIN=reject` makes the
    /// login fail instead.
    #[cfg(unix)]
    fn scripted_app_server(dir: &Path) -> String {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("codex");
        std::fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) printf '%s\n' '{"id":1,"result":{}}' ;;
    *'"method":"account/login/start"'*)
      if [ "$FAKE_LOGIN" = reject ]; then
        printf '%s\n' '{"id":3,"error":{"code":-32600,"message":"invalid tokens"}}'
      else
        ( sleep 0.3
          printf '%s' '{"models":[{"slug":"gpt-6-astra"}]}' > "$CODEX_HOME/models_cache.json"
          printf '%s\n' '{"method":"account/updated","params":{}}' '{"id":3,"result":{}}' ) &
      fi ;;
    *'"method":"model/list"'*) printf '%s\n' '{"id":2,"result":{"data":[]}}' ;;
  esac
done
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        script.to_string_lossy().into_owned()
    }

    #[cfg(unix)]
    #[test]
    fn fresh_home_gets_a_session_catalog_from_a_login_message() {
        let bin = tempfile::tempdir().unwrap();
        let codex = scripted_app_server(bin.path());
        // A fresh Codex home: no auth.json and no previously published cache,
        // so only the login message can make Codex write the catalog.
        let home = tempfile::tempdir().unwrap();
        // The stand-in never reads the params, so they are not subscription-
        // shaped: real ones come only from `Subscriptions::codex_session_login`.
        let login = json!({"type": "scriptedLogin"});
        refresh_codex_model_cache(&codex, home.path(), Some(login), &HashMap::new()).unwrap();
        assert_eq!(
            std::fs::read_to_string(home.path().join("models_cache.json")).unwrap(),
            r#"{"models":[{"slug":"gpt-6-astra"}]}"#
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejected_discovery_login_fails_discovery() {
        let bin = tempfile::tempdir().unwrap();
        let codex = scripted_app_server(bin.path());
        let home = tempfile::tempdir().unwrap();
        let env = HashMap::from([("FAKE_LOGIN".to_string(), "reject".to_string())]);
        let error = discover_codex_models(&codex, home.path(), Some(json!({})), &env).unwrap_err();
        assert!(error.contains("invalid tokens"), "{error}");
    }

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
    fn fallback_covers_known_selectable_models() {
        let models = fallback_codex_models();
        let slugs: Vec<&str> = models.iter().map(|m| m.model.as_str()).collect();
        assert_eq!(
            slugs,
            [
                "gpt-6-astra",
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna"
            ]
        );
        assert_eq!(
            models[0].default_reasoning_effort.as_deref(),
            Some("medium")
        );
        assert_eq!(models[0].context_window, Some(272_000));
        assert_eq!(
            models[0]
                .supported_reasoning_efforts
                .iter()
                .map(|effort| effort.reasoning_effort.as_str())
                .collect::<Vec<_>>(),
            ["low", "medium", "high", "xhigh", "max", "ultra"]
        );

        // Every fallback is visible and carries selectable reasoning efforts, so
        // the effort control renders (an empty list hides it in the UI).
        for model in &models {
            assert!(!model.hidden);
            assert!(!model.is_default);
            assert!(!model.supported_reasoning_efforts.is_empty());
        }

        // Astra/sol/terra expose `ultra`; luna does not (matches models.json).
        let has_ultra = |m: &DiscoveredModel| {
            m.supported_reasoning_efforts
                .iter()
                .any(|e| e.reasoning_effort == "ultra")
        };
        assert!(has_ultra(&models[0]));
        assert!(has_ultra(&models[1]));
        assert!(has_ultra(&models[2]));
        assert!(!has_ultra(&models[3]));
    }

    #[test]
    fn merge_prepends_all_when_discovery_lacks_them() {
        let mut discovered = vec![fallback_model("gpt-5.5", "high", vec![])];
        merge_fallback_models(&mut discovered);
        let slugs: Vec<&str> = discovered.iter().map(|m| m.model.as_str()).collect();
        assert_eq!(
            slugs,
            [
                "gpt-6-astra",
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna",
                "gpt-5.5"
            ]
        );
    }

    #[test]
    fn cache_refresh_succeeds_when_discovery_succeeds() {
        // A successful refresh can start a session only with a published cache.
        let ok = || Ok(vec![fallback_model("gpt-5.6-sol", "low", vec![])]);
        assert!(resolve_cache_refresh(ok(), true).is_ok());
        let error = resolve_cache_refresh(ok(), false).unwrap_err();
        assert!(error.contains("no published session catalog exists"));
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
        // Once the CLI enumerates Astra, discovery's entry is authoritative
        // and the fallback for that slug is dropped (no duplicate).
        let mut native = fallback_model("gpt-6-astra", "high", vec![]);
        native.context_window = Some(1_050_000);
        native.is_default = true;
        let mut discovered = vec![native];
        merge_fallback_models(&mut discovered);
        let slugs: Vec<&str> = discovered.iter().map(|m| m.model.as_str()).collect();
        assert_eq!(
            slugs,
            [
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna",
                "gpt-6-astra"
            ]
        );
        assert_eq!(slugs.iter().filter(|s| **s == "gpt-6-astra").count(), 1);
        let astra = discovered.last().unwrap();
        assert_eq!(astra.default_reasoning_effort.as_deref(), Some("high"));
        assert_eq!(astra.context_window, Some(1_050_000));
        assert!(astra.is_default);
        assert!(astra.supported_reasoning_efforts.is_empty());
        let once = serde_json::to_value(&discovered).unwrap();
        merge_fallback_models(&mut discovered);
        assert_eq!(serde_json::to_value(&discovered).unwrap(), once);
    }
}
