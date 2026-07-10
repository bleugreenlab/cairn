use crate::backends::{DiscoveredModel, DiscoveredReasoningEffort};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

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

pub(super) fn discover_codex_models() -> Result<Vec<DiscoveredModel>, String> {
    let codex_path = crate::env::find_binary("codex")?;
    // Point discovery at the Cairn-owned CODEX_HOME like every other Codex
    // spawn. Without it, model discovery loads the user's native ~/.codex and
    // launches all of their native MCP servers (uvx/npx/...) just to list
    // models. Cairn never loads native MCP servers — only the cairn gateway.
    let mut child = Command::new(&codex_path)
        .arg("app-server")
        .env("CODEX_HOME", super::config::cairn_codex_home())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to start Codex app-server: {}", e))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "Failed to capture Codex app-server stdin".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture Codex app-server stdout".to_string())?;
    let mut reader = BufReader::new(stdout);

    for message in [
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
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "model/list",
            "params": {}
        }),
    ] {
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
        let _ = child.kill();
        let _ = child.wait();
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
        .stderr
        .take()
        .and_then(|mut stderr| {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut stderr, &mut buf).ok()?;
            Some(buf)
        })
        .unwrap_or_default();
    let _ = child.kill();
    let _ = child.wait();
    Err(if stderr.trim().is_empty() {
        "Codex app-server exited before returning model/list".to_string()
    } else {
        format!(
            "Codex app-server exited before returning model/list: {}",
            stderr.trim()
        )
    })
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
