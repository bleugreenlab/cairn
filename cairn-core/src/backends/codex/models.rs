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
        return Ok(result
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
            })
            .collect());
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
