//! One-shot completion service for non-session AI invocations.
//!
//! Provides a backend-dispatching trait for quick completions (e.g. AI condition
//! evaluation, PR description generation). Uses the CLI tools directly — Claude
//! via `claude -p ...` and Codex via `codex exec ...`.

use crate::backends::backend_for_model;
use crate::models::Model;
use crate::services::process::{ProcessSpawner, SpawnConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Request for a one-shot completion.
#[derive(Debug, Clone)]
pub struct CompletionRequest {
    /// The prompt text to send.
    pub prompt: String,
    /// Model identifier (e.g. "haiku", "gpt-5.4-mini").
    pub model: Option<String>,
    /// Backend override ("claude", "codex"). Inferred from model if absent.
    pub backend: Option<String>,
    /// Output format.
    pub output_format: OutputFormat,
}

/// Output format for completions.
#[derive(Debug, Clone)]
pub enum OutputFormat {
    /// Plain text response.
    Text,
    /// JSON response with a schema for structured output.
    Json { schema: serde_json::Value },
}

/// Response from a one-shot completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResponse {
    /// The text content of the response.
    pub text: String,
}

/// Trait for one-shot AI completions (no session state).
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
pub trait CompletionService: Send + Sync {
    /// Execute a one-shot completion and return the response text.
    fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, String>;
}

/// Backend-dispatching implementation that resolves claude/codex from the model.
pub struct DispatchingCompletionService {
    process: Arc<dyn ProcessSpawner>,
}

impl DispatchingCompletionService {
    pub fn new(process: Arc<dyn ProcessSpawner>) -> Self {
        Self { process }
    }
}

impl CompletionService for DispatchingCompletionService {
    fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, String> {
        let backend = request
            .backend
            .as_deref()
            .or_else(|| request.model.as_deref().and_then(|m| backend_for_model(m)))
            .unwrap_or("claude");

        match backend {
            "codex" => self.complete_codex(&request),
            _ => self.complete_claude(&request),
        }
    }
}

impl DispatchingCompletionService {
    fn complete_claude(&self, request: &CompletionRequest) -> Result<CompletionResponse, String> {
        let binary = crate::env::find_binary("claude")?;
        let model = request.model.as_deref().unwrap_or("haiku");

        let mut config = SpawnConfig::new(&binary).args(["--model", model, "-p", &request.prompt]);

        // For JSON output, add schema flags
        if let OutputFormat::Json { ref schema } = request.output_format {
            let schema_str = schema.to_string();
            config = config.args(["--output-format", "json", "--json-schema", &schema_str]);
        }

        let output = self.process.run(config)?;

        if !output.success {
            return Err(format!("Claude command failed: {}", output.stderr));
        }

        match request.output_format {
            OutputFormat::Text => Ok(CompletionResponse {
                text: output.stdout.trim().to_string(),
            }),
            OutputFormat::Json { .. } => {
                // Claude CLI outputs NDJSON events; extract structured_output from result event
                extract_claude_json_response(&output.stdout)
            }
        }
    }

    fn complete_codex(&self, request: &CompletionRequest) -> Result<CompletionResponse, String> {
        let binary = crate::env::find_binary("codex")?;
        let model = request.model.as_deref().unwrap_or(Model::GPT_5_4_MINI);

        let mut config = SpawnConfig::new(&binary).args([
            "exec",
            "--model",
            model,
            "--ephemeral",
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
        ]);

        // For JSON output, write schema to temp file
        let _temp_file; // keep alive for duration of process
        if let OutputFormat::Json { ref schema } = request.output_format {
            let file = tempfile::NamedTempFile::new()
                .map_err(|e| format!("Failed to create temp file: {}", e))?;
            std::fs::write(file.path(), schema.to_string())
                .map_err(|e| format!("Failed to write schema: {}", e))?;
            config = config.args(["--output-schema", &file.path().to_string_lossy()]);
            _temp_file = file;
        }

        // JSONL output for parsing + prompt
        config = config.args(["--json", &request.prompt]);

        let output = self.process.run(config)?;

        if !output.success {
            return Err(format!("Codex command failed: {}", output.stderr));
        }

        extract_codex_response(&output.stdout, &request.output_format)
    }
}

/// Extract response from Claude CLI NDJSON output.
fn extract_claude_json_response(stdout: &str) -> Result<CompletionResponse, String> {
    // Try JSON array format first
    if let Ok(events) = serde_json::from_str::<Vec<serde_json::Value>>(stdout) {
        for event in &events {
            if event.get("type").and_then(|t| t.as_str()) == Some("result") {
                if let Some(structured) = event.get("structured_output") {
                    return Ok(CompletionResponse {
                        text: structured.to_string(),
                    });
                }
            }
        }
    }

    // Fall back to NDJSON (one JSON object per line)
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(line) {
            if event.get("type").and_then(|t| t.as_str()) == Some("result") {
                if let Some(structured) = event.get("structured_output") {
                    return Ok(CompletionResponse {
                        text: structured.to_string(),
                    });
                }
            }
        }
    }

    Err(format!(
        "No result event with structured_output found in Claude output ({} bytes)",
        stdout.len()
    ))
}

/// Extract response from Codex exec JSONL output.
fn extract_codex_response(
    stdout: &str,
    output_format: &OutputFormat,
) -> Result<CompletionResponse, String> {
    // Codex exec --json outputs JSONL events. The agent's final text appears in
    // agent_message events. Collect all agent message text.
    let mut agent_text = String::new();
    let mut found_any = false;

    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(line) {
            let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match event_type {
                "agent_message" => {
                    if let Some(msg) = event.get("message").and_then(|m| m.as_str()) {
                        agent_text.push_str(msg);
                        found_any = true;
                    }
                }
                "agent_message_delta" => {
                    if let Some(delta) = event.get("delta").and_then(|d| d.as_str()) {
                        agent_text.push_str(delta);
                        found_any = true;
                    }
                }
                _ => {}
            }
        }
    }

    if !found_any {
        // Fall back to treating the entire stdout as the response
        return Ok(CompletionResponse {
            text: stdout.trim().to_string(),
        });
    }

    match output_format {
        OutputFormat::Text => Ok(CompletionResponse {
            text: agent_text.trim().to_string(),
        }),
        OutputFormat::Json { .. } => {
            // The agent text should contain the JSON response
            Ok(CompletionResponse {
                text: agent_text.trim().to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_claude_json_ndjson_format() {
        let stdout = r#"{"type":"init","session_id":"s1"}
{"type":"result","structured_output":{"title":"Fix bug","body":"Fixed it"}}
"#;
        let resp = extract_claude_json_response(stdout).unwrap();
        assert!(resp.text.contains("Fix bug"));
    }

    #[test]
    fn extract_claude_json_array_format() {
        let stdout = r#"[{"type":"init","session_id":"s1"},{"type":"result","structured_output":{"answer":"yes"}}]"#;
        let resp = extract_claude_json_response(stdout).unwrap();
        assert!(resp.text.contains("yes"));
    }

    #[test]
    fn extract_claude_json_no_result() {
        let stdout = r#"{"type":"init","session_id":"s1"}"#;
        let resp = extract_claude_json_response(stdout);
        assert!(resp.is_err());
    }

    #[test]
    fn extract_codex_response_text() {
        let stdout = r#"{"type":"agent_message","message":"Hello world"}
{"type":"task_complete"}
"#;
        let resp = extract_codex_response(stdout, &OutputFormat::Text).unwrap();
        assert_eq!(resp.text, "Hello world");
    }

    #[test]
    fn extract_codex_response_deltas() {
        let stdout = r#"{"type":"agent_message_delta","delta":"Hello "}
{"type":"agent_message_delta","delta":"world"}
{"type":"task_complete"}
"#;
        let resp = extract_codex_response(stdout, &OutputFormat::Text).unwrap();
        assert_eq!(resp.text, "Hello world");
    }

    #[test]
    fn extract_codex_response_fallback() {
        let stdout = "Just plain text output\n";
        let resp = extract_codex_response(stdout, &OutputFormat::Text).unwrap();
        assert_eq!(resp.text, "Just plain text output");
    }

    #[test]
    fn dispatching_routes_claude_models() {
        // Verify backend_for_model returns None (claude) for claude models
        assert_eq!(backend_for_model("haiku"), None);
        assert_eq!(backend_for_model("sonnet"), None);
        assert_eq!(backend_for_model("opus"), None);
    }

    #[test]
    fn dispatching_routes_codex_models() {
        assert_eq!(backend_for_model("gpt-5.4-mini"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.4"), Some("codex"));
    }
}
