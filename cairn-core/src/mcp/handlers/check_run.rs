//! Authenticated manual configured-check producer.
//!
//! This tool is intentionally narrower than `run`: callers select a configured
//! suite and an optional logical coordinate, but cannot supply executable text,
//! identity hashes, environment claims, or a verdict.

use serde::Deserialize;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CheckRunPayload {
    suite: String,
    #[serde(default)]
    branch: Option<String>,
}

pub(crate) async fn handle_check_run(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let result = async {
        let run_id = request
            .run_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "authenticated check_run request is missing its run ID".to_string())?;
        let payload: CheckRunPayload = serde_json::from_value(request.payload.clone())
            .map_err(|error| format!("invalid check_run payload: {error}"))?;
        if payload.suite.trim().is_empty() {
            return Err("configured suite name must not be empty".to_string());
        }
        crate::execution::checks::run_manual_configured_check(
            orch,
            run_id,
            &payload.suite,
            payload.branch.as_deref(),
        )
        .await
    }
    .await;

    match result {
        Ok(result) => serde_json::json!({ "ok": true, "result": result }).to_string(),
        Err(error) => serde_json::json!({ "ok": false, "error": error }).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_accepts_only_suite_and_optional_branch() {
        let payload: CheckRunPayload = serde_json::from_value(serde_json::json!({
            "suite": "rust-tests",
            "branch": "main"
        }))
        .unwrap();
        assert_eq!(payload.suite, "rust-tests");
        assert_eq!(payload.branch.as_deref(), Some("main"));

        let asserted_command = serde_json::from_value::<CheckRunPayload>(serde_json::json!({
            "suite": "rust-tests",
            "command": "cargo test"
        }));
        assert!(
            asserted_command.is_err(),
            "raw commands must not cross the trusted producer boundary"
        );
    }
}
