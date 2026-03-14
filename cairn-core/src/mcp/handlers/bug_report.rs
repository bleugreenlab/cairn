//! Bug report handler — sends agent-reported issues to the Cairn team.

use serde::{Deserialize, Serialize};

use crate::config::settings::load_settings;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

use super::lookup_run;

const BUG_REPORT_API_URL: &str = "https://bug.cairn.md/api/reports";

// ============================================================================
// Payload Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BugReportPayload {
    pub category: String,
    pub title: String,
    pub description: String,
    pub tool_name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BugReportRequest {
    category: String,
    title: String,
    description: String,
    cairn_version: Option<String>,
    agent_type: Option<String>,
    tool_name: Option<String>,
    os: Option<String>,
}

// ============================================================================
// Handler
// ============================================================================

/// Handle bug_report tool call.
///
/// Checks opt-out setting, enriches with context, and fires off the report
/// to the Cairn bug report API in the background.
pub async fn handle_bug_report(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: BugReportPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    // Check opt-out setting
    let settings = load_settings(&orch.config_dir);
    if !settings.bug_reports {
        return "Bug reports are disabled in settings.".to_string();
    }

    // Look up agent context for enrichment
    let agent_type = {
        let conn_result = orch.db.conn.lock();
        match conn_result {
            Ok(mut conn) => lookup_run(&mut conn, request)
                .ok()
                .and_then(|ctx| ctx.job_name.or(Some(ctx.job_type))),
            Err(_) => None,
        }
    };

    let report = BugReportRequest {
        category: payload.category,
        title: payload.title.clone(),
        description: payload.description,
        cairn_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        agent_type,
        tool_name: payload.tool_name,
        os: Some(std::env::consts::OS.to_string()),
    };

    // Fire-and-forget HTTP POST
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build();

        let client = match client {
            Ok(c) => c,
            Err(e) => {
                log::warn!("bug_report: failed to create HTTP client: {}", e);
                return;
            }
        };

        match client.post(BUG_REPORT_API_URL).json(&report).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    log::warn!("bug_report: API returned status {}", resp.status());
                }
            }
            Err(e) => {
                log::warn!("bug_report: failed to send report: {}", e);
            }
        }
    });

    format!("Report submitted: {}", payload.title)
}
