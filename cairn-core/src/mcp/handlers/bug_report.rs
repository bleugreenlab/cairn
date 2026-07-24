//! Bug report handler — sends agent-reported issues to the Cairn team.

use serde::{Deserialize, Serialize};

use crate::config::settings::load_settings;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

// ============================================================================
// Payload Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BugReportPayload {
    category: String,
    title: String,
    description: String,
    tool_name: Option<String>,
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

/// Submit a bug report via `change → cairn://bug, append`.
///
/// Checks the opt-out setting, enriches with agent context, and fires the
/// report to the Cairn bug report API in the background.
///
/// `payload_value` must contain `category` (String), `title` (String), `description` (String),
/// and optionally `toolName` (String).
pub(crate) async fn submit_bug_report(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload_value: &serde_json::Value,
) -> Result<String, String> {
    let payload: BugReportPayload = serde_json::from_value(payload_value.clone())
        .map_err(|e| format!("Invalid payload: {e}"))?;

    // Check opt-out setting
    let settings = load_settings(&orch.config_dir);
    if !settings.bug_reports {
        return Ok("Bug reports are disabled in settings.".to_string());
    }

    // Look up agent context for enrichment
    let agent_type = match lookup_agent_type(&orch.db.local, request).await {
        Ok(agent_type) => agent_type,
        Err(error) => {
            log::debug!("bug_report: agent context lookup failed: {}", error);
            None
        }
    };

    let title = payload.title.clone();
    let report = BugReportRequest {
        category: payload.category,
        title: payload.title,
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

        let bug_report_url = crate::api::ApiConfig::default().bug_report_url();
        match client.post(&bug_report_url).json(&report).send().await {
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

    Ok(format!("Report submitted: {title}"))
}

async fn lookup_agent_type(db: &LocalDb, request: &McpCallbackRequest) -> DbResult<Option<String>> {
    if let Some(run_id) = request.run_id.as_deref() {
        let run_id = run_id.to_string();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.node_name,
                               CASE
                                   WHEN j.issue_id IS NULL THEN 'project'
                                   ELSE 'implementation'
                               END
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        WHERE r.id = ?1
                        LIMIT 1
                        ",
                        (run_id.as_str(),),
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| {
                        let job_name = row.opt_text(0)?;
                        let job_type = row.text(1)?;
                        Ok(job_name.or(Some(job_type)))
                    })
                    .transpose()
                    .map(|value| value.flatten())
            })
        })
        .await
    } else {
        let cwd = request.cwd.clone();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT j.node_name,
                               CASE
                                   WHEN j.issue_id IS NULL THEN 'project'
                                   ELSE 'implementation'
                               END
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        LEFT JOIN projects p ON j.project_id = p.id
                        WHERE r.status IN ('starting', 'live')
                          AND (j.worktree_path = ?1 OR (p.repo_path = ?1 AND j.issue_id IS NULL))
                        ORDER BY
                            CASE WHEN j.worktree_path = ?1 THEN 0 ELSE 1 END,
                            r.created_at DESC
                        LIMIT 1
                        ",
                        (cwd.as_str(),),
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| {
                        let job_name = row.opt_text(0)?;
                        let job_type = row.text(1)?;
                        Ok(job_name.or(Some(job_type)))
                    })
                    .transpose()
                    .map(|value| value.flatten())
            })
        })
        .await
    }
}
