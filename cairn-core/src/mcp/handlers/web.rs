//! Web and local-PDF reads, routed through typed services.
//!
//! `read` detects `http(s)://` targets and local `.pdf` paths in `cairn-cmd`
//! and forwards them here as a `read_web` callback. PDF targets (local files and
//! remote `.pdf` URLs) route to the typed PDF service ([`super::pdf`]);
//! everything else routes to the typed web-fetch service ([`super::fetch_web`]).
//!
//! Running through cairn-core (rather than directly in `cairn-cmd`) keeps PATH
//! and MCP resolution correct in the signed app and gives `cairn-server`
//! headless parity via the shared dispatcher. Fetch / extraction failures
//! surface as clean guidance text — never a panic.

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ReadWebPayload {
    /// URL (`http(s)://…`) or absolute local file path (e.g. a `.pdf`).
    target: String,
}

/// Handle a `read_web` callback. Thin String entry kept for the legacy dispatch
/// route; the batch path calls [`read_web_markdown`] so it can window the
/// markdown and surface a failure as an `Error`-kind segment.
pub async fn handle_read_web(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    match read_web_markdown(orch, request).await {
        Ok(markdown) => markdown,
        Err(message) => message,
    }
}

/// Convert a URL or local PDF to markdown via the active typed service. `Ok` is
/// the raw markdown; `Err` is a clean guidance message.
pub(crate) async fn read_web_markdown(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<String, String> {
    let payload: ReadWebPayload = super::parse_payload(request)?;

    // `file:` targets (local PDFs) resolve against the worktree first so the
    // extractor receives an absolute path and traversal stays confined. URLs
    // pass through unchanged.
    let is_file = payload.target.starts_with("file:");
    let resolved_target = if is_file {
        let worktree = std::path::Path::new(&request.cwd);
        match crate::mcp::file_targets::validate_read_path(worktree, &payload.target) {
            Ok(resolved) => resolved.full_path.display().to_string(),
            Err(error) => return Err(format!("Invalid file target: {error}")),
        }
    } else {
        payload.target.clone()
    };

    if super::pdf::is_pdf_target(&payload.target, is_file) {
        super::pdf::read_pdf_markdown(orch, &resolved_target, &payload.target, is_file).await
    } else {
        super::fetch_web::read_fetch_markdown(orch, &resolved_target).await
    }
}
