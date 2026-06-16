//! Web and local-PDF reads, routed through the `bmd` CLI.
//!
//! `read` detects `http(s)://` targets and local `.pdf` paths in `cairn-cli`
//! and forwards them here as a `read_web` callback. We shell out to `bmd`
//! (`bmd <target> --md --stdout`), which emits markdown on stdout. Running
//! through cairn-core (rather than directly in `cairn-cli`) keeps PATH
//! resolution correct in the signed app and gives `cairn-server` headless
//! parity via the shared dispatcher.
//!
//! `bmd` is a hosted-service client. When it is not installed or not logged in
//! we surface a clean guidance message as the tool text — never a panic.

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ReadWebPayload {
    /// URL (`http(s)://…`) or absolute local file path (e.g. a `.pdf`).
    target: String,
}

/// Handle a `read_web` callback: convert a URL or local PDF to markdown via
/// `bmd`. Thin String entry kept for the legacy dispatch route; the batch path
/// calls [`read_web_markdown`] so it can window the markdown and surface a bmd
/// failure as an `Error`-kind segment. Windowing now lives in the view layer, so
/// this returns the full markdown.
pub async fn handle_read_web(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    match read_web_markdown(orch, request).await {
        Ok(markdown) => markdown,
        Err(message) => message,
    }
}

/// Convert a URL or local PDF to markdown via `bmd`. `Ok` is the raw markdown;
/// `Err` is a clean guidance message (not installed / not logged in / failed).
pub(crate) async fn read_web_markdown(
    _orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<String, String> {
    let payload: ReadWebPayload = super::parse_payload(request)?;

    // URLs go straight to bmd. `file:` targets (local PDFs) are resolved against
    // the worktree first so bmd receives an absolute path and traversal stays
    // confined to the worktree.
    let bmd_arg = if payload.target.starts_with("file:") {
        let worktree = std::path::Path::new(&request.cwd);
        match crate::mcp::git::validate_read_path(worktree, &payload.target) {
            Ok(resolved) => resolved.full_path.display().to_string(),
            Err(error) => return Err(format!("Invalid file target: {error}")),
        }
    } else {
        payload.target.clone()
    };

    let output = crate::env::command("bmd")
        .args([bmd_arg.as_str(), "--md", "--stdout"])
        .output();

    match output {
        Ok(out) if out.status.success() => Ok(String::from_utf8_lossy(&out.stdout).to_string()),
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let combined = if stderr.trim().is_empty() {
                String::from_utf8_lossy(&out.stdout).to_string()
            } else {
                stderr.to_string()
            };
            if mentions_login(&combined) {
                Err(
                    "bmd is not logged in — run `bmd login` to enable web and PDF reads."
                        .to_string(),
                )
            } else {
                Err(format!(
                    "bmd failed to read `{}`: {}",
                    payload.target,
                    combined.trim()
                ))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(
            "bmd is not installed — see https://become.md to install it, then `bmd login`."
                .to_string(),
        ),
        Err(e) => Err(format!("Failed to run bmd: {}", e)),
    }
}

/// Heuristic: does bmd's output indicate an auth/login failure?
fn mentions_login(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("login")
        || lower.contains("log in")
        || lower.contains("unauthorized")
        || lower.contains("not authenticated")
        || lower.contains("authentication")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mentions_login_detects_auth_failures() {
        assert!(mentions_login("Error: please run bmd login"));
        assert!(mentions_login("401 Unauthorized"));
        assert!(mentions_login("Not authenticated"));
        assert!(!mentions_login("404 page not found"));
        assert!(!mentions_login("connection refused"));
    }
}
