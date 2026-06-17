//! Web and local-PDF reads, routed through a configurable web-fetch provider.
//!
//! `read` detects `http(s)://` targets and local `.pdf` paths in `cairn-cli`
//! and forwards them here as a `read_web` callback. The active provider
//! (`config::web_services`) decides how the target becomes markdown:
//!
//! - **regular** (the default, depending on nothing): an async reqwest GET,
//!   content-type aware — `text/html` is converted to markdown via `htmd`, while
//!   JSON / text / markdown pass through unchanged. A plain HTTP GET cannot
//!   extract PDF text, so `.pdf` targets return a clear guidance message.
//! - **command** (e.g. `bmd {url} --md --stdout`): spawn the CLI and read its
//!   markdown from stdout. Local PDFs work because the command receives the
//!   resolved absolute path via `{url}`.
//! - **http** (e.g. Jina Reader): a reqwest request with templated `url`,
//!   `method`, and `headers`; `${VAR}` secrets resolve from the OS keychain.
//!
//! Running through cairn-core (rather than directly in `cairn-cli`) keeps PATH
//! resolution correct in the signed app and gives `cairn-server` headless
//! parity via the shared dispatcher. Not-installed / not-logged-in / fetch
//! failures surface as clean guidance text — never a panic.

use crate::config::web_services::ActiveFetch;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ReadWebPayload {
    /// URL (`http(s)://…`) or absolute local file path (e.g. a `.pdf`).
    target: String,
}

/// Handle a `read_web` callback: convert a URL or local PDF to markdown via the
/// active web-fetch provider. Thin String entry kept for the legacy dispatch
/// route; the batch path calls [`read_web_markdown`] so it can window the
/// markdown and surface a failure as an `Error`-kind segment.
pub async fn handle_read_web(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    match read_web_markdown(orch, request).await {
        Ok(markdown) => markdown,
        Err(message) => message,
    }
}

/// Convert a URL or local PDF to markdown via the active web-fetch provider.
/// `Ok` is the raw markdown; `Err` is a clean guidance message (not installed /
/// not logged in / fetch failed / unsupported).
pub(crate) async fn read_web_markdown(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<String, String> {
    let payload: ReadWebPayload = super::parse_payload(request)?;

    // `file:` targets (local PDFs) resolve against the worktree first, so a
    // command provider receives an absolute path and traversal stays confined.
    // URLs pass through unchanged.
    let is_file = payload.target.starts_with("file:");
    let resolved_target = if is_file {
        let worktree = std::path::Path::new(&request.cwd);
        match crate::mcp::git::validate_read_path(worktree, &payload.target) {
            Ok(resolved) => resolved.full_path.display().to_string(),
            Err(error) => return Err(format!("Invalid file target: {error}")),
        }
    } else {
        payload.target.clone()
    };

    match crate::config::web_services::resolve_active_fetch(&orch.config_dir) {
        ActiveFetch::Regular => regular_fetch(&resolved_target, &payload.target, is_file).await,
        ActiveFetch::Provider { name, config } => match config.transport.as_str() {
            "command" => command_fetch(&name, &config, &resolved_target, &payload.target),
            "http" => http_fetch(&name, &config, &resolved_target).await,
            other => Err(format!(
                "Web-fetch provider `{name}` has an unknown transport `{other}` (expected `command` or `http`)."
            )),
        },
    }
}

/// The built-in plain-HTTP fetch: an async reqwest GET with content-type-aware
/// conversion. PDFs cannot be extracted by a plain GET, so they return guidance.
async fn regular_fetch(target: &str, original: &str, is_file: bool) -> Result<String, String> {
    if is_pdf_target(original, is_file) {
        return Err(pdf_guidance());
    }
    let resp = reqwest::get(target)
        .await
        .map_err(|e| format!("Failed to fetch `{original}`: {e}"))?;
    read_markdown_response(resp, original).await
}

/// An `http`-transport provider: a reqwest request with templated url / method /
/// headers (`${VAR}` secrets resolved from the keychain under the provider name).
async fn http_fetch(
    name: &str,
    config: &crate::config::web_services::WebServiceConfig,
    target: &str,
) -> Result<String, String> {
    let url = config
        .expanded_url(target)
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| format!("Web-fetch provider `{name}` has no `url` configured."))?;
    let method = reqwest::Method::from_bytes(config.method().to_uppercase().as_bytes())
        .map_err(|_| {
            format!(
                "Web-fetch provider `{name}` has an invalid HTTP method `{}`.",
                config.method()
            )
        })?;
    let client = reqwest::Client::new();
    let mut req = client.request(method, &url);
    for (key, value) in config.expanded_headers(target, name) {
        req = req.header(key, value);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Failed to fetch `{target}` via `{name}`: {e}"))?;
    read_markdown_response(resp, target).await
}

/// A `command`-transport provider: spawn the CLI with `{url}`-expanded args and
/// read markdown from stdout. Generalizes the former hardcoded bmd shell-out.
fn command_fetch(
    name: &str,
    config: &crate::config::web_services::WebServiceConfig,
    resolved_target: &str,
    original_target: &str,
) -> Result<String, String> {
    let program = match config.command.as_deref().filter(|s| !s.trim().is_empty()) {
        Some(program) => program.to_string(),
        None => return Err(format!("Web-fetch provider `{name}` has no `command` configured.")),
    };
    let args = config.expanded_args(resolved_target);
    let output = crate::env::command(&program).args(&args).output();

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
                Err(format!(
                    "`{program}` is not authenticated — log in to enable web and PDF reads (e.g. `{program} login`)."
                ))
            } else {
                Err(format!(
                    "`{program}` failed to read `{original_target}`: {}",
                    combined.trim()
                ))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(format!(
            "`{program}` is not installed — install the `{name}` web-fetch provider's command, or switch providers in Settings → Web Services."
        )),
        Err(e) => Err(format!("Failed to run `{program}`: {e}")),
    }
}

/// Read a reqwest response into markdown: non-2xx becomes a guidance error,
/// `text/html` is converted, everything else passes through.
async fn read_markdown_response(resp: reqwest::Response, what: &str) -> Result<String, String> {
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read response from `{what}`: {e}"))?;
    if !status.is_success() {
        let snippet: String = body.chars().take(200).collect();
        let tail = if snippet.trim().is_empty() {
            String::new()
        } else {
            format!(" — {}", snippet.trim())
        };
        return Err(format!(
            "Fetch of `{what}` failed: HTTP {}{tail}",
            status.as_u16()
        ));
    }
    Ok(convert_body(&content_type, body))
}

/// Convert an HTML body to markdown; pass non-HTML bodies through unchanged. A
/// conversion error degrades to the raw body rather than failing the read.
fn convert_body(content_type: &str, body: String) -> String {
    if content_type.to_lowercase().contains("html") {
        htmd::convert(&body).unwrap_or(body)
    } else {
        body
    }
}

/// Whether the target is a PDF: a local `file:` target (only PDFs reach here) or
/// a URL whose path ends in `.pdf` (ignoring any query string).
fn is_pdf_target(target: &str, is_file: bool) -> bool {
    is_file
        || target
            .split('?')
            .next()
            .unwrap_or(target)
            .to_lowercase()
            .ends_with(".pdf")
}

/// Guidance returned when a PDF is requested under the built-in regular fetch.
fn pdf_guidance() -> String {
    "PDF extraction needs a configured web-fetch provider such as bmd. The built-in regular fetch is plain HTTP and cannot read PDFs — set an active provider in Settings → Web Services.".to_string()
}

/// Heuristic: does a command provider's output indicate an auth/login failure?
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

    #[test]
    fn is_pdf_target_detects_files_and_urls() {
        assert!(is_pdf_target("file:docs/design.pdf", true));
        assert!(is_pdf_target("https://example.com/a.pdf", false));
        assert!(is_pdf_target("https://example.com/a.PDF", false));
        assert!(is_pdf_target("https://example.com/a.pdf?x=1", false));
        assert!(!is_pdf_target("https://example.com/page", false));
        assert!(!is_pdf_target("https://example.com/notpdf.html", false));
    }

    #[test]
    fn convert_body_converts_html_and_passes_through_others() {
        let md = convert_body("text/html; charset=utf-8", "<h1>Hello</h1>".to_string());
        assert_eq!(md.trim(), "# Hello");
        // JSON passes through untouched.
        let json = convert_body("application/json", "{\"a\":1}".to_string());
        assert_eq!(json, "{\"a\":1}");
        // Plain text / markdown passes through untouched.
        let text = convert_body("text/plain", "# already md".to_string());
        assert_eq!(text, "# already md");
    }

    #[test]
    fn pdf_guidance_mentions_a_provider() {
        let msg = pdf_guidance();
        assert!(msg.to_lowercase().contains("pdf"));
        assert!(msg.contains("bmd"));
    }
}
