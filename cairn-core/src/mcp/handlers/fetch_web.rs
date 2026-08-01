//! Typed web-fetch adapters.
//!
//! The active provider (`config::web_fetch`) decides how an `http(s)://` target
//! becomes markdown. Each provider is a real adapter that knows its own request
//! and response shape:
//!
//! - **Regular** (the default, depending on nothing): an async reqwest GET,
//!   content-type aware — `text/html` is converted via `htmd`, while JSON / text
//!   / markdown pass through unchanged.
//! - **Jina / Firecrawl**: a reqwest request to the provider's endpoint with an
//!   API key from the OS keychain, normalized to markdown.
//! - **bmd**: calls bmd's `fetch` tool through the host MCP gateway, reusing the
//!   OAuth connection the user established for the bmd MCP server. No pasteable
//!   key — auth rides on the existing MCP connection.
//!
//! PDF targets are handled by [`super::pdf`], not here.

use crate::config::mcp_servers::McpServerConfig;
use crate::config::web_fetch::{self, ActiveFetch, FetchProviderId};
use crate::mcp::gateway::McpGateway;
use crate::orchestrator::Orchestrator;
use serde::Deserialize;
use std::collections::HashMap;

/// Convert an `http(s)://` target to markdown via the active fetch provider.
pub(crate) async fn read_fetch_markdown(
    orch: &Orchestrator,
    target: &str,
) -> Result<String, String> {
    match web_fetch::resolve_active_fetch(&orch.config_dir) {
        ActiveFetch::Regular => regular_fetch(target).await,
        ActiveFetch::Provider { id, options } => match id {
            FetchProviderId::Jina => jina_fetch(target).await,
            FetchProviderId::Firecrawl => firecrawl_fetch(target, &options).await,
            FetchProviderId::Bmd => bmd_fetch(orch, target).await,
        },
    }
}

/// The built-in plain-HTTP fetch: an async reqwest GET with content-type-aware
/// conversion.
async fn regular_fetch(target: &str) -> Result<String, String> {
    let resp = reqwest::get(target)
        .await
        .map_err(|e| format!("Failed to fetch `{target}`: {e}"))?;
    read_markdown_response(resp, target).await
}

/// Jina Reader: GET `https://r.jina.ai/<url>` with the API key, returning the
/// page already rendered as markdown.
async fn jina_fetch(target: &str) -> Result<String, String> {
    let key = match provider_key(FetchProviderId::Jina) {
        Some(k) => k,
        None => return Ok(missing_key_message(FetchProviderId::Jina)),
    };
    let url = format!("https://r.jina.ai/{target}");
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {key}"))
        .header("X-Return-Format", "markdown")
        .send()
        .await
        .map_err(|e| format!("Failed to fetch `{target}` via Jina: {e}"))?;
    read_markdown_response(resp, target).await
}

/// Firecrawl: POST to the scrape endpoint asking for markdown, then pull the
/// `data.markdown` field out of the JSON response.
async fn firecrawl_fetch(
    target: &str,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<String, String> {
    let key = match provider_key(FetchProviderId::Firecrawl) {
        Some(k) => k,
        None => return Ok(missing_key_message(FetchProviderId::Firecrawl)),
    };
    let only_main = options
        .get("onlyMainContent")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let body = serde_json::json!({
        "url": target,
        "formats": ["markdown"],
        "onlyMainContent": only_main,
    });
    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.firecrawl.dev/v1/scrape")
        .header("Authorization", format!("Bearer {key}"))
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| format!("Failed to fetch `{target}` via Firecrawl: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read Firecrawl response for `{target}`: {e}"))?;
    if !status.is_success() {
        let snippet: String = text.chars().take(200).collect();
        return Err(format!(
            "Firecrawl fetch of `{target}` failed: HTTP {} — {}",
            status.as_u16(),
            snippet.trim()
        ));
    }
    let parsed: FirecrawlResponse = serde_json::from_str(&text)
        .map_err(|e| format!("Firecrawl returned an unexpected response for `{target}`: {e}"))?;
    parsed
        .data
        .and_then(|d| d.markdown)
        .ok_or_else(|| format!("Firecrawl returned no markdown for `{target}`."))
}

#[derive(Deserialize)]
struct FirecrawlResponse {
    data: Option<FirecrawlData>,
}

#[derive(Deserialize)]
struct FirecrawlData {
    markdown: Option<String>,
}

/// bmd: call bmd's `fetch` tool through the host MCP gateway, reusing the OAuth
/// connection established for the configured bmd MCP server.
pub(crate) async fn bmd_fetch(orch: &Orchestrator, source: &str) -> Result<String, String> {
    let Some(gateway) = orch.mcp_gateway() else {
        return Err(
            "The bmd web-fetch provider needs the MCP gateway, which is not available in this host."
                .to_string(),
        );
    };
    let servers = crate::config::mcp_servers::load_workspace_mcp_servers(&orch.config_dir);
    let Some(config) = servers.get("bmd") else {
        return Ok(bmd_setup_message());
    };
    let credential_key = crate::config::secrets::credential_key("bmd", None);
    let expanded = config.expanded(&credential_key);
    bmd_fetch_via(gateway.as_ref(), &expanded, &credential_key, source).await
}

/// The gateway call for bmd's `fetch` tool, split out so it is unit-testable
/// against a mock gateway.
async fn bmd_fetch_via(
    gateway: &dyn McpGateway,
    config: &McpServerConfig,
    credential_key: &str,
    source: &str,
) -> Result<String, String> {
    gateway
        .call_tool(
            "cairn-web-fetch",
            credential_key,
            config,
            "fetch",
            serde_json::json!({ "source": source }),
            Some(120_000),
        )
        .await
        // Web fetch is text-only; an image block from a fetch tool has no place
        // in markdown output, so collapse to the composed text.
        .map(|result| result.text)
}

fn bmd_setup_message() -> String {
    "The bmd web-fetch provider is not configured. Add and connect the bmd MCP server in Settings → Web Services, or switch providers."
        .to_string()
}

/// The keychain API key for an `ApiKey` provider, if set and non-empty.
fn provider_key(id: FetchProviderId) -> Option<String> {
    let var = id.auth().secret_var()?;
    let key = crate::config::secrets::credential_key(id.as_str(), None);
    crate::config::secrets::get_secret(&key, var).filter(|k| !k.trim().is_empty())
}

fn missing_key_message(id: FetchProviderId) -> String {
    format!(
        "No API key set for {} web fetch. Add it in Settings → Web Services.",
        id.label()
    )
}

/// Read a reqwest response into markdown: non-2xx becomes a guidance error,
/// and the body is decided by its content type rather than assumed textual.
async fn read_markdown_response(resp: reqwest::Response, what: &str) -> Result<String, String> {
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read response from `{what}`: {e}"))?;
    if !status.is_success() {
        let snippet: String = String::from_utf8_lossy(&bytes).chars().take(200).collect();
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
    body_to_markdown(&content_type, &bytes, what)
}

/// Turn a fetched body into markdown, refusing anything that is not text.
///
/// A tool result is conversation: whatever this returns is persisted into the
/// agent's transcript and replayed on every later turn. Decoded binary was
/// observed corrupting a stored conversation outright — a PDF fetched here (a
/// PDF URL whose path carries no `.pdf` suffix routes to web fetch, not to the
/// PDF service) was passed through as bytes and broke the harness's transcript
/// across thirteen lines, destroying a tool result. So a PDF is extracted like
/// any other PDF, and a body that is not text at all becomes guidance instead of
/// noise.
fn body_to_markdown(content_type: &str, bytes: &[u8], what: &str) -> Result<String, String> {
    let kind = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_lowercase();

    if kind == "application/pdf" || kind == "application/x-pdf" {
        return pdf_extract::extract_text_from_mem(bytes)
            .map_err(|e| format!("Failed to extract text from the PDF at `{what}`: {e}"));
    }
    if !kind.is_empty() && !is_textual_type(&kind) {
        return Err(format!(
            "`{what}` returned {kind} ({} bytes), which is not text. Fetch a textual representation, or download it with `run` and read the file.",
            bytes.len()
        ));
    }
    // An absent or lying content type is common, so the bytes get the final say.
    let body = std::str::from_utf8(bytes).map_err(|_| {
        format!(
            "`{what}` returned {} bytes that are not valid UTF-8 text. Fetch a textual representation, or download it with `run` and read the file.",
            bytes.len()
        )
    })?;
    if body.contains('\0') {
        return Err(format!(
            "`{what}` returned binary content ({} bytes). Fetch a textual representation, or download it with `run` and read the file.",
            bytes.len()
        ));
    }
    Ok(convert_body(&kind, body.to_string()))
}

/// Whether a content type carries text an agent can read. Everything outside
/// this set is refused rather than decoded.
fn is_textual_type(kind: &str) -> bool {
    kind.starts_with("text/")
        || [
            "json",
            "xml",
            "html",
            "javascript",
            "ecmascript",
            "markdown",
            "yaml",
            "csv",
            "x-www-form-urlencoded",
        ]
        .iter()
        .any(|textual| kind.contains(textual))
}

/// Convert an HTML body to markdown; pass non-HTML bodies through unchanged.
pub(crate) fn convert_body(content_type: &str, body: String) -> String {
    if content_type.to_lowercase().contains("html") {
        htmd::convert(&body).unwrap_or(body)
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::gateway::{McpResourceDef, McpToolCallResult, McpToolCatalog};
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockGateway {
        last_tool: Mutex<Option<String>>,
        last_args: Mutex<Option<serde_json::Value>>,
    }

    #[async_trait]
    impl McpGateway for MockGateway {
        async fn list_tools(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
        ) -> Result<McpToolCatalog, String> {
            Ok(McpToolCatalog::default())
        }
        async fn list_resources(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
        ) -> Result<Vec<McpResourceDef>, String> {
            Ok(vec![])
        }
        async fn read_resource(
            &self,
            _: &str,
            _: &str,
            _: &McpServerConfig,
            _: &str,
        ) -> Result<String, String> {
            Ok(String::new())
        }
        async fn call_tool(
            &self,
            _session: &str,
            _server: &str,
            _config: &McpServerConfig,
            tool: &str,
            args: serde_json::Value,
            _timeout: Option<u32>,
        ) -> Result<McpToolCallResult, String> {
            *self.last_tool.lock().unwrap() = Some(tool.to_string());
            *self.last_args.lock().unwrap() = Some(args.clone());
            Ok(McpToolCallResult {
                text: "# bmd markdown".to_string(),
                images: Vec::new(),
            })
        }
        async fn close_session(&self, _: &str) {}
    }

    fn bmd_config() -> McpServerConfig {
        McpServerConfig {
            transport: "http".into(),
            command: None,
            args: vec![],
            env: HashMap::new(),
            url: Some("https://bmd.example/mcp".into()),
            headers: HashMap::new(),
            enabled: true,
            oauth: None,
        }
    }

    #[tokio::test]
    async fn bmd_fetch_via_calls_fetch_tool_with_source() {
        let gw = MockGateway::default();
        let out = bmd_fetch_via(&gw, &bmd_config(), "bmd", "https://example.com")
            .await
            .unwrap();
        assert_eq!(out, "# bmd markdown");
        assert_eq!(gw.last_tool.lock().unwrap().as_deref(), Some("fetch"));
        let args = gw.last_args.lock().unwrap().clone().unwrap();
        assert_eq!(args["source"], "https://example.com");
    }

    #[test]
    fn binary_bodies_are_refused_rather_than_decoded_into_the_conversation() {
        // The observed corruption: a PDF fetched through web fetch (its URL has
        // no `.pdf` suffix, so it never reaches the PDF service).
        let pdf = b"%PDF-1.4\n\x00\x01\x02 not really a pdf";
        let error = body_to_markdown("application/pdf", pdf, "https://arxiv.org/pdf/2511.11847")
            .unwrap_err();
        assert!(error.contains("extract text from the PDF"), "{error}");

        let error = body_to_markdown("image/png", b"\x89PNG\r\n\x1a\n", "https://x/y").unwrap_err();
        assert!(error.contains("image/png"), "{error}");

        // No content type at all: the bytes decide.
        let error = body_to_markdown("", b"\xff\xfe\x00\x01", "https://x/y").unwrap_err();
        assert!(error.contains("not valid UTF-8"), "{error}");

        // Decodable but NUL-bearing: still binary, still refused.
        let error = body_to_markdown("text/plain", b"head\0tail", "https://x/y").unwrap_err();
        assert!(error.contains("binary content"), "{error}");
    }

    #[test]
    fn textual_bodies_still_convert_and_pass_through() {
        let html = body_to_markdown("text/html; charset=utf-8", b"<h1>Hello</h1>", "u").unwrap();
        assert_eq!(html.trim(), "# Hello");
        let json = body_to_markdown("application/json", b"{\"a\":1}", "u").unwrap();
        assert_eq!(json, "{\"a\":1}");
        let unknown = body_to_markdown("", b"plain words", "u").unwrap();
        assert_eq!(unknown, "plain words");
    }

    #[test]
    fn convert_body_converts_html_and_passes_through_others() {
        let md = convert_body("text/html; charset=utf-8", "<h1>Hello</h1>".to_string());
        assert_eq!(md.trim(), "# Hello");
        let json = convert_body("application/json", "{\"a\":1}".to_string());
        assert_eq!(json, "{\"a\":1}");
        let text = convert_body("text/plain", "# already md".to_string());
        assert_eq!(text, "# already md");
    }
}
