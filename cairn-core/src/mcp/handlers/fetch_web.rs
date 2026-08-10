//! Typed web-fetch adapters.
//!
//! The active provider (`config::web_fetch`) decides how an `http(s)://` target
//! becomes markdown. Each provider is a real adapter that knows its own request
//! and response shape:
//!
//! - **Regular** (the default, depending on nothing): an async reqwest GET,
//!   content-type aware — `text/html` is converted via `htmd`, while JSON / text
//!   / markdown pass through unchanged.
//! - **Jina / Firecrawl**: provider API-key requests normalized to markdown.
//! - **Cloudflare**: Browser Run with a refreshed provider OAuth token.
//! - **bmd**: calls bmd's `fetch` tool through the host MCP gateway, reusing the
//!   OAuth connection the user established for the bmd MCP server. No pasteable
//!   key — auth rides on the existing MCP connection.
//!
//! PDF targets are handled by [`super::pdf`], not here.

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
            FetchProviderId::Cloudflare => cloudflare_fetch(target, &options).await,
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
        .header("Authorization", format!("Bearer {}", key.expose()))
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
        .header("Authorization", format!("Bearer {}", key.expose()))
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

/// Cloudflare Browser Run: POST to the markdown Quick Action and select either
/// the Kitesurf isolate browser or full Chromium through the query string.
async fn cloudflare_fetch(
    target: &str,
    options: &HashMap<String, serde_yaml::Value>,
) -> Result<String, String> {
    // Brokered: the response body below is returned to the agent verbatim on
    // failure, and Cloudflare's error envelope quotes the request it rejected.
    let Some(token) =
        crate::security::broker::mcp_oauth_token("web-fetch/cloudflare", "cloudflare web fetch")
            .await
    else {
        return Ok(
            "Cloudflare web fetch is not authorized. Connect it in Settings → Web Services."
                .to_string(),
        );
    };
    let Some(request) = cloudflare_request(target, options) else {
        return Ok(cloudflare_setup_message());
    };
    let response = reqwest::Client::new()
        .post(&request.url)
        .bearer_auth(token.expose())
        .json(&request.body)
        .send()
        .await
        .map_err(|error| {
            format!("Failed to fetch `{target}` via Cloudflare Browser Run: {error}")
        })?;
    let status = response.status();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let text = response.text().await.map_err(|error| {
        format!("Failed to read Cloudflare Browser Run response for `{target}`: {error}")
    })?;

    if !status.is_success() {
        let detail = cloudflare_error_message(&text).unwrap_or_else(|| {
            text.chars()
                .take(200)
                .collect::<String>()
                .trim()
                .to_string()
        });
        let retry = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            retry_after
                .map(|value| format!(" Retry-After: {value}."))
                .unwrap_or_default()
        } else {
            String::new()
        };
        let detail = if detail.is_empty() {
            String::new()
        } else {
            format!(" — {detail}")
        };
        return Err(format!(
            "Cloudflare Browser Run fetch of `{target}` failed: HTTP {}{detail}.{retry}",
            status.as_u16()
        ));
    }

    parse_cloudflare_response(&text, target)
}

struct CloudflareRequest {
    url: String,
    body: serde_json::Value,
}

fn cloudflare_request(
    target: &str,
    options: &HashMap<String, serde_yaml::Value>,
) -> Option<CloudflareRequest> {
    let account_id = options
        .get("accountId")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let browser = options
        .get("browser")
        .and_then(|value| value.as_str())
        .unwrap_or("kitesurf");
    let wait_until = options
        .get("waitUntil")
        .and_then(|value| value.as_str())
        .unwrap_or("load");
    let mut body = serde_json::json!({ "url": target });
    if wait_until == "networkidle0" {
        body["gotoOptions"] = serde_json::json!({ "waitUntil": "networkidle0" });
    }
    Some(CloudflareRequest {
        url: format!(
            "https://api.cloudflare.com/client/v4/accounts/{account_id}/browser-rendering/markdown?browser={browser}"
        ),
        body,
    })
}

#[derive(Deserialize)]
struct CloudflareResponse {
    success: bool,
    result: Option<String>,
    #[serde(default)]
    errors: Vec<CloudflareError>,
}

#[derive(Deserialize)]
struct CloudflareError {
    message: Option<String>,
}

fn parse_cloudflare_response(text: &str, target: &str) -> Result<String, String> {
    let response: CloudflareResponse = serde_json::from_str(text).map_err(|error| {
        format!("Cloudflare Browser Run returned an unexpected response for `{target}`: {error}")
    })?;
    if !response.success {
        let detail = response
            .errors
            .iter()
            .find_map(|error| error.message.as_deref())
            .unwrap_or("the request was unsuccessful");
        return Err(format!(
            "Cloudflare Browser Run fetch of `{target}` failed: {detail}"
        ));
    }
    response
        .result
        .ok_or_else(|| format!("Cloudflare Browser Run returned no markdown for `{target}`."))
}

fn cloudflare_error_message(text: &str) -> Option<String> {
    serde_json::from_str::<CloudflareResponse>(text)
        .ok()?
        .errors
        .into_iter()
        .find_map(|error| error.message)
}

fn cloudflare_setup_message() -> String {
    "The Cloudflare Browser Run web-fetch provider is missing its account ID. Set your Cloudflare account ID in Settings → Web Services."
        .to_string()
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
    let expanded = config.brokered(&credential_key, "bmd web fetch");
    bmd_fetch_via(gateway.as_ref(), &expanded, &credential_key, source).await
}

/// The gateway call for bmd's `fetch` tool, split out so it is unit-testable
/// against a mock gateway.
async fn bmd_fetch_via(
    gateway: &dyn McpGateway,
    config: &crate::security::BrokeredMcpConfig,
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

/// The stored API key for an `ApiKey` provider, if set and non-empty.
///
/// Brokered, so the key is registered for scrubbing before it reaches an
/// `Authorization` header. A fetch provider returns page content the agent
/// reads directly; an error body that quotes the rejected key would otherwise
/// surface it verbatim.
fn provider_key(id: FetchProviderId) -> Option<crate::security::BrokeredSecret> {
    let var = id.auth().secret_var()?;
    crate::security::broker::web_provider_key(id.as_str(), var, "web fetch request")
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
            _: &crate::security::BrokeredMcpConfig,
        ) -> Result<McpToolCatalog, String> {
            Ok(McpToolCatalog::default())
        }
        async fn list_resources(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
        ) -> Result<Vec<McpResourceDef>, String> {
            Ok(vec![])
        }
        async fn read_resource(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            _: &str,
        ) -> Result<String, String> {
            Ok(String::new())
        }
        async fn call_tool(
            &self,
            _session: &str,
            _server: &str,
            _config: &crate::security::BrokeredMcpConfig,
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

    fn bmd_config() -> crate::security::BrokeredMcpConfig {
        let authored: crate::config::mcp_servers::McpServerConfig = serde_json::from_value(
            serde_json::json!({"type": "http", "url": "https://bmd.example/mcp"}),
        )
        .unwrap();
        authored.brokered("bmd", "test")
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

    #[test]
    fn cloudflare_request_builds_kitesurf_and_chromium_variants() {
        let kitesurf = HashMap::from([
            ("accountId".to_string(), "account-123".into()),
            ("browser".to_string(), "kitesurf".into()),
            ("waitUntil".to_string(), "networkidle0".into()),
        ]);
        let request = cloudflare_request("https://example.com", &kitesurf).unwrap();
        assert_eq!(
            request.url,
            "https://api.cloudflare.com/client/v4/accounts/account-123/browser-rendering/markdown?browser=kitesurf"
        );
        assert_eq!(request.body["url"], "https://example.com");
        assert_eq!(request.body["gotoOptions"]["waitUntil"], "networkidle0");

        let chromium = HashMap::from([
            ("accountId".to_string(), "account-123".into()),
            ("browser".to_string(), "chromium".into()),
            ("waitUntil".to_string(), "load".into()),
        ]);
        let request = cloudflare_request("https://example.com", &chromium).unwrap();
        assert!(request.url.ends_with("?browser=chromium"));
        assert!(request.body.get("gotoOptions").is_none());
    }

    #[test]
    fn cloudflare_response_parses_success_and_errors() {
        assert_eq!(
            parse_cloudflare_response(
                r##"{"success":true,"result":"# Rendered","errors":[]}"##,
                "https://example.com"
            )
            .unwrap(),
            "# Rendered"
        );
        let error = parse_cloudflare_response(
            r#"{"success":false,"errors":[{"code":10000,"message":"Authentication error"}]}"#,
            "https://example.com",
        )
        .unwrap_err();
        assert!(error.contains("Authentication error"), "{error}");

        let error =
            parse_cloudflare_response(r#"{"success":true,"errors":[]}"#, "https://example.com")
                .unwrap_err();
        assert!(error.contains("returned no markdown"), "{error}");
    }
}
