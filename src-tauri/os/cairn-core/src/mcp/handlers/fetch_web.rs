//! Web fetch behind `read http(s)://`.
//!
//! The `web-fetch` slot's contract lives here: the built-in **Regular** fetch
//! (the zero-config default), guidance when the selected provider cannot run,
//! the one failure text every provider shares, and the content-type-aware
//! markdown conversion. Which provider answers, and how its request and
//! response look, is a pack declaration resolved by [`crate::web_providers`]:
//! an HTTP adapter run by [`cairn_packs::provider::http`], or an MCP-tool
//! binding called through the readiness-gated MCP layer. There is no
//! per-provider code here.
//!
//! A provider failure (non-2xx, `success: false`, a refused tool call) is an
//! `Err`, so the read renders as `[error]`. A configuration gap (not
//! installed, no key, not authorized, needs setup, MCP server not connected)
//! is `Ok` guidance naming the Library.
//!
//! PDF targets are handled by [`super::pdf`], not here.

use cairn_packs::provider::http::{self, HttpAdapter, Inputs, MarkdownFailure, ResponseMapping};
use cairn_packs::provider::{Adapter, CredentialKind, Slot};

use crate::orchestrator::Orchestrator;
use crate::web_providers::choices::{self, Gap};
use crate::web_providers::{self, McpOutcome, Resolved, LIBRARY_LOCATION};

/// Convert an `http(s)://` target to markdown via the active fetch provider.
pub(crate) async fn read_fetch_markdown(
    orch: &Orchestrator,
    target: &str,
) -> Result<String, String> {
    match web_providers::resolve(&orch.config_dir, Slot::WebFetch) {
        Resolved::Unconfigured => regular_fetch(target).await,
        Resolved::NotInstalled { name } => Ok(not_installed_message(&name)),
        Resolved::Invalid { name, error } => Err(format!(
            "The web-fetch provider `{name}` is installed, but its declaration does not \
             load: {error}. Update or reinstall it from {LIBRARY_LOCATION}."
        )),
        Resolved::Provider(mut active) => pack_fetch(orch, &mut active, target).await,
    }
}

fn not_installed_message(name: &str) -> String {
    format!(
        "The web-fetch provider `{name}` is selected but not installed. Install it from \
         {LIBRARY_LOCATION}, or select Regular there."
    )
}

fn no_key_message(label: &str) -> String {
    format!("No API key set for {label} web fetch. Add it in {LIBRARY_LOCATION}.")
}

fn not_authorized_message(label: &str) -> String {
    format!("{label} web fetch is not authorized. Connect it in {LIBRARY_LOCATION}.")
}

fn no_credential_message(label: &str, kind: &CredentialKind) -> String {
    match kind {
        CredentialKind::Secret { .. } => no_key_message(label),
        CredentialKind::OAuth { .. } => not_authorized_message(label),
    }
}

fn needs_option_message(label: &str, option: &str) -> String {
    format!("{label} web fetch needs its {option}. Set it in {LIBRARY_LOCATION}.")
}

fn not_connected_message(label: &str, server: &str) -> String {
    format!(
        "{label} web fetch needs its MCP server `{server}` connected. Connect it in \
         {LIBRARY_LOCATION}."
    )
}

/// A pack's web-fetch provider: a required choice the credential can fill is
/// filled, any other required option left blank is guidance, then the adapter
/// runs.
async fn pack_fetch(
    orch: &Orchestrator,
    active: &mut web_providers::Active,
    target: &str,
) -> Result<String, String> {
    let label = active.provider.provider.label.clone();
    match choices::fill_required(&orch.config_dir, active).await {
        Gap::Ready => {}
        Gap::Blank(option) => return Ok(needs_option_message(&label, &option)),
        Gap::Credential(kind) => return Ok(no_credential_message(&label, &kind)),
        gap => {
            return choices::choice_guidance(&label, "web fetch", &gap)
                .expect("every other gap is a choice gap")
        }
    }
    let active = &*active;
    let provider = &active.provider;
    let label = label.as_str();
    match &provider.provider.adapter {
        Adapter::Http(adapter) => http_fetch(active, adapter, target).await,
        Adapter::Mcp(binding) => {
            let inputs = Inputs {
                query: None,
                target: Some(target),
                credential: "",
                options: &active.options,
            };
            match web_providers::mcp_call(orch, provider, binding, &inputs).await? {
                McpOutcome::Text(text) => Ok(text),
                McpOutcome::NotConnected => Ok(not_connected_message(label, &binding.server)),
            }
        }
    }
}

/// An HTTP provider: its declared request, the credential sent only to the
/// hosts it lists, and the response read by the slot's contract.
async fn http_fetch(
    active: &web_providers::Active,
    adapter: &HttpAdapter,
    target: &str,
) -> Result<String, String> {
    let provider = &active.provider;
    let label = provider.provider.label.as_str();
    let Some(credential) = &provider.credential else {
        return Err(format!(
            "The {label} web-fetch provider names no credential"
        ));
    };
    let Some(token) =
        web_providers::credential_value(&provider.pack, credential, "web fetch request").await?
    else {
        return Ok(no_credential_message(label, &credential.kind));
    };
    let prepared = http::prepare(
        &provider.provider,
        adapter,
        &Inputs {
            query: None,
            target: Some(target),
            credential: token.expose(),
            options: &active.options,
        },
    );
    let resp = web_providers::send(&provider.pack, credential, &prepared)
        .await
        .map_err(|e| format!("Failed to fetch `{target}` via {label}: {e}"))?;
    let status = resp.status().as_u16();
    let retry_after = header(&resp, reqwest::header::RETRY_AFTER);
    let content_type = header(&resp, reqwest::header::CONTENT_TYPE).unwrap_or_default();
    let body = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read the {label} response for `{target}`: {e}"))?;
    provider_response(
        label,
        &adapter.response,
        target,
        &Response {
            status,
            retry_after: retry_after.as_deref(),
            content_type: &content_type,
            body: &body,
        },
    )
}

fn header(resp: &reqwest::Response, name: reqwest::header::HeaderName) -> Option<String> {
    resp.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// A provider's HTTP response, separated from the network so the slot's
/// contract is testable.
struct Response<'a> {
    status: u16,
    retry_after: Option<&'a str>,
    content_type: &'a str,
    body: &'a [u8],
}

/// The slot's contract for a provider response. Non-2xx is one failure text
/// for every provider, carrying the declared error detail (else the start of
/// the body) and, on a 429, the `Retry-After` the provider sent.
fn provider_response(
    label: &str,
    mapping: &ResponseMapping,
    target: &str,
    response: &Response<'_>,
) -> Result<String, String> {
    let text = String::from_utf8_lossy(response.body);
    if !(200..300).contains(&response.status) {
        let detail = match mapping {
            ResponseMapping::MarkdownJson(mapping) => http::error_detail(mapping, &text),
            _ => None,
        }
        .or_else(|| {
            let snippet: String = text.chars().take(200).collect();
            let snippet = snippet.trim();
            (!snippet.is_empty()).then(|| snippet.to_string())
        })
        .map(|detail| format!(" — {detail}"))
        .unwrap_or_default();
        let retry = match response.retry_after {
            Some(value) if response.status == 429 => format!(" Retry-After: {value}."),
            _ => String::new(),
        };
        return Err(format!(
            "{label} fetch of `{target}` failed: HTTP {}{detail}.{retry}",
            response.status
        ));
    }
    match mapping {
        ResponseMapping::MarkdownBody => {
            body_to_markdown(response.content_type, response.body, target)
        }
        ResponseMapping::MarkdownJson(mapping) => {
            http::map_markdown(mapping, &text).map_err(|failure| match failure {
                MarkdownFailure::Unsuccessful(detail) => format!(
                    "{label} fetch of `{target}` failed: {}",
                    detail.as_deref().unwrap_or("the request was unsuccessful")
                ),
                MarkdownFailure::Missing => {
                    format!("{label} returned no markdown for `{target}`.")
                }
                MarkdownFailure::Shape(error) => {
                    format!("{label} returned an unexpected response for `{target}`: {error}")
                }
            })
        }
        ResponseMapping::Search(_) => Err(format!(
            "The {label} web-fetch provider maps search results, not markdown"
        )),
    }
}

/// The built-in plain-HTTP fetch: an async reqwest GET with content-type-aware
/// conversion.
async fn regular_fetch(target: &str) -> Result<String, String> {
    let resp = cairn_tls::client()
        .get(target)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch `{target}`: {e}"))?;
    read_markdown_response(resp, target).await
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
    use cairn_packs::provider::{load_pack_provider, PackProvider};
    use serde_json::Value;
    use std::path::Path;

    fn bundled(pack: &str) -> PackProvider {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../packs")
            .join(pack);
        load_pack_provider(&root, pack, Slot::WebFetch)
            .unwrap()
            .unwrap()
    }

    fn goldens() -> Value {
        serde_json::from_str(include_str!(
            "../../../../cairn-packs/src/provider/testdata/fetch_goldens.json"
        ))
        .unwrap()
    }

    fn now(case: &Value) -> Value {
        let mut case = case.clone();
        if let Some(Value::Object(changed)) = case.get("now").cloned() {
            for (key, value) in changed {
                case[key.as_str()] = value;
            }
        }
        case
    }

    /// What an agent read from each captured response before the port is what
    /// it reads after it, except where a case records a deliberate change.
    #[test]
    fn every_captured_response_reads_as_recorded() {
        let goldens = goldens();
        let target = goldens["target"].as_str().unwrap();
        for case in goldens["responses"].as_array().unwrap() {
            let case = now(case);
            let pack = case["provider"].as_str().unwrap();
            let provider = bundled(pack);
            let got = provider_response(
                &provider.provider.label,
                &provider.provider.http().unwrap().response,
                target,
                &Response {
                    status: case["status"].as_u64().unwrap() as u16,
                    retry_after: case["retryAfter"].as_str(),
                    content_type: case["contentType"].as_str().unwrap_or("application/json"),
                    body: case["body"].as_str().unwrap().as_bytes(),
                },
            );
            match (case.get("ok").and_then(Value::as_str), got) {
                (Some(ok), Ok(text)) => assert_eq!(text, ok, "{pack}: {}", case["body"]),
                (None, Err(error)) => assert_eq!(
                    error,
                    case["err"].as_str().unwrap(),
                    "{pack}: {}",
                    case["body"]
                ),
                (want, got) => panic!("{pack}: wanted {want:?}, got {got:?}"),
            }
        }
    }

    /// Guidance names the provider by its declared label and the Library.
    #[test]
    fn guidance_reads_as_recorded() {
        let guidance = &goldens()["guidance"];
        let now = |key: &str| guidance[key]["now"].as_str().unwrap().to_string();
        assert_eq!(no_key_message("Firecrawl"), now("firecrawl_no_key"));
        assert_eq!(
            not_authorized_message("Cloudflare Browser Run"),
            now("cloudflare_not_authorized")
        );
        assert_eq!(
            needs_option_message("Cloudflare Browser Run", "Cloudflare account"),
            now("cloudflare_no_account")
        );
        let choice = |value: &str, label: &str| cairn_packs::provider_options::Choice {
            value: value.into(),
            label: label.into(),
        };
        let guidance = |gap: Gap| {
            choices::choice_guidance("Cloudflare Browser Run", "web fetch", &gap).unwrap()
        };
        let choose = guidance(Gap::Choose {
            option: "Cloudflare account".into(),
            choices: vec![
                choice("6f0c1e2a9b", "Acme"),
                choice("9d3b7c4e1f", "Personal"),
            ],
        });
        assert_eq!(
            choose.as_deref(),
            Ok(now("cloudflare_choose_account").as_str())
        );
        let unreachable = guidance(Gap::Unreachable {
            option: "Cloudflare account".into(),
        });
        assert_eq!(
            unreachable.as_deref(),
            Ok(now("cloudflare_no_reachable_account").as_str())
        );
        // The operator never sees an account id: guidance names accounts only.
        assert!(!choose.unwrap().contains("6f0c1e2a9b"));
        let many = guidance(Gap::Choose {
            option: "Cloudflare account".into(),
            choices: (1..=7)
                .map(|n| choice(&format!("id-{n}"), &format!("A{n}")))
                .collect(),
        })
        .unwrap();
        assert!(
            many.contains("reaches 7: A1, A2, A3, A4, A5, 2 more."),
            "{many}"
        );
        assert!(!many.contains("id-"), "{many}");
        assert_eq!(
            not_connected_message("bmd", "bmd"),
            now("bmd_not_connected")
        );
        for message in [
            not_installed_message("ghost"),
            no_key_message("X"),
            not_authorized_message("X"),
            needs_option_message("X", "Y"),
            not_connected_message("X", "s"),
        ] {
            assert!(message.contains(LIBRARY_LOCATION), "{message}");
            assert!(!message.contains("Settings"), "{message}");
        }
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
