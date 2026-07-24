//! Typed web search behind `cairn://websearch?q=`.
//!
//! Each provider is a real typed adapter: it builds the request the provider
//! actually expects ([`build_request`]) and deserializes that provider's real
//! JSON shape into a common [`SearchResult`] ([`parse`]). There is no generic
//! `{query}` template and no JSON-guessing normalizer — adding a provider means
//! writing an adapter here and a catalog entry in [`crate::config::web_search`].
//!
//! The active provider is resolved from `config::web_search` (selected by
//! `activeWebSearch`); the API key comes from the OS keychain keyed by provider
//! id. Unconfigured / missing-key / auth-failure paths return clean guidance
//! text — never a panic.

use crate::config::web_search::{self, ActiveSearch, SearchProviderId};
use crate::orchestrator::Orchestrator;
use serde::Deserialize;
use std::collections::HashMap;

type Options = HashMap<String, serde_yaml::Value>;

/// A normalized search result row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SearchResult {
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub score: Option<f64>,
}

/// A fully-resolved HTTP request, separated from the network so it is
/// unit-testable. Built by [`build_request`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}

/// Run a web search for `query` through the active typed provider and return a
/// rendered ranked list. Guidance (unconfigured / missing key / auth failure)
/// is returned as `Ok` text so the read surfaces it as the body; `Err` is
/// reserved for unexpected transport/parse failures (also surfaced as the body).
pub(crate) async fn search_web(orch: &Orchestrator, query: &str) -> Result<String, String> {
    let (id, options) = match web_search::resolve_active_search(&orch.config_dir) {
        ActiveSearch::Unconfigured => return Ok(no_provider_message()),
        ActiveSearch::Provider { id, options } => (id, options),
    };

    let api_key = match crate::config::secrets::get_secret(
        &crate::config::secrets::credential_key(id.as_str(), None),
        id.secret_var(),
    ) {
        Some(k) if !k.trim().is_empty() => k,
        _ => return Ok(missing_key_message(id)),
    };

    let prepared = build_request(id, query, &options, &api_key);
    let client = reqwest::Client::new();
    let resp = send(&client, &prepared)
        .send()
        .await
        .map_err(|e| format!("Web search via {} failed: {e}", id.label()))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("Web search via {} failed to read response: {e}", id.label()))?;
    if !status.is_success() {
        return Ok(http_error_message(id, status.as_u16(), &body));
    }
    let results = parse(id, &body)?;
    Ok(render(id, query, &results))
}

/// Build the `reqwest` request from a [`PreparedRequest`].
fn send(client: &reqwest::Client, prepared: &PreparedRequest) -> reqwest::RequestBuilder {
    let method =
        reqwest::Method::from_bytes(prepared.method.as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut req = client.request(method, &prepared.url);
    for (key, value) in &prepared.headers {
        req = req.header(key, value);
    }
    if let Some(body) = &prepared.body {
        req = req.body(body.clone());
    }
    req
}

/// Build the provider-specific request (endpoint, method, auth header, body).
/// Pure: no network, no keychain — the api key is passed in.
fn build_request(
    id: SearchProviderId,
    query: &str,
    options: &Options,
    api_key: &str,
) -> PreparedRequest {
    match id {
        SearchProviderId::Tavily => tavily_request(query, options, api_key),
        SearchProviderId::Exa => exa_request(query, options, api_key),
        SearchProviderId::Brave => brave_request(query, options, api_key),
        SearchProviderId::Jina => jina_request(query, options, api_key),
    }
}

/// Parse a provider's raw JSON body into the common result shape.
fn parse(id: SearchProviderId, body: &str) -> Result<Vec<SearchResult>, String> {
    match id {
        SearchProviderId::Tavily => parse_tavily(body),
        SearchProviderId::Exa => parse_exa(body),
        SearchProviderId::Brave => parse_brave(body),
        SearchProviderId::Jina => parse_jina(body),
    }
}

// --- Tavily -----------------------------------------------------------------

fn tavily_request(query: &str, options: &Options, api_key: &str) -> PreparedRequest {
    let body = serde_json::json!({
        "query": query,
        "search_depth": opt_str(options, "searchDepth", "basic"),
        "max_results": opt_u64(options, "maxResults", 5),
        "topic": opt_str(options, "topic", "general"),
    });
    PreparedRequest {
        method: "POST".to_string(),
        url: "https://api.tavily.com/search".to_string(),
        headers: vec![
            ("Authorization".to_string(), format!("Bearer {api_key}")),
            ("Content-Type".to_string(), "application/json".to_string()),
        ],
        body: Some(body.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct TavilyResponse {
    #[serde(default)]
    results: Vec<TavilyResult>,
}

#[derive(Debug, Deserialize)]
struct TavilyResult {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    score: Option<f64>,
}

fn parse_tavily(body: &str) -> Result<Vec<SearchResult>, String> {
    let parsed: TavilyResponse = decode(SearchProviderId::Tavily, body)?;
    Ok(parsed
        .results
        .into_iter()
        .map(|r| SearchResult {
            title: pick_title(r.title, &r.url),
            url: r.url,
            snippet: r.content,
            score: r.score,
        })
        .collect())
}

// --- Exa --------------------------------------------------------------------

fn exa_request(query: &str, options: &Options, api_key: &str) -> PreparedRequest {
    let body = serde_json::json!({
        "query": query,
        "numResults": opt_u64(options, "numResults", 10),
        "type": opt_str(options, "type", "auto"),
        "contents": { "text": true },
    });
    PreparedRequest {
        method: "POST".to_string(),
        url: "https://api.exa.ai/search".to_string(),
        headers: vec![
            ("x-api-key".to_string(), api_key.to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
        ],
        body: Some(body.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct ExaResponse {
    #[serde(default)]
    results: Vec<ExaResult>,
}

#[derive(Debug, Deserialize)]
struct ExaResult {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    url: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    score: Option<f64>,
}

fn parse_exa(body: &str) -> Result<Vec<SearchResult>, String> {
    let parsed: ExaResponse = decode(SearchProviderId::Exa, body)?;
    Ok(parsed
        .results
        .into_iter()
        .map(|r| SearchResult {
            title: pick_title(r.title.unwrap_or_default(), &r.url),
            url: r.url,
            snippet: r.text,
            score: r.score,
        })
        .collect())
}

// --- Brave ------------------------------------------------------------------

fn brave_request(query: &str, options: &Options, api_key: &str) -> PreparedRequest {
    let url = format!(
        "https://api.search.brave.com/res/v1/web/search?q={}&count={}&safesearch={}",
        encode_component(query),
        opt_u64(options, "count", 10),
        encode_component(&opt_str(options, "safesearch", "moderate")),
    );
    PreparedRequest {
        method: "GET".to_string(),
        url,
        headers: vec![
            ("X-Subscription-Token".to_string(), api_key.to_string()),
            ("Accept".to_string(), "application/json".to_string()),
        ],
        body: None,
    }
}

#[derive(Debug, Deserialize)]
struct BraveResponse {
    #[serde(default)]
    web: Option<BraveWeb>,
}

#[derive(Debug, Deserialize)]
struct BraveWeb {
    #[serde(default)]
    results: Vec<BraveResult>,
}

#[derive(Debug, Deserialize)]
struct BraveResult {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    description: String,
}

fn parse_brave(body: &str) -> Result<Vec<SearchResult>, String> {
    let parsed: BraveResponse = decode(SearchProviderId::Brave, body)?;
    Ok(parsed
        .web
        .map(|w| w.results)
        .unwrap_or_default()
        .into_iter()
        .map(|r| SearchResult {
            title: pick_title(r.title, &r.url),
            url: r.url,
            snippet: r.description,
            score: None,
        })
        .collect())
}

// --- Jina -------------------------------------------------------------------

fn jina_request(query: &str, options: &Options, api_key: &str) -> PreparedRequest {
    let url = format!(
        "https://s.jina.ai/?q={}&count={}",
        encode_component(query),
        opt_u64(options, "count", 10),
    );
    PreparedRequest {
        method: "GET".to_string(),
        url,
        headers: vec![
            ("Authorization".to_string(), format!("Bearer {api_key}")),
            ("Accept".to_string(), "application/json".to_string()),
        ],
        body: None,
    }
}

#[derive(Debug, Deserialize)]
struct JinaResponse {
    #[serde(default)]
    data: Vec<JinaResult>,
}

#[derive(Debug, Deserialize)]
struct JinaResult {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    content: String,
}

fn parse_jina(body: &str) -> Result<Vec<SearchResult>, String> {
    let parsed: JinaResponse = decode(SearchProviderId::Jina, body)?;
    Ok(parsed
        .data
        .into_iter()
        .map(|r| {
            let snippet = if r.description.trim().is_empty() {
                r.content
            } else {
                r.description
            };
            SearchResult {
                title: pick_title(r.title, &r.url),
                url: r.url,
                snippet,
                score: None,
            }
        })
        .collect())
}

// --- Shared helpers ---------------------------------------------------------

fn decode<T: for<'de> Deserialize<'de>>(id: SearchProviderId, body: &str) -> Result<T, String> {
    serde_json::from_str(body).map_err(|e| {
        format!(
            "Web search via {} returned an unexpected response shape ({e}). Raw response:\n\n{}",
            id.label(),
            truncate(body, 1000)
        )
    })
}

fn pick_title(title: String, url: &str) -> String {
    if title.trim().is_empty() {
        url.to_string()
    } else {
        title
    }
}

fn opt_str(options: &Options, key: &str, default: &str) -> String {
    options
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| default.to_string())
}

fn opt_u64(options: &Options, key: &str, default: u64) -> u64 {
    options
        .get(key)
        .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|i| i.max(0) as u64)))
        .unwrap_or(default)
}

/// Percent-encode a URL query-component (RFC 3986 unreserved set passes through).
fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn truncate(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max).collect();
        format!("{head}\u{2026}")
    }
}

/// Render results as a ranked `title · url · snippet` markdown list.
fn render(id: SearchProviderId, query: &str, results: &[SearchResult]) -> String {
    if results.is_empty() {
        return format!(
            "Web search for \"{query}\" via {} returned no results.",
            id.label()
        );
    }
    let mut out = format!(
        "Web search · \"{query}\" · via {} · {} result(s)\n\n",
        id.label(),
        results.len()
    );
    for (i, r) in results.iter().enumerate() {
        out.push_str(&format!("{}. {}\n   {}\n", i + 1, r.title, r.url));
        let snippet = truncate(&r.snippet.replace('\n', " "), 300);
        if !snippet.is_empty() {
            out.push_str(&format!("   {snippet}\n"));
        }
        out.push('\n');
    }
    out.trim_end().to_string()
}

/// Guidance returned when no web-search provider is configured.
fn no_provider_message() -> String {
    "No web-search provider is configured. Set one up in Settings → Web Services — choose a search provider (Tavily, Exa, Brave, or Jina) and paste its API key — then read `cairn://websearch?q=<your query>`.".to_string()
}

/// Guidance returned when the active provider has no API key stored.
fn missing_key_message(id: SearchProviderId) -> String {
    format!(
        "No API key is set for the {} web-search provider. Add it in Settings → Web Services, then read `cairn://websearch?q=<your query>`.",
        id.label()
    )
}

/// Map a non-2xx provider response to guidance text.
fn http_error_message(id: SearchProviderId, status: u16, body: &str) -> String {
    if status == 401 || status == 403 {
        format!(
            "The API key for {} is missing or invalid — set it in Settings → Web Services.",
            id.label()
        )
    } else {
        format!(
            "Web search via {} failed (HTTP {status}): {}",
            id.label(),
            truncate(body, 300)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, serde_yaml::Value)]) -> Options {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn no_provider_message_points_at_settings() {
        let msg = no_provider_message();
        assert!(msg.contains("Web Services"));
        assert!(msg.contains("websearch"));
    }

    #[test]
    fn tavily_request_is_typed_post() {
        let req = build_request(
            SearchProviderId::Tavily,
            "rust async",
            &opts(&[
                ("searchDepth", serde_yaml::Value::from("advanced")),
                ("maxResults", serde_yaml::Value::from(3)),
            ]),
            "tok",
        );
        assert_eq!(req.method, "POST");
        assert_eq!(req.url, "https://api.tavily.com/search");
        assert!(req
            .headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer tok"));
        let body: serde_json::Value = serde_json::from_str(req.body.as_deref().unwrap()).unwrap();
        assert_eq!(body["query"], "rust async");
        assert_eq!(body["search_depth"], "advanced");
        assert_eq!(body["max_results"], 3);
    }

    #[test]
    fn exa_request_uses_x_api_key() {
        let req = build_request(SearchProviderId::Exa, "q", &opts(&[]), "k");
        assert_eq!(req.method, "POST");
        assert_eq!(req.url, "https://api.exa.ai/search");
        assert!(req
            .headers
            .iter()
            .any(|(k, v)| k == "x-api-key" && v == "k"));
        let body: serde_json::Value = serde_json::from_str(req.body.as_deref().unwrap()).unwrap();
        assert_eq!(body["numResults"], 10);
        assert_eq!(body["contents"]["text"], true);
    }

    #[test]
    fn brave_request_encodes_query_and_uses_token_header() {
        let req = build_request(SearchProviderId::Brave, "a b&c", &opts(&[]), "tok");
        assert_eq!(req.method, "GET");
        assert!(req.url.contains("q=a%20b%26c"), "{}", req.url);
        assert!(req.url.contains("count=10"));
        assert!(req.body.is_none());
        assert!(req
            .headers
            .iter()
            .any(|(k, v)| k == "X-Subscription-Token" && v == "tok"));
    }

    #[test]
    fn jina_request_is_get_with_bearer() {
        let req = build_request(SearchProviderId::Jina, "doc", &opts(&[]), "tok");
        assert_eq!(req.method, "GET");
        assert!(req.url.starts_with("https://s.jina.ai/?q=doc"));
        assert!(req
            .headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer tok"));
    }

    #[test]
    fn parse_tavily_maps_results() {
        let raw = r#"{"results":[
            {"title":"Rust","url":"https://rust-lang.org","content":"systems language","score":0.9},
            {"title":"Tokio","url":"https://tokio.rs","content":"async runtime"}
        ]}"#;
        let results = parse(SearchProviderId::Tavily, raw).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Rust");
        assert_eq!(results[0].url, "https://rust-lang.org");
        assert_eq!(results[0].snippet, "systems language");
        assert_eq!(results[0].score, Some(0.9));
    }

    #[test]
    fn parse_exa_handles_null_title() {
        let raw = r#"{"results":[{"title":null,"url":"https://x.test","text":"hi"}]}"#;
        let results = parse(SearchProviderId::Exa, raw).unwrap();
        assert_eq!(results[0].title, "https://x.test");
        assert_eq!(results[0].snippet, "hi");
    }

    #[test]
    fn parse_brave_reads_nested_web_results() {
        let raw = r#"{"web":{"results":[{"title":"Brave","url":"https://brave.com","description":"a browser"}]}}"#;
        let results = parse(SearchProviderId::Brave, raw).unwrap();
        assert_eq!(results[0].url, "https://brave.com");
        assert_eq!(results[0].snippet, "a browser");
    }

    #[test]
    fn parse_jina_prefers_description_then_content() {
        let raw = r#"{"data":[
            {"title":"Doc","url":"https://e.com/doc","description":"","content":"body text"}
        ]}"#;
        let results = parse(SearchProviderId::Jina, raw).unwrap();
        assert_eq!(results[0].snippet, "body text");
    }

    #[test]
    fn parse_rejects_non_json_with_guidance() {
        let err = parse(SearchProviderId::Tavily, "not json").unwrap_err();
        assert!(err.contains("unexpected response shape"), "{err}");
    }

    #[test]
    fn render_formats_ranked_list() {
        let results = vec![SearchResult {
            title: "T".to_string(),
            url: "https://t.test".to_string(),
            snippet: "snip".to_string(),
            score: None,
        }];
        let out = render(SearchProviderId::Brave, "q", &results);
        assert!(out.contains("1. T"));
        assert!(out.contains("https://t.test"));
        assert!(out.contains("snip"));
        assert!(out.contains("1 result"));
    }

    #[test]
    fn render_empty_is_clean() {
        let out = render(SearchProviderId::Exa, "q", &[]);
        assert!(out.contains("no results"), "{out}");
    }

    #[test]
    fn http_error_maps_auth_failures() {
        assert!(http_error_message(SearchProviderId::Tavily, 401, "").contains("invalid"));
        assert!(http_error_message(SearchProviderId::Tavily, 500, "oops").contains("HTTP 500"));
    }
}
