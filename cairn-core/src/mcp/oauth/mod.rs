//! OAuth 2.1 (authorization-code + PKCE) client for remote http/SSE MCP
//! servers. Cairn is the **client**: it discovers the authorization server for a
//! protected MCP endpoint, drives a browser authorize flow, and stores the
//! resulting bearer token so the gateway can attach it transparently.
//!
//! This module owns the protocol pieces the MCP Authorization spec
//! (2025-11-25) makes mandatory and that off-the-shelf wrappers do not provide:
//!
//! - **RFC8707 `resource` binding** on authorize, token, and refresh requests.
//! - **`state` generation and verification** (CSRF protection).
//! - **Protected Resource Metadata (RFC9728)** discovery: parse
//!   `WWW-Authenticate`, fetch PRM, select an authorization server.
//! - **Authorization-server metadata** discovery (RFC8414 + OIDC Discovery), in
//!   the spec's priority order, requiring PKCE `S256`.
//! - **PKCE `S256`** challenge/verifier generation.
//! - Token exchange + refresh and **Dynamic Client Registration** (RFC7591).
//!
//! Persistence lives in [`store`]. The interactive loopback/browser driver lives
//! in the Tauri host (`src-tauri/src/mcp/oauth_flow.rs`); everything here is
//! transport-agnostic and, for the parsing/derivation pieces, pure and
//! unit-tested.

pub mod store;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Per-request/connect timeout for OAuth HTTP calls (discovery, token, refresh).
const OAUTH_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Refresh an access token this many seconds before its actual expiry, so a
/// token never expires mid-call.
pub(crate) const EXPIRY_SKEW_SECS: i64 = 60;

// ---------------------------------------------------------------------------
// WWW-Authenticate (RFC9728 §5.1, RFC6750 §3)
// ---------------------------------------------------------------------------

/// The auth-params Cairn cares about from a `401`/`403` `WWW-Authenticate`
/// challenge on a protected MCP endpoint.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct WwwAuthenticate {
    /// `resource_metadata` — the PRM document URL (RFC9728).
    pub resource_metadata: Option<String>,
    /// `scope` — space-delimited scopes the resource is asking for.
    pub scope: Option<String>,
    /// `error` — e.g. `insufficient_scope` on a `403` step-up.
    error: Option<String>,
}

impl WwwAuthenticate {
    /// Whether this challenge is an `insufficient_scope` step-up (RFC6750 §3.1).
    pub fn is_insufficient_scope(&self) -> bool {
        self.error.as_deref() == Some("insufficient_scope")
    }
}

/// Parse the auth-params out of a `WWW-Authenticate` header value. Tolerant of
/// the `Bearer ` scheme prefix, quoted or bare values, and surrounding
/// whitespace. Unknown params are ignored.
fn parse_www_authenticate(header: &str) -> WwwAuthenticate {
    let mut out = WwwAuthenticate::default();
    // Drop the leading scheme token (`Bearer`, `DPoP`, …) if present.
    let rest = match header.split_once(char::is_whitespace) {
        Some((scheme, params)) if !scheme.contains('=') => params,
        _ => header,
    };
    for (key, value) in parse_auth_params(rest) {
        match key.as_str() {
            "resource_metadata" => out.resource_metadata = Some(value),
            "scope" => out.scope = Some(value),
            "error" => out.error = Some(value),
            _ => {}
        }
    }
    out
}

/// Split a comma-separated `key=value` / `key="value"` param list. Values may
/// contain commas only when quoted (URLs in `resource_metadata` are quoted).
fn parse_auth_params(input: &str) -> Vec<(String, String)> {
    let mut params = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip separators / whitespace.
        while i < bytes.len() && (bytes[i] == b',' || bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
        // Read key up to '='.
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key = input[key_start..i].trim().to_string();
        i += 1; // skip '='
        let value = if i < bytes.len() && bytes[i] == b'"' {
            i += 1; // opening quote
            let val_start = i;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            let v = input[val_start..i].to_string();
            if i < bytes.len() {
                i += 1; // closing quote
            }
            v
        } else {
            let val_start = i;
            while i < bytes.len() && bytes[i] != b',' {
                i += 1;
            }
            input[val_start..i].trim().to_string()
        };
        if !key.is_empty() {
            params.push((key, value));
        }
    }
    params
}

// ---------------------------------------------------------------------------
// Protected Resource Metadata (RFC9728)
// ---------------------------------------------------------------------------

/// A Protected Resource Metadata document (RFC9728), trimmed to the fields the
/// authorize flow consumes.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProtectedResourceMetadata {
    #[serde(default)]
    pub resource: Option<String>,
    #[serde(default)]
    authorization_servers: Vec<String>,
    #[serde(default)]
    pub scopes_supported: Vec<String>,
}

/// Parse a PRM JSON document.
fn parse_protected_resource_metadata(json: &str) -> Result<ProtectedResourceMetadata, String> {
    serde_json::from_str(json).map_err(|e| format!("Invalid protected resource metadata: {e}"))
}

/// Select an authorization server from PRM (RFC9728 §7.6: pick from the list;
/// we take the first advertised server).
pub fn select_authorization_server(prm: &ProtectedResourceMetadata) -> Option<String> {
    prm.authorization_servers.first().cloned()
}

/// Candidate PRM well-known URLs for a resource URL, in priority order
/// (RFC9728 §3.1): path-aware well-known first, then the root well-known. Used
/// only when the `401` omits `resource_metadata`.
pub fn prm_well_known_urls(resource_url: &str) -> Vec<String> {
    let Ok(url) = reqwest::Url::parse(resource_url) else {
        return Vec::new();
    };
    let origin = origin_string(&url);
    let path = url.path().trim_end_matches('/');
    let mut urls = Vec::new();
    if !path.is_empty() {
        urls.push(format!(
            "{origin}/.well-known/oauth-protected-resource{path}"
        ));
    }
    urls.push(format!("{origin}/.well-known/oauth-protected-resource"));
    urls
}

// ---------------------------------------------------------------------------
// Authorization Server Metadata (RFC8414 + OIDC Discovery 1.0)
// ---------------------------------------------------------------------------

/// Authorization-server metadata (RFC8414 / OIDC), trimmed to what the flow
/// needs.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthServerMetadata {
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub authorization_endpoint: String,
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default)]
    pub registration_endpoint: Option<String>,
    #[serde(default)]
    pub scopes_supported: Vec<String>,
    #[serde(default)]
    code_challenge_methods_supported: Vec<String>,
    #[serde(default)]
    pub grant_types_supported: Vec<String>,
}

/// Parse an authorization-server metadata JSON document.
fn parse_auth_server_metadata(json: &str) -> Result<AuthServerMetadata, String> {
    serde_json::from_str(json).map_err(|e| format!("Invalid authorization server metadata: {e}"))
}

/// Whether the server advertises PKCE `S256` support — mandatory; the flow
/// refuses to proceed without it.
fn supports_s256(meta: &AuthServerMetadata) -> bool {
    meta.code_challenge_methods_supported
        .iter()
        .any(|m| m == "S256")
}

/// Candidate AS-metadata discovery URLs for an issuer, in the spec's priority
/// order (MCP Authorization § "Authorization Server Metadata Discovery"):
/// RFC8414 path-aware, OIDC path-aware, then OIDC path-insertion; for an
/// issuer with no path, the root well-knowns.
fn as_metadata_urls(issuer: &str) -> Vec<String> {
    let Ok(url) = reqwest::Url::parse(issuer) else {
        return Vec::new();
    };
    let origin = origin_string(&url);
    let path = url.path().trim_end_matches('/');
    if path.is_empty() {
        vec![
            format!("{origin}/.well-known/oauth-authorization-server"),
            format!("{origin}/.well-known/openid-configuration"),
        ]
    } else {
        vec![
            format!("{origin}/.well-known/oauth-authorization-server{path}"),
            format!("{origin}/.well-known/openid-configuration{path}"),
            format!("{origin}{path}/.well-known/openid-configuration"),
        ]
    }
}

// ---------------------------------------------------------------------------
// Canonical resource URI (RFC8707 §2)
// ---------------------------------------------------------------------------

/// Normalize a server URL to its canonical RFC8707 resource identifier:
/// lowercased scheme + host, default port omitted, query and fragment dropped,
/// and no trailing slash (a bare-origin path collapses to empty). This is the
/// value bound as `resource` on every authorize/token/refresh request.
pub fn canonical_resource_uri(url: &str) -> Result<String, String> {
    let parsed =
        reqwest::Url::parse(url).map_err(|e| format!("Invalid server URL '{url}': {e}"))?;
    let origin = origin_string(&parsed);
    let path = parsed.path().trim_end_matches('/');
    Ok(format!("{origin}{path}"))
}

/// `scheme://host[:port]` with the scheme/host lowercased (reqwest::Url already
/// lowercases them) and a default port omitted.
fn origin_string(url: &reqwest::Url) -> String {
    let scheme = url.scheme();
    let host = url.host_str().unwrap_or("");
    match url.port() {
        // `port()` returns None when the port is the scheme default, so a
        // present port is always non-default and worth keeping.
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

/// Whether a URL uses HTTPS (or is a loopback http URL, allowed for local test
/// servers). Authorization/token endpoints must be HTTPS per the spec.
fn is_secure_endpoint(url: &str) -> bool {
    match reqwest::Url::parse(url) {
        Ok(u) if u.scheme() == "https" => true,
        Ok(u) if u.scheme() == "http" => is_loopback_host(u.host_str()),
        _ => false,
    }
}

fn is_loopback_host(host: Option<&str>) -> bool {
    matches!(host, Some("127.0.0.1") | Some("localhost") | Some("[::1]"))
}

// ---------------------------------------------------------------------------
// Scope strategy
// ---------------------------------------------------------------------------

/// Pick the scope string to request, per the spec's progressive strategy:
/// the challenge `scope` if the server asked for specific scopes, else the
/// resource/server `scopes_supported`, else omit the parameter entirely.
/// `requested` (user-configured scopes) wins over discovery when non-empty.
pub fn choose_scope(
    requested: &[String],
    challenge_scope: Option<&str>,
    prm_scopes: &[String],
    as_scopes: &[String],
) -> Option<String> {
    if !requested.is_empty() {
        return Some(requested.join(" "));
    }
    if let Some(scope) = challenge_scope {
        let trimmed = scope.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if !prm_scopes.is_empty() {
        return Some(prm_scopes.join(" "));
    }
    if !as_scopes.is_empty() {
        return Some(as_scopes.join(" "));
    }
    None
}

// ---------------------------------------------------------------------------
// PKCE + state
// ---------------------------------------------------------------------------

/// A PKCE verifier/challenge pair. The verifier is held until token exchange;
/// only the `S256` challenge travels to the authorization endpoint.
#[derive(Debug, Clone)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
    pub method: &'static str,
}

/// Generate a fresh PKCE pair (RFC7636): a 32-byte random verifier (base64url,
/// no padding) and its `S256` challenge.
pub fn generate_pkce() -> Pkce {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    Pkce {
        verifier,
        challenge,
        method: "S256",
    }
}

/// Generate a random `state` value for CSRF protection (32 bytes, base64url).
pub fn generate_state() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

// ---------------------------------------------------------------------------
// Authorize URL
// ---------------------------------------------------------------------------

/// Build the authorization-endpoint URL for an authorization-code + PKCE
/// request, binding the RFC8707 `resource` and the CSRF `state`.
pub fn build_authorize_url(
    authorization_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    resource: &str,
    state: &str,
    code_challenge: &str,
    scope: Option<&str>,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(authorization_endpoint)
        .map_err(|e| format!("Invalid authorization endpoint: {e}"))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("response_type", "code");
        q.append_pair("client_id", client_id);
        q.append_pair("redirect_uri", redirect_uri);
        q.append_pair("state", state);
        q.append_pair("code_challenge", code_challenge);
        q.append_pair("code_challenge_method", "S256");
        q.append_pair("resource", resource);
        if let Some(scope) = scope {
            q.append_pair("scope", scope);
        }
    }
    Ok(url.to_string())
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// A token-endpoint response, normalized with an absolute expiry.
#[derive(Debug, Clone)]
pub struct TokenSet {
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Absolute expiry (unix seconds), if the server returned `expires_in`.
    pub expires_at: Option<i64>,
    pub scope: Option<String>,
    pub token_type: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponseBody {
    access_token: String,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

impl TokenResponseBody {
    fn into_token_set(self, prior_refresh: Option<String>) -> TokenSet {
        let expires_at = self
            .expires_in
            .map(|secs| chrono::Utc::now().timestamp() + secs);
        TokenSet {
            access_token: self.access_token,
            // A refresh response may omit a rotated refresh token; keep the
            // prior one in that case (non-rotating servers).
            refresh_token: self.refresh_token.or(prior_refresh),
            expires_at,
            scope: self.scope,
            token_type: self.token_type.unwrap_or_else(|| "Bearer".to_string()),
        }
    }
}

/// Build the shared HTTP client for OAuth calls. Redirects are disabled — token
/// and discovery endpoints must not bounce the client to another origin.
fn oauth_http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(OAUTH_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("Failed to build OAuth HTTP client: {e}"))
}

/// Exchange an authorization code for tokens, binding the RFC8707 `resource`.
#[allow(clippy::too_many_arguments)]
pub async fn exchange_code(
    token_endpoint: &str,
    client_id: &str,
    client_secret: Option<&str>,
    code: &str,
    code_verifier: &str,
    redirect_uri: &str,
    resource: &str,
) -> Result<TokenSet, String> {
    if !is_secure_endpoint(token_endpoint) {
        return Err(format!("Token endpoint must be HTTPS: {token_endpoint}"));
    }
    let client = oauth_http_client()?;
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", redirect_uri.to_string()),
        ("client_id", client_id.to_string()),
        ("code_verifier", code_verifier.to_string()),
        ("resource", resource.to_string()),
    ];
    if let Some(secret) = client_secret {
        form.push(("client_secret", secret.to_string()));
    }
    post_token_request(&client, token_endpoint, &form, None).await
}

/// Refresh an access token via the refresh-token grant, re-binding `resource`.
pub(crate) async fn refresh_access_token(
    token_endpoint: &str,
    client_id: &str,
    client_secret: Option<&str>,
    refresh_token: &str,
    resource: &str,
    scope: Option<&str>,
) -> Result<TokenSet, String> {
    if !is_secure_endpoint(token_endpoint) {
        return Err(format!("Token endpoint must be HTTPS: {token_endpoint}"));
    }
    let client = oauth_http_client()?;
    let mut form = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.to_string()),
        ("client_id", client_id.to_string()),
        ("resource", resource.to_string()),
    ];
    if let Some(secret) = client_secret {
        form.push(("client_secret", secret.to_string()));
    }
    if let Some(scope) = scope {
        form.push(("scope", scope.to_string()));
    }
    post_token_request(
        &client,
        token_endpoint,
        &form,
        Some(refresh_token.to_string()),
    )
    .await
}

/// POST a form to the token endpoint and parse the (success or error) response.
async fn post_token_request(
    client: &reqwest::Client,
    token_endpoint: &str,
    form: &[(&str, String)],
    prior_refresh: Option<String>,
) -> Result<TokenSet, String> {
    let resp = client
        .post(token_endpoint)
        .form(form)
        .send()
        .await
        .map_err(|e| format!("Token request failed: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("Token endpoint returned {status}: {body}"));
    }
    let parsed: TokenResponseBody =
        serde_json::from_str(&body).map_err(|e| format!("Invalid token response: {e}: {body}"))?;
    Ok(parsed.into_token_set(prior_refresh))
}

// ---------------------------------------------------------------------------
// Discovery fetches (network)
// ---------------------------------------------------------------------------

/// Probe a protected MCP endpoint, returning the parsed `WWW-Authenticate`
/// challenge when the server answers `401`/`403`. A non-401/403 response means
/// the endpoint did not demand authorization here.
pub struct Probe {
    pub requires_auth: bool,
    pub challenge: WwwAuthenticate,
}

/// A minimal MCP `initialize` request body. MCP servers gate authorization on
/// the JSON-RPC POST (a bare GET often `405`s in method routing *before* the
/// auth middleware runs), so the auth challenge only surfaces on this request.
const INITIALIZE_PROBE_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"cairn","version":"0"}}}"#;

/// Probe the endpoint for an auth challenge. Tries an `initialize` POST first
/// (the streamable-HTTP case), then falls back to a GET (legacy SSE endpoints
/// that challenge on the stream open). Returns the first response that demands
/// authorization; otherwise the POST result.
pub async fn probe_endpoint(url: &str) -> Result<Probe, String> {
    let client = oauth_http_client()?;
    let post = probe_request(&client, url, true).await;
    if matches!(post, Some(ref p) if p.requires_auth) {
        return Ok(post.unwrap());
    }
    if let Some(get) = probe_request(&client, url, false).await {
        if get.requires_auth {
            return Ok(get);
        }
    }
    post.ok_or_else(|| format!("Probe request to '{url}' failed"))
}

/// The result of a quick auth probe, for the settings status view. The third
/// arm matters: a probe that times out or fails at the transport level is NOT
/// the same as a server that answered "no auth needed". Collapsing the two (the
/// old `bool` did) let a slow or briefly-unreachable server look like it needs
/// no authorization, which hid the Authorize affordance and left a remote
/// server with no usable token unrecoverable from the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The endpoint answered `401`/`403`: it demands authorization.
    RequiresAuth,
    /// The endpoint answered and did NOT demand auth (public, or gated by a
    /// static bearer header rather than OAuth).
    NoAuth,
    /// The endpoint could not be reached (timeout or transport error) on either
    /// attempt, so whether it requires auth is unknown.
    Unknown,
}

impl ProbeOutcome {
    /// Map a probe outcome to the OAuth affordance for a remote server we hold
    /// no usable token for. A server that demands auth — *or that we could not
    /// reach to find out* — gets the Authorize affordance; only a definitive
    /// non-auth answer suppresses it. This keeps a flaky or slow probe from
    /// erasing the one control a user needs to (re-)authorize a server.
    pub fn into_unauthenticated_status(self) -> store::OAuthStatus {
        match self {
            ProbeOutcome::RequiresAuth | ProbeOutcome::Unknown => store::OAuthStatus::needs_auth(),
            ProbeOutcome::NoAuth => store::OAuthStatus::none(),
        }
    }
}

/// Quick check of whether an endpoint requires OAuth, for the settings status
/// view. Uses a short timeout so the settings page never blocks on a slow or
/// unreachable server. A transport error or timeout on both attempts yields
/// [`ProbeOutcome::Unknown`] (NOT a definitive "no auth"), so the caller can
/// still offer authorization for a server it otherwise cannot use.
pub async fn probe_requires_auth_quick(url: &str) -> ProbeOutcome {
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    else {
        return ProbeOutcome::Unknown;
    };
    let post = probe_request(&client, url, true).await;
    if matches!(post, Some(ref p) if p.requires_auth) {
        return ProbeOutcome::RequiresAuth;
    }
    let get = probe_request(&client, url, false).await;
    if matches!(get, Some(ref p) if p.requires_auth) {
        return ProbeOutcome::RequiresAuth;
    }
    // Neither attempt saw an auth challenge. Only call it a definitive "no auth"
    // when at least one request actually completed; if both failed at the
    // transport level we genuinely don't know.
    if post.is_some() || get.is_some() {
        ProbeOutcome::NoAuth
    } else {
        ProbeOutcome::Unknown
    }
}

/// One probe request (POST `initialize` or GET), parsing any challenge header.
async fn probe_request(client: &reqwest::Client, url: &str, post: bool) -> Option<Probe> {
    let builder = if post {
        client
            .post(url)
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(INITIALIZE_PROBE_BODY)
    } else {
        client.get(url).header(
            reqwest::header::ACCEPT,
            "application/json, text/event-stream",
        )
    };
    let resp = builder.send().await.ok()?;
    let requires_auth = resp.status() == reqwest::StatusCode::UNAUTHORIZED
        || resp.status() == reqwest::StatusCode::FORBIDDEN;
    let challenge = resp
        .headers()
        .get(reqwest::header::WWW_AUTHENTICATE)
        .and_then(|v| v.to_str().ok())
        .map(parse_www_authenticate)
        .unwrap_or_default();
    Some(Probe {
        requires_auth,
        challenge,
    })
}

/// Fetch and parse Protected Resource Metadata, trying each candidate URL until
/// one parses.
pub async fn fetch_protected_resource_metadata(
    candidate_urls: &[String],
) -> Result<ProtectedResourceMetadata, String> {
    let client = oauth_http_client()?;
    let mut last_err = String::from("no PRM candidate URLs");
    for url in candidate_urls {
        match fetch_json(&client, url).await {
            Ok(body) => match parse_protected_resource_metadata(&body) {
                Ok(prm) => return Ok(prm),
                Err(e) => last_err = e,
            },
            Err(e) => last_err = e,
        }
    }
    Err(format!(
        "Protected resource metadata discovery failed: {last_err}"
    ))
}

/// Discover authorization-server metadata for an issuer, trying the spec's
/// priority-ordered well-known URLs and requiring PKCE `S256`.
pub async fn fetch_auth_server_metadata(issuer: &str) -> Result<AuthServerMetadata, String> {
    let client = oauth_http_client()?;
    let mut last_err = String::from("no AS metadata candidate URLs");
    for url in as_metadata_urls(issuer) {
        match fetch_json(&client, &url).await {
            Ok(body) => match parse_auth_server_metadata(&body) {
                Ok(meta) if meta.authorization_endpoint.is_empty() => {
                    last_err = format!("AS metadata at {url} has no authorization_endpoint");
                }
                Ok(meta) => {
                    if !supports_s256(&meta) {
                        return Err(format!(
                            "Authorization server {issuer} does not advertise PKCE S256 \
                             (code_challenge_methods_supported); refusing to proceed."
                        ));
                    }
                    return Ok(meta);
                }
                Err(e) => last_err = e,
            },
            Err(e) => last_err = e,
        }
    }
    Err(format!(
        "Authorization server metadata discovery failed: {last_err}"
    ))
}

async fn fetch_json(client: &reqwest::Client, url: &str) -> Result<String, String> {
    let resp = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url} returned {}", resp.status()));
    }
    resp.text()
        .await
        .map_err(|e| format!("Reading {url} failed: {e}"))
}

// ---------------------------------------------------------------------------
// Dynamic Client Registration (RFC7591)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct RegistrationRequest<'a> {
    client_name: &'a str,
    redirect_uris: Vec<&'a str>,
    grant_types: Vec<&'a str>,
    response_types: Vec<&'a str>,
    token_endpoint_auth_method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct RegistrationResponse {
    client_id: String,
    #[serde(default)]
    client_secret: Option<String>,
}

/// A registered (or pre-registered) OAuth client.
#[derive(Debug, Clone)]
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: Option<String>,
}

/// Scope the DCR `grant_types` to what the authorization server advertises:
/// `authorization_code` (always) plus `refresh_token` only when supported. An
/// empty `supported` list (server didn't advertise) requests both, best-effort.
fn scoped_grant_types(supported: &[String]) -> Vec<&'static str> {
    if supported.is_empty() {
        return vec!["authorization_code", "refresh_token"];
    }
    let mut grants = vec!["authorization_code"];
    if supported.iter().any(|g| g == "refresh_token") {
        grants.push("refresh_token");
    }
    grants
}

/// Register a public client via Dynamic Client Registration (RFC7591). Cairn
/// registers as a public client (no secret) using the loopback redirect. The
/// requested `grant_types` are scoped to what the AS advertises so a strict
/// server doesn't reject an unsupported grant.
pub async fn register_client(
    registration_endpoint: &str,
    redirect_uri: &str,
    client_name: &str,
    scope: Option<&str>,
    grant_types_supported: &[String],
) -> Result<ClientCredentials, String> {
    if !is_secure_endpoint(registration_endpoint) {
        return Err(format!(
            "Registration endpoint must be HTTPS: {registration_endpoint}"
        ));
    }
    let client = oauth_http_client()?;
    let req = RegistrationRequest {
        client_name,
        redirect_uris: vec![redirect_uri],
        grant_types: scoped_grant_types(grant_types_supported),
        response_types: vec!["code"],
        token_endpoint_auth_method: "none",
        scope,
    };
    let resp = client
        .post(registration_endpoint)
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("Client registration request failed: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("Client registration returned {status}: {body}"));
    }
    let parsed: RegistrationResponse = serde_json::from_str(&body)
        .map_err(|e| format!("Invalid client registration response: {e}: {body}"))?;
    Ok(ClientCredentials {
        client_id: parsed.client_id,
        client_secret: parsed.client_secret,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_www_authenticate_extracts_resource_metadata_and_scope() {
        let header = r#"Bearer resource_metadata="https://api.example.com/.well-known/oauth-protected-resource", scope="read write""#;
        let w = parse_www_authenticate(header);
        assert_eq!(
            w.resource_metadata.as_deref(),
            Some("https://api.example.com/.well-known/oauth-protected-resource")
        );
        assert_eq!(w.scope.as_deref(), Some("read write"));
        assert!(w.error.is_none());
        assert!(!w.is_insufficient_scope());
    }

    #[test]
    fn parse_www_authenticate_detects_insufficient_scope() {
        let header =
            r#"Bearer error="insufficient_scope", scope="admin", error_description="need admin""#;
        let w = parse_www_authenticate(header);
        assert!(w.is_insufficient_scope());
        assert_eq!(w.scope.as_deref(), Some("admin"));
    }

    #[test]
    fn parse_www_authenticate_handles_bare_values_and_no_scheme() {
        let w = parse_www_authenticate("error=invalid_token");
        assert_eq!(w.error.as_deref(), Some("invalid_token"));
    }

    #[test]
    fn prm_parse_and_select_first_server() {
        let json = r#"{"resource":"https://api.example.com","authorization_servers":["https://auth.example.com","https://auth2.example.com"],"scopes_supported":["read","write"]}"#;
        let prm = parse_protected_resource_metadata(json).unwrap();
        assert_eq!(
            select_authorization_server(&prm).as_deref(),
            Some("https://auth.example.com")
        );
        assert_eq!(prm.scopes_supported, vec!["read", "write"]);
    }

    #[test]
    fn prm_well_known_urls_are_path_aware_then_root() {
        let urls = prm_well_known_urls("https://api.example.com/mcp/v1");
        assert_eq!(
            urls,
            vec![
                "https://api.example.com/.well-known/oauth-protected-resource/mcp/v1".to_string(),
                "https://api.example.com/.well-known/oauth-protected-resource".to_string(),
            ]
        );
        // No path -> only the root well-known.
        let urls = prm_well_known_urls("https://api.example.com");
        assert_eq!(
            urls,
            vec!["https://api.example.com/.well-known/oauth-protected-resource".to_string()]
        );
    }

    #[test]
    fn as_metadata_urls_priority_order_with_path() {
        let urls = as_metadata_urls("https://auth.example.com/tenant1");
        assert_eq!(
            urls,
            vec![
                "https://auth.example.com/.well-known/oauth-authorization-server/tenant1"
                    .to_string(),
                "https://auth.example.com/.well-known/openid-configuration/tenant1".to_string(),
                "https://auth.example.com/tenant1/.well-known/openid-configuration".to_string(),
            ]
        );
    }

    #[test]
    fn as_metadata_urls_root_issuer() {
        let urls = as_metadata_urls("https://auth.example.com");
        assert_eq!(
            urls,
            vec![
                "https://auth.example.com/.well-known/oauth-authorization-server".to_string(),
                "https://auth.example.com/.well-known/openid-configuration".to_string(),
            ]
        );
    }

    #[test]
    fn s256_required() {
        let mut meta = AuthServerMetadata {
            code_challenge_methods_supported: vec!["plain".to_string()],
            ..Default::default()
        };
        assert!(!supports_s256(&meta));
        meta.code_challenge_methods_supported
            .push("S256".to_string());
        assert!(supports_s256(&meta));
    }

    #[test]
    fn canonical_resource_uri_normalizes() {
        assert_eq!(
            canonical_resource_uri("HTTPS://API.Example.com:443/mcp/").unwrap(),
            "https://api.example.com/mcp"
        );
        assert_eq!(
            canonical_resource_uri("https://api.example.com/").unwrap(),
            "https://api.example.com"
        );
        assert_eq!(
            canonical_resource_uri("https://api.example.com:8443/mcp#frag").unwrap(),
            "https://api.example.com:8443/mcp"
        );
    }

    #[test]
    fn is_secure_endpoint_https_and_loopback() {
        assert!(is_secure_endpoint("https://auth.example.com/token"));
        assert!(is_secure_endpoint("http://127.0.0.1:8080/token"));
        assert!(is_secure_endpoint("http://localhost/token"));
        assert!(!is_secure_endpoint("http://auth.example.com/token"));
        assert!(!is_secure_endpoint("ftp://auth.example.com"));
    }

    #[test]
    fn choose_scope_strategy() {
        // Requested (user-configured) wins.
        assert_eq!(
            choose_scope(&["a".into(), "b".into()], Some("x"), &["y".into()], &[]),
            Some("a b".to_string())
        );
        // Then the challenge scope.
        assert_eq!(
            choose_scope(&[], Some("read write"), &["y".into()], &[]),
            Some("read write".to_string())
        );
        // Then PRM scopes_supported.
        assert_eq!(
            choose_scope(&[], None, &["p1".into(), "p2".into()], &["a".into()]),
            Some("p1 p2".to_string())
        );
        // Then AS scopes_supported.
        assert_eq!(
            choose_scope(&[], None, &[], &["a1".into()]),
            Some("a1".to_string())
        );
        // Otherwise omit.
        assert_eq!(choose_scope(&[], None, &[], &[]), None);
        // Blank challenge scope is ignored.
        assert_eq!(choose_scope(&[], Some("   "), &[], &[]), None);
    }

    #[test]
    fn pkce_challenge_is_s256_of_verifier() {
        let pkce = generate_pkce();
        assert_eq!(pkce.method, "S256");
        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(pkce.verifier.as_bytes()));
        assert_eq!(pkce.challenge, expected);
        // Distinct pairs across calls.
        assert_ne!(generate_pkce().verifier, generate_pkce().verifier);
    }

    #[test]
    fn scoped_grant_types_filters_to_advertised() {
        assert_eq!(
            scoped_grant_types(&["authorization_code".to_string()]),
            vec!["authorization_code"]
        );
        assert_eq!(
            scoped_grant_types(&[
                "authorization_code".to_string(),
                "refresh_token".to_string()
            ]),
            vec!["authorization_code", "refresh_token"]
        );
        assert_eq!(
            scoped_grant_types(&[]),
            vec!["authorization_code", "refresh_token"]
        );
    }

    #[test]
    fn state_is_random_and_urlsafe() {
        let a = generate_state();
        let b = generate_state();
        assert_ne!(a, b);
        assert!(!a.contains('+') && !a.contains('/') && !a.contains('='));
    }

    #[test]
    fn build_authorize_url_binds_resource_state_and_pkce() {
        let url = build_authorize_url(
            "https://auth.example.com/authorize",
            "client123",
            "http://127.0.0.1:5000/callback",
            "https://api.example.com/mcp",
            "state-xyz",
            "challenge-abc",
            Some("read write"),
        )
        .unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        let pairs: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("response_type").map(String::as_str), Some("code"));
        assert_eq!(
            pairs.get("client_id").map(String::as_str),
            Some("client123")
        );
        assert_eq!(
            pairs.get("resource").map(String::as_str),
            Some("https://api.example.com/mcp")
        );
        assert_eq!(pairs.get("state").map(String::as_str), Some("state-xyz"));
        assert_eq!(
            pairs.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        assert_eq!(pairs.get("scope").map(String::as_str), Some("read write"));
    }

    #[test]
    fn build_authorize_url_omits_scope_when_none() {
        let url = build_authorize_url(
            "https://auth.example.com/authorize",
            "c",
            "http://127.0.0.1:5000/callback",
            "https://api.example.com",
            "s",
            "ch",
            None,
        )
        .unwrap();
        assert!(!url.contains("scope="));
    }

    #[test]
    fn probe_outcome_offers_auth_unless_definitively_unprotected() {
        // A server that demands auth, or one we couldn't reach to find out,
        // both surface the Authorize affordance; only a definitive non-auth
        // answer suppresses it.
        assert_eq!(
            ProbeOutcome::RequiresAuth
                .into_unauthenticated_status()
                .state,
            "needs_auth"
        );
        assert_eq!(
            ProbeOutcome::Unknown.into_unauthenticated_status().state,
            "needs_auth"
        );
        assert_eq!(
            ProbeOutcome::NoAuth.into_unauthenticated_status().state,
            "none"
        );
    }

    #[test]
    fn token_response_computes_absolute_expiry_and_keeps_prior_refresh() {
        let body = TokenResponseBody {
            access_token: "at".into(),
            token_type: None,
            expires_in: Some(3600),
            refresh_token: None,
            scope: Some("read".into()),
        };
        let before = chrono::Utc::now().timestamp();
        let set = body.into_token_set(Some("old-refresh".into()));
        assert_eq!(set.token_type, "Bearer");
        assert_eq!(set.refresh_token.as_deref(), Some("old-refresh"));
        let expires_at = set.expires_at.unwrap();
        assert!(expires_at >= before + 3600 && expires_at <= before + 3601 + 2);
    }
}
