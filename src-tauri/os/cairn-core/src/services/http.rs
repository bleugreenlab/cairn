//! HTTP client service for external API calls.
//!
//! Abstracts HTTP operations to enable testing without real network calls.

use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Configuration for HTTP client retry behavior.
pub struct HttpConfig {
    timeout: Duration,
    max_retries: u32,
    initial_backoff: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_retries: 3,
            initial_backoff: Duration::from_secs(1),
        }
    }
}

/// Check if error is retryable (network issues, timeouts, 5xx).
fn is_retryable_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

/// Check if response status is retryable.
fn is_retryable_status(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}

/// Where a redirect points.
///
/// A redirect target is not merely a URL. Providers routinely redirect to a
/// *capability* URL whose query string carries a signature that is itself the
/// entire authorization: GitHub's workflow-log download is exactly this,
/// answering `/logs` with a redirect to blob storage bearing a `?sig=` token.
/// Anyone holding that URL can fetch the object.
///
/// So this type refuses to render itself. It has no `Display`, and its `Debug`
/// prints only the safe summary — because the ways a URL reaches observed
/// output are mostly incidental rather than deliberate: a `{url}` interpolated
/// into an error, a `{:?}` on the response it rode in on, a log line written
/// while chasing something else. Code that needs to *send* the target asks for
/// [`Self::as_str`]; code that needs to *describe* one gets scheme, host, and
/// path, and never the query, fragment, or userinfo.
#[derive(Clone, PartialEq, Eq)]
pub struct RedirectTarget(String);

impl RedirectTarget {
    pub fn new(url: impl Into<String>) -> Self {
        Self(url.into())
    }

    /// The complete target. The one way to the full URL, for sending it.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Scheme, host, and path — enough to say where something went, with the
    /// part that can be a credential dropped.
    ///
    /// A target that does not parse is described rather than quoted, since a
    /// value that is not a URL is not one whose query can be separated out.
    pub fn summary(&self) -> String {
        match reqwest::Url::parse(&self.0) {
            Ok(url) => format!(
                "{}://{}{}",
                url.scheme(),
                url.host_str().unwrap_or("<no host>"),
                url.path()
            ),
            Err(_) => "<unparseable redirect target>".to_string(),
        }
    }
}

impl std::fmt::Debug for RedirectTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// HTTP response wrapper.
#[derive(Debug, Clone, Default)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    /// Where a redirect points, if this is one.
    ///
    /// Carried because this transport does not follow redirects itself (see
    /// [`RealHttpClient::with_config`]), so a 3xx arrives here as an ordinary
    /// response and the caller decides what to do about it. See
    /// [`RedirectTarget`] for why it is not a `String`.
    pub location: Option<RedirectTarget>,
}

type HttpResultFuture<'a> = Pin<Box<dyn Future<Output = Result<HttpResponse, String>> + Send + 'a>>;

impl HttpResponse {
    /// A response carrying no `Location`.
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body,
            location: None,
        }
    }

    /// A redirect to `location`.
    pub fn redirect(status: u16, location: &str) -> Self {
        Self {
            status,
            body: Vec::new(),
            location: Some(RedirectTarget::new(location)),
        }
    }

    /// Check if status is 2xx.
    pub(crate) fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Where this response redirects to, if it is a redirect with a target.
    ///
    /// A 3xx without a `Location` is not actionable, so it is not a redirect as
    /// far as any caller is concerned.
    pub fn redirect_target(&self) -> Option<&RedirectTarget> {
        match self.status {
            301 | 302 | 303 | 307 | 308 => self.location.as_ref(),
            _ => None,
        }
    }

    /// Get body as string.
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    /// Parse body as JSON.
    pub(crate) fn json<T: DeserializeOwned>(&self) -> Result<T, String> {
        serde_json::from_slice(&self.body).map_err(|e| format!("Failed to parse JSON: {}", e))
    }
}

/// Trait for HTTP operations.
///
/// This abstraction allows tests to mock HTTP responses
/// without making real network requests.
///
/// Note: Uses serde_json::Value instead of generics for dyn-compatibility.
/// We use boxed futures for object safety since async traits
/// aren't directly object-safe.
pub trait HttpClient: Send + Sync {
    /// Perform a GET request.
    fn get(&self, url: &str, headers: HeaderMap) -> HttpResultFuture<'_>;

    /// Perform a POST request with JSON body.
    fn post(&self, url: &str, body: Value, headers: HeaderMap) -> HttpResultFuture<'_>;

    /// Perform a PUT request with JSON body.
    fn put(&self, url: &str, body: Value, headers: HeaderMap) -> HttpResultFuture<'_>;

    /// Perform a PATCH request with JSON body.
    fn patch(&self, url: &str, body: Value, headers: HeaderMap) -> HttpResultFuture<'_>;

    /// Perform a DELETE request.
    fn delete(&self, url: &str, headers: HeaderMap) -> HttpResultFuture<'_>;
}

/// Which HTTP method a request uses.
///
/// Public because a caller that handles its own redirects has to be able to
/// name the method it is repeating — and, for a 303 or a redirected POST, the
/// different method it must switch to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub fn label(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }

    fn reqwest_method(self) -> reqwest::Method {
        match self {
            Self::Get => reqwest::Method::GET,
            Self::Post => reqwest::Method::POST,
            Self::Put => reqwest::Method::PUT,
            Self::Patch => reqwest::Method::PATCH,
            Self::Delete => reqwest::Method::DELETE,
        }
    }
}

/// Production HTTP client using reqwest.
pub struct RealHttpClient {
    client: reqwest::Client,
    config: HttpConfig,
}

impl RealHttpClient {
    pub fn new() -> Self {
        Self::with_config(HttpConfig::default())
    }

    /// Build the client.
    ///
    /// # Why redirects are refused here
    ///
    /// This transport carries credentials. Following a redirect is the decision
    /// to *resend* a request — headers included — to a URL the server picked,
    /// and a transport is the wrong layer to make that decision, because it has
    /// no idea which of the headers it was handed is a bearer token.
    ///
    /// It is tempting to rely on the HTTP library for this: reqwest does strip
    /// sensitive headers when a redirect crosses to a different host. But that
    /// check compares host and port, *not* scheme, so a same-host
    /// `https://api.github.com` → `http://api.github.com` redirect keeps the
    /// `Authorization` header and puts the bearer on the wire in cleartext. A
    /// credential's containment must not rest on a dependency's heuristic that
    /// no test here exercises.
    ///
    /// So a 3xx arrives at the caller as an ordinary response with its
    /// `location` filled in, and the caller decides — see
    /// `security::broker::github`, which revalidates every hop against the
    /// lease's audience before resending, and drops the credential rather than
    /// following one off GitHub.
    fn with_config(config: HttpConfig) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(config.timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Failed to create HTTP client"),
            config,
        }
    }

    async fn to_response(resp: reqwest::Response) -> Result<HttpResponse, String> {
        let status = resp.status().as_u16();
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(RedirectTarget::new);
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("Failed to read response: {}", e))?
            .to_vec();
        Ok(HttpResponse {
            status,
            body,
            location,
        })
    }

    /// Execute request with retry logic.
    async fn with_retry<F, Fut>(config: &HttpConfig, operation: F) -> Result<HttpResponse, String>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let mut attempts = 0;

        loop {
            attempts += 1;

            match operation().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if is_retryable_status(status) && attempts <= config.max_retries {
                        let backoff = config.initial_backoff * 2u32.pow(attempts - 1);
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Self::to_response(resp).await;
                }
                Err(e) => {
                    if is_retryable_error(&e) && attempts <= config.max_retries {
                        let backoff = config.initial_backoff * 2u32.pow(attempts - 1);
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    // A reqwest error renders the request URL, and after a
                    // redirect that URL can be a capability carrying a signed
                    // query. `without_url` is reqwest's own affordance for
                    // exactly this, and the failure is still legible without it.
                    return Err(format!("Request failed: {}", e.without_url()));
                }
            }
        }
    }

    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Value>,
        headers: HeaderMap,
    ) -> HttpResultFuture<'_> {
        let url = url.to_string();
        Box::pin(async move {
            Self::with_retry(&self.config, || {
                let request = self
                    .client
                    .request(method.reqwest_method(), &url)
                    .headers(headers.clone());
                let request = match &body {
                    Some(body) => request
                        .header("Content-Type", "application/json")
                        .json(body),
                    None => request,
                };
                request.send()
            })
            .await
            .map_err(|e| format!("{} request failed: {}", method.label(), e))
        })
    }
}

impl Default for RealHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient for RealHttpClient {
    fn get(&self, url: &str, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.request(HttpMethod::Get, url, None, headers)
    }

    fn post(&self, url: &str, body: Value, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.request(HttpMethod::Post, url, Some(body), headers)
    }

    fn put(&self, url: &str, body: Value, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.request(HttpMethod::Put, url, Some(body), headers)
    }

    fn patch(&self, url: &str, body: Value, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.request(HttpMethod::Patch, url, Some(body), headers)
    }

    fn delete(&self, url: &str, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.request(HttpMethod::Delete, url, None, headers)
    }
}

/// Mock HTTP client for testing.
///
/// Configure responses for specific URL patterns.
#[cfg(any(test, feature = "test-utils"))]
pub struct MockHttpClient {
    responses: std::sync::Mutex<Vec<(String, HttpResponse)>>,
    /// Sequenced responses keyed by URL pattern. Each matching request consumes
    /// the next entry; once a sequence is down to its last entry that entry is
    /// returned for every subsequent request (mirroring an upstream value that
    /// has settled). Used to model GitHub's async mergeability window, where the
    /// first GET returns `mergeable: null` and a later GET returns the computed
    /// value.
    sequences: std::sync::Mutex<Vec<(String, std::collections::VecDeque<HttpResponse>)>>,
    /// Every request this mock was handed, in order.
    requests: std::sync::Mutex<Vec<RecordedRequest>>,
}

/// One request a [`MockHttpClient`] received, headers included.
///
/// The headers are the point. A test that a credential reached the right place
/// can be written against a URL, but a test that a credential did *not* reach
/// somewhere can only be written if the transport remembers what it was handed
/// — and until this existed, the mock discarded headers, which is precisely why
/// nothing caught the transport resending an `Authorization` across a redirect.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HeaderMap,
}

#[cfg(any(test, feature = "test-utils"))]
impl RecordedRequest {
    /// Whether this request carried any credential at all.
    pub fn is_authenticated(&self) -> bool {
        self.headers.contains_key(reqwest::header::AUTHORIZATION)
    }

    /// The `Authorization` value this request carried, if any.
    pub fn authorization(&self) -> Option<&str> {
        self.headers
            .get(reqwest::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl MockHttpClient {
    pub fn new() -> Self {
        Self {
            responses: std::sync::Mutex::new(Vec::new()),
            sequences: std::sync::Mutex::new(Vec::new()),
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Every request received so far, in order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }

    /// Add a response for any request to URLs containing the pattern.
    pub fn respond_to(self, url_contains: &str, response: HttpResponse) -> Self {
        self.responses
            .lock()
            .unwrap()
            .push((url_contains.to_string(), response));
        self
    }

    /// Add an ordered sequence of responses for URLs containing the pattern.
    ///
    /// Successive matching requests consume successive entries; the final entry
    /// is then repeated for any further requests. Fixed `respond_to` responses
    /// take precedence, so a more specific pattern (e.g. `"reviews"`) still
    /// resolves before a broader sequenced one (e.g. `"/pulls/7"`).
    pub fn respond_to_sequence(self, url_contains: &str, responses: Vec<HttpResponse>) -> Self {
        self.sequences
            .lock()
            .unwrap()
            .push((url_contains.to_string(), responses.into()));
        self
    }

    fn find_response(&self, url: &str) -> Result<HttpResponse, String> {
        // Fixed responses win over sequences so a specific pattern resolves
        // ahead of a broader sequenced one matching the same URL.
        {
            let responses = self.responses.lock().unwrap();
            for (pattern, response) in responses.iter() {
                if url.contains(pattern) {
                    return Ok(response.clone());
                }
            }
        }
        let mut sequences = self.sequences.lock().unwrap();
        for (pattern, queue) in sequences.iter_mut() {
            if url.contains(pattern.as_str()) {
                if queue.len() > 1 {
                    return Ok(queue.pop_front().unwrap());
                }
                if let Some(last) = queue.front() {
                    return Ok(last.clone());
                }
            }
        }
        Err(format!("No mock response configured for URL: {}", url))
    }

    fn response_future(
        &self,
        method: HttpMethod,
        url: &str,
        headers: HeaderMap,
    ) -> HttpResultFuture<'_> {
        self.requests.lock().unwrap().push(RecordedRequest {
            method,
            url: url.to_string(),
            headers,
        });
        let result = self.find_response(url);
        Box::pin(async move { result })
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for MockHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl HttpClient for MockHttpClient {
    fn get(&self, url: &str, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(HttpMethod::Get, url, headers)
    }

    fn post(&self, url: &str, _body: Value, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(HttpMethod::Post, url, headers)
    }

    fn put(&self, url: &str, _body: Value, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(HttpMethod::Put, url, headers)
    }

    fn patch(&self, url: &str, _body: Value, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(HttpMethod::Patch, url, headers)
    }

    fn delete(&self, url: &str, headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(HttpMethod::Delete, url, headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // HttpResponse tests
    // =========================================================================

    #[test]
    fn http_response_is_success_200() {
        let resp = HttpResponse::new(200, vec![]);
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_is_success_201() {
        let resp = HttpResponse::new(201, vec![]);
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_is_success_204() {
        let resp = HttpResponse::new(204, vec![]);
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_is_success_299() {
        let resp = HttpResponse::new(299, vec![]);
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_not_success_300() {
        let resp = HttpResponse::new(300, vec![]);
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_400() {
        let resp = HttpResponse::new(400, vec![]);
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_404() {
        let resp = HttpResponse::new(404, vec![]);
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_500() {
        let resp = HttpResponse::new(500, vec![]);
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_199() {
        let resp = HttpResponse::new(199, vec![]);
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_text_simple() {
        let resp = HttpResponse::new(200, b"hello world".to_vec());
        assert_eq!(resp.text(), "hello world");
    }

    #[test]
    fn http_response_text_empty() {
        let resp = HttpResponse::new(200, vec![]);
        assert_eq!(resp.text(), "");
    }

    #[test]
    fn http_response_text_unicode() {
        let resp = HttpResponse::new(200, "こんにちは".as_bytes().to_vec());
        assert_eq!(resp.text(), "こんにちは");
    }

    #[test]
    fn http_response_json_object() {
        let resp = HttpResponse::new(200, br#"{"key": "value"}"#.to_vec());
        let parsed: serde_json::Value = resp.json().unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn http_response_json_array() {
        let resp = HttpResponse::new(200, br#"[1, 2, 3]"#.to_vec());
        let parsed: Vec<i32> = resp.json().unwrap();
        assert_eq!(parsed, vec![1, 2, 3]);
    }

    #[test]
    fn http_response_json_nested() {
        let resp = HttpResponse::new(200, br#"{"user": {"name": "Alice", "age": 30}}"#.to_vec());
        let parsed: serde_json::Value = resp.json().unwrap();
        assert_eq!(parsed["user"]["name"], "Alice");
        assert_eq!(parsed["user"]["age"], 30);
    }

    #[test]
    fn http_response_json_invalid() {
        let resp = HttpResponse::new(200, b"not valid json".to_vec());
        let result: Result<serde_json::Value, _> = resp.json();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to parse JSON"));
    }

    #[test]
    fn http_response_clone() {
        let resp = HttpResponse::new(200, b"data".to_vec());
        let cloned = resp.clone();
        assert_eq!(cloned.status, resp.status);
        assert_eq!(cloned.body, resp.body);
    }

    // =========================================================================
    // Retry logic tests
    // =========================================================================

    #[test]
    fn is_retryable_status_429() {
        assert!(is_retryable_status(429));
    }

    #[test]
    fn is_retryable_status_5xx() {
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(502));
        assert!(is_retryable_status(503));
        assert!(is_retryable_status(504));
        assert!(is_retryable_status(599));
    }

    #[test]
    fn is_retryable_status_not_4xx() {
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(401));
        assert!(!is_retryable_status(403));
        assert!(!is_retryable_status(404));
    }

    #[test]
    fn is_retryable_status_not_2xx() {
        assert!(!is_retryable_status(200));
        assert!(!is_retryable_status(201));
        assert!(!is_retryable_status(204));
    }

    #[test]
    fn is_retryable_status_not_3xx() {
        assert!(!is_retryable_status(301));
        assert!(!is_retryable_status(302));
        assert!(!is_retryable_status(304));
    }

    #[test]
    fn http_config_defaults() {
        let config = HttpConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff, Duration::from_secs(1));
    }
}
