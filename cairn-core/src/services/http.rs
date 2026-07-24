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

/// HTTP response wrapper.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

type HttpResultFuture<'a> = Pin<Box<dyn Future<Output = Result<HttpResponse, String>> + Send + 'a>>;

impl HttpResponse {
    /// Check if status is 2xx.
    pub(crate) fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
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

#[derive(Clone, Copy)]
enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    fn label(self) -> &'static str {
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

    fn with_config(config: HttpConfig) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(config.timeout)
                .redirect(reqwest::redirect::Policy::limited(10))
                .build()
                .expect("Failed to create HTTP client"),
            config,
        }
    }

    async fn to_response(resp: reqwest::Response) -> Result<HttpResponse, String> {
        let status = resp.status().as_u16();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("Failed to read response: {}", e))?
            .to_vec();
        Ok(HttpResponse { status, body })
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
                    return Err(format!("Request failed: {}", e));
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
}

#[cfg(any(test, feature = "test-utils"))]
impl MockHttpClient {
    pub fn new() -> Self {
        Self {
            responses: std::sync::Mutex::new(Vec::new()),
            sequences: std::sync::Mutex::new(Vec::new()),
        }
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

    fn response_future(&self, url: &str) -> HttpResultFuture<'_> {
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
    fn get(&self, url: &str, _headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(url)
    }

    fn post(&self, url: &str, _body: Value, _headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(url)
    }

    fn put(&self, url: &str, _body: Value, _headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(url)
    }

    fn patch(&self, url: &str, _body: Value, _headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(url)
    }

    fn delete(&self, url: &str, _headers: HeaderMap) -> HttpResultFuture<'_> {
        self.response_future(url)
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
        let resp = HttpResponse {
            status: 200,
            body: vec![],
        };
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_is_success_201() {
        let resp = HttpResponse {
            status: 201,
            body: vec![],
        };
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_is_success_204() {
        let resp = HttpResponse {
            status: 204,
            body: vec![],
        };
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_is_success_299() {
        let resp = HttpResponse {
            status: 299,
            body: vec![],
        };
        assert!(resp.is_success());
    }

    #[test]
    fn http_response_not_success_300() {
        let resp = HttpResponse {
            status: 300,
            body: vec![],
        };
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_400() {
        let resp = HttpResponse {
            status: 400,
            body: vec![],
        };
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_404() {
        let resp = HttpResponse {
            status: 404,
            body: vec![],
        };
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_500() {
        let resp = HttpResponse {
            status: 500,
            body: vec![],
        };
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_not_success_199() {
        let resp = HttpResponse {
            status: 199,
            body: vec![],
        };
        assert!(!resp.is_success());
    }

    #[test]
    fn http_response_text_simple() {
        let resp = HttpResponse {
            status: 200,
            body: b"hello world".to_vec(),
        };
        assert_eq!(resp.text(), "hello world");
    }

    #[test]
    fn http_response_text_empty() {
        let resp = HttpResponse {
            status: 200,
            body: vec![],
        };
        assert_eq!(resp.text(), "");
    }

    #[test]
    fn http_response_text_unicode() {
        let resp = HttpResponse {
            status: 200,
            body: "こんにちは".as_bytes().to_vec(),
        };
        assert_eq!(resp.text(), "こんにちは");
    }

    #[test]
    fn http_response_json_object() {
        let resp = HttpResponse {
            status: 200,
            body: br#"{"key": "value"}"#.to_vec(),
        };
        let parsed: serde_json::Value = resp.json().unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn http_response_json_array() {
        let resp = HttpResponse {
            status: 200,
            body: br#"[1, 2, 3]"#.to_vec(),
        };
        let parsed: Vec<i32> = resp.json().unwrap();
        assert_eq!(parsed, vec![1, 2, 3]);
    }

    #[test]
    fn http_response_json_nested() {
        let resp = HttpResponse {
            status: 200,
            body: br#"{"user": {"name": "Alice", "age": 30}}"#.to_vec(),
        };
        let parsed: serde_json::Value = resp.json().unwrap();
        assert_eq!(parsed["user"]["name"], "Alice");
        assert_eq!(parsed["user"]["age"], 30);
    }

    #[test]
    fn http_response_json_invalid() {
        let resp = HttpResponse {
            status: 200,
            body: b"not valid json".to_vec(),
        };
        let result: Result<serde_json::Value, _> = resp.json();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to parse JSON"));
    }

    #[test]
    fn http_response_clone() {
        let resp = HttpResponse {
            status: 200,
            body: b"data".to_vec(),
        };
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
