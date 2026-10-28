//! The Decisions transport: the request body and its one POST.

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Value};

use super::wire::{DecisionsResponse, WireQuestion};
use crate::{upstream_error_detail, CompletionError};

/// Where a decision deployment lives and how it authenticates, whichever of
/// the two decision wires it speaks (this one, or [`crate::systemone`]): both
/// are one whole JSON POST to one URL.
#[derive(Clone)]
pub struct DecisionsEndpoint {
    /// What to call this provider in an error a user will read.
    pub provider_name: String,
    /// The backend key usage is attributed to.
    pub backend_key: String,
    /// The full URL a decision is posted to.
    pub url: String,
    pub headers: Vec<(String, String)>,
    /// This deployment's context limits, as a sentence a request that
    /// exceeds them is told (what they are, and what lifts them). `None`
    /// when the provider documents none.
    pub context_limits: Option<String>,
}

/// Header values carry bearer keys; only their names are printed.
impl std::fmt::Debug for DecisionsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionsEndpoint")
            .field("provider_name", &self.provider_name)
            .field("backend_key", &self.backend_key)
            .field("url", &self.url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| format!("{name}: <redacted>"))
                    .collect::<Vec<_>>(),
            )
            .field("context_limits", &self.context_limits)
            .finish()
    }
}

impl DecisionsEndpoint {
    fn header_map(&self) -> Result<HeaderMap, String> {
        let mut map = HeaderMap::new();
        for (name, value) in &self.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| format!("invalid {} header name: {error}", self.provider_name))?;
            let value = HeaderValue::from_str(value)
                .map_err(|error| format!("invalid {} header value: {error}", self.provider_name))?;
            map.insert(name, value);
        }
        Ok(map)
    }
}

/// Build a Decisions request body. `input` is asked every question.
pub fn build_body(model: &str, input: &str, questions: &[WireQuestion]) -> Value {
    json!({
        "model": model,
        "input": input,
        "questions": questions,
    })
}

/// Post one decision request and read its reply.
pub fn post_decisions(
    endpoint: &DecisionsEndpoint,
    body: Value,
    timeout: Duration,
) -> Result<DecisionsResponse, CompletionError> {
    read(&post_whole(endpoint, &body, timeout)?.into_body(endpoint)?)
}

/// What one decision POST got back, whatever its HTTP status, so a wire can
/// read a failure's body before it becomes an error.
pub(crate) struct WholeReply {
    pub status: reqwest::StatusCode,
    pub text: String,
}

impl WholeReply {
    /// The reply body on success; otherwise the provider's own sentence, as
    /// an upstream error.
    pub(crate) fn into_body(self, endpoint: &DecisionsEndpoint) -> Result<String, CompletionError> {
        if self.status.is_success() {
            return Ok(self.text);
        }
        Err(CompletionError::Upstream(format!(
            "{} decision returned HTTP {}: {}",
            endpoint.provider_name,
            self.status.as_u16(),
            upstream_error_detail(&self.text)
        )))
    }
}

/// One decision POST, either wire's. Only a request that never got a reply
/// is an error here; an HTTP failure comes back as its status and body.
pub(crate) fn post_whole(
    endpoint: &DecisionsEndpoint,
    body: &Value,
    timeout: Duration,
) -> Result<WholeReply, CompletionError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| CompletionError::Upstream(error.to_string()))?;
    let response = client
        .post(&endpoint.url)
        .headers(
            endpoint
                .header_map()
                .map_err(CompletionError::InvalidRequest)?,
        )
        .json(body)
        .send()
        .map_err(|error| {
            if error.is_timeout() {
                CompletionError::Timeout
            } else {
                CompletionError::Upstream(format!(
                    "{} decision request failed: {error}",
                    endpoint.provider_name
                ))
            }
        })?;
    let status = response.status();
    let text = response
        .text()
        .map_err(|error| CompletionError::InvalidResponse(error.to_string()))?;
    Ok(WholeReply { status, text })
}

/// Decode a Decisions reply body.
pub fn read(text: &str) -> Result<DecisionsResponse, CompletionError> {
    serde_json::from_str(text).map_err(|error| {
        CompletionError::InvalidResponse(format!("unreadable decision reply: {error}: {text}"))
    })
}
