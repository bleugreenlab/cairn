//! The embedding wires: texts in, one vector per text out, one non-streamed
//! POST.
//!
//! Two families. [`openai`] is OpenAI's `embeddings` shape, which OpenRouter,
//! Ollama (`/v1`), llama.cpp, vLLM and most local hubs also take. [`gateway`]
//! is Cairn's own cloud `/embed`. Like [`crate::decisions`], nothing here
//! branches on which deployment it is talking to: the URL and authentication
//! arrive on the endpoint. The types are the providers' JSON; the facet's
//! `embed` verb checks widths, order and identity on top of them.

pub mod gateway;
pub mod openai;

#[cfg(test)]
mod tests;

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::withdrawal::{self, SendError, Withdrawal};
use crate::{upstream_error_detail, CompletionError};

/// Header values carry bearer keys and device tokens; only their names are
/// ever printed.
fn redacted(headers: &[(String, String)]) -> Vec<String> {
    headers
        .iter()
        .map(|(name, _)| format!("{name}: <redacted>"))
        .collect()
}

fn header_map(provider_name: &str, headers: &[(String, String)]) -> Result<HeaderMap, String> {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| format!("invalid {provider_name} header name: {error}"))?;
        let value = HeaderValue::from_str(value)
            .map_err(|error| format!("invalid {provider_name} header value: {error}"))?;
        map.insert(name, value);
    }
    Ok(map)
}

/// One POST of `body` to `url`, answering the reply text of a 2xx.
///
/// A non-2xx is [`CompletionError::Http`]: its status and `Retry-After` as
/// values, and a message naming the status, so a host can tell exhausted
/// credits (402) from a rate limit (429), followed by the sentence the
/// provider wrote. `hint` adds a clause for a status that usually means the
/// wrong kind of model. A request that never reached the provider is
/// [`CompletionError::Unreachable`]; one cut off by `withdrawal` is
/// [`CompletionError::Withdrawn`].
#[allow(clippy::too_many_arguments)]
fn post(
    provider_name: &str,
    what: &str,
    url: &str,
    headers: &[(String, String)],
    body: &Value,
    timeout: Duration,
    hint: fn(u16) -> Option<&'static str>,
    withdrawal: Option<&Withdrawal>,
) -> Result<String, CompletionError> {
    let client = cairn_tls::client_builder()
        .timeout(timeout)
        .build()
        .map_err(|error| CompletionError::Upstream(error.to_string()))?;
    let request = client
        .post(url)
        .headers(header_map(provider_name, headers).map_err(CompletionError::InvalidRequest)?)
        .json(body);
    let response = withdrawal::send(request, withdrawal).map_err(|error| match error {
        SendError::Withdrawn => CompletionError::withdrawn(provider_name),
        SendError::Request(error) if error.is_timeout() => CompletionError::Timeout,
        SendError::Request(error) => {
            CompletionError::Unreachable(format!("{provider_name} {what} request failed: {error}"))
        }
    })?;
    let status = response.status();
    let wait = crate::retry_after(response.headers());
    let text = response.text().map_err(|error| match error.kind() {
        std::io::ErrorKind::TimedOut => CompletionError::Timeout,
        std::io::ErrorKind::ConnectionAborted
            if withdrawal.is_some_and(Withdrawal::is_withdrawn) =>
        {
            CompletionError::withdrawn(provider_name)
        }
        _ => CompletionError::InvalidResponse(error.to_string()),
    })?;
    if !status.is_success() {
        let code = status.as_u16();
        let hint = hint(code)
            .map(|hint| format!(" ({hint})"))
            .unwrap_or_default();
        return Err(CompletionError::Http {
            status: code,
            message: format!(
                "{provider_name} {what} returned HTTP {code}: {}{hint}",
                upstream_error_detail(&text)
            ),
            retry_after: wait,
        });
    }
    Ok(text)
}
