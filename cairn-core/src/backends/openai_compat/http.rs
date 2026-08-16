//! Provider-neutral streaming chat-completions transport.

use super::wire::{ChatResponse, ChatStreamChunk, StreamingAggregate};
use crate::backends::http_loop::AssistantStreamState;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const STREAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub(crate) struct Endpoint {
    pub(crate) provider_name: &'static str,
    pub(crate) backend_key: &'static str,
    pub(crate) chat_url: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) extra_body: Option<Value>,
}

pub(crate) fn post_chat_completion(
    endpoint: &Endpoint,
    mut body: Value,
    timeout: Duration,
) -> Result<ChatResponse, crate::backends::CompletionError> {
    use crate::backends::CompletionError;
    if let Some(extra) = &endpoint.extra_body {
        if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            body.extend(extra.clone());
        }
    }
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| CompletionError::Upstream(error.to_string()))?;
    let response = client
        .post(&endpoint.chat_url)
        .headers(
            endpoint
                .header_map()
                .map_err(CompletionError::InvalidRequest)?,
        )
        .json(&body)
        .send()
        .map_err(|error| {
            if error.is_timeout() {
                CompletionError::Timeout
            } else {
                CompletionError::Upstream(error.to_string())
            }
        })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().unwrap_or_default();
        return Err(CompletionError::Upstream(format!(
            "{} chat completion returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        )));
    }
    response
        .json()
        .map_err(|error| CompletionError::InvalidResponse(error.to_string()))
}

/// The sentence a provider wrote inside a JSON error body.
///
/// Providers on this path nest the message the same way — `{"error":
/// {"message": ...}}`, with or without a sibling `type` — so a refusal reaches
/// the transcript as the explanation the provider gave ("No payment method",
/// "only available hosted in China and requires explicit opt in") instead of a
/// wall of JSON the reader has to decode. Anything that is not JSON, or that
/// carries no message, passes through untouched: an unrecognized body is still
/// evidence, and dropping it would leave the failure unexplained.
pub(crate) fn upstream_error_detail(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|parsed| {
            let error = parsed.get("error").unwrap_or(&parsed);
            error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| body.to_string())
}

/// Inject the OpenAI-native structured-output constraint into a chat-completions
/// request body. `response_format` json_schema with `strict` demands
/// conformance; Cairn's server-side validation of the stored artifact is the
/// backstop if a provider honors it loosely. A no-op when the run carries no
/// output schema, leaving schema-less sessions bit-for-bit unchanged.
pub(crate) fn apply_output_schema(body: &mut Value, schema: Option<&Value>) {
    let Some(schema) = schema else {
        return;
    };
    body["response_format"] = serde_json::json!({
        "type": "json_schema",
        "json_schema": {
            "name": "cairn_output",
            "strict": true,
            "schema": schema,
        }
    });
}

impl Endpoint {
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn post_chat_completion_streaming(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    endpoint: &Endpoint,
    mut body: Value,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    if let Some(extra) = &endpoint.extra_body {
        if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            body.extend(extra.clone());
        }
    }
    let client = reqwest::blocking::Client::builder()
        .timeout(STREAM_REQUEST_TIMEOUT)
        .build()
        .map_err(|error| {
            format!(
                "Failed to build {} streaming client: {error}",
                endpoint.provider_name
            )
        })?;
    let response = client
        .post(&endpoint.chat_url)
        .headers(endpoint.header_map()?)
        .json(&body)
        .send()
        .map_err(|error| {
            format!(
                "{} streaming request failed: {error}",
                endpoint.provider_name
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let text = response
            .text()
            .unwrap_or_else(|error| format!("<failed to read error body: {error}>"));
        return Err(format!(
            "{} chat completion returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        ));
    }

    let mut aggregate = StreamingAggregate::default();
    let mut stream_state: Option<AssistantStreamState> = None;
    let reader = BufReader::new(response);
    for line in reader.lines() {
        // The cancel flag is observed at SSE line boundaries. During a streaming
        // turn tokens flow near-continuously, so this fires promptly in practice;
        // dropping the response (on break) closes the TCP connection so the
        // provider stops generating promptly.
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        let line = line.map_err(|error: std::io::Error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                format!(
                    "{} generation exceeded {}s (an over-limit or hung upstream request); finalizing the turn instead of waiting indefinitely.",
                    endpoint.provider_name, STREAM_REQUEST_TIMEOUT.as_secs()
                )
            } else {
                format!("{} stream read failed: {error}", endpoint.provider_name)
            }
        })?;
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            break;
        }
        let chunk: ChatStreamChunk = serde_json::from_str(data).map_err(|error| {
            format!(
                "Failed to parse {} stream chunk: {error}: {data}",
                endpoint.provider_name
            )
        })?;
        if let Some(message) = chunk.error_message() {
            // OpenRouter delivers post-stream-start errors in-band (HTTP stays
            // 200). Finalize any open live stream so it is not left dangling,
            // then surface the error; the turn handler records it and crashes
            // the run.
            if let Some(state) = stream_state.take() {
                state.finalize(
                    orch,
                    run_id,
                    session_id,
                    aggregate.text.clone(),
                    aggregate.reasoning.clone(),
                    aggregate.reasoning_detail_values(),
                    aggregate.usage.as_ref(),
                    aggregate.id.as_deref(),
                    aggregate.model.as_deref(),
                )?;
            }
            return Err(message);
        }
        aggregate.apply_chunk(&chunk);
        let reasoning_delta = chunk
            .choices
            .iter()
            .filter_map(|choice| choice.delta.reasoning.as_deref())
            .collect::<String>();
        let content_delta = chunk
            .choices
            .iter()
            .filter_map(|choice| choice.delta.content.as_deref())
            .collect::<String>();
        // Reasoning typically streams before content, so either delta opens the
        // live stream.
        if !reasoning_delta.is_empty() {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_db,
                run_id,
                session_id,
                turn_id,
                endpoint,
            )?;
            state.append_thinking(orch, run_id, &reasoning_delta)?;
        }
        if !content_delta.is_empty() {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_db,
                run_id,
                session_id,
                turn_id,
                endpoint,
            )?;
            state.append(orch, run_id, &content_delta)?;
        }
    }

    let streamed_text = stream_state.is_some();
    if let Some(state) = stream_state {
        state.finalize(
            orch,
            run_id,
            session_id,
            aggregate.text.clone(),
            aggregate.reasoning.clone(),
            aggregate.reasoning_detail_values(),
            aggregate.usage.as_ref(),
            aggregate.id.as_deref(),
            aggregate.model.as_deref(),
        )?;
    }

    Ok(aggregate.into_response(streamed_text))
}

#[allow(clippy::too_many_arguments)]
fn open_stream_state<'a>(
    state: &'a mut Option<AssistantStreamState>,
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    endpoint: &Endpoint,
) -> Result<&'a mut AssistantStreamState, String> {
    if state.is_none() {
        *state = Some(AssistantStreamState::open(
            orch,
            run_db.clone(),
            run_id,
            session_id,
            turn_id,
            endpoint.backend_key,
        )?);
    }
    Ok(state.as_mut().expect("stream state just initialized"))
}

#[cfg(test)]
mod tests {
    use super::{apply_output_schema, upstream_error_detail};
    use serde_json::json;

    #[test]
    fn a_json_error_body_reaches_the_reader_as_the_providers_own_sentence() {
        assert_eq!(
            upstream_error_detail(
                r#"{"type":"error","error":{"type":"CreditsError","message":"No payment method."}}"#
            ),
            "No payment method."
        );
        // The OpenAI shape, with no sibling `type`, resolves identically.
        assert_eq!(
            upstream_error_detail(r#"{"error":{"message":"model not found"}}"#),
            "model not found"
        );
    }

    #[test]
    fn an_unrecognized_body_is_preserved_rather_than_swallowed() {
        assert_eq!(
            upstream_error_detail("<html>502</html>"),
            "<html>502</html>"
        );
        assert_eq!(upstream_error_detail(r#"{"error":{}}"#), r#"{"error":{}}"#);
        assert_eq!(upstream_error_detail(""), "");
    }

    #[test]
    fn no_schema_leaves_the_body_unconstrained() {
        let mut body = json!({ "model": "m" });
        apply_output_schema(&mut body, None);
        assert!(body.get("response_format").is_none());
    }

    #[test]
    fn a_schema_becomes_a_strict_response_format() {
        let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}});
        let mut body = json!({ "model": "m" });
        apply_output_schema(&mut body, Some(&schema));
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["strict"], true);
        assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
    }
}
