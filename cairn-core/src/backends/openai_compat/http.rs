//! Provider-neutral streaming chat-completions transport.

use super::wire::{ChatResponse, ChatStreamChunk, StreamingAggregate};
use crate::backends::http_loop::{require_terminal_event, AssistantStreamState};
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

// Error-body extraction is shared by every HTTP protocol family, so it lives at
// the neutral boundary and is re-exported here for this module's callers.
pub(crate) use crate::backends::http_loop::upstream_error_detail;

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

/// What completion looks like on the chat-completions wire.
///
/// Either signal satisfies the rule because OpenAI-compatible servers are
/// reliable about different ones: requiring the sentinel alone would fail turns
/// that genuinely finished on a server that never sends it.
pub(crate) const TERMINAL_EVENTS: &str = "finish_reason or [DONE]";

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
    let mut saw_done_sentinel = false;
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
            saw_done_sentinel = true;
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

    // Checked after finalizing so a truncated stream does not also leave a live
    // assistant message dangling in the UI, and before building the response so
    // no partial generation reaches the turn loop as a whole one. The cancel flag
    // is read here rather than taken from the loop: a cancellation requested
    // while the reader was blocked never reaches a line boundary at all.
    require_terminal_event(
        endpoint.provider_name,
        TERMINAL_EVENTS,
        saw_done_sentinel || aggregate.finish_reason().is_some(),
        cancel.load(Ordering::SeqCst),
    )?;

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

// === Premature end of stream ===

#[cfg(test)]
mod terminal_event_tests {
    use super::TERMINAL_EVENTS;
    use crate::backends::http_loop::require_terminal_event;
    use crate::backends::openai_compat::wire::{ChatStreamChunk, StreamingAggregate};

    const PROVIDER: &str = "Actual";

    /// Replay an SSE fixture through the same parse-and-aggregate steps the
    /// streaming loop uses, and report whether the protocol said it was done.
    /// Everything between the socket and the completion verdict, with no HTTP.
    fn replay(fixture: &str) -> (StreamingAggregate, bool) {
        let mut aggregate = StreamingAggregate::default();
        let mut saw_done_sentinel = false;
        for line in fixture.lines() {
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data.is_empty() {
                continue;
            }
            if data == "[DONE]" {
                saw_done_sentinel = true;
                break;
            }
            let chunk: ChatStreamChunk =
                serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
            aggregate.apply_chunk(&chunk);
        }
        (aggregate, saw_done_sentinel)
    }

    fn verdict(fixture: &str) -> Result<(), String> {
        let (aggregate, saw_done_sentinel) = replay(fixture);
        require_terminal_event(
            PROVIDER,
            TERMINAL_EVENTS,
            saw_done_sentinel || aggregate.finish_reason().is_some(),
            false,
        )
    }

    const TEXT_CUT_OFF: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Deleting \"}}]}\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"nothing\"}}]}\n",
    );

    const FINISH_REASON_NO_SENTINEL: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"}}]}\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n",
    );

    const SENTINEL_NO_FINISH_REASON: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"}}]}\n",
        "data: [DONE]\n",
    );

    /// A tool call whose arguments stopped arriving partway through. This is the
    /// case the invariant exists for: the JSON is a valid prefix, so nothing
    /// downstream can tell it was cut off.
    const TOOL_CALL_CUT_OFF: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"run\",\"arguments\":\"\"}}]}}]}\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"command\\\": \\\"rm -rf \"}}]}}]}\n",
    );

    #[test]
    fn a_connection_that_closes_mid_generation_is_not_a_finished_turn() {
        let error =
            verdict(TEXT_CUT_OFF).expect_err("a stream with no terminal event is truncated");
        assert!(error.contains(PROVIDER), "names the provider: {error}");
        assert!(
            error.contains("finish_reason") && error.contains("[DONE]"),
            "says which terminal events were missing: {error}"
        );
    }

    #[test]
    fn a_tool_call_cut_off_mid_arguments_is_refused_rather_than_dispatched() {
        // Left to the byte stream this looks like a complete turn carrying one
        // tool call, and the truncated argument JSON is a valid prefix of the
        // real one. Dispatching it would run a command the model never finished
        // asking for.
        assert!(
            verdict(TOOL_CALL_CUT_OFF).is_err(),
            "a half-streamed tool call must not reach the turn loop"
        );
    }

    #[test]
    fn a_terminal_finish_reason_completes_a_stream_that_sends_no_sentinel() {
        // Not every OpenAI-compatible server emits `[DONE]`; requiring the
        // sentinel alone would fail turns that genuinely finished.
        assert!(verdict(FINISH_REASON_NO_SENTINEL).is_ok());
    }

    #[test]
    fn the_done_sentinel_completes_a_stream_that_reports_no_finish_reason() {
        assert!(verdict(SENTINEL_NO_FINISH_REASON).is_ok());
    }

    #[test]
    fn cancellation_is_the_explicit_early_exit_and_is_never_truncation() {
        // A cancelled turn ends without a terminal event by construction. That
        // is the user's decision, not a broken stream, so it must not surface
        // as a provider error.
        let (aggregate, saw_done_sentinel) = replay(TEXT_CUT_OFF);
        assert!(require_terminal_event(
            PROVIDER,
            TERMINAL_EVENTS,
            saw_done_sentinel || aggregate.finish_reason().is_some(),
            true,
        )
        .is_ok());
    }
}
