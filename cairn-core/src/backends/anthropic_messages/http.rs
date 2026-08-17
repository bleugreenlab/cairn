//! Anthropic Messages transport: request construction, the named-event SSE
//! reader, and one-shot completion.
//!
//! Provider-neutral. Everything specific to a deployment — the URL, the auth
//! header, the output cap its models accept — arrives as [`MessagesEndpoint`],
//! so this module serves OpenCode Go's gateway and a direct Anthropic key
//! without branching on either.

use super::wire::{
    ContentBlock, MessagesMessage, MessagesResponse, StreamEvent, StreamingMessage, SYSTEM_ROLE,
};
use crate::backends::http_loop::{
    cairn_tool_definitions, upstream_error_detail, AssistantStreamState,
};
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const STREAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Where a Messages deployment lives and how it authenticates.
#[derive(Debug, Clone)]
pub(crate) struct MessagesEndpoint {
    pub(crate) provider_name: &'static str,
    pub(crate) backend_key: &'static str,
    pub(crate) url: String,
    pub(crate) headers: Vec<(String, String)>,
    /// The `max_tokens` every request carries.
    ///
    /// Unlike chat/completions, this protocol REQUIRES the field — omitting it
    /// is an upstream 500, not a default — so a deployment states the cap its
    /// models accept rather than letting one be guessed per request.
    pub(crate) max_output_tokens: i64,
}

impl MessagesEndpoint {
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

/// Cairn's tools in the Messages wrapper: name and description at the top level,
/// with the argument schema under `input_schema`.
pub(crate) fn tool_schemas() -> Vec<Value> {
    cairn_tool_definitions()
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
            })
        })
        .collect()
}

/// Split the flat conversation into this protocol's two request fields.
///
/// The system prompt is a top-level field here rather than a leading message,
/// and a message left with no content after replay repair is dropped: an empty
/// content array is rejected.
fn split_system(messages: &[MessagesMessage]) -> (Vec<&ContentBlock>, Vec<&MessagesMessage>) {
    let mut system = Vec::new();
    let mut turns = Vec::new();
    for message in messages {
        if message.role == SYSTEM_ROLE {
            system.extend(message.content.iter());
        } else if !message.content.is_empty() {
            turns.push(message);
        }
    }
    (system, turns)
}

/// Build the request body for one generation.
///
/// Reasoning is expressed as a THINKING BUDGET on this protocol, not as an
/// effort word, so a configured effort string is deliberately not translated: an
/// invented effort-to-budget ladder would spend a user's tokens according to a
/// mapping nobody chose. An explicit thinking-token budget is passed through, and
/// clamped below `max_tokens` because the provider rejects a budget that meets or
/// exceeds it.
///
/// Structured output has no native equivalent here — there is no `response_format`
/// — so a schema-constrained run relies on the prompt plus Cairn's server-side
/// validation of the stored artifact, which is the same backstop the other
/// families already depend on when a provider honors the constraint loosely.
pub(crate) fn build_body(
    endpoint: &MessagesEndpoint,
    model: &str,
    messages: &[MessagesMessage],
    config: &SessionConfig,
    stream: bool,
) -> Value {
    let (system, turns) = split_system(messages);
    let mut body = json!({
        "model": model,
        "max_tokens": endpoint.max_output_tokens,
        "messages": turns,
        "tools": tool_schemas(),
        "tool_choice": { "type": "auto" },
        "stream": stream,
    });
    if !system.is_empty() {
        body["system"] = json!(system);
    }
    if let Some(budget) = config.max_thinking_tokens {
        let budget = (budget as i64).min(endpoint.max_output_tokens - 1);
        if budget > 0 {
            body["thinking"] = json!({ "type": "enabled", "budget_tokens": budget });
        }
    }
    body
}

/// POST one non-streaming generation, for the tool-free one-shot completion path.
pub(crate) fn post_message(
    endpoint: &MessagesEndpoint,
    body: Value,
    timeout: Duration,
) -> Result<MessagesResponse, crate::backends::CompletionError> {
    use crate::backends::CompletionError;
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
            "{} messages returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        )));
    }
    response
        .json()
        .map_err(|error| CompletionError::InvalidResponse(error.to_string()))
}

/// POST one streaming generation, pushing text and thinking to the live
/// transcript as they arrive and returning the rebuilt message.
#[allow(clippy::too_many_arguments)]
pub(crate) fn post_message_streaming(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    endpoint: &MessagesEndpoint,
    body: Value,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    cancel: &Arc<AtomicBool>,
) -> Result<MessagesResponse, String> {
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
        .post(&endpoint.url)
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
            "{} messages returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        ));
    }

    let mut aggregate = StreamingMessage::default();
    let mut stream_state: Option<AssistantStreamState> = None;
    let reader = BufReader::new(response);
    for line in reader.lines() {
        // The cancel flag is observed at SSE line boundaries. Dropping the
        // response (on break) closes the TCP connection so the provider stops
        // generating — and stops billing — promptly.
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        let line = line.map_err(|error: std::io::Error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                format!(
                    "{} generation exceeded {}s (an over-limit or hung upstream request); finalizing the turn instead of waiting indefinitely.",
                    endpoint.provider_name,
                    STREAM_REQUEST_TIMEOUT.as_secs()
                )
            } else {
                format!("{} stream read failed: {error}", endpoint.provider_name)
            }
        })?;
        // The event name is carried in the `data` payload's own `type` field, so
        // the `event:` lines are redundant and skipped. This protocol has no
        // `[DONE]` sentinel: the stream ends when the connection closes.
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        let event: StreamEvent = serde_json::from_str(data).map_err(|error| {
            format!(
                "Failed to parse {} stream event: {error}: {data}",
                endpoint.provider_name
            )
        })?;

        if let StreamEvent::Error { error } = &event {
            // An error delivered after the stream started (HTTP stayed 200).
            // Finalize any open live stream so it is not left dangling, then
            // surface the failure rather than storing a partial success.
            finalize_stream(&mut stream_state, orch, run_id, session_id, &aggregate)?;
            return Err(format!(
                "{} stream reported {}: {}",
                endpoint.provider_name,
                error.r#type.as_deref().unwrap_or("error"),
                error.message.as_deref().unwrap_or("no message")
            ));
        }

        let (text_delta, thinking_delta) = match &event {
            StreamEvent::ContentBlockDelta { delta, .. } => match delta {
                super::wire::BlockDelta::TextDelta { text } => (Some(text.clone()), None),
                super::wire::BlockDelta::ThinkingDelta { thinking } => {
                    (None, Some(thinking.clone()))
                }
                _ => (None, None),
            },
            _ => (None, None),
        };

        aggregate.apply(&event);

        // Thinking typically streams before text, so either delta opens the live
        // stream.
        if let Some(thinking) = thinking_delta.filter(|delta| !delta.is_empty()) {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_db,
                run_id,
                session_id,
                turn_id,
                endpoint,
            )?;
            state.append_thinking(orch, run_id, &thinking)?;
        }
        if let Some(text) = text_delta.filter(|delta| !delta.is_empty()) {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_db,
                run_id,
                session_id,
                turn_id,
                endpoint,
            )?;
            state.append(orch, run_id, &text)?;
        }

        // The gateway attaches the turn's cost to a keep-alive AFTER
        // `message_stop`, so the stream is read past the stop to collect it;
        // that ping is the real end of the exchange.
        if matches!(event, StreamEvent::Ping { .. }) && aggregate.saw_terminal() {
            break;
        }
    }

    let streamed_text = stream_state.is_some();
    finalize_stream(&mut stream_state, orch, run_id, session_id, &aggregate)?;
    require_terminal_event(
        endpoint.provider_name,
        aggregate.saw_terminal(),
        cancel.load(Ordering::SeqCst),
    )?;
    Ok(aggregate.into_response(streamed_text))
}

/// Refuse a stream that ended without the protocol saying it was done.
///
/// A chunked response can be closed by a proxy or a dropped upstream without
/// surfacing a read error, so reaching end-of-stream is not evidence that the
/// message completed. Treating it as success would store truncated assistant
/// text as a finished turn and — worse — hand the turn loop a tool call that
/// merely happened to parse, with no stop reason to judge it by, which is
/// exactly the signal the loop's truncation guard depends on.
///
/// A cancelled turn is the one legitimate way to end early: the user asked for
/// it, the caller already knows, and the run lands idle rather than failed.
pub(crate) fn require_terminal_event(
    provider_name: &str,
    saw_terminal: bool,
    cancelled: bool,
) -> Result<(), String> {
    if cancelled || saw_terminal {
        return Ok(());
    }
    Err(format!(
        "{provider_name} stream ended before the message completed (no message_stop). The \
         connection was closed mid-generation, so this turn's output is incomplete and is not \
         being recorded as a result."
    ))
}

fn finalize_stream(
    stream_state: &mut Option<AssistantStreamState>,
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    aggregate: &StreamingMessage,
) -> Result<(), String> {
    let Some(state) = stream_state.take() else {
        return Ok(());
    };
    let usage = aggregate
        .usage
        .clone()
        .map(|usage| usage.into_turn_usage(aggregate.cost));
    state.finalize(
        orch,
        run_id,
        session_id,
        aggregate.text(),
        aggregate.thinking(),
        Vec::new(),
        usage.as_ref(),
        aggregate.id.as_deref(),
        aggregate.model.as_deref(),
    )
}

#[allow(clippy::too_many_arguments)]
fn open_stream_state<'a>(
    state: &'a mut Option<AssistantStreamState>,
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    endpoint: &MessagesEndpoint,
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
