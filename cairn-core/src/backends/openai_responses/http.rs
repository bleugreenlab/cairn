//! OpenAI Responses transport: request construction, the typed lifecycle SSE
//! reader, and one-shot completion.
//!
//! Provider-neutral. The URL and authentication arrive as
//! [`ResponsesEndpoint`], so this module serves OpenCode Go's gateway today and
//! a direct OpenAI key later without branching on either.

use super::wire::{
    ResponseStreamEvent, ResponsesItem, ResponsesResponse, ResponsesTurn, StreamingResponse,
};
use crate::backends::http_loop::{
    cairn_tool_definitions, require_terminal_event, upstream_error_detail, AssistantStreamState,
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

/// Where a Responses deployment lives and how it authenticates.
#[derive(Debug, Clone)]
pub(crate) struct ResponsesEndpoint {
    pub(crate) provider_name: &'static str,
    pub(crate) backend_key: &'static str,
    pub(crate) url: String,
    pub(crate) headers: Vec<(String, String)>,
}

impl ResponsesEndpoint {
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

/// Cairn's tools in the Responses wrapper: name and parameters flattened beside
/// a `type` discriminator, with an explicit `strict` flag.
pub(crate) fn tool_schemas() -> Vec<Value> {
    cairn_tool_definitions()
        .into_iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                // Cairn's tool schemas allow extra keys by design (a `write`
                // change payload varies by target), which strict mode forbids.
                "strict": false,
            })
        })
        .collect()
}

/// Split the flat turn list into this protocol's `instructions` and `input`.
fn split_instructions(turns: &[ResponsesTurn]) -> (String, Vec<&ResponsesItem>) {
    let mut instructions = String::new();
    let mut input = Vec::new();
    for turn in turns {
        for item in &turn.items {
            match item {
                ResponsesItem::Instructions { text } => instructions.push_str(text),
                ResponsesItem::Unknown => {}
                item => input.push(item),
            }
        }
    }
    (instructions, input)
}

/// Build the request body for one generation.
///
/// The whole input list is sent every turn rather than a `previous_response_id`:
/// Cairn resumes a session from its own transcript, so a conversation that only
/// exists on the provider's side is one Cairn cannot continue.
pub(crate) fn build_body(
    model: &str,
    turns: &[ResponsesTurn],
    config: &SessionConfig,
    stream: bool,
) -> Value {
    let (instructions, input) = split_instructions(turns);
    let mut body = json!({
        "model": model,
        "input": input,
        "tools": tool_schemas(),
        "tool_choice": "auto",
        "stream": stream,
    });
    if !instructions.is_empty() {
        body["instructions"] = json!(instructions);
    }
    if let Some(effort) = config.reasoning_effort.as_deref() {
        // Responses takes an effort word natively, so the configured value passes
        // through as written; an effort a model does not accept comes back as
        // the provider's own refusal, which says more than a silent coercion.
        body["reasoning"] = json!({ "effort": effort });
    }
    apply_output_schema(&mut body, config.output_schema.as_ref());
    body
}

/// Constrain the response to Cairn's output schema using this protocol's native
/// JSON-schema format. Cairn's server-side validation of the stored artifact
/// remains the backstop if a provider honors it loosely. A no-op when the run
/// carries no schema.
pub(crate) fn apply_output_schema(body: &mut Value, schema: Option<&Value>) {
    let Some(schema) = schema else {
        return;
    };
    body["text"] = json!({
        "format": {
            "type": "json_schema",
            "name": "cairn_output",
            "strict": true,
            "schema": schema,
        }
    });
}

/// POST one non-streaming generation, for the tool-free one-shot completion path.
pub(crate) fn post_response(
    endpoint: &ResponsesEndpoint,
    body: Value,
    timeout: Duration,
) -> Result<ResponsesResponse, crate::backends::CompletionError> {
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
            "{} responses returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        )));
    }
    let envelope: super::wire::ResponseEnvelope = response
        .json()
        .map_err(|error| CompletionError::InvalidResponse(error.to_string()))?;
    if let Some(error) = &envelope.error {
        return Err(CompletionError::Upstream(format!(
            "{} responses failed ({}): {}",
            endpoint.provider_name,
            error.code.as_deref().unwrap_or("error"),
            error.message.as_deref().unwrap_or("no message")
        )));
    }
    Ok(ResponsesResponse::from_envelope(envelope))
}

/// POST one streaming generation, pushing text and reasoning summaries to the
/// live transcript as they arrive and returning the rebuilt response.
#[allow(clippy::too_many_arguments)]
pub(crate) fn post_response_streaming(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    endpoint: &ResponsesEndpoint,
    body: Value,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    cancel: &Arc<AtomicBool>,
) -> Result<ResponsesResponse, String> {
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
            "{} responses returned HTTP {}: {}",
            endpoint.provider_name,
            status.as_u16(),
            upstream_error_detail(&text)
        ));
    }

    let mut aggregate = StreamingResponse::default();
    let mut stream_state: Option<AssistantStreamState> = None;
    let reader = BufReader::new(response);
    for line in reader.lines() {
        // The cancel flag is observed at SSE line boundaries. Dropping the
        // response closes the connection so the provider stops generating.
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
        // the `event:` lines are redundant. This protocol has no `[DONE]`
        // sentinel: a terminal lifecycle event ends the exchange.
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        let event: ResponseStreamEvent = serde_json::from_str(data).map_err(|error| {
            format!(
                "Failed to parse {} stream event: {error}: {data}",
                endpoint.provider_name
            )
        })?;

        if let ResponseStreamEvent::Failed { response } = &event {
            // A failure after the stream started. Finalize any open live stream
            // so it is not left dangling, then surface the failure rather than
            // storing a partial success.
            finalize_stream(&mut stream_state, orch, run_id, session_id, &aggregate)?;
            let error = response.error.as_ref();
            return Err(format!(
                "{} response failed ({}): {}",
                endpoint.provider_name,
                error.and_then(|e| e.code.as_deref()).unwrap_or("error"),
                error
                    .and_then(|e| e.message.as_deref())
                    .unwrap_or("no message")
            ));
        }

        let (text_delta, reasoning_delta) = match &event {
            ResponseStreamEvent::OutputTextDelta { delta, .. } => (Some(delta.clone()), None),
            ResponseStreamEvent::ReasoningSummaryDelta { delta, .. } => (None, Some(delta.clone())),
            _ => (None, None),
        };

        aggregate.apply(&event);

        if let Some(reasoning) = reasoning_delta.filter(|delta| !delta.is_empty()) {
            let state = open_stream_state(
                &mut stream_state,
                orch,
                run_db,
                run_id,
                session_id,
                turn_id,
                endpoint,
            )?;
            state.append_thinking(orch, run_id, &reasoning)?;
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

        // An incomplete response is a real, storable outcome (the output cap was
        // reached), not an error — it just ends the exchange here.
        if aggregate.saw_terminal() {
            break;
        }
    }

    let streamed_text = stream_state.is_some();
    finalize_stream(&mut stream_state, orch, run_id, session_id, &aggregate)?;
    require_terminal_event(
        endpoint.provider_name,
        TERMINAL_EVENTS,
        aggregate.saw_terminal(),
        cancel.load(Ordering::SeqCst),
    )?;
    Ok(aggregate.into_response(streamed_text))
}

/// What completion looks like on the Responses wire.
pub(crate) const TERMINAL_EVENTS: &str = "response.completed or response.incomplete";

fn finalize_stream(
    stream_state: &mut Option<AssistantStreamState>,
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    aggregate: &StreamingResponse,
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
        aggregate.reasoning_text(),
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
    endpoint: &ResponsesEndpoint,
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
