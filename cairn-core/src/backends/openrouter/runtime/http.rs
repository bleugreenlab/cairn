//! The streaming chat-completions POST and its SSE reader: build the request
//! body, stream deltas into the live assistant stream and the response
//! aggregator, observe cancel at line boundaries, and detect in-band errors.

use super::persist::AssistantStreamState;
use super::tools::tool_schemas;
use super::wire::{ChatMessage, ChatResponse, ChatStreamChunk, StreamingAggregate};
use crate::backends::SessionConfig;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CHAT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
// Bound on a single streaming generation request. Each tool-loop iteration is
// one POST for one assistant response (the loop re-POSTs per tool round-trip),
// so this caps one generation, not a whole multi-iteration turn. A stalled
// upstream — including an over-limit request the provider hangs on — hits this
// and surfaces a clear error instead of hanging forever (the old `timeout(None)`
// waited indefinitely). Sized generously so a high-effort reasoning generation
// is never mistaken for a hang.
const STREAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[allow(clippy::too_many_arguments)]
pub(super) fn post_chat_completion(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    api_key: &str,
    model: &str,
    session_id: &str,
    messages: &[ChatMessage],
    config: &SessionConfig,
    run_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    provider: &Value,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    let mut body = json!({
        "model": model,
        "messages": messages,
        "tools": tool_schemas(),
        "tool_choice": "auto",
        "stream": true,
    });
    if let Some(effort) = config.reasoning_effort.as_deref() {
        body["reasoning"] = json!({ "effort": effort });
    }
    body["provider"] = provider.clone();
    apply_output_schema(&mut body, config.output_schema.as_ref());
    post_chat_completion_streaming(
        orch, run_db, api_key, body, run_id, session_id, turn_id, sequence, cancel,
    )
}

/// Inject the native structured-output constraint into an OpenRouter request
/// body for a schema-constrained call (CAIRN-2505). `response_format` json_schema
/// with `strict` demands conformance; `provider.require_parameters` routes ONLY
/// to providers that honor it, so a routed provider can't silently drop the
/// schema. Cairn's server-side validation of the stored artifact is the backstop
/// if one still does — non-conformance is a loud failure, never corrupt data. A
/// model with no schema-capable provider then fails the request loudly (an HTTP
/// error naming the model), which is the intended behavior. A no-op when the run
/// carries no output schema, leaving schema-less sessions bit-for-bit unchanged.
fn apply_output_schema(body: &mut Value, schema: Option<&Value>) {
    let Some(schema) = schema else {
        return;
    };
    body["response_format"] = json!({
        "type": "json_schema",
        "json_schema": {
            "name": "cairn_output",
            "strict": true,
            "schema": schema,
        }
    });
    match body.get_mut("provider").and_then(Value::as_object_mut) {
        Some(obj) => {
            obj.insert("require_parameters".to_string(), json!(true));
        }
        None => {
            body["provider"] = json!({ "require_parameters": true });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn post_chat_completion_streaming(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    api_key: &str,
    body: Value,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    cancel: &Arc<AtomicBool>,
) -> Result<ChatResponse, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(STREAM_REQUEST_TIMEOUT)
        .build()
        .map_err(|error| format!("Failed to build OpenRouter streaming client: {error}"))?;
    let response = client
        .post(CHAT_URL)
        .headers(openrouter_headers(api_key)?)
        .json(&body)
        .send()
        .map_err(|error| format!("OpenRouter streaming request failed: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        let text = response
            .text()
            .unwrap_or_else(|error| format!("<failed to read error body: {error}>"));
        return Err(format!(
            "OpenRouter chat completion returned HTTP {}: {}",
            status.as_u16(),
            text
        ));
    }

    let mut aggregate = StreamingAggregate::default();
    let mut stream_state: Option<AssistantStreamState> = None;
    let reader = BufReader::new(response);
    for line in reader.lines() {
        // The cancel flag is observed at SSE line boundaries. During a streaming
        // turn tokens flow near-continuously, so this fires promptly in practice;
        // dropping the response (on break) closes the TCP connection, which is
        // what makes OpenRouter stop billing.
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        let line = line.map_err(|error: std::io::Error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                format!(
                    "OpenRouter generation exceeded {}s (an over-limit or hung upstream request); finalizing the turn instead of waiting indefinitely.",
                    STREAM_REQUEST_TIMEOUT.as_secs()
                )
            } else {
                format!("OpenRouter stream read failed: {error}")
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
        let chunk: ChatStreamChunk = serde_json::from_str(data)
            .map_err(|error| format!("Failed to parse OpenRouter stream chunk: {error}: {data}"))?;
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
                sequence,
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
                sequence,
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
    sequence: i32,
) -> Result<&'a mut AssistantStreamState, String> {
    if state.is_none() {
        *state = Some(AssistantStreamState::open(
            orch,
            run_db.clone(),
            run_id,
            session_id,
            turn_id,
            sequence,
        )?);
    }
    Ok(state.as_mut().expect("stream state just initialized"))
}

fn openrouter_headers(api_key: &str) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|error| format!("invalid OpenRouter API key header: {error}"))?,
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        "HTTP-Referer",
        HeaderValue::from_static("https://cairn.computer"),
    );
    headers.insert("X-OpenRouter-Title", HeaderValue::from_static("Cairn"));
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::apply_output_schema;
    use serde_json::json;

    #[test]
    fn no_schema_leaves_body_unconstrained() {
        let mut body = json!({ "model": "m", "provider": { "sort": "throughput" } });
        apply_output_schema(&mut body, None);
        assert!(body.get("response_format").is_none());
        // The existing provider object is untouched.
        assert!(body["provider"].get("require_parameters").is_none());
    }

    #[test]
    fn schema_sets_response_format_and_require_parameters() {
        let schema = json!({
            "type": "object",
            "required": ["answer"],
            "properties": {"answer": {"type": "string"}}
        });
        let mut body = json!({ "model": "m", "provider": { "sort": "throughput" } });
        apply_output_schema(&mut body, Some(&schema));

        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["strict"], true);
        assert_eq!(body["response_format"]["json_schema"]["schema"], schema);
        // require_parameters is merged into the existing provider object, not
        // clobbering its routing prefs.
        assert_eq!(body["provider"]["require_parameters"], true);
        assert_eq!(body["provider"]["sort"], "throughput");
    }

    #[test]
    fn schema_creates_provider_object_when_absent() {
        let schema = json!({ "type": "object" });
        let mut body = json!({ "model": "m" });
        apply_output_schema(&mut body, Some(&schema));
        assert_eq!(body["provider"]["require_parameters"], true);
    }
}
