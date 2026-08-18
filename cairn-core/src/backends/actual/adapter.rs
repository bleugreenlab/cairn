use super::{target_for_model, Target, ACTUAL_BACKEND_KEY, ACTUAL_BACKEND_NAME};
use crate::backends::http_loop::{render_tool_result, Connection, Generation, WireAdapter};
use crate::backends::openai_compat::generation::into_generation;
use crate::backends::openai_compat::http::{post_chat_completion_streaming, Endpoint};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage};
use crate::backends::openai_compat::{context, conversation};
use crate::backends::SessionConfig;
use crate::backends::{
    CompletionError, CompletionOutcome, CompletionRequest, CompletionRole, CompletionTokens,
};
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::json;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

pub(super) struct ActualAdapter {
    target: Target,
    endpoint: Endpoint,
}

impl ActualAdapter {
    pub(super) fn new(
        orch: &Orchestrator,
        model: &str,
        project_id: Option<&str>,
    ) -> Result<Self, String> {
        Ok(Self::for_target(target_for_model(orch, model, project_id)?))
    }

    pub(super) fn for_target(target: Target) -> Self {
        let endpoint = Endpoint {
            provider_name: ACTUAL_BACKEND_NAME,
            backend_key: ACTUAL_BACKEND_KEY,
            chat_url: target.url("/v1/chat/completions"),
            headers: target.headers(),
            extra_body: None,
        };
        Self { target, endpoint }
    }

    pub(super) fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionOutcome, CompletionError> {
        use crate::backends::openai_compat::http::post_chat_completion;

        let requested_model = request.model.clone();
        let body = completion_body(&request)?;
        let started = Instant::now();
        let response = post_chat_completion(&self.endpoint, body, request.timeout).map_err(
            |error| match error {
                CompletionError::Upstream(message) => {
                    CompletionError::Upstream(explain_upstream(&self.target, message))
                }
                other => other,
            },
        )?;
        let text = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .and_then(ChatContent::as_text)
            .ok_or_else(|| CompletionError::InvalidResponse("missing text choice".to_string()))?
            .to_string();
        let usage = response.usage.as_ref();
        Ok(CompletionOutcome {
            text,
            parsed: None,
            model: response.model.unwrap_or(requested_model),
            tokens: CompletionTokens {
                input: usage.and_then(|usage| usage.prompt_tokens.map(|value| value as u64)),
                output: usage.and_then(|usage| usage.completion_tokens.map(|value| value as u64)),
            },
            // Inference runs on hardware the user owns. There is no
            // provider-side price to report, and deriving one from token counts
            // would invent a cost that nobody is charging.
            cost: None,
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

/// Turn a transport error into something that names the readiness state it
/// actually represents.
///
/// Actual's two most common failures are both documented but neither is
/// self-describing on the wire: a 503 means the server is up and holding no
/// loaded model, and the relay's 401 body is the bare token
/// `missing_or_malformed_credential`, which does not say which target rejected
/// it. Left alone, both reach the transcript as an HTTP status and a shrug.
pub(super) fn explain_upstream(target: &Target, message: String) -> String {
    // Before anything else: the upstream body is echoed into the transcript, and
    // a gateway that reflects the request would put the `ac_` key there durably.
    let message = target.redact_credential(message);
    let label = &target.label;
    if message.contains("HTTP 503") {
        return format!(
            "{message}. Actual on {label} is reachable but has no model loaded; load one with `actual models list` and `actual models download` on that device, then retry."
        );
    }
    if message.contains("HTTP 401") || message.contains("HTTP 403") {
        return format!(
            "{message}. Actual rejected the credential for {label}; check the ac_ inference credential on this target."
        );
    }
    message
}

fn completion_body(request: &CompletionRequest) -> Result<serde_json::Value, CompletionError> {
    if request.messages.is_empty() {
        return Err(CompletionError::InvalidRequest(
            "at least one message is required".to_string(),
        ));
    }
    let mut messages = Vec::with_capacity(request.messages.len() + 1);
    if let Some(system) = &request.system {
        messages.push(json!({"role": "system", "content": system}));
    }
    messages.extend(request.messages.iter().map(|message| {
        let role = match message.role {
            CompletionRole::User => "user",
            CompletionRole::Assistant => "assistant",
        };
        json!({"role": role, "content": message.content})
    }));
    let mut body = json!({
        "model": request.model,
        "messages": messages,
        "stream": false,
    });
    if let Some(extras) = request.extras.as_object() {
        body.as_object_mut()
            .expect("completion body is an object")
            .extend(extras.clone());
    } else if !request.extras.is_null() {
        return Err(CompletionError::InvalidRequest(
            "extras must be an object or null".to_string(),
        ));
    }
    if let Some(schema) = &request.output_schema {
        body["response_format"] = json!({
            "type": "json_schema",
            "json_schema": {"name": "response", "strict": true, "schema": schema},
        });
    }
    Ok(body)
}

impl WireAdapter for ActualAdapter {
    type Message = ChatMessage;

    fn backend_key(&self) -> &'static str {
        ACTUAL_BACKEND_KEY
    }

    fn backend_name(&self) -> &'static str {
        ACTUAL_BACKEND_NAME
    }

    fn default_model(&self) -> &'static str {
        ""
    }

    fn connection(&self, _: &Orchestrator) -> Result<Connection, String> {
        // The credential travels in this target's headers, held with the
        // endpoint it authenticates to, rather than as a loose api key the loop
        // would have to pair back up with a base URL.
        Ok(Connection { api_key: None })
    }

    fn build_conversation(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        system_prompt: &str,
    ) -> Result<Vec<ChatMessage>, String> {
        conversation::build_conversation_messages(orch, config, session_id, system_prompt)
    }

    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64> {
        orch.context_window_for_context_tokens(ACTUAL_BACKEND_KEY, Some(model))
    }

    fn fit_conversation<'a>(
        &self,
        messages: &'a [ChatMessage],
        window: Option<i64>,
    ) -> Cow<'a, [ChatMessage]> {
        context::fit_conversation(messages, window)
    }

    #[allow(clippy::too_many_arguments)]
    fn post_generation(
        &self,
        orch: &Orchestrator,
        run_db: &Arc<LocalDb>,
        _: &Connection,
        model: &str,
        session_id: &str,
        outgoing: &[ChatMessage],
        _: &SessionConfig,
        run_id: &str,
        turn_id: Option<&str>,
        cancel: &Arc<AtomicBool>,
    ) -> Result<Generation<ChatMessage>, String> {
        let body = json!({
            "model": model,
            "messages": outgoing,
            "tools": crate::backends::openai_compat::tool_schemas(),
            "tool_choice": "auto",
            "stream": true,
            "stream_options": {"include_usage": true}
        });
        into_generation(
            post_chat_completion_streaming(
                orch,
                run_db,
                &self.endpoint,
                body,
                run_id,
                session_id,
                turn_id,
                cancel,
            )
            .map_err(|error| explain_upstream(&self.target, error))?,
            ACTUAL_BACKEND_NAME,
        )
    }

    fn render_tool_result_message(&self, id: &str, output: DispatchOutput) -> ChatMessage {
        ChatMessage::tool(id.to_string(), render_tool_result(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::CompletionMessage;
    use crate::identity::ActualTargetKind;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn local_target() -> Target {
        Target {
            account_id: "acc".into(),
            label: "Studio".into(),
            kind: ActualTargetKind::Local,
            base_url: "http://127.0.0.1:1".into(),
            api_key: None,
            cluster_id: None,
        }
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            system: Some("Be concise".into()),
            messages: vec![
                CompletionMessage {
                    role: CompletionRole::User,
                    content: "hello".into(),
                },
                CompletionMessage {
                    role: CompletionRole::Assistant,
                    content: "hi".into(),
                },
            ],
            model: "google/gemma-4-e2b-it".into(),
            project_id: None,
            extras: json!({"temperature": 0.2}),
            output_schema: None,
            timeout: Duration::from_secs(1),
        }
    }

    /// A one-shot HTTP server that captures the request it received, so header
    /// assertions test the bytes on the wire rather than the struct.
    fn adapter_for(response: String) -> (ActualAdapter, std::sync::mpsc::Receiver<String>) {
        adapter_for_target(response, local_target(), Duration::ZERO)
    }

    fn adapter_for_target(
        response: String,
        mut target: Target,
        delay: Duration,
    ) -> (ActualAdapter, std::sync::mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0; 8192];
            let read = stream.read(&mut buffer).unwrap_or(0);
            let _ = tx.send(String::from_utf8_lossy(&buffer[..read]).to_string());
            thread::sleep(delay);
            let _ = stream.write_all(response.as_bytes());
        });
        target.base_url = format!("http://{address}");
        (ActualAdapter::for_target(target), rx)
    }

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn completion_body_is_tool_free_and_preserves_messages_and_options() {
        let mut request = request();
        request.output_schema = Some(json!({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"]
        }));
        let body = completion_body(&request).unwrap();
        assert_eq!(body["model"], "google/gemma-4-e2b-it");
        assert_eq!(body["stream"], false);
        assert_eq!(body["temperature"], 0.2);
        assert_eq!(
            body["messages"][0],
            json!({"role": "system", "content": "Be concise"})
        );
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert!(body.get("tools").is_none());
    }

    #[test]
    fn a_local_turn_carries_no_authorization_header() {
        let body = json!({
            "model": "gemma",
            "choices": [{"message": {"role": "assistant", "content": "done"}}]
        })
        .to_string();
        let (adapter, requests) = adapter_for(http_response("200 OK", &body));
        adapter.complete(request()).unwrap();
        let sent = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            !sent.to_lowercase().contains("authorization"),
            "loopback must not send a credential: {sent}"
        );
        assert!(!sent.to_lowercase().contains("x-cluster-id"));
    }

    #[test]
    fn a_relay_turn_sends_the_bearer_credential_and_cluster_pin() {
        let body = json!({
            "model": "gemma",
            "choices": [{"message": {"role": "assistant", "content": "done"}}]
        })
        .to_string();
        let mut relay = local_target();
        relay.kind = ActualTargetKind::Relay;
        relay.api_key = Some("ac_live_secret".into());
        relay.cluster_id = Some("cluster-7".into());
        let (adapter, requests) =
            adapter_for_target(http_response("200 OK", &body), relay, Duration::ZERO);
        adapter.complete(request()).unwrap();
        let sent = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            sent.contains("authorization: Bearer ac_live_secret"),
            "{sent}"
        );
        assert!(sent.contains("x-cluster-id: cluster-7"), "{sent}");
    }

    #[test]
    fn a_503_is_reported_as_no_model_loaded_rather_than_a_bare_status() {
        let (adapter, _requests) = adapter_for(http_response(
            "503 Service Unavailable",
            "no model is currently loaded",
        ));
        let error = adapter.complete(request()).unwrap_err();
        let CompletionError::Upstream(message) = error else {
            panic!("expected an upstream error, got {error:?}");
        };
        assert!(message.contains("HTTP 503"), "{message}");
        assert!(message.contains("no model loaded"), "{message}");
        assert!(
            message.contains("Studio"),
            "the target must be named: {message}"
        );
    }

    /// The relay answers `text/plain` with a bare token rather than an OpenAI
    /// JSON error envelope, so the classifier must not depend on parsing one.
    #[test]
    fn a_relay_401_names_the_credential_and_keeps_the_opaque_body() {
        let mut relay = local_target();
        relay.kind = ActualTargetKind::Relay;
        relay.api_key = Some("ac_bad".into());
        let (adapter, _requests) = adapter_for_target(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 31\r\nConnection: close\r\n\r\nmissing_or_malformed_credential"
                .to_string(),
            relay,
            Duration::ZERO,
        );
        let error = adapter.complete(request()).unwrap_err();
        let CompletionError::Upstream(message) = error else {
            panic!("expected an upstream error, got {error:?}");
        };
        assert!(
            message.contains("missing_or_malformed_credential"),
            "{message}"
        );
        assert!(message.contains("rejected the credential"), "{message}");
    }

    /// Whatever an error message says, it must never say the secret.
    ///
    /// The body has to actually carry the credential for this to test anything.
    /// A gateway that reflects the request it rejected is the realistic source,
    /// and asserting against a body that never contained the key would pass just
    /// as happily with no redaction at all.
    #[test]
    fn upstream_explanations_never_echo_the_credential() {
        let mut relay = local_target();
        relay.kind = ActualTargetKind::Relay;
        relay.api_key = Some("ac_live_supersecret".into());
        for status in ["HTTP 503", "HTTP 401", "HTTP 500"] {
            let reflected = format!(
                "Actual returned {status}: rejected request \
                 {{\"headers\":{{\"authorization\":\"Bearer ac_live_supersecret\"}}}}"
            );
            let explained = explain_upstream(&relay, reflected);
            assert!(
                !explained.contains("ac_live_supersecret"),
                "credential leaked into {explained}"
            );
            assert!(
                explained.contains(status),
                "redaction must not cost the diagnosis: {explained}"
            );
        }
    }

    #[test]
    fn completion_rejects_invalid_requests_before_http() {
        let mut empty = request();
        empty.messages.clear();
        assert!(matches!(
            completion_body(&empty),
            Err(CompletionError::InvalidRequest(_))
        ));
        let mut bad_extras = request();
        bad_extras.extras = json!("not an object");
        assert!(matches!(
            completion_body(&bad_extras),
            Err(CompletionError::InvalidRequest(_))
        ));
    }

    #[test]
    fn completion_respects_the_caller_timeout() {
        let (adapter, _requests) = adapter_for_target(
            http_response("200 OK", r#"{"choices":[]}"#),
            local_target(),
            Duration::from_millis(150),
        );
        let mut request = request();
        request.timeout = Duration::from_millis(20);
        assert_eq!(
            adapter.complete(request).unwrap_err(),
            CompletionError::Timeout
        );
    }

    /// Drive payloads through the same aggregation the streaming transport uses
    /// and the same neutralization the turn loop consumes. This is the contract
    /// that matters for tool use: everything between the socket and
    /// `Generation`, with none of the orchestrator.
    fn generation_from_stream(chunks: Vec<serde_json::Value>) -> Generation<ChatMessage> {
        use crate::backends::openai_compat::wire::{ChatStreamChunk, StreamingAggregate};

        let mut aggregate = StreamingAggregate::default();
        for chunk in chunks {
            let parsed: ChatStreamChunk = serde_json::from_value(chunk.clone())
                .unwrap_or_else(|error| panic!("{chunk}: {error}"));
            aggregate.apply_chunk(&parsed);
        }
        into_generation(aggregate.into_response(false), ACTUAL_BACKEND_NAME).unwrap()
    }

    /// One tool-call delta, as a chunk carrying only the fields present.
    fn tool_delta(
        id: Option<&str>,
        name: Option<&str>,
        arguments: Option<&str>,
    ) -> serde_json::Value {
        let mut function = serde_json::Map::new();
        if let Some(name) = name {
            function.insert("name".into(), json!(name));
        }
        if let Some(arguments) = arguments {
            function.insert("arguments".into(), json!(arguments));
        }
        let mut call = serde_json::Map::new();
        call.insert("index".into(), json!(0));
        if let Some(id) = id {
            call.insert("id".into(), json!(id));
            call.insert("type".into(), json!("function"));
        }
        call.insert("function".into(), serde_json::Value::Object(function));
        json!({"choices": [{"index": 0, "delta": {"tool_calls": [serde_json::Value::Object(call)]}}]})
    }

    /// A local model streams tool-call arguments a few characters at a time.
    /// Reassembly has to survive the JSON being split mid-key, mid-string, and
    /// across an escape, because no single fragment is valid JSON on its own.
    #[test]
    fn a_tool_call_fragmented_across_chunks_reassembles_into_one_call() {
        let generation = generation_from_stream(vec![
            json!({"id": "gen-1", "model": "google/gemma-4-e2b-it", "choices": [{"index": 0, "delta": {"role": "assistant"}}]}),
            // The name itself arrives split, so a reader that took only the
            // first fragment would dispatch a tool called "re".
            tool_delta(Some("call_1"), Some("re"), Some("")),
            tool_delta(None, Some("ad"), Some("{\"pa")),
            tool_delta(None, None, Some("ths\": [\"file:src/li")),
            tool_delta(None, None, Some("b.rs\"], \"note\": \"a \\\"q")),
            tool_delta(None, None, Some("uoted\\\" word\"}")),
            json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}], "usage": {"prompt_tokens": 42, "completion_tokens": 7}}),
        ]);

        assert_eq!(generation.tool_calls.len(), 1);
        let call = &generation.tool_calls[0];
        assert_eq!(call.id, "call_1");
        assert_eq!(call.name, "read");
        // The reassembled arguments must be valid JSON, with the escape intact.
        let arguments: serde_json::Value =
            serde_json::from_str(&call.arguments).unwrap_or_else(|error| {
                panic!("arguments were not valid JSON: {error}: {}", call.arguments)
            });
        assert_eq!(arguments["paths"][0], "file:src/lib.rs");
        assert_eq!(arguments["note"], "a \"quoted\" word");
        assert_eq!(generation.finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(
            generation.response_model.as_deref(),
            Some("google/gemma-4-e2b-it")
        );
        assert_eq!(generation.generation_id.as_deref(), Some("gen-1"));
    }

    /// Two calls in one turn must stay two calls. The index, not arrival order,
    /// is what separates them, and a model can interleave their fragments.
    #[test]
    fn interleaved_parallel_tool_calls_stay_separate() {
        fn indexed(
            index: u64,
            id: Option<&str>,
            name: Option<&str>,
            arguments: &str,
        ) -> serde_json::Value {
            let mut chunk = tool_delta(id, name, Some(arguments));
            chunk["choices"][0]["delta"]["tool_calls"][0]["index"] = json!(index);
            chunk
        }
        let generation = generation_from_stream(vec![
            indexed(0, Some("call_a"), Some("read"), "{\"paths\":"),
            indexed(1, Some("call_b"), Some("run"), "{\"commands\":"),
            indexed(0, None, None, " [\"file:a\"]}"),
            indexed(1, None, None, " [{\"command\": \"ls\"}]}"),
        ]);

        assert_eq!(generation.tool_calls.len(), 2);
        let read = generation
            .tool_calls
            .iter()
            .find(|call| call.id == "call_a")
            .unwrap();
        let run = generation
            .tool_calls
            .iter()
            .find(|call| call.id == "call_b")
            .unwrap();
        assert_eq!(read.name, "read");
        assert_eq!(run.name, "run");
        let read_args: serde_json::Value = serde_json::from_str(&read.arguments).unwrap();
        assert_eq!(read_args["paths"][0], "file:a");
        let run_args: serde_json::Value = serde_json::from_str(&run.arguments).unwrap();
        assert_eq!(run_args["commands"][0]["command"], "ls");
    }

    /// Text and tool calls in the same turn: the assistant's prose must survive
    /// alongside the call rather than being replaced by it.
    #[test]
    fn streamed_text_survives_alongside_a_tool_call() {
        let generation = generation_from_stream(vec![
            json!({"choices": [{"index": 0, "delta": {"content": "Reading "}}]}),
            json!({"choices": [{"index": 0, "delta": {"content": "the file."}}]}),
            tool_delta(Some("call_1"), Some("read"), Some("{}")),
        ]);
        assert_eq!(generation.assistant_text, "Reading the file.");
        assert_eq!(generation.tool_calls.len(), 1);
    }

    /// Actual reports usage in the OpenAI shape; a turn that never streams one
    /// must not invent counts.
    #[test]
    fn usage_is_taken_only_when_the_stream_reports_it() {
        let without = generation_from_stream(vec![
            json!({"choices": [{"index": 0, "delta": {"content": "hi"}}]}),
        ]);
        assert!(without.usage.is_none());
    }

    #[test]
    fn user_owned_inference_reports_no_cost() {
        let body = json!({
            "model": "gemma",
            "choices": [{"message": {"role": "assistant", "content": "done"}}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 5}
        })
        .to_string();
        let (adapter, _requests) = adapter_for(http_response("200 OK", &body));
        let outcome = adapter.complete(request()).unwrap();
        assert_eq!(outcome.tokens.input, Some(11));
        assert_eq!(outcome.tokens.output, Some(5));
        // Tokens are real and worth recording; a price for them is not.
        assert_eq!(outcome.cost, None);
    }
}
