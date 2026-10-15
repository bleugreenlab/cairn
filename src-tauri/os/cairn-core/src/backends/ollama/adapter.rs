use super::{ollama_host_for_model_in_project, OLLAMA_BACKEND_KEY, OLLAMA_BACKEND_NAME};
use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::openai_compat::conversation;
use crate::backends::openai_compat::http::{ChatDialect, ChatReasoning, Endpoint};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage};
use crate::backends::SessionConfig;
use crate::backends::{CompletionError, CompletionOutcome, CompletionRequest, CompletionTokens};
use crate::orchestrator::Orchestrator;
use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionReply, CompletionWire,
};
use cairn_models::turn::ChatFamily;
use serde_json::Map;
use std::time::Instant;

pub(super) struct OllamaAdapter {
    endpoint: Endpoint,
    /// The Ollama host account serving the model.
    account_id: Option<String>,
}

impl OllamaAdapter {
    pub(super) fn new(
        orch: &Orchestrator,
        model: &str,
        project_id: Option<&str>,
    ) -> Result<Self, String> {
        let (account_id, base_url) = ollama_host_for_model_in_project(orch, model, project_id)
            .ok_or_else(|| {
                "Ollama host not configured. Add an Ollama host in Settings → Providers."
                    .to_string()
            })?;
        Ok(Self {
            account_id: Some(account_id),
            endpoint: Endpoint {
                provider_name: OLLAMA_BACKEND_NAME.to_string(),
                backend_key: OLLAMA_BACKEND_KEY.to_string(),
                chat_url: format!("{}/v1/chat/completions", base_url.trim_end_matches('/')),
                headers: vec![("content-type".into(), "application/json".into())],
            },
        })
    }

    pub(super) fn complete(
        &self,
        llm: &dyn Complete,
        request: CompletionRequest,
    ) -> Result<CompletionOutcome, CompletionError> {
        let requested_model = request.model.clone();
        let body = completion_body(&request)?;
        let started = Instant::now();
        let response = llm
            .complete(CompletionCall::whole(
                CompletionWire::Chat(&self.endpoint),
                body,
                request.timeout,
            ))
            .and_then(CompletionReply::into_chat)?;
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
            cost: None,
            latency_ms: started.elapsed().as_millis() as u64,
            account_id: None,
        })
    }
}

use crate::backends::openai_compat::completion_body;
impl WireAdapter for OllamaAdapter {
    type Family = ChatFamily;
    fn backend_key(&self) -> &str {
        OLLAMA_BACKEND_KEY
    }
    fn account_id(&self) -> Option<String> {
        self.account_id.clone()
    }

    fn backend_name(&self) -> &str {
        OLLAMA_BACKEND_NAME
    }
    fn default_model(&self) -> &'static str {
        ""
    }
    fn connection(&self, _: &Orchestrator) -> Result<Connection, String> {
        Ok(Connection { api_key: None })
    }
    fn family(&self, _: &Connection) -> Result<ChatFamily, String> {
        Ok(ChatFamily {
            endpoint: self.endpoint.clone(),
            dialect: ChatDialect {
                reasoning: ChatReasoning::Omit,
                response_format: false,
                ..ChatDialect::default()
            },
            extras: Map::new(),
        })
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
        orch.context_window_for_context_tokens(OLLAMA_BACKEND_KEY, Some(model))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{CompletionMessage, CompletionRole};
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

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
            model: "qwen:test".into(),
            project_id: None,
            extras: json!({"temperature": 0.2}),
            output_schema: None,
            timeout: Duration::from_secs(1),
        }
    }

    fn adapter_for(response: String) -> OllamaAdapter {
        adapter_for_after(response, Duration::ZERO)
    }

    fn adapter_for_after(response: String, delay: Duration) -> OllamaAdapter {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0; 8192];
            let _ = stream.read(&mut buffer);
            thread::sleep(delay);
            stream.write_all(response.as_bytes()).unwrap();
        });
        OllamaAdapter {
            account_id: None,
            endpoint: Endpoint {
                provider_name: OLLAMA_BACKEND_NAME.to_string(),
                backend_key: OLLAMA_BACKEND_KEY.to_string(),
                chat_url: format!("http://{address}/v1/chat/completions"),
                headers: vec![("content-type".into(), "application/json".into())],
            },
        }
    }

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// The byte-exact request body of Ollama's owned turn. See
    /// `http_loop::scripted` for how it is recorded.
    #[tokio::test]
    async fn golden_owned_turn() {
        use crate::backends::http_loop::scripted::{
            answer_schema, assert_golden, chat_text, record_turn, session_config,
        };
        let mut config = session_config("qwen:test");
        config.reasoning_effort = Some("high".to_string());
        config.output_schema = Some(answer_schema());
        let bodies = record_turn(config, vec![chat_text("done")], |_| OllamaAdapter {
            account_id: Some("acc_ollama".into()),
            endpoint: Endpoint {
                provider_name: OLLAMA_BACKEND_NAME.to_string(),
                backend_key: OLLAMA_BACKEND_KEY.to_string(),
                chat_url: "http://127.0.0.1:1/v1/chat/completions".to_string(),
                headers: vec![("content-type".into(), "application/json".into())],
            },
        })
        .await;
        // The caller's effort and schema are not sent on this owned loop.
        assert_golden(
            &bodies,
            &[
                r#"{"messages":[{"content":"SYSTEM","role":"system"},{"content":"hi","role":"user"}],"model":"qwen:test","stream":true,"stream_options":{"include_usage":true},"tool_choice":"auto","tools":["read","write","run"]}"#,
            ],
        );
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
        assert_eq!(body["model"], "qwen:test");
        assert_eq!(body["stream"], false);
        assert_eq!(body["temperature"], 0.2);
        assert_eq!(
            body["messages"][0],
            json!({"role": "system", "content": "Be concise"})
        );
        assert_eq!(
            body["messages"][1],
            json!({"role": "user", "content": "hello"})
        );
        assert_eq!(
            body["messages"][2],
            json!({"role": "assistant", "content": "hi"})
        );
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert!(body.get("tools").is_none());
    }

    #[test]
    fn completion_extracts_text_model_and_usage() {
        let body = json!({
            "model": "qwen:resolved",
            "choices": [{"message": {"role": "assistant", "content": "done"}}],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3}
        })
        .to_string();
        let response = http_response("200 OK", &body);
        let outcome = adapter_for(response)
            .complete(&cairn_models::complete::HttpComplete, request())
            .unwrap();
        assert_eq!(outcome.text, "done");
        assert_eq!(outcome.model, "qwen:resolved");
        assert_eq!(outcome.tokens.input, Some(7));
        assert_eq!(outcome.tokens.output, Some(3));
        assert_eq!(outcome.cost, None);
    }

    #[test]
    fn completion_maps_http_and_invalid_response_errors() {
        let response = http_response("500 Internal Server Error", "model failed");
        let error = adapter_for(response)
            .complete(&cairn_models::complete::HttpComplete, request())
            .unwrap_err();
        assert!(
            matches!(error, CompletionError::Upstream(message) if message.contains("HTTP 500"))
        );

        let response = http_response("200 OK", r#"{"model":"qwen","choices":[]}"#);
        let error = adapter_for(response)
            .complete(&cairn_models::complete::HttpComplete, request())
            .unwrap_err();
        assert_eq!(
            error,
            CompletionError::InvalidResponse("missing text choice".into())
        );
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
        let response = http_response("200 OK", r#"{"choices":[]}"#);
        let adapter = adapter_for_after(response, Duration::from_millis(100));
        let mut request = request();
        request.timeout = Duration::from_millis(20);
        assert_eq!(
            adapter
                .complete(&cairn_models::complete::HttpComplete, request)
                .unwrap_err(),
            CompletionError::Timeout
        );
    }
}
