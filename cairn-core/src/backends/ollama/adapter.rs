use super::{ollama_host_for_model_in_project, OLLAMA_BACKEND_KEY, OLLAMA_BACKEND_NAME};
use crate::backends::http_loop::{
    render_tool_result, repair, Connection, Generation, TurnToolCall, WireAdapter,
};
use crate::backends::openai_compat::http::{post_chat_completion_streaming, Endpoint};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage, ChatResponse};
use crate::backends::openai_compat::{context, conversation};
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::json;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub(super) struct OllamaAdapter {
    endpoint: Endpoint,
}
impl OllamaAdapter {
    pub(super) fn new(
        orch: &Orchestrator,
        model: &str,
        project_id: Option<&str>,
    ) -> Result<Self, String> {
        let (_, base_url) =
            ollama_host_for_model_in_project(orch, model, project_id).ok_or_else(|| {
                "Ollama host not configured. Add an Ollama host in Settings → Providers."
                    .to_string()
            })?;
        Ok(Self {
            endpoint: Endpoint {
                provider_name: OLLAMA_BACKEND_NAME,
                backend_key: OLLAMA_BACKEND_KEY,
                chat_url: format!("{}/v1/chat/completions", base_url.trim_end_matches('/')),
                headers: vec![("content-type".into(), "application/json".into())],
                extra_body: None,
            },
        })
    }
}
impl WireAdapter for OllamaAdapter {
    type Message = ChatMessage;
    fn backend_key(&self) -> &'static str {
        OLLAMA_BACKEND_KEY
    }
    fn backend_name(&self) -> &'static str {
        OLLAMA_BACKEND_NAME
    }
    fn default_model(&self) -> &'static str {
        ""
    }
    fn connection(&self, _: &Orchestrator) -> Result<Connection, String> {
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
        orch.context_window_for_context_tokens(OLLAMA_BACKEND_KEY, Some(model))
    }
    fn fit_conversation<'a>(
        &self,
        messages: &'a [ChatMessage],
        window: Option<i64>,
    ) -> Cow<'a, [ChatMessage]> {
        context::fit_conversation(messages, window)
    }
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
        let body = json!({"model": model, "messages": outgoing, "tools": crate::backends::openai_compat::tool_schemas(), "tool_choice": "auto", "stream": true, "stream_options": {"include_usage": true}});
        into_generation(post_chat_completion_streaming(
            orch,
            run_db,
            &self.endpoint,
            body,
            run_id,
            session_id,
            turn_id,
            cancel,
        )?)
    }
    fn render_tool_result_message(&self, id: &str, output: DispatchOutput) -> ChatMessage {
        ChatMessage::tool(id.to_string(), render_tool_result(output))
    }
}
fn into_generation(response: ChatResponse) -> Result<Generation<ChatMessage>, String> {
    let ChatResponse {
        id,
        model,
        choices,
        usage,
        streamed_text,
        finish_reason,
    } = response;
    let Some(choice) = choices.into_iter().next() else {
        return Err("Ollama response did not include choices".to_string());
    };
    let mut assistant_message = choice.message;
    if let Some(calls) = assistant_message.tool_calls.as_mut() {
        for call in calls {
            if let Some(verb) = repair::normalize_tool_name(&call.function.name) {
                call.function.name = verb.to_string();
            }
        }
    }
    let assistant_text = assistant_message
        .content
        .as_ref()
        .and_then(ChatContent::as_text)
        .unwrap_or_default()
        .to_string();
    let tool_calls = assistant_message
        .tool_calls
        .as_ref()
        .map(|calls| {
            calls
                .iter()
                .map(|call| TurnToolCall {
                    id: call.id.clone(),
                    name: call.function.name.clone(),
                    arguments: call.function.arguments.clone(),
                })
                .collect()
        })
        .unwrap_or_default();
    let reasoning_details = assistant_message.reasoning_details.clone();
    Ok(Generation {
        assistant_message,
        assistant_text,
        tool_calls,
        reasoning_details,
        usage,
        finish_reason,
        generation_id: id,
        response_model: model,
        streamed_text,
    })
}
