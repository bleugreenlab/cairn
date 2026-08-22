//! The wire adapter for a custom provider.
//!
//! There is no custom transport here. The endpoint is handed to the shared
//! OpenAI-compatible chat-completions transport that OpenRouter, OpenCode, and
//! Ollama already use, so a custom provider inherits the same streaming
//! aggregation, fragmented-tool-call reassembly, reasoning handling, terminal-
//! event invariant, and cancellation semantics as every other provider on that
//! wire — and cannot drift from them.

use super::CustomProviderConfig;
use crate::backends::http_loop::{render_tool_result, Connection, Generation, WireAdapter};
use crate::backends::openai_compat::generation::into_generation;
use crate::backends::openai_compat::http::{
    post_chat_completion, post_chat_completion_streaming, Endpoint,
};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage};
use crate::backends::openai_compat::{completion_body, context, conversation};
use crate::backends::SessionConfig;
use crate::backends::{CompletionError, CompletionOutcome, CompletionRequest, CompletionTokens};
use crate::dispatch::DispatchOutput;
use crate::identity::CustomProtocol;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::json;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

/// A single tool-loop generation must stay small enough to complete promptly on
/// operator-hosted models. Without an explicit cap, OpenAI-compatible servers
/// commonly generate up to the model's remaining context, turning a degenerate
/// response into tens of minutes of apparent session silence.
pub(super) fn generation_body(
    model: &str,
    outgoing: &[ChatMessage],
    sampling: &crate::identity::CustomSampling,
    max_output_tokens: i64,
) -> serde_json::Value {
    let mut body = json!({
        "model": model,
        "messages": outgoing,
        "tools": crate::backends::openai_compat::tool_schemas(),
        "tool_choice": "auto",
        "max_tokens": max_output_tokens,
        "stream": true,
        "stream_options": {"include_usage": true},
    });
    let object = body.as_object_mut().expect("generation body is an object");
    if let Some(value) = sampling.repetition_penalty {
        object.insert("repetition_penalty".into(), json!(value));
    }
    if let Some(value) = sampling.temperature {
        object.insert("temperature".into(), json!(value));
    }
    if let Some(value) = sampling.top_p {
        object.insert("top_p".into(), json!(value));
    }
    if let Some(value) = sampling.top_k {
        object.insert("top_k".into(), json!(value));
    }
    body
}

pub(super) struct CustomAdapter {
    config: CustomProviderConfig,
    endpoint: Endpoint,
    backend_key: String,
}

impl CustomAdapter {
    pub(super) fn new(config: CustomProviderConfig) -> Self {
        let backend_key = config.backend_key();
        let endpoint = Endpoint {
            provider_name: config.display_name.clone(),
            backend_key: backend_key.clone(),
            chat_url: config.chat_url(),
            headers: config.headers.clone(),
            extra_body: None,
        };
        Self {
            config,
            endpoint,
            backend_key,
        }
    }

    pub(super) fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionOutcome, CompletionError> {
        // The single protocol Cairn serves here. Matching exhaustively rather
        // than assuming means adding a family to `CustomProtocol` cannot
        // silently route through the wrong transport.
        match self.config.protocol {
            CustomProtocol::OpenAiChatCompletions => {}
        }
        let requested_model = request.model.clone();
        let body = completion_body(&request)?;
        let started = Instant::now();
        let response = post_chat_completion(&self.endpoint, body, request.timeout)?;
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
            // Inference on hardware the operator owns has no per-token price to
            // report. Reporting zero would claim a measurement nobody made.
            cost: None,
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

impl WireAdapter for CustomAdapter {
    type Message = ChatMessage;

    fn backend_key(&self) -> &str {
        &self.backend_key
    }

    fn backend_name(&self) -> &str {
        &self.config.display_name
    }

    /// A custom provider has no default model. Every model it serves is one the
    /// operator's endpoint decides on, so guessing one would put an id on the
    /// wire that nothing there answers to.
    fn default_model(&self) -> &'static str {
        ""
    }

    /// The credential rides in the endpoint's headers, resolved when this
    /// adapter was built, so there is nothing further to look up per turn.
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
        orch.context_window_for_context_tokens(&self.backend_key, Some(model))
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
        let body = generation_body(
            model,
            outgoing,
            &self.config.sampling,
            self.config.max_output_tokens,
        );
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
            )?,
            &self.config.display_name,
        )
    }

    fn render_tool_result_message(&self, id: &str, output: DispatchOutput) -> ChatMessage {
        ChatMessage::tool(id.to_string(), render_tool_result(output))
    }
}
