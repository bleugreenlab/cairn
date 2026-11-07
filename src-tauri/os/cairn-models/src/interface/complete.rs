//! The facet's one door for HTTP model calls: a completion over one of the
//! three turn families (Chat, Messages, Responses), one of the two decision
//! wires (OpenAI's Decisions, System One), or one of the two embedding wires
//! (OpenAI `embeddings`, Cairn's gateway `/embed`).
//!
//! The seam sits at the request body on purpose. Bodies already differ per
//! provider (OpenRouter's routing object, Ollama's host, custom sampling), and
//! each adapter keeps building its own; what passes through here is the POST
//! itself. That is where a proxy, payload capture, or routing hooks in, and
//! where a test installs a recorder, without any adapter knowing.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use cairn_models_types::catalog::Wire;
use serde_json::Value;

use crate::wire::anthropic_messages::http::MessagesEndpoint;
use crate::wire::anthropic_messages::wire::MessagesResponse;
use crate::wire::decisions::http::DecisionsEndpoint;
use crate::wire::decisions::wire::DecisionsResponse;
use crate::wire::embeddings::gateway::{GatewayEmbedEndpoint, GatewayEmbedResponse};
use crate::wire::embeddings::openai::{EmbeddingsEndpoint, EmbeddingsResponse};
use crate::wire::openai_compat::http::Endpoint;
use crate::wire::openai_compat::wire::ChatResponse;
use crate::wire::openai_responses::http::ResponsesEndpoint;
use crate::wire::openai_responses::wire::ResponsesResponse;
use crate::wire::stream::StreamSink;
use crate::wire::systemone::wire::SystemOneResponse;
pub use crate::wire::CompletionError;

/// Which protocol family a completion speaks, and the deployment it goes to.
#[derive(Debug, Clone, Copy)]
pub enum CompletionWire<'a> {
    Chat(&'a Endpoint),
    Messages(&'a MessagesEndpoint),
    Responses(&'a ResponsesEndpoint),
    /// Typed questions answered with probabilities over OpenAI's Decisions
    /// API; never streamed.
    Decisions(&'a DecisionsEndpoint),
    /// Named questions answered with probabilities over System One; never
    /// streamed.
    SystemOne(&'a DecisionsEndpoint),
    /// Texts in, vectors out, over OpenAI's `embeddings`; never streamed.
    Embeddings(&'a EmbeddingsEndpoint),
    /// Texts in, vectors out, over Cairn's gateway `/embed`; never streamed.
    GatewayEmbed(&'a GatewayEmbedEndpoint),
}

impl CompletionWire<'_> {
    /// The backend key transcript rows and usage are attributed to.
    pub fn backend_key(&self) -> &str {
        match self {
            Self::Chat(endpoint) => &endpoint.backend_key,
            Self::Messages(endpoint) => &endpoint.backend_key,
            Self::Responses(endpoint) => &endpoint.backend_key,
            Self::Decisions(endpoint) | Self::SystemOne(endpoint) => &endpoint.backend_key,
            Self::Embeddings(endpoint) => &endpoint.backend_key,
            Self::GatewayEmbed(endpoint) => &endpoint.backend_key,
        }
    }

    pub fn family(&self) -> &'static str {
        match self {
            Self::Chat(_) => "chat",
            Self::Messages(_) => "messages",
            Self::Responses(_) => "responses",
            Self::Decisions(_) => "decisions",
            Self::SystemOne(_) => "systemone",
            Self::Embeddings(_) => "embeddings",
            Self::GatewayEmbed(_) => "gateway-embed",
        }
    }

    /// The catalog's name for the protocol this route speaks. This is where a
    /// launch's route meets the wire an offer declares: compare through it,
    /// never by family string.
    pub fn wire(&self) -> Wire {
        match self {
            Self::Chat(_) => Wire::OpenAiChatCompletions,
            Self::Messages(_) => Wire::AnthropicMessages,
            Self::Responses(_) => Wire::OpenAiResponses,
            Self::Decisions(_) => Wire::OpenAiDecisions,
            Self::SystemOne(_) => Wire::SystemOne,
            Self::Embeddings(_) => Wire::OpenAiEmbeddings,
            Self::GatewayEmbed(_) => Wire::CairnEmbed,
        }
    }
}

/// How the reply comes back.
pub enum Delivery<'a> {
    /// Streamed: each delta reaches `sink` as it arrives, and `cancel` stops
    /// the read at the next line boundary.
    Stream {
        sink: &'a mut dyn StreamSink,
        cancel: &'a Arc<AtomicBool>,
    },
    /// One non-streamed POST, for tool-free one-shot completions.
    Whole { timeout: Duration },
}

pub struct CompletionCall<'a> {
    pub wire: CompletionWire<'a>,
    pub body: Value,
    pub delivery: Delivery<'a>,
}

impl<'a> CompletionCall<'a> {
    pub fn stream(
        wire: CompletionWire<'a>,
        body: Value,
        sink: &'a mut dyn StreamSink,
        cancel: &'a Arc<AtomicBool>,
    ) -> Self {
        Self {
            wire,
            body,
            delivery: Delivery::Stream { sink, cancel },
        }
    }

    pub fn whole(wire: CompletionWire<'a>, body: Value, timeout: Duration) -> Self {
        Self {
            wire,
            body,
            delivery: Delivery::Whole { timeout },
        }
    }
}

/// The provider's reply, in the family the call spoke.
#[derive(Debug)]
pub enum CompletionReply {
    Chat(ChatResponse),
    Messages(MessagesResponse),
    Responses(ResponsesResponse),
    Decisions(DecisionsResponse),
    SystemOne(SystemOneResponse),
    Embeddings(EmbeddingsResponse),
    GatewayEmbed(GatewayEmbedResponse),
}

fn mismatched(expected: &str, reply: &CompletionReply) -> CompletionError {
    let got = match reply {
        CompletionReply::Chat(_) => "chat",
        CompletionReply::Messages(_) => "messages",
        CompletionReply::Responses(_) => "responses",
        CompletionReply::Decisions(_) => "decisions",
        CompletionReply::SystemOne(_) => "systemone",
        CompletionReply::Embeddings(_) => "embeddings",
        CompletionReply::GatewayEmbed(_) => "gateway-embed",
    };
    CompletionError::InvalidResponse(format!("expected a {expected} reply, got {got}"))
}

impl CompletionReply {
    pub fn into_chat(self) -> Result<ChatResponse, CompletionError> {
        match self {
            Self::Chat(response) => Ok(response),
            other => Err(mismatched("chat", &other)),
        }
    }

    pub fn into_messages(self) -> Result<MessagesResponse, CompletionError> {
        match self {
            Self::Messages(response) => Ok(response),
            other => Err(mismatched("messages", &other)),
        }
    }

    pub fn into_responses(self) -> Result<ResponsesResponse, CompletionError> {
        match self {
            Self::Responses(response) => Ok(response),
            other => Err(mismatched("responses", &other)),
        }
    }

    pub fn into_decisions(self) -> Result<DecisionsResponse, CompletionError> {
        match self {
            Self::Decisions(response) => Ok(response),
            other => Err(mismatched("decisions", &other)),
        }
    }

    pub fn into_systemone(self) -> Result<SystemOneResponse, CompletionError> {
        match self {
            Self::SystemOne(response) => Ok(response),
            other => Err(mismatched("systemone", &other)),
        }
    }

    pub fn into_embeddings(self) -> Result<EmbeddingsResponse, CompletionError> {
        match self {
            Self::Embeddings(response) => Ok(response),
            other => Err(mismatched("embeddings", &other)),
        }
    }

    pub fn into_gateway_embed(self) -> Result<GatewayEmbedResponse, CompletionError> {
        match self {
            Self::GatewayEmbed(response) => Ok(response),
            other => Err(mismatched("gateway-embed", &other)),
        }
    }
}

/// Makes HTTP model calls. The only implementation that reaches a provider is
/// [`crate::complete::HttpComplete`].
///
/// A streamed call fails with [`CompletionError::Stream`], whose text is the
/// transport's own message unchanged, so what reaches a transcript is exactly
/// what the provider path reported.
pub trait Complete: Send + Sync {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError>;
}

/// Maximum plain reasoning returned with a completion, in UTF-8 bytes.
pub const MAX_REASONING_BYTES: usize = 16 * 1024;

fn bounded_reasoning<'a>(parts: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut text = String::new();
    for part in parts {
        if part.len() > MAX_REASONING_BYTES - text.len() {
            return None;
        }
        text.push_str(part);
    }
    (!text.is_empty()).then_some(text)
}

/// Plain reasoning from the first chat choice, never encrypted data or signatures.
/// Structured details take precedence over the duplicate plain-string form.
pub fn chat_reasoning_text(response: &ChatResponse) -> Option<String> {
    let message = &response.choices.first()?.message;
    if let Some(details) = message.reasoning_details.as_ref().and_then(Value::as_array) {
        let mut visible = details
            .iter()
            .filter_map(|detail| match detail.get("type").and_then(Value::as_str) {
                Some("reasoning.text") => detail.get("text").and_then(Value::as_str),
                Some("reasoning.summary") => detail.get("summary").and_then(Value::as_str),
                _ => None,
            })
            .filter(|text| !text.is_empty())
            .peekable();
        if visible.peek().is_some() {
            return bounded_reasoning(visible);
        }
    }
    bounded_reasoning(message.reasoning.as_ref().map(|reasoning| reasoning.text()))
}

/// Visible thinking blocks only; redacted thinking and signatures stay opaque.
pub fn messages_reasoning_text(response: &MessagesResponse) -> Option<String> {
    use crate::wire::anthropic_messages::wire::ContentBlock;
    bounded_reasoning(response.content.iter().filter_map(|block| match block {
        ContentBlock::Thinking { thinking, .. } => Some(thinking.as_str()),
        _ => None,
    }))
}

/// Visible reasoning summaries only, never encrypted continuation state.
pub fn responses_reasoning_text(response: &ResponsesResponse) -> Option<String> {
    use crate::wire::openai_responses::wire::ResponsesItem;
    bounded_reasoning(
        response
            .output
            .iter()
            .filter_map(|item| match item {
                ResponsesItem::Reasoning { summary, .. } => Some(summary),
                _ => None,
            })
            .flatten()
            .filter(|part| part.kind == "summary_text")
            .map(|part| part.text.as_str()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat(message: Value) -> ChatResponse {
        serde_json::from_value(json!({"choices": [{"message": message}]})).unwrap()
    }

    #[test]
    fn chat_reasoning_accepts_both_plain_spellings_and_only_the_first_choice() {
        for key in ["reasoning", "reasoning_content"] {
            let mut message = json!({"role": "assistant", "content": "answer"});
            message[key] = json!("visible");
            assert_eq!(chat_reasoning_text(&chat(message)), Some("visible".into()));
        }
        assert_eq!(
            chat_reasoning_text(&chat(json!({"role": "assistant"}))),
            None
        );
        assert_eq!(
            chat_reasoning_text(&chat(json!({"role": "assistant", "reasoning": ""}))),
            None
        );
        let response: ChatResponse = serde_json::from_value(json!({"choices": [
            {"message": {"role": "assistant"}},
            {"message": {"role": "assistant", "reasoning": "not the selected answer"}}
        ]}))
        .unwrap();
        assert_eq!(chat_reasoning_text(&response), None);
        assert_eq!(
            chat_reasoning_text(&serde_json::from_value(json!({"choices": []})).unwrap()),
            None
        );
    }

    #[test]
    fn chat_structured_reasoning_wins_and_excludes_opaque_details() {
        let response = chat(json!({"role": "assistant", "reasoning": "duplicate",
        "reasoning_details": [
            {"type": "reasoning.text", "text": "first", "signature": "secret"},
            {"type": "reasoning.encrypted", "data": "secret"},
            {"type": "reasoning.summary", "summary": "second"},
            {"type": "unknown", "text": "secret"}
        ]}));
        assert_eq!(chat_reasoning_text(&response), Some("firstsecond".into()));
        let opaque = chat(json!({"role": "assistant", "reasoning": "fallback",
            "reasoning_details": [{"type": "reasoning.encrypted", "data": "secret"}]}));
        assert_eq!(chat_reasoning_text(&opaque), Some("fallback".into()));
        for details in [json!([]), json!([{"type": "reasoning.text", "text": ""}])] {
            assert_eq!(
                chat_reasoning_text(&chat(json!({"role": "assistant",
                "reasoning_content": "fallback", "reasoning_details": details}))),
                Some("fallback".into())
            );
        }
        assert_eq!(
            chat_reasoning_text(&chat(json!({"role": "assistant",
            "reasoning_details": [{"type": "reasoning.encrypted", "data": "secret"}]}))),
            None
        );
    }

    #[test]
    fn reasoning_limit_counts_utf8_bytes_without_truncating() {
        let at_limit = "é".repeat(MAX_REASONING_BYTES / 2);
        assert_eq!(
            bounded_reasoning([at_limit.as_str()]),
            Some(at_limit.clone())
        );
        let over = format!("{at_limit}é");
        assert_eq!(bounded_reasoning([over.as_str()]), None);
        assert_eq!(bounded_reasoning([at_limit.as_str(), "x"]), None);
        assert_eq!(bounded_reasoning(["", ""]), None);
        for (text, expected) in [
            (at_limit.as_str(), Some(at_limit.clone())),
            (over.as_str(), None),
        ] {
            assert_eq!(
                chat_reasoning_text(&chat(json!({"role": "assistant", "reasoning": text}))),
                expected
            );
        }
        assert_eq!(
            chat_reasoning_text(&chat(json!({"role": "assistant",
            "reasoning": "must not bypass cap",
            "reasoning_details": [
                {"type": "reasoning.text", "text": at_limit},
                {"type": "reasoning.summary", "summary": "x"}
            ]}))),
            None
        );
    }

    #[test]
    fn messages_reasoning_includes_only_visible_thinking_and_bounds_the_aggregate() {
        let response: MessagesResponse = serde_json::from_value(json!({"content": [
            {"type": "text", "text": "answer"},
            {"type": "thinking", "thinking": "first", "signature": "secret"},
            {"type": "redacted_thinking", "data": "secret"},
            {"type": "thinking", "thinking": "second", "signature": "secret"}
        ]}))
        .unwrap();
        assert_eq!(
            messages_reasoning_text(&response),
            Some("firstsecond".into())
        );
        assert_eq!(messages_reasoning_text(&MessagesResponse::default()), None);
        for (extra, expected) in [("", Some("é".repeat(MAX_REASONING_BYTES / 2))), ("x", None)] {
            let response = serde_json::from_value(json!({"content": [
                {"type": "thinking", "thinking": "é".repeat(MAX_REASONING_BYTES / 2), "signature": "secret"},
                {"type": "thinking", "thinking": extra, "signature": "secret"}
            ]})).unwrap();
            assert_eq!(messages_reasoning_text(&response), expected);
        }
        let opaque = serde_json::from_value(json!({"content": [
            {"type": "redacted_thinking", "data": "secret"}
        ]}))
        .unwrap();
        assert_eq!(messages_reasoning_text(&opaque), None);
    }

    #[test]
    fn responses_reasoning_includes_only_summary_text_and_bounds_the_aggregate() {
        fn response(items: Value) -> ResponsesResponse {
            ResponsesResponse {
                output: serde_json::from_value(items).unwrap(),
                ..Default::default()
            }
        }
        let visible = response(json!([
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "answer"}]},
            {"type": "reasoning", "encrypted_content": "secret", "summary": [
                {"type": "summary_text", "text": "first"},
                {"type": "unknown", "text": "secret"}
            ]},
            {"type": "reasoning", "summary": [{"type": "summary_text", "text": "second"}]}
        ]));
        assert_eq!(
            responses_reasoning_text(&visible),
            Some("firstsecond".into())
        );
        assert_eq!(
            responses_reasoning_text(&ResponsesResponse::default()),
            None
        );
        let opaque = response(json!([{"type": "reasoning", "encrypted_content": "secret"}]));
        assert_eq!(responses_reasoning_text(&opaque), None);
        for (extra, expected) in [("", Some("é".repeat(MAX_REASONING_BYTES / 2))), ("x", None)] {
            let reply = response(json!([
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "é".repeat(MAX_REASONING_BYTES / 2)}]},
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": extra}]}
            ]));
            assert_eq!(responses_reasoning_text(&reply), expected);
        }
    }
}
