//! One protocol family's moves in an agent turn.
//!
//! A [`ProtocolFamily`] knows how its wire builds a request, reads a reply,
//! trims a conversation to fit, and records a tool result, all over its own
//! native message type. Each implementation here is a thin composition of the
//! wire crate's functions for that family; what varies per deployment
//! (endpoint, chat dialect, extra body keys) is plain data on the struct, so a
//! provider is configured rather than implemented.

use std::borrow::Cow;

use serde_json::{Map, Value};

use crate::interface::complete::{CompletionError, CompletionReply, CompletionWire};
use crate::wire::anthropic_messages::{self as messages, http::MessagesEndpoint};
use crate::wire::openai_compat::{self as chat, http::ChatDialect, http::Endpoint};
use crate::wire::openai_responses::{self as responses, http::ResponsesEndpoint};
use crate::wire::{Generation, GenerationConfig, MessageContent, ToolDefinition};

/// A protocol family's request, reply, and conversation moves.
pub trait ProtocolFamily {
    /// The family's native conversation message.
    type Message: Clone + Send;

    /// What to call this provider in diagnostics and errors.
    fn provider_name(&self) -> &str;
    /// Where a generation is posted.
    fn wire(&self) -> CompletionWire<'_>;
    /// A streaming request body: the model, the conversation, the advertised
    /// tools, the configured reasoning and schema knobs, and the extras.
    fn body(&self, model: &str, conversation: &[Self::Message], config: &GenerationConfig)
        -> Value;
    /// Read a reply into a neutral generation that carries the native
    /// assistant message. Tool names resolve only onto `advertised` tools.
    fn read(
        &self,
        reply: CompletionReply,
        advertised: &[ToolDefinition],
    ) -> Result<Generation<Self::Message>, String>;
    /// The conversation as sent: borrowed when it fits `window`, trimmed when
    /// it does not.
    fn fit<'c>(
        &self,
        conversation: &'c [Self::Message],
        window: Option<i64>,
    ) -> Cow<'c, [Self::Message]>;
    /// Record one tool result in the shape replay rebuilds it in.
    fn push_tool_result(
        &self,
        conversation: &mut Vec<Self::Message>,
        call_id: &str,
        content: String,
    );
    /// A fresh conversation: the system prompt, when there is one, and the
    /// first user message.
    fn start_conversation(&self, system: &str, user: &MessageContent) -> Vec<Self::Message>;
}

/// OpenAI-compatible chat completions.
#[derive(Debug, Clone)]
pub struct ChatFamily {
    pub endpoint: Endpoint,
    pub dialect: ChatDialect,
    /// Keys this provider adds to every body (OpenRouter's routing object,
    /// a custom server's sampling), merged after the dialect's knobs.
    pub extras: Map<String, Value>,
}

impl ProtocolFamily for ChatFamily {
    type Message = chat::wire::ChatMessage;

    fn provider_name(&self) -> &str {
        &self.endpoint.provider_name
    }

    fn wire(&self) -> CompletionWire<'_> {
        CompletionWire::Chat(&self.endpoint)
    }

    fn body(
        &self,
        model: &str,
        conversation: &[Self::Message],
        config: &GenerationConfig,
    ) -> Value {
        chat::http::build_body(
            &self.dialect,
            model,
            conversation,
            config,
            &self.extras,
            true,
        )
    }

    fn read(
        &self,
        reply: CompletionReply,
        advertised: &[ToolDefinition],
    ) -> Result<Generation<Self::Message>, String> {
        let response = reply
            .into_chat()
            .map_err(CompletionError::into_stream_text)?;
        chat::generation::into_generation(response, self.provider_name(), advertised)
    }

    fn fit<'c>(
        &self,
        conversation: &'c [Self::Message],
        window: Option<i64>,
    ) -> Cow<'c, [Self::Message]> {
        chat::context::fit_conversation(conversation, window)
    }

    fn push_tool_result(
        &self,
        conversation: &mut Vec<Self::Message>,
        call_id: &str,
        content: String,
    ) {
        conversation.push(chat::wire::ChatMessage::tool(call_id.to_string(), content));
    }

    fn start_conversation(&self, system: &str, user: &MessageContent) -> Vec<Self::Message> {
        let mut conversation = Vec::with_capacity(2);
        if !system.is_empty() {
            conversation.push(chat::wire::ChatMessage::system(system.to_string()));
        }
        conversation.push(chat::wire::ChatMessage::user_content(user));
        conversation
    }
}

/// Anthropic Messages.
#[derive(Debug, Clone)]
pub struct MessagesFamily {
    pub endpoint: MessagesEndpoint,
    /// Keys this provider adds to every body, merged last.
    pub extras: Map<String, Value>,
}

impl ProtocolFamily for MessagesFamily {
    type Message = messages::wire::MessagesMessage;

    fn provider_name(&self) -> &str {
        &self.endpoint.provider_name
    }

    fn wire(&self) -> CompletionWire<'_> {
        CompletionWire::Messages(&self.endpoint)
    }

    fn body(
        &self,
        model: &str,
        conversation: &[Self::Message],
        config: &GenerationConfig,
    ) -> Value {
        messages::http::build_body(
            &self.endpoint,
            model,
            conversation,
            config,
            &self.extras,
            true,
        )
    }

    fn read(
        &self,
        reply: CompletionReply,
        advertised: &[ToolDefinition],
    ) -> Result<Generation<Self::Message>, String> {
        let response = reply
            .into_messages()
            .map_err(CompletionError::into_stream_text)?;
        messages::generation::into_generation(response, self.provider_name(), advertised)
    }

    fn fit<'c>(
        &self,
        conversation: &'c [Self::Message],
        window: Option<i64>,
    ) -> Cow<'c, [Self::Message]> {
        messages::conversation::fit_conversation(conversation, window)
    }

    fn push_tool_result(
        &self,
        conversation: &mut Vec<Self::Message>,
        call_id: &str,
        content: String,
    ) {
        messages::conversation::push_tool_result(conversation, call_id.to_string(), content);
    }

    fn start_conversation(&self, system: &str, user: &MessageContent) -> Vec<Self::Message> {
        let mut conversation = Vec::with_capacity(2);
        if !system.is_empty() {
            conversation.push(messages::wire::MessagesMessage::system(system.to_string()));
        }
        conversation.push(messages::wire::MessagesMessage::user_content(user));
        conversation
    }
}

/// OpenAI Responses.
#[derive(Debug, Clone)]
pub struct ResponsesFamily {
    pub endpoint: ResponsesEndpoint,
    /// Keys this provider adds to every body, merged after the knobs and
    /// before the output schema.
    pub extras: Map<String, Value>,
}

impl ProtocolFamily for ResponsesFamily {
    type Message = responses::wire::ResponsesTurn;

    fn provider_name(&self) -> &str {
        &self.endpoint.provider_name
    }

    fn wire(&self) -> CompletionWire<'_> {
        CompletionWire::Responses(&self.endpoint)
    }

    fn body(
        &self,
        model: &str,
        conversation: &[Self::Message],
        config: &GenerationConfig,
    ) -> Value {
        responses::http::build_body(model, conversation, config, &self.extras, true)
    }

    fn read(
        &self,
        reply: CompletionReply,
        advertised: &[ToolDefinition],
    ) -> Result<Generation<Self::Message>, String> {
        let response = reply
            .into_responses()
            .map_err(CompletionError::into_stream_text)?;
        responses::generation::into_generation(response, self.provider_name(), advertised)
    }

    fn fit<'c>(
        &self,
        conversation: &'c [Self::Message],
        window: Option<i64>,
    ) -> Cow<'c, [Self::Message]> {
        responses::conversation::fit_conversation(conversation, window)
    }

    fn push_tool_result(
        &self,
        conversation: &mut Vec<Self::Message>,
        call_id: &str,
        content: String,
    ) {
        conversation.push(responses::wire::ResponsesTurn::one(
            responses::wire::ResponsesItem::FunctionCallOutput {
                call_id: call_id.to_string(),
                output: content,
            },
        ));
    }

    fn start_conversation(&self, system: &str, user: &MessageContent) -> Vec<Self::Message> {
        use responses::wire::{ResponsesItem, ResponsesTurn};
        let mut conversation = Vec::with_capacity(2);
        if !system.is_empty() {
            conversation.push(ResponsesTurn::one(ResponsesItem::Instructions {
                text: system.to_string(),
            }));
        }
        conversation.push(ResponsesTurn::one(responses::conversation::user_item(user)));
        conversation
    }
}
