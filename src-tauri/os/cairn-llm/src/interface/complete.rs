//! The facet's one door for HTTP tokens: a completion over one of the three
//! protocol families.
//!
//! The seam sits at the request body on purpose. Bodies already differ per
//! provider (OpenRouter's routing object, Ollama's host, custom sampling), and
//! each adapter keeps building its own; what passes through here is the POST
//! itself. That is where a proxy, payload capture, or routing hooks in, and
//! where a test installs a recorder, without any adapter knowing.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::wire::anthropic_messages::http::MessagesEndpoint;
use crate::wire::anthropic_messages::wire::MessagesResponse;
use crate::wire::openai_compat::http::Endpoint;
use crate::wire::openai_compat::wire::ChatResponse;
use crate::wire::openai_responses::http::ResponsesEndpoint;
use crate::wire::openai_responses::wire::ResponsesResponse;
use crate::wire::stream::StreamSink;
pub use crate::wire::CompletionError;

/// Which protocol family a completion speaks, and the deployment it goes to.
#[derive(Debug, Clone, Copy)]
pub enum CompletionWire<'a> {
    Chat(&'a Endpoint),
    Messages(&'a MessagesEndpoint),
    Responses(&'a ResponsesEndpoint),
}

impl CompletionWire<'_> {
    /// The backend key transcript rows and usage are attributed to.
    pub fn backend_key(&self) -> &str {
        match self {
            Self::Chat(endpoint) => &endpoint.backend_key,
            Self::Messages(endpoint) => &endpoint.backend_key,
            Self::Responses(endpoint) => &endpoint.backend_key,
        }
    }

    pub fn family(&self) -> &'static str {
        match self {
            Self::Chat(_) => "chat",
            Self::Messages(_) => "messages",
            Self::Responses(_) => "responses",
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
}

fn mismatched(expected: &str, reply: &CompletionReply) -> CompletionError {
    let got = match reply {
        CompletionReply::Chat(_) => "chat",
        CompletionReply::Messages(_) => "messages",
        CompletionReply::Responses(_) => "responses",
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
}

/// Produces HTTP tokens. The only implementation that reaches a provider is
/// [`crate::complete::HttpComplete`].
///
/// A streamed call fails with [`CompletionError::Stream`], whose text is the
/// transport's own message unchanged, so what reaches a transcript is exactly
/// what the provider path reported.
pub trait Complete: Send + Sync {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError>;
}
