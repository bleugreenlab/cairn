//! The facet's relay door: an outside client's OpenAI-compatible request sent
//! to the provider that serves its model, with the provider's bytes returned
//! unchanged.
//!
//! The other verbs build their own bodies and read typed replies. A relay does
//! neither, because an outside client sends the whole OpenAI surface (tools,
//! `response_format`, images, logprobs) and a typed round trip would drop what
//! the types do not name. What the facet still owns is everything around the
//! bytes: which provider and account the call goes to ([`RelayRoute`], built by
//! the backend's own endpoint code), and what it consumed ([`RelayUsage`], read
//! by a tap as the bytes pass).

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use cairn_models_types::catalog::{VerbKind, Wire};
use serde_json::{Map, Value};

pub use crate::relay::RelayBody;

/// Which OpenAI-compatible endpoint a client called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelayDoor {
    ChatCompletions,
    /// The legacy text completions endpoint, which OpenAI-compatible servers
    /// answer beside chat.
    Completions,
    Responses,
    Embeddings,
}

impl RelayDoor {
    /// The path below the provider's OpenAI base URL (the URL ending `/v1`).
    pub fn path(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat/completions",
            Self::Completions => "completions",
            Self::Responses => "responses",
            Self::Embeddings => "embeddings",
        }
    }

    /// The verb the catalog must admit the model for.
    pub fn verb(self) -> VerbKind {
        match self {
            Self::Embeddings => VerbKind::Embed,
            _ => VerbKind::Complete,
        }
    }

    /// The protocol the door speaks.
    pub fn wire(self) -> Wire {
        match self {
            Self::ChatCompletions | Self::Completions => Wire::OpenAiChatCompletions,
            Self::Responses => Wire::OpenAiResponses,
            Self::Embeddings => Wire::OpenAiEmbeddings,
        }
    }

    /// The name usage rows and logs use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat",
            Self::Completions => "completions",
            Self::Responses => "responses",
            Self::Embeddings => "embeddings",
        }
    }

    pub fn parse(door: &str) -> Option<Self> {
        [
            Self::ChatCompletions,
            Self::Completions,
            Self::Responses,
            Self::Embeddings,
        ]
        .into_iter()
        .find(|candidate| candidate.as_str() == door)
    }

    /// Whether a streamed reply is read for usage chunk by chunk, with
    /// `stream_options.include_usage` asked for on the client's behalf.
    fn streams_usage_chunks(self) -> bool {
        matches!(self, Self::ChatCompletions | Self::Completions)
    }
}

/// Where a relayed request goes, as the provider's own endpoint code builds it.
#[derive(Clone)]
pub struct RelayRoute {
    /// The provider's OpenAI base URL, ending in its version segment
    /// (`https://openrouter.ai/api/v1`, `http://spark:8000/v1`).
    pub base_url: String,
    /// Headers the provider needs, credentials included.
    pub headers: Vec<(String, String)>,
    pub backend_key: String,
    /// The provider's name as a person reads it.
    pub provider_name: String,
    /// The model name the provider is sent, replacing the client's.
    pub sent_model: String,
    /// The account whose credential the headers carry, when one does.
    pub account_id: Option<String>,
}

impl RelayRoute {
    pub fn url(&self, door: RelayDoor) -> String {
        format!("{}/{}", self.base_url.trim_end_matches('/'), door.path())
    }
}

/// Headers carry bearer keys; their values never reach a log.
impl std::fmt::Debug for RelayRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayRoute")
            .field("base_url", &self.base_url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| format!("{name}: <redacted>"))
                    .collect::<Vec<_>>(),
            )
            .field("backend_key", &self.backend_key)
            .field("provider_name", &self.provider_name)
            .field("sent_model", &self.sent_model)
            .field("account_id", &self.account_id)
            .finish()
    }
}

/// One relayed request.
pub struct RelayCall {
    pub door: RelayDoor,
    pub route: RelayRoute,
    /// The client's body, as it sent it.
    pub body: Map<String, Value>,
    /// Set when the client goes away; the read stops at the next chunk.
    pub cancel: Arc<AtomicBool>,
}

/// What the tap read from a relayed reply. Tokens are as the provider counts
/// them: `input_tokens` is the whole prompt, with `cached_tokens` inside it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RelayUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    /// What the provider says the call cost, in USD (OpenRouter's `usage.cost`).
    pub cost: Option<f64>,
    /// The provider's id for the request (`x-request-id`, `request-id` or
    /// `x-generation-id`).
    pub upstream_request_id: Option<String>,
    /// The bytes the client was sent.
    pub bytes: u64,
}

/// The provider's answer: its status and headers now, its body as the client
/// reads it.
pub struct Relayed {
    pub status: u16,
    /// The provider's headers minus hop-by-hop headers and `content-length`
    /// (the tap may shorten a stream).
    pub headers: Vec<(String, String)>,
    pub body: RelayBody,
    /// Whether the client asked for a stream.
    pub stream: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayError {
    /// The client's request cannot be sent as it is (an embeddings stream).
    InvalidRequest(String),
    /// The provider could not be reached or sent no answer.
    Unreachable(String),
}

impl std::fmt::Display for RelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest(message) | Self::Unreachable(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RelayError {}

/// Sends relayed requests. The only implementation that reaches a provider is
/// [`crate::complete::HttpComplete`].
pub trait Relay: Send + Sync {
    fn relay(&self, call: RelayCall) -> Result<Relayed, RelayError>;
}

/// The body the provider is sent, and whether the tap removes the usage-only
/// chunk Cairn asked for on the client's behalf.
///
/// Only `model` changes, except for a chat or completions stream whose client
/// did not ask for usage: Cairn asks (`stream_options.include_usage`) so the
/// call is metered, and the tap removes the extra chunk so the client reads
/// what it would have read without Cairn in the path.
pub fn prepare_body(
    door: RelayDoor,
    sent_model: &str,
    mut body: Map<String, Value>,
) -> Result<PreparedBody, RelayError> {
    let stream = body.get("stream").and_then(Value::as_bool) == Some(true);
    if stream && door == RelayDoor::Embeddings {
        return Err(RelayError::InvalidRequest(
            "embeddings are answered whole; a stream cannot be asked for".into(),
        ));
    }
    body.insert("model".into(), Value::String(sent_model.to_string()));
    let mut strip_usage_chunk = false;
    if stream && door.streams_usage_chunks() {
        let asked = body
            .get("stream_options")
            .and_then(|options| options.get("include_usage"))
            .and_then(Value::as_bool)
            == Some(true);
        if !asked {
            let options = body
                .entry("stream_options")
                .or_insert_with(|| Value::Object(Map::new()));
            if !options.is_object() {
                *options = Value::Object(Map::new());
            }
            if let Some(options) = options.as_object_mut() {
                options.insert("include_usage".into(), Value::Bool(true));
            }
            strip_usage_chunk = true;
        }
    }
    Ok(PreparedBody {
        body: Value::Object(body),
        stream,
        strip_usage_chunk,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedBody {
    pub body: Value,
    pub stream: bool,
    pub strip_usage_chunk: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn only_the_model_changes_on_a_plain_request() {
        let body = object(serde_json::json!({
            "model": "big", "messages": [], "tools": [{"type": "function"}],
            "response_format": {"type": "json_object"}
        }));
        let prepared = prepare_body(RelayDoor::ChatCompletions, "served", body).unwrap();
        assert_eq!(
            prepared.body,
            serde_json::json!({
                "model": "served", "messages": [], "tools": [{"type": "function"}],
                "response_format": {"type": "json_object"}
            })
        );
        assert!(!prepared.stream);
        assert!(!prepared.strip_usage_chunk);
    }

    #[test]
    fn a_stream_asks_for_usage_only_when_the_client_did_not() {
        let body = object(serde_json::json!({"model": "m", "stream": true}));
        let prepared = prepare_body(RelayDoor::Completions, "m", body).unwrap();
        assert_eq!(
            prepared.body["stream_options"],
            serde_json::json!({"include_usage": true})
        );
        assert!(prepared.strip_usage_chunk);

        let body = object(serde_json::json!({
            "model": "m", "stream": true, "stream_options": {"include_usage": true}
        }));
        let prepared = prepare_body(RelayDoor::ChatCompletions, "m", body).unwrap();
        assert!(!prepared.strip_usage_chunk);

        let body = object(serde_json::json!({"model": "m", "stream": true}));
        let prepared = prepare_body(RelayDoor::Responses, "m", body).unwrap();
        assert!(prepared.body.get("stream_options").is_none());
        assert!(!prepared.strip_usage_chunk);
    }

    #[test]
    fn an_embeddings_stream_is_refused() {
        let body = object(serde_json::json!({"model": "m", "stream": true}));
        assert!(matches!(
            prepare_body(RelayDoor::Embeddings, "m", body),
            Err(RelayError::InvalidRequest(_))
        ));
    }
}
