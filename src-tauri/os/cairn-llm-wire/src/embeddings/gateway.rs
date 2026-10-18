//! Cairn's cloud gateway `/embed` (Cohere Embed v4 on Bedrock): the request
//! body, its one POST, and the reply, as `api/src/routes/embed.ts` defines
//! them.

use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::CompletionError;

/// The gateway, authenticated with this device's Cairn token.
#[derive(Clone)]
pub struct GatewayEmbedEndpoint {
    /// What to call the gateway in an error a user will read.
    pub provider_name: String,
    /// The backend key usage is attributed to.
    pub backend_key: String,
    /// The full `{api}/embed` URL.
    pub url: String,
    /// `authorization: Bearer <device token>`.
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for GatewayEmbedEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayEmbedEndpoint")
            .field("provider_name", &self.provider_name)
            .field("backend_key", &self.backend_key)
            .field("url", &self.url)
            .field("headers", &super::redacted(&self.headers))
            .finish()
    }
}

/// One gateway request. `input_type` is `search_document` or `search_query`.
///
/// No `dims`: the gateway accepts the field and never reads it, and installed
/// clients already send it, so a width cannot be asked for through it.
pub fn build_body(texts: &[String], input_type: &str) -> Value {
    json!({
        "texts": texts,
        "input_type": input_type,
    })
}

/// Post one gateway request and read its reply.
pub fn post_gateway_embed(
    endpoint: &GatewayEmbedEndpoint,
    body: Value,
    timeout: Duration,
) -> Result<GatewayEmbedResponse, CompletionError> {
    let text = super::post(
        &endpoint.provider_name,
        "embed",
        &endpoint.url,
        &endpoint.headers,
        &body,
        timeout,
        |_| None,
    )?;
    read(&text)
}

/// Decode a gateway reply body.
pub fn read(text: &str) -> Result<GatewayEmbedResponse, CompletionError> {
    serde_json::from_str(text).map_err(|error| {
        let shown: String = text.chars().take(400).collect();
        CompletionError::InvalidResponse(format!(
            "unreadable embed gateway reply: {error}: {shown}"
        ))
    })
}

/// The gateway's 200 body. Vectors are in input order.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GatewayEmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    /// The model the gateway ran. Cairn runs this service, so unlike a third
    /// party's `model` field this one is authoritative.
    pub model: String,
    pub dims: u32,
    /// The gateway's own estimate (characters over four); Bedrock reports no
    /// token count for Cohere.
    #[serde(default)]
    pub input_tokens: Option<u64>,
}
