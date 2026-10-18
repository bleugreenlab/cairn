//! OpenAI's `embeddings`: the request body, its one POST, and the reply.

use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::CompletionError;

/// Where an OpenAI-compatible embeddings deployment lives and how it
/// authenticates.
#[derive(Clone)]
pub struct EmbeddingsEndpoint {
    /// What to call this provider in an error a user will read.
    pub provider_name: String,
    /// The backend key usage is attributed to.
    pub backend_key: String,
    /// The full `{base}/embeddings` URL.
    pub url: String,
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for EmbeddingsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingsEndpoint")
            .field("provider_name", &self.provider_name)
            .field("backend_key", &self.backend_key)
            .field("url", &self.url)
            .field("headers", &super::redacted(&self.headers))
            .finish()
    }
}

/// One embeddings request. Vectors are always asked for as floats;
/// `dimensions` and `input_type` are sent only when set, because servers that
/// do not know them differ in whether they ignore or refuse them.
pub fn build_body(
    model: &str,
    input: &[String],
    dimensions: Option<u32>,
    input_type: Option<&str>,
) -> Value {
    let mut body = json!({
        "model": model,
        "input": input,
        "encoding_format": "float",
    });
    if let Some(dimensions) = dimensions {
        body["dimensions"] = json!(dimensions);
    }
    if let Some(input_type) = input_type {
        body["input_type"] = json!(input_type);
    }
    body
}

/// A 404 or 501 from an embeddings route is how a chat model answers there
/// (llama.cpp hubs and vLLM say 404, Ollama says 501).
fn hint(status: u16) -> Option<&'static str> {
    matches!(status, 404 | 501).then_some("is this an embedding model?")
}

/// Post one embeddings request and read its reply.
pub fn post_embeddings(
    endpoint: &EmbeddingsEndpoint,
    body: Value,
    timeout: Duration,
) -> Result<EmbeddingsResponse, CompletionError> {
    let text = super::post(
        &endpoint.provider_name,
        "embeddings",
        &endpoint.url,
        &endpoint.headers,
        &body,
        timeout,
        hint,
    )?;
    read(&text)
}

/// Decode an embeddings reply body.
pub fn read(text: &str) -> Result<EmbeddingsResponse, CompletionError> {
    serde_json::from_str(text).map_err(|error| {
        let shown: String = text.chars().take(400).collect();
        CompletionError::InvalidResponse(format!("unreadable embeddings reply: {error}: {shown}"))
    })
}

/// The reply. Unknown fields (`object`, `id`, `is_byok`) are ignored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EmbeddingsResponse {
    pub data: Vec<Embedding>,
    /// What the server says it ran. Not an identity: hubs answer with the
    /// backend's served name, not the id they were sent.
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub usage: Option<EmbeddingsUsage>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Embedding {
    /// The input position this vector belongs to. Servers are not required to
    /// answer in order.
    pub index: u32,
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EmbeddingsUsage {
    #[serde(default)]
    pub prompt_tokens: Option<u64>,
    #[serde(default)]
    pub total_tokens: Option<u64>,
    /// OpenRouter's reported cost, in USD credits.
    #[serde(default)]
    pub cost: Option<f64>,
}
