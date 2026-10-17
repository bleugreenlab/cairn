//! OpenRouter's Image API: one JSON `POST {base}/images` per generation, and
//! the public discovery listing (`GET {base}/images/models`, then each
//! model's `/endpoints` record) that says what every endpoint takes and what
//! it costs.
//!
//! Reference images ride as `input_references` (data URLs here); there is no
//! mask. The reply is base64 images with an optional `media_type`, and usage
//! with the call's `cost` in USD. A generation is all or nothing: one that
//! fails returns 502 and is not billed.

pub mod discovery;
pub mod wire;

#[cfg(test)]
mod tests;

use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::{http, merge_extras, ImageWireError, ImagesEndpoint};
use wire::ImagesResponse;

/// OpenRouter's API root.
pub const API_BASE: &str = "https://openrouter.ai/api/v1";

/// The body keys the typed request owns.
pub const CORE_KEYS: &[&str] = &[
    "model",
    "prompt",
    "n",
    "size",
    "aspect_ratio",
    "input_references",
];

/// What one generation asks for, already admitted by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generation<'a> {
    pub model: &'a str,
    pub prompt: &'a str,
    /// Omitted from the body when 1, the provider default.
    pub n: u32,
    /// `2:3`, or `auto`.
    pub aspect_ratio: Option<String>,
    /// Exact pixels, `1024x1536`.
    pub size: Option<String>,
    /// Each reference image as a URL (a `data:` URL for inline bytes).
    pub input_references: Vec<String>,
}

/// The request body: `extras` under the typed fields.
pub fn build_body(
    generation: &Generation<'_>,
    extras: &Map<String, Value>,
) -> Result<Value, ImageWireError> {
    let mut core = Map::new();
    core.insert("model".into(), json!(generation.model));
    core.insert("prompt".into(), json!(generation.prompt));
    if generation.n > 1 {
        core.insert("n".into(), json!(generation.n));
    }
    if let Some(aspect_ratio) = &generation.aspect_ratio {
        core.insert("aspect_ratio".into(), json!(aspect_ratio));
    }
    if let Some(size) = &generation.size {
        core.insert("size".into(), json!(size));
    }
    if !generation.input_references.is_empty() {
        core.insert(
            "input_references".into(),
            Value::Array(
                generation
                    .input_references
                    .iter()
                    .map(|url| json!({"type": "image_url", "image_url": {"url": url}}))
                    .collect(),
            ),
        );
    }
    merge_extras(core, extras, CORE_KEYS).map(Value::Object)
}

/// Post one generation and read its reply.
pub fn post_images(
    endpoint: &ImagesEndpoint,
    body: &Value,
    timeout: Duration,
) -> Result<ImagesResponse, ImageWireError> {
    let request = http::client(timeout)?
        .post(endpoint.url("images"))
        .headers(http::header_map(endpoint)?)
        .json(body);
    let text = http::send(request, &endpoint.provider_name)?;
    read(&text)
}

/// Decode a generation reply body.
pub fn read(text: &str) -> Result<ImagesResponse, ImageWireError> {
    http::read(text, "OpenRouter image reply")
}
