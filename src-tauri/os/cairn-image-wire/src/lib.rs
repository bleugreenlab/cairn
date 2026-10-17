//! Cairn's image wires: OpenRouter's Image API ([`openrouter`]) and the OpenAI
//! Images API ([`openai`]), which OpenAI and every OpenAI-compatible image
//! server (LocalAI, vLLM-Omni, the stable-diffusion.cpp server) answer alike.
//!
//! A leaf: it reaches no other workspace crate. Each protocol is a module of
//! request builders, one blocking POST per call, and the provider's own JSON
//! as DTOs; the cairn-models facet decides what to send and maps replies onto
//! its catalog and usage vocabulary. Image replies carry megabytes of base64,
//! and edits are multipart, which is why these wires do not ride the LLM
//! `Complete` seam.

pub mod error;
mod http;
pub mod openai;
pub mod openrouter;
pub mod sniff;

use base64::Engine;
use serde_json::{Map, Value};

pub use error::ImageWireError;

/// Where an image deployment lives and how it authenticates.
#[derive(Clone)]
pub struct ImagesEndpoint {
    /// What to call this provider in an error a user will read.
    pub provider_name: String,
    /// The backend key usage is attributed to.
    pub backend_key: String,
    /// The API root (`https://openrouter.ai/api/v1`, `https://api.openai.com/v1`,
    /// `http://spark:8080/v1`); every path is one join onto it.
    pub base_url: String,
    /// Sent with every request. A `content-type` here is ignored: each body
    /// sets its own, and a multipart body's carries its boundary.
    pub headers: Vec<(String, String)>,
}

/// Header values carry bearer keys; only their names are printed.
impl std::fmt::Debug for ImagesEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImagesEndpoint")
            .field("provider_name", &self.provider_name)
            .field("backend_key", &self.backend_key)
            .field("base_url", &self.base_url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| format!("{name}: <redacted>"))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ImagesEndpoint {
    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}/{path}", self.base_url.trim_end_matches('/'))
    }
}

/// `extras` merged under `core`, refusing any extra that names a core key.
///
/// The core fields are the capability contract the facet admitted the call
/// against, so an extra may add a provider knob but never replace one of
/// them, even with the same value.
pub fn merge_extras(
    core: Map<String, Value>,
    extras: &Map<String, Value>,
    core_keys: &[&str],
) -> Result<Map<String, Value>, ImageWireError> {
    if let Some(key) = extras.keys().find(|key| core_keys.contains(&key.as_str())) {
        return Err(ImageWireError::InvalidRequest(format!(
            "extras may not set '{key}': the typed request owns it"
        )));
    }
    let mut merged = extras.clone();
    merged.extend(core);
    Ok(merged)
}

/// `bytes` as a `data:` URL.
pub fn data_url(media_type: &str, bytes: &[u8]) -> String {
    format!(
        "data:{media_type};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

/// Decode a reply's base64 image.
pub fn decode_base64(text: &str) -> Result<Vec<u8>, ImageWireError> {
    base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .map_err(|error| ImageWireError::InvalidResponse(format!("image is not base64: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extras_add_knobs_but_never_replace_a_core_key() {
        let core = json!({"model": "m", "prompt": "p"});
        let extras = json!({"quality": "high"});
        let merged = merge_extras(
            core.as_object().unwrap().clone(),
            extras.as_object().unwrap(),
            &["model", "prompt", "n"],
        )
        .unwrap();
        assert_eq!(
            Value::Object(merged),
            json!({"model": "m", "prompt": "p", "quality": "high"})
        );
        let colliding = json!({"n": 4});
        let error = merge_extras(
            core.as_object().unwrap().clone(),
            colliding.as_object().unwrap(),
            &["model", "prompt", "n"],
        )
        .unwrap_err();
        assert_eq!(
            error,
            ImageWireError::InvalidRequest(
                "extras may not set 'n': the typed request owns it".into()
            )
        );
    }

    #[test]
    fn data_urls_round_trip() {
        let url = data_url("image/png", b"\x89PNG");
        assert_eq!(url, "data:image/png;base64,iVBORw==");
        assert_eq!(decode_base64("iVBORw==").unwrap(), b"\x89PNG");
        assert!(decode_base64("not base64!").is_err());
    }

    #[test]
    fn the_endpoint_never_prints_its_key() {
        let endpoint = ImagesEndpoint {
            provider_name: "OpenRouter".into(),
            backend_key: "openrouter".into(),
            base_url: "https://openrouter.ai/api/v1/".into(),
            headers: vec![("authorization".into(), "Bearer sk-or-test".into())],
        };
        let printed = format!("{endpoint:?}");
        assert!(!printed.contains("sk-or-test"), "{printed}");
        assert!(printed.contains("authorization: <redacted>"), "{printed}");
        assert_eq!(
            endpoint.url("images"),
            "https://openrouter.ai/api/v1/images"
        );
    }
}
