//! The OpenAI Images route: OpenAI itself, or any OpenAI-compatible image
//! server a host or operator points at.

use crate::image_wire::ImagesEndpoint;
use crate::interface::image::ImageRoute;

/// The route to an Images API at `base_url` (the API root, such as
/// `https://api.openai.com/v1`), authenticated with `api_key` when one is
/// given, plus the operator's own `headers`.
pub fn route_for(
    provider_name: &str,
    backend_key: &str,
    base_url: &str,
    api_key: Option<&str>,
    headers: &[(String, String)],
) -> ImageRoute {
    let mut sent = Vec::new();
    if let Some(api_key) = api_key.filter(|key| !key.trim().is_empty()) {
        sent.push(("authorization".to_string(), format!("Bearer {api_key}")));
    }
    sent.extend(headers.iter().cloned());
    ImageRoute::OpenAi(ImagesEndpoint {
        provider_name: provider_name.to_string(),
        backend_key: backend_key.to_string(),
        base_url: base_url.trim_end_matches('/').to_string(),
        headers: sent,
    })
}
