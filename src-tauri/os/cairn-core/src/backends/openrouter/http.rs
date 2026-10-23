//! OpenRouter's endpoint and provider-routing object. Its headers are the
//! facet's (`cairn_models::openrouter`), shared with the image verb.

use crate::models::{OpenRouterRouting, OpenRouterSort};
use serde_json::{json, Value};

pub(super) const CHAT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

/// Build the OpenRouter `provider` object from routing settings.
///
/// `require_parameters` is always sent: tool schemas are unconditional, so this
/// only formalizes the existing effective behavior (route only to tool-capable
/// providers). `zdr`/`sort` are added only when the user opts in.
pub(super) fn build_provider_object(routing: &OpenRouterRouting) -> Value {
    let mut provider = json!({ "require_parameters": true });
    if routing.zero_data_retention {
        provider["zdr"] = json!(true);
    }
    if let Some(sort) = routing.sort {
        provider["sort"] = json!(match sort {
            OpenRouterSort::Price => "price",
            OpenRouterSort::Throughput => "throughput",
            OpenRouterSort::Latency => "latency",
        });
    }
    provider
}

/// The headers Cairn's OpenRouter requests carry: the facet's, attributed to
/// Cairn.
pub(super) fn openrouter_headers(api_key: &str) -> Vec<(String, String)> {
    cairn_models::openrouter::headers(api_key, &cairn_models::openrouter::Attribution::cairn())
}
