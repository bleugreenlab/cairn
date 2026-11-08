//! OpenRouter's endpoint and provider-routing object. Its headers are the
//! facet's (`cairn_models::openrouter`), shared with the image verb.

use crate::backends::retention::ZdrAdmission;
use crate::models::{OpenRouterRouting, OpenRouterSort};
use serde_json::{json, Map, Value};

pub(super) const CHAT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

/// Which request a routing object is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Request {
    /// A chat turn or completion, which sends tool schemas and may send an
    /// output schema, so it always requires providers that honour every
    /// parameter sent.
    Chat,
    /// A decision or an embedding, whose parameters every endpoint of the
    /// model takes; nothing is required beyond what the routing asks.
    Whole,
}

/// The OpenRouter `provider` object for one request: the one builder every
/// OpenRouter request Cairn sends goes through (agent turns, completions,
/// decisions, embeddings). `None` when there is nothing to say.
///
/// - `require_parameters` on every chat request: tool schemas are
///   unconditional, so this only formalizes routing to tool-capable providers.
/// - `zdr` when the call's zero-data-retention requirement was admitted, with
///   `ignore` naming the model's Safety Retention endpoints, which `zdr` alone
///   would still route to on an account that allows them. The workspace's
///   setting sends `zdr` even without an admission, so no path that skipped
///   the gate can send less than the operator asked.
/// - `sort` when the workspace sets one.
pub(super) fn provider_object(
    routing: &OpenRouterRouting,
    retention: Option<&ZdrAdmission>,
    request: Request,
) -> Option<Value> {
    let mut provider = Map::new();
    if request == Request::Chat {
        provider.insert("require_parameters".into(), json!(true));
    }
    if retention.is_some() || routing.zero_data_retention {
        provider.insert("zdr".into(), json!(true));
    }
    if let Some(exclude) = retention
        .map(|admitted| &admitted.exclude)
        .filter(|exclude| !exclude.is_empty())
    {
        provider.insert("ignore".into(), json!(exclude));
    }
    if let Some(sort) = routing.sort {
        provider.insert(
            "sort".into(),
            json!(match sort {
                OpenRouterSort::Price => "price",
                OpenRouterSort::Throughput => "throughput",
                OpenRouterSort::Latency => "latency",
            }),
        );
    }
    (!provider.is_empty()).then_some(Value::Object(provider))
}

/// The `provider` object as a request's extras: the field to merge into its
/// body, or none.
pub(super) fn provider_extras(
    routing: &OpenRouterRouting,
    retention: Option<&ZdrAdmission>,
    request: Request,
) -> Map<String, Value> {
    provider_object(routing, retention, request)
        .map(|provider| Map::from_iter([("provider".to_string(), provider)]))
        .unwrap_or_default()
}

/// The headers Cairn's OpenRouter requests carry: the facet's, attributed to
/// Cairn.
pub(super) fn openrouter_headers(api_key: &str) -> Vec<(String, String)> {
    cairn_models::openrouter::headers(api_key, &cairn_models::openrouter::Attribution::cairn())
}
