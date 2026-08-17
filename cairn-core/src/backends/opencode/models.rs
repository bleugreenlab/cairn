//! OpenCode Go model catalog discovery.
//!
//! Two sources compose one catalog:
//!
//! - `https://opencode.ai/zen/go/v1/models` is authoritative for WHICH models a
//!   Go subscription serves right now. It answers with bare ids and nothing
//!   else, and OpenCode rotates the list as it tests and negotiates models, so
//!   the set is discovered rather than hardcoded.
//! - models.dev is where OpenCode publishes the metadata for those ids: display
//!   name, context window, price, reasoning efforts, and the AI SDK package each
//!   model is served through. It is the same metadata OpenCode's own client
//!   reads, not a third-party guess about them.
//!
//! That last field decides how Cairn talks to a model. Go fronts three protocol
//! families behind one base URL, and Cairn speaks all three, so the package is
//! recorded on the catalog entry as its [`DiscoveredWireProtocol`] and the
//! backend routes each session and completion by it. A package Cairn has no
//! mapping for stays unselectable rather than being assumed compatible: that
//! guess fails in the middle of a session instead of before one starts.
//!
//! Metadata is required, not decorative: without it Cairn knows neither the
//! protocol nor the context window, so a metadata failure is a catalog failure.
//! The orchestrator retains the last good catalog through one, which is the
//! behavior that makes this safe to insist on.

use crate::backends::{
    DiscoveredModel, DiscoveredModelPricing, DiscoveredReasoningEffort, DiscoveredWireProtocol,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

const SUBSCRIPTION_MODELS_URL: &str = "https://opencode.ai/zen/go/v1/models";
const PUBLISHED_METADATA_URL: &str = "https://models.dev/api.json";
/// OpenCode's provider key in the published metadata.
const METADATA_PROVIDER: &str = "opencode-go";
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(20);

// === Subscription catalog (ids) ===

#[derive(Debug, Deserialize)]
struct SubscriptionModels {
    #[serde(default)]
    data: Vec<SubscriptionModel>,
}

#[derive(Debug, Deserialize)]
struct SubscriptionModel {
    id: String,
}

// === Published metadata (models.dev) ===

#[derive(Debug, Deserialize)]
struct MetadataProvider {
    #[serde(default)]
    models: HashMap<String, PublishedModel>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct PublishedModel {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    tool_call: Option<bool>,
    #[serde(default)]
    reasoning: Option<bool>,
    #[serde(default)]
    temperature: Option<bool>,
    #[serde(default)]
    structured_output: Option<bool>,
    #[serde(default)]
    reasoning_options: Vec<PublishedReasoningOption>,
    #[serde(default)]
    limit: Option<PublishedLimit>,
    #[serde(default)]
    cost: Option<PublishedCost>,
    /// Per-model override of the provider's default SDK package. Its absence is
    /// meaningful: it means the model uses the provider default, which for
    /// OpenCode Go is the OpenAI-compatible chat/completions package.
    #[serde(default)]
    provider: Option<PublishedProviderOverride>,
}

#[derive(Debug, Deserialize)]
struct PublishedProviderOverride {
    #[serde(default)]
    npm: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublishedLimit {
    #[serde(default)]
    context: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PublishedCost {
    #[serde(default)]
    input: Option<f64>,
    #[serde(default)]
    output: Option<f64>,
    #[serde(default)]
    cache_read: Option<f64>,
    #[serde(default)]
    cache_write: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PublishedReasoningOption {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    values: Vec<String>,
}

// === Protocol ===

/// The wire protocol Go serves a model over, named by the AI SDK package
/// OpenCode publishes for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    /// OpenAI-compatible `chat/completions` — the family Cairn's turn loop speaks.
    ChatCompletions,
    /// Anthropic-style `messages`.
    AnthropicMessages,
    /// OpenAI `responses`.
    OpenAiResponses,
    /// A package Cairn has no mapping for. Treated as unservable rather than
    /// assumed compatible: a wrong guess here fails in the middle of a session.
    Unrecognized,
}

impl Protocol {
    fn from_npm(npm: Option<&str>) -> Self {
        match npm {
            // No override means the provider default, which is openai-compatible.
            None | Some("@ai-sdk/openai-compatible") => Protocol::ChatCompletions,
            Some("@ai-sdk/anthropic") => Protocol::AnthropicMessages,
            Some("@ai-sdk/openai") => Protocol::OpenAiResponses,
            Some(_) => Protocol::Unrecognized,
        }
    }

    /// The durable routing fact stored on the catalog entry. This is what makes
    /// the protocol survive serialization and a catalog refresh, so a session
    /// started tomorrow reaches the same endpoint as one started today.
    fn discovered(self) -> DiscoveredWireProtocol {
        match self {
            Protocol::ChatCompletions => DiscoveredWireProtocol::OpenAiChatCompletions,
            Protocol::AnthropicMessages => DiscoveredWireProtocol::AnthropicMessages,
            Protocol::OpenAiResponses => DiscoveredWireProtocol::OpenAiResponses,
            Protocol::Unrecognized => DiscoveredWireProtocol::Unknown,
        }
    }

    /// Whether Cairn has an adapter for this family.
    fn is_served(self) -> bool {
        !matches!(self, Protocol::Unrecognized)
    }
}

// === Discovery ===

pub(crate) fn discover_models_blocking(
    api_key: Option<&str>,
) -> Result<Vec<DiscoveredModel>, String> {
    let ids = fetch_subscription_model_ids(api_key)?;
    let published = fetch_published_metadata()?;
    Ok(build_catalog(&ids, &published))
}

/// The ids a subscription serves. The endpoint answers unauthenticated with the
/// full Go line-up, so the catalog is browsable before a key is pasted; the key
/// rides along when there is one so the answer is scoped to the account.
fn fetch_subscription_model_ids(api_key: Option<&str>) -> Result<Vec<String>, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(DISCOVERY_TIMEOUT)
        .build()
        .map_err(|error| format!("OpenCode Go discovery client failed: {error}"))?;
    let mut request = client.get(SUBSCRIPTION_MODELS_URL);
    if let Some(api_key) = api_key.map(str::trim).filter(|key| !key.is_empty()) {
        request = request.bearer_auth(api_key);
    }
    let response = request
        .send()
        .map_err(|error| format!("OpenCode Go model catalog request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("OpenCode Go model catalog body failed: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "OpenCode Go model catalog returned HTTP {}: {}",
            status.as_u16(),
            crate::backends::openai_compat::http::upstream_error_detail(&body)
        ));
    }
    decode_subscription_model_ids(&body)
}

/// The published index covers every provider models.dev knows, so this pulls a
/// few megabytes to read one provider's entry. That is affordable because a
/// catalog refresh is startup maintenance and an explicit user action, not a
/// timer: there is no per-provider endpoint to ask instead (the per-provider
/// paths redirect to the site root).
fn fetch_published_metadata() -> Result<HashMap<String, PublishedModel>, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(DISCOVERY_TIMEOUT)
        .build()
        .map_err(|error| format!("OpenCode Go metadata client failed: {error}"))?;
    let response = client
        .get(PUBLISHED_METADATA_URL)
        .send()
        .map_err(|error| format!("OpenCode Go model metadata request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("OpenCode Go model metadata body failed: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "OpenCode Go model metadata returned HTTP {}",
            status.as_u16()
        ));
    }
    decode_published_metadata(&body)
}

pub(crate) fn decode_subscription_model_ids(body: &str) -> Result<Vec<String>, String> {
    let response: SubscriptionModels = serde_json::from_str(body)
        .map_err(|error| format!("OpenCode Go model catalog JSON failed: {error}"))?;
    Ok(response.data.into_iter().map(|model| model.id).collect())
}

/// Pull OpenCode's own provider entry out of the published metadata index. An
/// index that no longer carries the provider is a failure rather than an empty
/// result: silently returning nothing would present as "your subscription serves
/// no models".
pub(crate) fn decode_published_metadata(
    body: &str,
) -> Result<HashMap<String, PublishedModel>, String> {
    let index: HashMap<String, serde_json::Value> = serde_json::from_str(body)
        .map_err(|error| format!("OpenCode Go model metadata JSON failed: {error}"))?;
    let provider = index.get(METADATA_PROVIDER).ok_or_else(|| {
        format!("OpenCode Go model metadata carries no '{METADATA_PROVIDER}' provider")
    })?;
    let provider: MetadataProvider = serde_json::from_value(provider.clone())
        .map_err(|error| format!("OpenCode Go model metadata shape failed: {error}"))?;
    Ok(provider.models)
}

/// Join the subscription's ids to their published metadata, in the order the
/// subscription returned them.
pub(crate) fn build_catalog(
    ids: &[String],
    published: &HashMap<String, PublishedModel>,
) -> Vec<DiscoveredModel> {
    ids.iter()
        .map(|id| match published.get(id) {
            Some(metadata) => model_from_metadata(id, metadata),
            None => unpublished_model(id),
        })
        .collect()
}

fn model_from_metadata(id: &str, metadata: &PublishedModel) -> DiscoveredModel {
    let protocol = Protocol::from_npm(
        metadata
            .provider
            .as_ref()
            .and_then(|provider| provider.npm.as_deref()),
    );
    let servable = protocol.is_served();
    let description = match servable {
        true => metadata.description.clone(),
        false => Some(unservable_note(metadata.description.as_deref())),
    };

    DiscoveredModel {
        id: id.to_string(),
        model: id.to_string(),
        display_name: metadata.name.clone().unwrap_or_else(|| id.to_string()),
        description,
        hidden: !servable,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: reasoning_efforts(metadata),
        context_window: metadata.limit.as_ref().and_then(|limit| limit.context),
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: metadata.cost.as_ref().map(|cost| DiscoveredModelPricing {
            prompt: per_token(cost.input),
            completion: per_token(cost.output),
            request: None,
            image: None,
            web_search: None,
            internal_reasoning: None,
            input_cache_read: per_token(cost.cache_read),
            input_cache_write: per_token(cost.cache_write),
        }),
        supported_parameters: supported_parameters(metadata),
        router: false,
        architecture_modality: None,
        wire_protocol: Some(protocol.discovered()),
    }
}

/// An id the subscription serves but the metadata index has not caught up with.
/// Carried so the model is accounted for, unselectable because Cairn cannot tell
/// which endpoint would serve it or how much context it has.
fn unpublished_model(id: &str) -> DiscoveredModel {
    DiscoveredModel {
        id: id.to_string(),
        model: id.to_string(),
        display_name: id.to_string(),
        description: Some(
            "Your subscription serves this model, but OpenCode has not published its metadata yet, \
             so Cairn cannot tell which endpoint serves it."
                .to_string(),
        ),
        hidden: true,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: Vec::new(),
        context_window: None,
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: None,
        supported_parameters: Vec::new(),
        router: false,
        architecture_modality: None,
        wire_protocol: None,
    }
}

fn unservable_note(description: Option<&str>) -> String {
    let note =
        "Served over an endpoint family Cairn has no mapping for, so Cairn cannot tell how to \
         talk to it."
            .to_string();
    match description {
        Some(description) if !description.trim().is_empty() => format!("{note} {description}"),
        _ => note,
    }
}

fn reasoning_efforts(metadata: &PublishedModel) -> Vec<DiscoveredReasoningEffort> {
    metadata
        .reasoning_options
        .iter()
        .filter(|option| option.kind == "effort")
        .flat_map(|option| option.values.iter())
        .map(|effort| DiscoveredReasoningEffort {
            reasoning_effort: effort.clone(),
            description: None,
        })
        .collect()
}

/// The capability flags the catalog exposes as parameters. `tools` is the
/// load-bearing one: Cairn drives an entire run through tool calls, so a model
/// without it cannot run an agent at all and the settings surface says so.
fn supported_parameters(metadata: &PublishedModel) -> Vec<String> {
    let mut parameters = Vec::new();
    if metadata.tool_call.unwrap_or(false) {
        parameters.push("tools".to_string());
    }
    if metadata.reasoning.unwrap_or(false) {
        parameters.push("reasoning".to_string());
    }
    if metadata.temperature.unwrap_or(false) {
        parameters.push("temperature".to_string());
    }
    if metadata.structured_output.unwrap_or(false) {
        parameters.push("structured_outputs".to_string());
    }
    parameters
}

/// models.dev publishes price per million tokens; the catalog carries price per
/// token, the unit every other provider's pricing already uses. Fixed notation
/// keeps the value parseable by the settings surface, which multiplies it back
/// out for display.
fn per_token(per_million: Option<f64>) -> Option<String> {
    per_million.map(|value| format!("{:.12}", value / 1_000_000.0))
}
