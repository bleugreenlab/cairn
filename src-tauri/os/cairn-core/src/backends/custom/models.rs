//! Model inventory for one custom provider.
//!
//! Two shapes, chosen by the operator when they configure the provider:
//! `/v1/models` for a server that enumerates, and an explicit list for one that
//! does not. Neither is a fallback for the other. A discovery failure on a
//! discovering provider is an error to report, not a cue to invent a list; an
//! explicitly listed provider is never probed, so it stays selectable while its
//! endpoint is briefly down.
//!
//! Model ids are preserved byte for byte in both shapes. The id is what goes
//! back on the wire as `model`, so a tidied-up version of it is a name that no
//! longer resolves.

use super::CustomProviderConfig;
use crate::credentials::{CustomModelPolicy, CustomProtocol};
use cairn_llm::types::catalog::DiscoveredModel;
use serde::Deserialize;
use std::error::Error;
use std::time::Duration;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    data: Vec<ModelEntry>,
}

#[derive(Debug, Deserialize)]
struct ModelEntry {
    id: String,
    /// vLLM and several other servers report the served context length here.
    /// Absent on plenty of others, which is why it is optional rather than
    /// defaulted to something plausible.
    #[serde(default)]
    max_model_len: Option<i64>,
}

/// The models one custom provider serves, and any error worth surfacing.
///
/// Returns both rather than one or the other so a provider that answered
/// partially still contributes what it has while the panel says what went
/// wrong.
pub(crate) fn discover_blocking(
    config: &CustomProviderConfig,
) -> (Vec<DiscoveredModel>, Option<String>) {
    match &config.models {
        CustomModelPolicy::Explicit { ids } => (
            ids.iter()
                .map(|id| model(config, id.clone(), None))
                .collect(),
            None,
        ),
        CustomModelPolicy::Discover => match fetch(config) {
            Ok(entries) => (
                entries
                    .into_iter()
                    .map(|entry| model(config, entry.id, entry.max_model_len))
                    .collect(),
                None,
            ),
            Err(error) => (Vec::new(), Some(error)),
        },
    }
}

fn fetch(config: &CustomProviderConfig) -> Result<Vec<ModelEntry>, String> {
    match config.protocol {
        CustomProtocol::OpenAiChatCompletions => {}
        // Unreachable through configuration, which requires a Decisions
        // provider to list its models; refused rather than probed regardless.
        CustomProtocol::Decisions => {
            return Err("a Decisions provider lists its models explicitly".to_string())
        }
    }
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(DISCOVERY_TIMEOUT)
        .timeout(DISCOVERY_TIMEOUT)
        .build()
        .map_err(|error| with_causes("client failed", &error))?;
    let mut request = client.get(config.models_url());
    for (name, value) in &config.headers {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .map_err(|error| with_causes("model listing failed", &error))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| with_causes("model listing body failed", &error))?;
    if !status.is_success() {
        // The body can echo a bad credential back, so only the status is
        // reported. The status alone distinguishes the cases that matter:
        // unauthorized, not found, or the server being unwell.
        return Err(format!("model listing returned HTTP {}", status.as_u16()));
    }
    let parsed: ModelsResponse = serde_json::from_str(&body)
        .map_err(|error| format!("model listing was not an OpenAI model list: {error}"))?;
    if parsed.data.is_empty() {
        return Err(
            "the endpoint listed no models; list them explicitly if it does not enumerate"
                .to_string(),
        );
    }
    Ok(parsed.data)
}

fn model(
    config: &CustomProviderConfig,
    id: String,
    context_window: Option<i64>,
) -> DiscoveredModel {
    DiscoveredModel {
        id: id.clone(),
        model: id.clone(),
        display_name: id,
        description: None,
        hidden: false,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: vec![],
        context_window,
        canonical_slug: None,
        // The provider is the account, so exactly one account serves every
        // model it offers. This is what keeps two custom providers offering the
        // same id from merging into one entry.
        serving_account_ids: vec![config.account_id.clone()],
        pricing: None,
        // Cairn drives this provider through tool calls, so a model that cannot
        // make them cannot run an agent. Neither the OpenAI model list nor an
        // operator's list says whether a model supports tools, and claiming it
        // does would be an assertion nothing here checked — the first turn is
        // what actually answers it.
        supported_parameters: vec![],
        router: false,
        architecture_modality: None,
        wire_protocol: Some(match config.protocol {
            CustomProtocol::OpenAiChatCompletions => {
                cairn_llm::types::catalog::DiscoveredWireProtocol::OpenAiChatCompletions
            }
            // No turn family serves it; the catalog's decision method says what
            // it does serve.
            CustomProtocol::Decisions => cairn_llm::types::catalog::DiscoveredWireProtocol::Unknown,
        }),
    }
}

fn with_causes(context: &str, error: &dyn Error) -> String {
    let mut message = format!("{context}: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}
