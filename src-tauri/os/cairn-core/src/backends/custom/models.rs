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
use cairn_models::types::catalog::DiscoveredModel;
use cairn_models::types::tokenizers::tokenizer_for;
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
    /// vLLM's source for the served model: the Hugging Face repository when it
    /// was started from one (a local path otherwise, which pins nothing).
    #[serde(default)]
    root: Option<String>,
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
                .map(|id| model(config, id.clone(), None, None))
                .collect(),
            None,
        ),
        CustomModelPolicy::Discover => match fetch(config) {
            Ok(entries) => (
                entries
                    .into_iter()
                    .map(|entry| model(config, entry.id, entry.max_model_len, entry.root))
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
        CustomProtocol::OpenAiImages => {
            return Err("an OpenAI Images provider lists its models explicitly".to_string())
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
    root: Option<String>,
) -> DiscoveredModel {
    // A reported root is the server's statement of what it loaded, and is
    // authoritative: an unknown root (a local path, a fine-tune, an unpinned
    // repository) selects nothing even when the served id looks like a pinned
    // repository. Only with no root at all is the id the upstream identity,
    // since vLLM serves a model under its repository id unless renamed.
    let backend_key = config.backend_key();
    let upstream = root.as_deref().unwrap_or(&id);
    let tokenizer = tokenizer_for(&backend_key, &id, Some(upstream));
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
        modalities: None,
        // What a custom provider serves is fixed by its configured protocol, so
        // the provider declares it once for every model it lists.
        serves: None,
        tokenizer,
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
