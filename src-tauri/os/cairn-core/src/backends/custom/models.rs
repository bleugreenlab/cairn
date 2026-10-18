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
//!
//! Embedding models the operator declared ride on top of either shape: a
//! listed id they name serves `embed` (and only `embed`), and one the listing
//! did not name is added. Because they are the operator's word, they stay
//! offered when the listing fails.

use super::CustomProviderConfig;
use crate::credentials::{CustomModelPolicy, CustomProtocol};
use cairn_models::embed::{families, local_server_limits};
use cairn_models::interface::speak;
use cairn_models::types::catalog::{DiscoveredModel, Modalities, Modality, ServedVerb, Verb, Wire};
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
    if config.protocol.is_speech() {
        return discover_speech(config);
    }
    let (mut models, error) = listed(config);
    declare_embeddings(config, &mut models);
    (models, error)
}

/// Each declared embedding model, serving `embed` over the embeddings route at
/// its declared width. Purpose handling comes from the model's family.
fn declare_embeddings(config: &CustomProviderConfig, models: &mut Vec<DiscoveredModel>) {
    for declared in &config.embeddings {
        let mut capabilities = families::capabilities(
            &declared.id,
            Some(declared.dims),
            false,
            local_server_limits(None),
        );
        capabilities.widths = declared.widths.clone();
        let serves = Some(vec![ServedVerb {
            verb: Verb::Embed(capabilities),
            wire: Wire::OpenAiEmbeddings,
        }]);
        let modalities = Some(Modalities {
            input: vec![Modality::Text],
            output: vec![Modality::Embeddings],
        });
        match models.iter_mut().find(|model| model.id == declared.id) {
            Some(model) => {
                model.serves = serves;
                model.modalities = modalities;
            }
            None => {
                let mut model = model(config, declared.id.clone(), None, None);
                model.serves = serves;
                model.modalities = modalities;
                models.push(model);
            }
        }
    }
}

fn listed(config: &CustomProviderConfig) -> (Vec<DiscoveredModel>, Option<String>) {
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

/// An ElevenLabs `GET /v1/models` entry.
#[derive(Debug, Deserialize)]
struct ElevenLabsModel {
    model_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    can_do_text_to_speech: Option<bool>,
}

/// A speech provider's models: the speech models its listing shows, plus any
/// the operator declared (an explicit list adds to discovery here rather than
/// replacing it, for servers whose ids Cairn does not recognise). Declared
/// models survive a failed listing: they are the operator's word.
fn discover_speech(config: &CustomProviderConfig) -> (Vec<DiscoveredModel>, Option<String>) {
    let declared: &[String] = match &config.models {
        CustomModelPolicy::Explicit { ids } => ids,
        CustomModelPolicy::Discover => &[],
    };
    let (mut models, error) = match fetch_speech(config) {
        Ok(found) => (found, None),
        Err(error) if declared.is_empty() => (Vec::new(), Some(error)),
        Err(error) => (
            Vec::new(),
            Some(format!("{error}; the declared models are still offered")),
        ),
    };
    for id in declared {
        if !models.iter().any(|found| &found.model == id) {
            models.push(model(config, id.clone(), None, None));
        }
    }
    (models, error)
}

/// The speech models a speech provider lists. ElevenLabs marks them itself
/// (`can_do_text_to_speech`); an OpenAI-compatible listing mixes chat and
/// speech ids, so it is filtered to the ones the facet recognises
/// (`speak::recognises`).
fn fetch_speech(config: &CustomProviderConfig) -> Result<Vec<DiscoveredModel>, String> {
    let body = listing(config)?;
    let found: Vec<DiscoveredModel> = match config.protocol {
        CustomProtocol::ElevenLabs => serde_json::from_str::<Vec<ElevenLabsModel>>(&body)
            .map_err(|error| format!("model listing was not an ElevenLabs model list: {error}"))?
            .into_iter()
            .filter(|entry| entry.can_do_text_to_speech == Some(true))
            .map(|entry| {
                let mut found = model(config, entry.model_id, None, None);
                if let Some(name) = entry.name {
                    found.display_name = name;
                }
                found.description = entry.description;
                found
            })
            .collect(),
        _ => serde_json::from_str::<ModelsResponse>(&body)
            .map_err(|error| format!("model listing was not an OpenAI model list: {error}"))?
            .data
            .into_iter()
            .filter(|entry| speak::recognises(config.wire(), &entry.id))
            .map(|entry| model(config, entry.id, None, None))
            .collect(),
    };
    if found.is_empty() {
        return Err(
            "the endpoint listed no speech model Cairn recognises; list its model ids".to_string(),
        );
    }
    Ok(found)
}

fn fetch(config: &CustomProviderConfig) -> Result<Vec<ModelEntry>, String> {
    // Unreachable through configuration, which requires a Decisions or OpenAI
    // Images provider to list its models; refused rather than probed
    // regardless.
    match config.protocol {
        CustomProtocol::Decisions => {
            return Err("a Decisions provider lists its models explicitly".to_string())
        }
        CustomProtocol::OpenAiImages => {
            return Err("an OpenAI Images provider lists its models explicitly".to_string())
        }
        CustomProtocol::OpenAiChatCompletions
        | CustomProtocol::OpenAiSpeech
        | CustomProtocol::ElevenLabs => {}
    }
    let body = listing(config)?;
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

/// The body of `GET {base}/models`, with the provider's own headers.
fn listing(config: &CustomProviderConfig) -> Result<String, String> {
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
    Ok(body)
}

fn model(
    config: &CustomProviderConfig,
    id: String,
    context_window: Option<i64>,
    root: Option<String>,
) -> DiscoveredModel {
    // A chat or Decisions provider's protocol fixes what every model serves,
    // so the provider declares it once. A speech model's capabilities depend
    // on the model, so each declares its own.
    let serves = speak::capabilities(config.wire(), &id).map(|capabilities| {
        vec![ServedVerb {
            verb: Verb::Speak(capabilities),
            wire: config.wire(),
        }]
    });
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
        serves,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{CustomEmbeddingModel, CustomSampling};
    use cairn_models::types::catalog::VerbKind;
    use cairn_models::types::embedding::EmbeddingWidths;

    fn config(base_url: &str, models: CustomModelPolicy) -> CustomProviderConfig {
        CustomProviderConfig {
            account_id: "acc_hub".into(),
            display_name: "Hub".into(),
            base_url: base_url.into(),
            protocol: CustomProtocol::OpenAiChatCompletions,
            headers: vec![],
            models,
            sampling: CustomSampling::default(),
            max_output_tokens: 1,
            images: None,
            embeddings: vec![
                CustomEmbeddingModel {
                    id: "qwen/qwen3-embedding-4b".into(),
                    dims: 2560,
                    widths: EmbeddingWidths::NativeOnly,
                },
                CustomEmbeddingModel {
                    id: "nomic-embed-text".into(),
                    dims: 768,
                    widths: EmbeddingWidths::UpTo { min: 64 },
                },
            ],
        }
    }

    fn find<'a>(models: &'a [DiscoveredModel], id: &str) -> &'a DiscoveredModel {
        models.iter().find(|model| model.id == id).expect(id)
    }

    fn embed_capabilities(
        model: &DiscoveredModel,
    ) -> &cairn_models::types::embedding::EmbeddingCapabilities {
        let served = model.serves.as_ref().expect("a per-model claim");
        assert_eq!(served.len(), 1, "serves only embed: {served:?}");
        assert_eq!(served[0].wire, Wire::OpenAiEmbeddings);
        match &served[0].verb {
            Verb::Embed(capabilities) => capabilities,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_declared_id_serves_only_embed_and_an_unlisted_one_is_added() {
        let (models, error) = discover_blocking(&config(
            "http://hub.invalid/v1",
            CustomModelPolicy::Explicit {
                ids: vec!["big".into(), "qwen/qwen3-embedding-4b".into()],
            },
        ));
        assert_eq!(error, None);
        assert_eq!(models.len(), 3);
        // A chat model keeps the provider-wide claim.
        assert_eq!(find(&models, "big").serves, None);
        let qwen = embed_capabilities(find(&models, "qwen/qwen3-embedding-4b"));
        assert_eq!(qwen.native_dims, Some(2560));
        // Purpose from the family: Qwen3 instructs its queries.
        assert!(matches!(
            &qwen.purpose,
            cairn_models::types::embedding::PurposeHandling::Prefix { document, .. } if document.is_empty()
        ));
        let nomic = find(&models, "nomic-embed-text");
        assert_eq!(nomic.serving_account_ids, vec!["acc_hub".to_string()]);
        assert_eq!(
            embed_capabilities(nomic).widths,
            EmbeddingWidths::UpTo { min: 64 }
        );
        assert!(!nomic
            .serves
            .as_ref()
            .unwrap()
            .iter()
            .any(|served| served.verb.kind() == VerbKind::Complete));
    }

    #[test]
    fn declared_models_stay_offered_when_the_listing_fails() {
        // Nothing listens on port 9: the listing fails at once.
        let (models, error) = discover_blocking(&config(
            "http://127.0.0.1:9/v1",
            CustomModelPolicy::Discover,
        ));
        assert!(error.unwrap().contains("model listing failed"));
        assert_eq!(
            models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            ["qwen/qwen3-embedding-4b", "nomic-embed-text"]
        );
    }
}
