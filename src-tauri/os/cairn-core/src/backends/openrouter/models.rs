use std::collections::BTreeMap;
use std::time::Duration;

use cairn_models::embed::families;
use cairn_models::types::catalog::{
    DiscoveredModel, DiscoveredReasoningEffort, ListedArchitecture, Modalities, Modality,
    ModelPricing, Price, ServedVerb, Verb, VerbKind, Wire,
};
use cairn_models::types::embedding::BatchLimits;
use cairn_models::types::tokenizers::tokenizer_for;
use cairn_models::types::usage::{BillingUnit, Charge, Meter};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::Deserialize;
use serde_json::Value;

const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";
const OPENROUTER_EMBEDDING_MODELS_URL: &str = "https://openrouter.ai/api/v1/embeddings/models";
/// The decision models: the default listing never includes them.
const OPENROUTER_DECISION_MODELS_URL: &str =
    "https://openrouter.ai/api/v1/models?output_modalities=decisions";

/// Bounds the embeddings listing, read beside the chat listing on a refresh.
const EMBEDDING_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(20);
/// Bounds the decisions listing, read beside the chat listing on a refresh.
const DECISION_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(20);

/// At most this many texts per OpenRouter embeddings request. Its API states
/// no cap; this keeps one request well under every upstream's (OpenAI's is
/// 2048, Cohere's 96 is enforced by OpenRouter itself).
const OPENROUTER_EMBED_MAX_TEXTS: u32 = 128;

/// Every model OpenRouter serves: the chat listing, with the image,
/// embeddings and decisions listings merged in, and what went wrong with any
/// of them. `previous` is the last catalog the refresh kept.
///
/// The four listings are read side by side, and a listing that fails is never
/// silent. A failed chat listing answers no models, so the refresh keeps its
/// last catalog whole rather than replacing it with media models alone. A
/// failed image listing keeps the chat models and says `image models
/// unavailable: ...`. A failed embeddings or decisions listing keeps the fresh
/// chat models, carries the last listing's offers of that verb onto them, and
/// says `embedding models unavailable: ...` or `decision models unavailable:
/// ...`.
pub(crate) fn discover_catalog_blocking(
    previous: &[DiscoveredModel],
) -> (Vec<DiscoveredModel>, Option<String>) {
    let (chat, images, embeddings, decisions) = std::thread::scope(|scope| {
        let images =
            scope.spawn(|| cairn_models::image::openrouter::discover(IMAGE_DISCOVERY_TIMEOUT));
        let embeddings = scope.spawn(discover_embedding_models_blocking);
        let decisions = scope.spawn(discover_decision_models_blocking);
        let chat = discover_models_blocking(None);
        let images = images
            .join()
            .unwrap_or_else(|_| Err("image discovery panicked".to_string()));
        let embeddings = embeddings
            .join()
            .unwrap_or_else(|_| Err("embedding discovery panicked".to_string()));
        let decisions = decisions
            .join()
            .unwrap_or_else(|_| Err("decision discovery panicked".to_string()));
        (chat, images, embeddings, decisions)
    });
    let (mut models, listing_error) = assemble(chat, embeddings, decisions, previous);
    if models.is_empty() {
        // The chat listing failed: answer nothing, keeping the last catalog.
        return (models, listing_error);
    }
    let mut errors: Vec<String> = listing_error.into_iter().collect();
    match images {
        Ok(found) => {
            if let Some(first) = found.failures.first() {
                errors.push(format!(
                    "image models unavailable: {} endpoint records could not be read (first, {first}); those models serve nothing until the next refresh",
                    found.failures.len()
                ));
            }
            merge_image_models(&mut models, found.models);
        }
        Err(error) => errors.push(format!("image models unavailable: {error}")),
    }
    (models, (!errors.is_empty()).then(|| errors.join("; ")))
}

fn assemble(
    chat: Result<Vec<DiscoveredModel>, String>,
    embeddings: Result<Vec<DiscoveredModel>, String>,
    decisions: Result<Vec<DiscoveredModel>, String>,
    previous: &[DiscoveredModel],
) -> (Vec<DiscoveredModel>, Option<String>) {
    let mut models = match chat {
        Ok(models) => models,
        Err(error) => return (Vec::new(), Some(error)),
    };
    let errors: Vec<String> = [
        merge_listing(
            &mut models,
            embeddings,
            previous,
            VerbKind::Embed,
            "embedding",
        ),
        merge_listing(
            &mut models,
            decisions,
            previous,
            VerbKind::Decide,
            "decision",
        ),
    ]
    .into_iter()
    .flatten()
    .collect();
    (models, (!errors.is_empty()).then(|| errors.join("; ")))
}

/// Merge one verb's listing into the chat listing, or, when it failed, the
/// last catalog's offers of that verb, saying so.
fn merge_listing(
    models: &mut Vec<DiscoveredModel>,
    listing: Result<Vec<DiscoveredModel>, String>,
    previous: &[DiscoveredModel],
    verb: VerbKind,
    what: &str,
) -> Option<String> {
    match listing {
        Ok(found) => {
            merge_listed_models(models, found);
            None
        }
        Err(error) => {
            merge_listed_models(models, previous_offers(previous, verb));
            Some(format!(
                "{what} models unavailable: {error}; the last listing's {what} models are kept"
            ))
        }
    }
}

/// The offers of `verb` in a previous catalog, each reduced to the verbs of
/// that kind it served, ready to merge onto a fresh chat listing.
fn previous_offers(previous: &[DiscoveredModel], verb: VerbKind) -> Vec<DiscoveredModel> {
    previous
        .iter()
        .filter_map(|model| {
            let kept: Vec<ServedVerb> = model
                .serves
                .as_ref()?
                .iter()
                .filter(|served| served.verb.kind() == verb)
                .cloned()
                .collect();
            (!kept.is_empty()).then(|| DiscoveredModel {
                serves: Some(kept),
                ..model.clone()
            })
        })
        .collect()
}

/// Merge a verb's listing (embeddings, decisions) into the chat listing by
/// id. A model already listed keeps what it serves and gains the listing's
/// verbs; a new one is added as the listing describes it.
pub(crate) fn merge_listed_models(models: &mut Vec<DiscoveredModel>, listed: Vec<DiscoveredModel>) {
    for entry in listed {
        let Some(model) = models.iter_mut().find(|model| model.id == entry.id) else {
            models.push(entry);
            continue;
        };
        // No claim from chat discovery meant OpenRouter's provider-wide
        // declaration, which is conversation over chat completions.
        let mut serves = model
            .serves
            .take()
            .unwrap_or_else(|| ServedVerb::conversation(Wire::OpenAiChatCompletions));
        serves.extend(entry.serves.unwrap_or_default());
        model.serves = Some(serves);
    }
}

/// The `output_modalities=decisions` listing.
fn discover_decision_models_blocking() -> Result<Vec<DiscoveredModel>, String> {
    let body = listing(
        OPENROUTER_DECISION_MODELS_URL,
        None,
        "decision model catalog",
        Some(DECISION_DISCOVERY_TIMEOUT),
    )?;
    decode_decision_models(&body)
}

/// Each decisions listing entry as a model serving `decide` over System One,
/// which is what OpenRouter's Decisions API speaks for every model it lists
/// there. Prices are the listing's own: input per token, output `0`.
///
/// Only an entry whose outputs name `decisions` is declared, so a listing
/// that ignored its filter cannot turn chat models into decision offers.
pub(crate) fn decode_decision_models(body: &str) -> Result<Vec<DiscoveredModel>, String> {
    let response: ModelsResponse = serde_json::from_str(body)
        .map_err(|error| format!("OpenRouter decision model catalog JSON failed: {error}"))?;
    let decisions = Modality::from(DECISIONS_MODALITY);
    Ok(response
        .data
        .into_iter()
        .map(map_model)
        .filter(|model| {
            model
                .modalities
                .as_ref()
                .is_some_and(|modalities| modalities.outputs(&decisions))
        })
        .map(|mut model| {
            model.serves = Some(vec![ServedVerb {
                verb: Verb::Decide,
                wire: Wire::SystemOne,
            }]);
            model
        })
        .collect())
}

/// What OpenRouter names a decision model's output.
const DECISIONS_MODALITY: &str = "decisions";

/// The public `/api/v1/embeddings/models` listing.
fn discover_embedding_models_blocking() -> Result<Vec<DiscoveredModel>, String> {
    let body = listing(
        OPENROUTER_EMBEDDING_MODELS_URL,
        None,
        "embedding model catalog",
        Some(EMBEDDING_DISCOVERY_TIMEOUT),
    )?;
    decode_embedding_models(&body)
}

/// Each embeddings listing entry as a model serving `embed` over OpenAI's
/// embeddings route.
///
/// The listing names no widths, so width and purpose come from the model's
/// family; a model the family table does not know has an unknown width and is
/// asked only for its native one. Shorter widths are offered only for OpenAI's
/// `text-embedding-3` models, the one family OpenRouter is known to pass
/// `dimensions` through for.
pub(crate) fn decode_embedding_models(body: &str) -> Result<Vec<DiscoveredModel>, String> {
    let response: ModelsResponse = serde_json::from_str(body)
        .map_err(|error| format!("OpenRouter embedding model catalog JSON failed: {error}"))?;
    Ok(response
        .data
        .into_iter()
        .map(|entry| {
            let mut model = map_model(entry);
            let limits = BatchLimits {
                max_texts: Some(OPENROUTER_EMBED_MAX_TEXTS),
                max_chars: None,
                max_input_tokens: model
                    .context_window
                    .and_then(|window| u32::try_from(window).ok()),
            };
            let matryoshka = model.id.starts_with("openai/text-embedding-3");
            model.serves = Some(vec![ServedVerb {
                verb: Verb::Embed(families::capabilities(&model.id, None, matryoshka, limits)),
                wire: Wire::OpenAiEmbeddings,
            }]);
            model
        })
        .collect())
}

/// One GET of a public OpenRouter listing, with a key when there is one.
fn listing(
    url: &str,
    api_key: Option<&str>,
    what: &str,
    timeout: Option<Duration>,
) -> Result<String, String> {
    let mut headers = HeaderMap::new();
    if let Some(api_key) = api_key.filter(|key| !key.trim().is_empty()) {
        let value = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|error| format!("invalid OpenRouter API key header: {error}"))?;
        headers.insert(AUTHORIZATION, value);
    }
    headers.insert(
        "HTTP-Referer",
        HeaderValue::from_static("https://cairn.computer"),
    );
    headers.insert("X-OpenRouter-Title", HeaderValue::from_static("Cairn"));

    let mut client = reqwest::blocking::Client::builder();
    if let Some(timeout) = timeout {
        client = client.timeout(timeout);
    }
    let response = client
        .build()
        .map_err(|error| format!("OpenRouter {what} client failed: {error}"))?
        .get(url)
        .headers(headers)
        .send()
        .map_err(|error| format!("OpenRouter {what} request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("OpenRouter {what} body failed: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "OpenRouter {what} returned HTTP {}: {}",
            status.as_u16(),
            body
        ));
    }
    Ok(body)
}

/// Bounds each request of the image discovery pass.
const IMAGE_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Merge the image listing into the chat listing by id. A model in both
/// (Gemini's image models) keeps what it serves in chat and gains `image`;
/// an image-only model is added as discovery found it.
pub(crate) fn merge_image_models(models: &mut Vec<DiscoveredModel>, images: Vec<DiscoveredModel>) {
    for image in images {
        let Some(model) = models.iter_mut().find(|model| model.id == image.id) else {
            models.push(image);
            continue;
        };
        // No claim from chat discovery meant OpenRouter's provider-wide
        // declaration, which is conversation over chat completions.
        let mut serves = model
            .serves
            .take()
            .unwrap_or_else(|| ServedVerb::conversation(Wire::OpenAiChatCompletions));
        serves.extend(image.serves.unwrap_or_default());
        model.serves = Some(serves);
        if let Some(image_pricing) = image.pricing {
            let pricing = model.pricing.get_or_insert_with(ModelPricing::default);
            for price in image_pricing.prices {
                if !pricing
                    .prices
                    .iter()
                    .any(|known| known.meter == price.meter)
                {
                    pricing.prices.push(price);
                }
            }
            for (key, usd) in image_pricing.unplaced {
                pricing.unplaced.entry(key).or_insert(usd);
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    data: Vec<OpenRouterModel>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterModel {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    context_length: Option<i64>,
    #[serde(default)]
    top_provider: Option<TopProvider>,
    #[serde(default)]
    architecture: Option<ListedArchitecture>,
    /// Every price key the provider sent, so a key Cairn cannot place is kept
    /// rather than dropped by a fixed struct.
    #[serde(default)]
    pricing: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    supported_parameters: Vec<String>,
    #[serde(default)]
    reasoning: Option<OpenRouterReasoning>,
    #[serde(default)]
    canonical_slug: Option<String>,
    /// The Hugging Face repository the weights come from (empty or absent for
    /// closed models). Selects a pinned tokenizer by exact identity.
    #[serde(default)]
    hugging_face_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopProvider {
    #[serde(default)]
    context_length: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterReasoning {
    #[serde(default)]
    supported_efforts: Vec<String>,
    #[serde(default)]
    default_effort: Option<String>,
}

pub fn discover_models_blocking(api_key: Option<&str>) -> Result<Vec<DiscoveredModel>, String> {
    let body = listing(OPENROUTER_MODELS_URL, api_key, "model catalog", None)?;
    decode_models(&body)
}

pub(crate) fn decode_models(body: &str) -> Result<Vec<DiscoveredModel>, String> {
    let response: ModelsResponse = serde_json::from_str(body)
        .map_err(|error| format!("OpenRouter model catalog JSON failed: {error}"))?;
    Ok(response.data.into_iter().map(map_model).collect())
}

fn map_model(model: OpenRouterModel) -> DiscoveredModel {
    let context_window = model.context_length.or_else(|| {
        model
            .top_provider
            .and_then(|provider| provider.context_length)
    });
    let reasoning = model.reasoning;
    let (modalities, serves) = capabilities(model.architecture);
    let tokenizer = tokenizer_for("openrouter", &model.id, model.hugging_face_id.as_deref());
    DiscoveredModel {
        id: model.id.clone(),
        model: model.id.clone(),
        display_name: model.name.unwrap_or_else(|| model.id.clone()),
        description: model.description,
        hidden: false,
        is_default: model.id == "openrouter/auto",
        default_reasoning_effort: reasoning.as_ref().and_then(|r| r.default_effort.clone()),
        supported_reasoning_efforts: reasoning
            .map(|r| {
                r.supported_efforts
                    .into_iter()
                    .map(|effort| DiscoveredReasoningEffort {
                        reasoning_effort: effort,
                        description: None,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        context_window,
        canonical_slug: model.canonical_slug,
        serving_account_ids: Vec::new(),
        pricing: model.pricing.map(pricing),
        supported_parameters: model.supported_parameters,
        router: model.id == "openrouter/auto",
        modalities,
        serves,
        tokenizer,
    }
}

/// What a model takes and gives, and therefore what it serves over chat. A
/// model that gives text serves Complete and Session there. One that gives no
/// text (an image or embeddings model) serves nothing over chat, which is said
/// rather than left to the provider-wide default; an embedding model gains
/// `embed` from the embeddings listing.
fn capabilities(
    architecture: Option<ListedArchitecture>,
) -> (Option<Modalities>, Option<Vec<ServedVerb>>) {
    let Some(architecture) = architecture else {
        return (None, None);
    };
    let serves = architecture.output_modalities.as_ref().map(|output| {
        if output.contains(&Modality::Text) {
            ServedVerb::conversation(Wire::OpenAiChatCompletions)
        } else {
            Vec::new()
        }
    });
    (architecture.modalities(), serves)
}

/// The meter each OpenRouter price key is charged on. Every price is USD per
/// one unit.
///
/// The docs name `image` and `audio` only as input costs. The live catalog
/// settles the unit: every model priced on `image` is a Gemini model whose
/// `image` price equals its `prompt` price, and `audio` matches Google's and
/// OpenAI's per-million-token audio rates, so both are per token. `image_output`
/// is per token the same way (an image model's $30 per million). A key not
/// listed here is not guessed at.
fn openrouter_meter(key: &str) -> Option<Meter> {
    use BillingUnit::{Requests, Tokens};
    let (charge, modality, unit) = match key {
        "prompt" => (Charge::Input, None, Tokens),
        "completion" => (Charge::Output, None, Tokens),
        "input_cache_read" => (Charge::CacheRead, None, Tokens),
        "input_cache_write" => (Charge::CacheWrite, None, Tokens),
        "internal_reasoning" => (Charge::Reasoning, None, Tokens),
        "request" => (Charge::Request, None, Requests),
        "web_search" => (Charge::WebSearch, None, Requests),
        "image" => (Charge::Input, Some(Modality::Image), Tokens),
        "image_output" => (Charge::Output, Some(Modality::Image), Tokens),
        "audio" => (Charge::Input, Some(Modality::Audio), Tokens),
        "audio_output" => (Charge::Output, Some(Modality::Audio), Tokens),
        _ => return None,
    };
    Some(Meter {
        charge,
        modality,
        unit,
    })
}

fn pricing(raw: BTreeMap<String, Value>) -> ModelPricing {
    let mut pricing = ModelPricing::default();
    for (key, value) in raw {
        // A non-string value (`overrides`, prices tiered by prompt size) is a
        // structure rather than a price on one meter.
        let Value::String(usd) = value else {
            continue;
        };
        match openrouter_meter(&key) {
            Some(meter) => pricing.prices.push(Price { meter, usd }),
            None => {
                pricing.unplaced.insert(key, usd);
            }
        }
    }
    pricing
}

#[cfg(test)]
mod tests {
    use super::{assemble, decode_decision_models, decode_embedding_models, decode_models};
    use cairn_models::types::catalog::{
        DiscoveredModel, Modality, ServedVerb, Verb, VerbKind, Wire,
    };
    use cairn_models::types::embedding::EmbeddingWidths;
    use cairn_models::types::usage::{BillingUnit, Charge};

    /// A slice of the live `/api/v1/embeddings/models` listing (2026-10-17).
    const EMBEDDINGS: &str = include_str!("testdata/embeddings_models.json");

    /// A slice of the live `/api/v1/models?output_modalities=decisions`
    /// listing (2026-10-20), verbatim.
    const DECISIONS: &str = include_str!("testdata/decisions_models.json");

    fn embed_capabilities(
        model: &DiscoveredModel,
    ) -> &cairn_models::types::embedding::EmbeddingCapabilities {
        model
            .serves
            .as_ref()
            .unwrap()
            .iter()
            .find_map(|served| match &served.verb {
                Verb::Embed(capabilities) if served.wire == Wire::OpenAiEmbeddings => {
                    Some(capabilities)
                }
                _ => None,
            })
            .expect("serves embed over the embeddings route")
    }

    #[test]
    fn the_embeddings_listing_merges_into_the_chat_listing_by_id() {
        let chat = decode_models(LIVE).unwrap();
        let chat_count = chat.len();
        let embeddings = decode_embedding_models(EMBEDDINGS).unwrap();
        let (models, error) = assemble(Ok(chat), Ok(embeddings), Ok(Vec::new()), &[]);
        assert_eq!(error, None);
        // voyage-code-4 is in both listings: merged, not duplicated.
        assert_eq!(models.len(), chat_count + 3);
        let voyage = model(&models, "voyageai/voyage-code-4");
        assert_eq!(voyage.serves.as_ref().unwrap().len(), 1);
        assert_eq!(embed_capabilities(voyage).native_dims, None);

        let small = model(&models, "openai/text-embedding-3-small");
        let capabilities = embed_capabilities(small);
        assert_eq!(capabilities.native_dims, Some(1536));
        assert_eq!(capabilities.widths, EmbeddingWidths::UpTo { min: 1 });
        assert_eq!(capabilities.limits.max_texts, Some(128));
        assert_eq!(capabilities.limits.max_input_tokens, Some(8192));
        assert_eq!(
            small.modalities.as_ref().unwrap().output,
            vec![Modality::Embeddings]
        );
        assert_eq!(
            small
                .pricing
                .as_ref()
                .unwrap()
                .usd_per(Charge::Input, None, BillingUnit::Tokens),
            Some("0.00000002")
        );

        // OpenRouter is not known to pass `dimensions` through for Qwen: the
        // family's width, native only.
        let qwen = embed_capabilities(model(&models, "qwen/qwen3-embedding-4b"));
        assert_eq!(qwen.native_dims, Some(2560));
        assert_eq!(qwen.widths, EmbeddingWidths::NativeOnly);

        // A model the family table does not know: width unknown, native only.
        let liquid = embed_capabilities(model(&models, "liquid/lfm-2.5-embedding-350m:free"));
        assert_eq!(liquid.native_dims, None);
        assert_eq!(liquid.validate(), Ok(()));

        // Chat models are untouched.
        assert_eq!(
            model(&models, "anthropic/claude-haiku-5.5").serves,
            Some(ServedVerb::conversation(Wire::OpenAiChatCompletions))
        );
    }

    #[test]
    fn a_failed_embeddings_listing_keeps_fresh_chat_models_and_the_last_embedding_offers() {
        let previous = previous_catalog();
        let chat = decode_models(LIVE).unwrap();
        let (models, error) = assemble(
            Ok(chat),
            Err("HTTP 503".into()),
            decode_decision_models(DECISIONS),
            &previous,
        );
        assert_eq!(
            error.as_deref(),
            Some("embedding models unavailable: HTTP 503; the last listing's embedding models are kept")
        );
        assert_eq!(models.len(), previous.len());
        // An embedding-only model the chat listing never names is kept.
        assert_eq!(
            embed_capabilities(model(&models, "qwen/qwen3-embedding-4b")).native_dims,
            Some(2560)
        );
        // A model in both listings keeps its embed verb, once.
        let voyage = model(&models, "voyageai/voyage-code-4");
        assert_eq!(voyage.serves.as_ref().unwrap().len(), 1);
        embed_capabilities(voyage);
        // Chat models are the fresh listing's.
        assert_eq!(
            model(&models, "anthropic/claude-haiku-5.5").serves,
            Some(ServedVerb::conversation(Wire::OpenAiChatCompletions))
        );
    }

    #[test]
    fn a_failed_chat_listing_answers_nothing_so_the_last_catalog_stands() {
        // Embedding models alone would replace the chat catalog the refresh
        // keeps; an empty answer with the error keeps it whole instead.
        let (models, error) = assemble(
            Err("chat down".into()),
            decode_embedding_models(EMBEDDINGS),
            decode_decision_models(DECISIONS),
            &previous_catalog(),
        );
        assert!(models.is_empty());
        assert_eq!(error.as_deref(), Some("chat down"));
    }

    /// The catalog a previous refresh kept: every listing, merged.
    fn previous_catalog() -> Vec<DiscoveredModel> {
        let (models, error) = assemble(
            decode_models(LIVE),
            decode_embedding_models(EMBEDDINGS),
            decode_decision_models(DECISIONS),
            &[],
        );
        assert_eq!(error, None);
        models
    }

    fn kinds(models: &[DiscoveredModel], id: &str) -> Vec<(VerbKind, Wire)> {
        model(models, id)
            .serves
            .as_ref()
            .unwrap()
            .iter()
            .map(|served| (served.verb.kind(), served.wire))
            .collect()
    }

    /// Every decision model serves `decide` over System One and nothing
    /// else, priced as listed: input per token, output free. TypeSafe's
    /// chat-listed router stays a chat model.
    #[test]
    fn the_decisions_listing_serves_decide_over_system_one_at_its_listed_prices() {
        let chat = decode_models(LIVE).unwrap();
        let chat_count = chat.len();
        let (models, error) = assemble(
            Ok(chat),
            Ok(Vec::new()),
            decode_decision_models(DECISIONS),
            &[],
        );
        assert_eq!(error, None);
        assert_eq!(models.len(), chat_count + 5);
        for (id, input) in [
            ("typesafe/jev-1.14", "0.000000042"),
            ("~typesafe/jev-latest", "0.000000042"),
            ("openai/gpt-6-luna-decisions", "0.0000001"),
        ] {
            assert_eq!(
                kinds(&models, id),
                vec![(VerbKind::Decide, Wire::SystemOne)],
                "{id}"
            );
            let found = model(&models, id);
            let pricing = found.pricing.as_ref().unwrap();
            assert_eq!(
                pricing.usd_per(Charge::Input, None, BillingUnit::Tokens),
                Some(input),
                "{id}"
            );
            assert_eq!(
                pricing.usd_per(Charge::Output, None, BillingUnit::Tokens),
                Some("0"),
                "{id}"
            );
            let modalities = found.modalities.as_ref().unwrap();
            assert_eq!(modalities.input, vec![Modality::Text, Modality::Image]);
            assert_eq!(modalities.output, vec![Modality::from("decisions")]);
        }
        assert_eq!(
            model(&models, "typesafe/jev-1.14").display_name,
            "TypeSafe: Jev 1.14"
        );
        assert_eq!(
            kinds(&models, "typesafe/jev-router"),
            vec![
                (VerbKind::Complete, Wire::OpenAiChatCompletions),
                (VerbKind::Session, Wire::OpenAiChatCompletions)
            ]
        );
    }

    #[test]
    fn a_failed_decisions_listing_keeps_fresh_chat_models_and_the_last_decision_offers() {
        let previous = previous_catalog();
        let (models, error) = assemble(
            decode_models(LIVE),
            decode_embedding_models(EMBEDDINGS),
            Err("HTTP 502".into()),
            &previous,
        );
        assert_eq!(
            error.as_deref(),
            Some("decision models unavailable: HTTP 502; the last listing's decision models are kept")
        );
        assert_eq!(models.len(), previous.len());
        assert_eq!(
            kinds(&models, "typesafe/jev-1.14"),
            vec![(VerbKind::Decide, Wire::SystemOne)]
        );
        // Both failing are both reported.
        let (_, error) = assemble(
            decode_models(LIVE),
            Err("HTTP 503".into()),
            Err("HTTP 502".into()),
            &previous,
        );
        let error = error.unwrap();
        assert!(
            error.starts_with("embedding models unavailable: HTTP 503"),
            "{error}"
        );
        assert!(
            error.contains("; decision models unavailable: HTTP 502"),
            "{error}"
        );
    }

    /// A listing that ignored its filter would hand back chat models; none of
    /// them is declared a decision model.
    #[test]
    fn only_entries_that_output_decisions_are_declared_decision_models() {
        assert!(decode_decision_models(LIVE).unwrap().is_empty());
    }

    /// A slice of the live `/api/v1/models` listing (2026-10-15), descriptions
    /// trimmed, plus OpenRouter's own documented example entry (`openai/gpt-4`),
    /// the only place `request` is priced. The pure image and embeddings entries
    /// come from the `output_modalities=image` and `=embeddings` listings, which
    /// the default listing Cairn reads leaves out.
    const LIVE: &str = include_str!("testdata/models.json");

    fn model<'a>(models: &'a [DiscoveredModel], id: &str) -> &'a DiscoveredModel {
        models
            .iter()
            .find(|model| model.id == id)
            .unwrap_or_else(|| panic!("{id} is in the fixture"))
    }

    #[test]
    fn the_hugging_face_id_selects_a_pinned_tokenizer_exactly() {
        let models = decode_models(
            r#"{"data":[
                {"id":"z-ai/glm-5.3","hugging_face_id":"zai-org/GLM-5.3"},
                {"id":"minimax/minimax-m3","hugging_face_id":"MiniMaxAI/Minimax-M3"},
                {"id":"z-ai/glm-5-turbo","hugging_face_id":""},
                {"id":"anthropic/claude-sonnet-4.5","hugging_face_id":null},
                {"id":"z-ai/glm-5.3-prime"}
            ]}"#,
        )
        .unwrap();
        let family = |id: &str| {
            model(&models, id)
                .tokenizer
                .as_ref()
                .map(|tokenizer| tokenizer.family.to_string())
        };
        assert_eq!(family("z-ai/glm-5.3").as_deref(), Some("glm-5"));
        assert_eq!(family("minimax/minimax-m3").as_deref(), Some("minimax-m3"));
        // No upstream id, no tokenizer: the slug's resemblance is not evidence.
        assert_eq!(family("z-ai/glm-5-turbo"), None);
        assert_eq!(family("anthropic/claude-sonnet-4.5"), None);
        assert_eq!(family("z-ai/glm-5.3-prime"), None);
    }

    #[test]
    fn decodes_openrouter_catalog_fields() {
        let body = r#"{
          "data": [{
            "id": "anthropic/claude-sonnet-4.5",
            "name": "Claude Sonnet 4.5",
            "description": "coding model",
            "context_length": 200000,
            "canonical_slug": "anthropic/claude-sonnet-4.5",
            "supported_parameters": ["tools", "reasoning"],
            "pricing": { "prompt": "0.000003", "completion": "0.000015", "input_cache_read": "0.0000003" },
            "reasoning": { "supported_efforts": ["low", "medium", "high"], "default_effort": "medium" }
          }]
        }"#;
        let models = decode_models(body).unwrap();
        let model = &models[0];
        assert_eq!(model.id, "anthropic/claude-sonnet-4.5");
        assert_eq!(model.display_name, "Claude Sonnet 4.5");
        assert_eq!(model.context_window, Some(200000));
        assert_eq!(model.default_reasoning_effort.as_deref(), Some("medium"));
        assert_eq!(model.supported_reasoning_efforts.len(), 3);
        assert_eq!(
            model
                .pricing
                .as_ref()
                .unwrap()
                .usd_per(Charge::Input, None, BillingUnit::Tokens),
            Some("0.000003")
        );
        assert!(model.supported_parameters.contains(&"tools".to_string()));
        // No architecture: no claim, so the provider-wide declaration applies.
        assert_eq!(model.modalities, None);
        assert_eq!(model.serves, None);
    }

    #[test]
    fn modalities_are_typed_and_an_unknown_one_is_kept_verbatim() {
        let models = decode_models(LIVE).unwrap();
        let haiku = model(&models, "anthropic/claude-haiku-5.5");
        let modalities = haiku.modalities.as_ref().unwrap();
        assert_eq!(
            modalities.input,
            vec![Modality::Text, Modality::Image, Modality::File]
        );
        assert_eq!(modalities.output, vec![Modality::Text]);
        let voyage = model(&models, "voyageai/voyage-code-4");
        assert_eq!(
            voyage.modalities.as_ref().unwrap().output,
            vec![Modality::Embeddings]
        );
    }

    #[test]
    fn a_model_that_gives_text_serves_conversation_over_chat_and_one_that_does_not_serves_nothing()
    {
        let models = decode_models(LIVE).unwrap();
        let conversation = Some(ServedVerb::conversation(Wire::OpenAiChatCompletions));
        // An image model that also writes text is a chat model that can draw.
        let gemini = model(&models, "google/gemini-3-pro-image");
        assert_eq!(
            gemini.modalities.as_ref().unwrap().output,
            vec![Modality::Image, Modality::Text]
        );
        assert_eq!(gemini.serves, conversation);
        assert_eq!(model(&models, "openai/gpt-audio").serves, conversation);
        // A pure image model and an embeddings model serve no verb Cairn has.
        for id in [
            "black-forest-labs/flux-3-image-klein",
            "voyageai/voyage-code-4",
        ] {
            let served = model(&models, id).serves.as_ref().unwrap();
            assert!(served.is_empty(), "{id} serves {served:?}");
            assert!(!served.iter().any(|s| s.verb.kind() == VerbKind::Complete));
        }
    }

    #[test]
    fn every_price_key_is_placed_on_its_meter_or_kept_unplaced() {
        let models = decode_models(LIVE).unwrap();
        let gemini = model(&models, "google/gemini-3-pro-image")
            .pricing
            .as_ref()
            .unwrap();
        let tokens = BillingUnit::Tokens;
        let image = Some(&Modality::Image);
        let audio = Some(&Modality::Audio);
        assert_eq!(
            gemini.usd_per(Charge::Input, None, tokens),
            Some("0.000002")
        );
        assert_eq!(
            gemini.usd_per(Charge::Output, None, tokens),
            Some("0.000012")
        );
        assert_eq!(
            gemini.usd_per(Charge::Input, image, tokens),
            Some("0.000002")
        );
        // Output image priced per token: $120 per million.
        assert_eq!(
            gemini.usd_per(Charge::Output, image, tokens),
            Some("0.00012")
        );
        assert_eq!(
            gemini.usd_per(Charge::Input, audio, tokens),
            Some("0.000002")
        );
        assert_eq!(
            gemini.usd_per(Charge::Reasoning, None, tokens),
            Some("0.000012")
        );
        assert_eq!(
            gemini.usd_per(Charge::CacheRead, None, tokens),
            Some("0.0000002")
        );
        assert_eq!(
            gemini.usd_per(Charge::CacheWrite, None, tokens),
            Some("0.000000375")
        );
        assert_eq!(
            gemini.usd_per(Charge::WebSearch, None, BillingUnit::Requests),
            Some("0.014")
        );
        assert_eq!(
            gemini.unplaced.get("input_audio_cache").map(String::as_str),
            Some("0.0000002")
        );

        let audio_model = model(&models, "openai/gpt-audio").pricing.as_ref().unwrap();
        assert_eq!(
            audio_model.usd_per(Charge::Output, audio, tokens),
            Some("0.000064")
        );

        let flux = model(&models, "black-forest-labs/flux-3-image-klein")
            .pricing
            .as_ref()
            .unwrap();
        assert!(flux.unplaced.contains_key("image_token"));

        // `overrides` is a list, not a price, so it is skipped rather than
        // stringified; the one-hour cache write has no meter.
        let haiku = model(&models, "anthropic/claude-haiku-5.5")
            .pricing
            .as_ref()
            .unwrap();
        assert!(!haiku.unplaced.contains_key("overrides"));
        assert!(haiku.unplaced.contains_key("input_cache_write_1h"));

        let gpt4 = model(&models, "openai/gpt-4").pricing.as_ref().unwrap();
        assert_eq!(
            gpt4.usd_per(Charge::Request, None, BillingUnit::Requests),
            Some("0")
        );
    }

    const IMAGE_MODELS: &str =
        include_str!("../../../../cairn-image-wire/testdata/openrouter/image_models.json");
    const IMAGE_ENDPOINTS: &str =
        include_str!("../../../../cairn-image-wire/testdata/openrouter/endpoints.json");

    /// The image listing fixture as discovery maps it.
    fn image_models() -> Vec<DiscoveredModel> {
        use cairn_models::image_wire::openrouter::discovery::{
            read_image_models, read_model_endpoints,
        };
        let endpoints: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(IMAGE_ENDPOINTS).unwrap();
        read_image_models(IMAGE_MODELS)
            .unwrap()
            .data
            .iter()
            .map(|listed| {
                let record = read_model_endpoints(&endpoints[&listed.id].to_string()).unwrap();
                cairn_models::image::openrouter::discovered_model(listed, Ok(&record))
            })
            .collect()
    }

    #[test]
    fn a_dual_listed_model_keeps_its_chat_verbs_and_an_image_only_model_serves_image() {
        let mut models = decode_models(LIVE).unwrap();
        let chat_count = models.len();
        let mut images = image_models();
        // The chat fixture's pure image model, as the image listing reports it.
        let mut klein = images
            .iter()
            .find(|model| model.id == "black-forest-labs/flux.2-klein-4b")
            .unwrap()
            .clone();
        klein.id = "black-forest-labs/flux-3-image-klein".into();
        klein.model = klein.id.clone();
        images.push(klein);
        let image_only = images
            .iter()
            .filter(|image| !models.iter().any(|model| model.id == image.id))
            .count();
        super::merge_image_models(&mut models, images);
        assert_eq!(models.len(), chat_count + image_only);

        let kinds = |id: &str| -> Vec<VerbKind> {
            model(&models, id)
                .serves
                .as_ref()
                .unwrap()
                .iter()
                .map(|served| served.verb.kind())
                .collect()
        };
        assert_eq!(
            kinds("google/gemini-3-pro-image"),
            vec![VerbKind::Complete, VerbKind::Session, VerbKind::Image]
        );
        let gemini = model(&models, "google/gemini-3-pro-image");
        let image_verb = gemini.serves.as_ref().unwrap().last().unwrap();
        assert_eq!(image_verb.wire, Wire::OpenRouterImages);
        // Its chat price stays; nothing the image listing adds displaces it.
        assert_eq!(
            gemini.pricing.as_ref().unwrap().usd_per(
                Charge::Output,
                Some(&Modality::Image),
                BillingUnit::Tokens
            ),
            Some("0.00012")
        );
        // The pure image model that served nothing in chat serves image.
        assert_eq!(
            kinds("black-forest-labs/flux-3-image-klein"),
            vec![VerbKind::Image]
        );
        assert_eq!(kinds("black-forest-labs/flux.2-pro"), vec![VerbKind::Image]);
        // Embeddings still serve nothing.
        assert!(kinds("voyageai/voyage-code-4").is_empty());
    }

    /// Reads the live decisions listing through discovery and, when
    /// `OPENROUTER_API_KEY` is set, asks `typesafe/jev-1.14` one predicate, one
    /// choice and one score question over System One through the facet's real
    /// `HttpComplete`, printing the typed answers and what the reply says it
    /// cost. Run with `--ignored --nocapture`.
    #[test]
    #[ignore = "reaches openrouter.ai, and spends from the key when one is set"]
    fn live_decision_models_and_one_decision() {
        use crate::models::decision::{DecisionOption, DecisionQuestion, QuestionKind, ScoreLevel};
        use cairn_models::interface::decide::{DecisionCall, DecisionRoute};
        use cairn_models::types::catalog::DiscoveredModel;
        use cairn_models::wire::decisions::http::DecisionsEndpoint;

        let models: Vec<DiscoveredModel> = super::discover_decision_models_blocking().unwrap();
        println!("{} decision models", models.len());
        for model in &models {
            let pricing = model.pricing.as_ref();
            println!(
                "  {}: in {:?}, out {:?}, {}",
                model.id,
                pricing.and_then(|p| p.usd_per(Charge::Input, None, BillingUnit::Tokens)),
                pricing.and_then(|p| p.usd_per(Charge::Output, None, BillingUnit::Tokens)),
                model
                    .modalities
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            );
        }
        for id in [
            "typesafe/jev-1.14",
            "~typesafe/jev-latest",
            "openai/gpt-6-luna-decisions",
        ] {
            assert!(models.iter().any(|model| model.id == id), "{id} is listed");
        }

        let Ok(api_key) = std::env::var("OPENROUTER_API_KEY") else {
            println!("OPENROUTER_API_KEY unset: no decision asked");
            return;
        };
        let route = DecisionRoute::SystemOne(DecisionsEndpoint {
            provider_name: "OpenRouter".into(),
            backend_key: "openrouter".into(),
            url: format!("{}/decisions", super::super::OPENROUTER_DECISIONS_BASE),
            headers: super::super::http::openrouter_headers(&api_key),
            context_limits: None,
        });
        let option = |value: &str, description: &str| DecisionOption {
            value: value.into(),
            description: Some(description.into()),
        };
        let questions = vec![
            DecisionQuestion {
                name: "is_bug".into(),
                instructions: "Is the customer reporting a software defect?".into(),
                kind: QuestionKind::Predicate,
            },
            DecisionQuestion {
                name: "team".into(),
                instructions: "Which team should own this ticket?".into(),
                kind: QuestionKind::Choice {
                    options: vec![
                        option("account", "Login, permissions, or profile issues."),
                        option(
                            "frontend",
                            "Rendering, layout, or browser compatibility issues.",
                        ),
                        option(
                            "payments",
                            "Checkout, billing, or payment processing issues.",
                        ),
                    ],
                },
            },
            DecisionQuestion {
                name: "urgency".into(),
                instructions: "How urgent is this ticket?".into(),
                kind: QuestionKind::Score {
                    levels: [
                        "Can wait for the next release",
                        "Should be fixed this week",
                        "Blocking revenue right now",
                    ]
                    .into_iter()
                    .map(|label| ScoreLevel {
                        label: label.into(),
                        description: None,
                    })
                    .collect(),
                },
            },
        ];
        let decision = cairn_models::decide::decide(
            &cairn_models::complete::HttpComplete,
            DecisionCall {
                route: &route,
                model: "typesafe/jev-1.14",
                input: "My checkout page shows a blank screen after I click Pay. I have tried two browsers. We are an enterprise customer.",
                questions: &questions,
                timeout: std::time::Duration::from_secs(30),
            },
        )
        .unwrap();
        println!("{}", serde_json::to_string_pretty(&decision).unwrap());
        assert!(decision
            .answers
            .iter()
            .all(|answer| answer.pick().is_some() && answer.confidence().is_some()));
    }

    /// Reads the real catalog through discovery. Run it with `--ignored` to
    /// see what the live listing holds right now.
    #[test]
    #[ignore = "reaches openrouter.ai"]
    fn live_catalog_inventory() {
        use std::collections::BTreeSet;
        let (models, error) = super::discover_catalog_blocking(&[]);
        println!("{} models; error: {error:?}", models.len());
        let embedders: Vec<_> = models
            .iter()
            .filter_map(|model| {
                model
                    .serves
                    .as_ref()?
                    .iter()
                    .find_map(|served| match &served.verb {
                        Verb::Embed(capabilities) => Some((model.id.as_str(), capabilities)),
                        _ => None,
                    })
            })
            .collect();
        let known = embedders
            .iter()
            .filter(|(_, capabilities)| capabilities.native_dims.is_some())
            .count();
        println!(
            "{} embedding models, {known} with a known width",
            embedders.len()
        );
        for (id, capabilities) in &embedders {
            println!("  {id}: {}", capabilities.summary());
        }
        let mut other = BTreeSet::new();
        let mut unplaced = BTreeSet::new();
        for model in &models {
            if let Some(modalities) = &model.modalities {
                for modality in modalities.input.iter().chain(&modalities.output) {
                    if let Modality::Other(value) = modality {
                        other.insert(value.clone());
                    }
                }
                if modalities.outputs(&Modality::Image) {
                    println!("image output: {} serves {:?}", model.id, model.serves);
                }
            }
            if let Some(pricing) = &model.pricing {
                unplaced.extend(pricing.unplaced.keys().cloned());
            }
        }
        println!("modalities kept as Other: {other:?}");
        println!("unplaced price keys: {unplaced:?}");
    }
}
