use std::collections::BTreeMap;

use cairn_models::types::catalog::{
    DiscoveredModel, DiscoveredReasoningEffort, Modalities, Modality, ModelPricing, Price,
    ServedVerb, Wire,
};
use cairn_models::types::tokenizers::tokenizer_for;
use cairn_models::types::usage::{BillingUnit, Charge, Meter};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::Deserialize;
use serde_json::Value;

const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";

/// Bounds each request of the image discovery pass.
const IMAGE_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Every model OpenRouter serves: the chat listing, with the image listing
/// merged in, and what went wrong with either.
///
/// The two listings are read side by side. A failed image listing keeps the
/// chat models and says `image models unavailable: ...`; it is never silent.
pub(crate) fn discover_catalog_blocking() -> (Vec<DiscoveredModel>, Option<String>) {
    let (chat, images) = std::thread::scope(|scope| {
        let images =
            scope.spawn(|| cairn_models::image::openrouter::discover(IMAGE_DISCOVERY_TIMEOUT));
        let chat = discover_models_blocking(None);
        let images = images
            .join()
            .unwrap_or_else(|_| Err("image discovery panicked".to_string()));
        (chat, images)
    });
    let mut errors = Vec::new();
    let mut models = chat.unwrap_or_else(|error| {
        errors.push(error);
        Vec::new()
    });
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
    architecture: Option<Architecture>,
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
struct Architecture {
    #[serde(default)]
    input_modalities: Option<Vec<String>>,
    #[serde(default)]
    output_modalities: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterReasoning {
    #[serde(default)]
    supported_efforts: Vec<String>,
    #[serde(default)]
    default_effort: Option<String>,
}

pub fn discover_models_blocking(api_key: Option<&str>) -> Result<Vec<DiscoveredModel>, String> {
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

    let response = reqwest::blocking::Client::new()
        .get(OPENROUTER_MODELS_URL)
        .headers(headers)
        .send()
        .map_err(|error| format!("OpenRouter model catalog request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|error| format!("OpenRouter model catalog body failed: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "OpenRouter model catalog returned HTTP {}: {}",
            status.as_u16(),
            body
        ));
    }
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

/// What a model takes and gives, and therefore what it serves. Every model
/// OpenRouter routes is reached over chat completions, so a model that gives
/// text serves Complete and Session there. One that gives no text (an image or
/// embeddings model) serves nothing Cairn has, which is said rather than left
/// to the provider-wide default.
fn capabilities(
    architecture: Option<Architecture>,
) -> (Option<Modalities>, Option<Vec<ServedVerb>>) {
    let Some(architecture) = architecture else {
        return (None, None);
    };
    let side = |values: Option<Vec<String>>| -> Option<Vec<Modality>> {
        values.map(|values| values.into_iter().map(Modality::from).collect())
    };
    let input = side(architecture.input_modalities);
    let output = side(architecture.output_modalities);
    let serves = output.as_ref().map(|output| {
        if output.contains(&Modality::Text) {
            ServedVerb::conversation(Wire::OpenAiChatCompletions)
        } else {
            Vec::new()
        }
    });
    let modalities = (input.is_some() || output.is_some()).then(|| Modalities {
        input: input.unwrap_or_default(),
        output: output.unwrap_or_default(),
    });
    (modalities, serves)
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
    use super::decode_models;
    use cairn_models::types::catalog::{DiscoveredModel, Modality, ServedVerb, VerbKind, Wire};
    use cairn_models::types::usage::{BillingUnit, Charge};

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
            vec![Modality::Other("embeddings".into())]
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

    /// Reads the real catalog through discovery. Run it with `--ignored` to
    /// see what the live listing holds right now.
    #[test]
    #[ignore = "reaches openrouter.ai"]
    fn live_catalog_inventory() {
        use std::collections::BTreeSet;
        let models = super::discover_models_blocking(None).unwrap();
        println!("{} models", models.len());
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
