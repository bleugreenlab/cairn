use crate::backends::{DiscoveredModel, DiscoveredModelPricing, DiscoveredReasoningEffort};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::Deserialize;

const OPENROUTER_MODELS_URL: &str = "https://openrouter.ai/api/v1/models";

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
    pricing: Option<OpenRouterPricing>,
    #[serde(default)]
    supported_parameters: Vec<String>,
    #[serde(default)]
    reasoning: Option<OpenRouterReasoning>,
    #[serde(default)]
    canonical_slug: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopProvider {
    #[serde(default)]
    context_length: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterPricing {
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    completion: Option<String>,
    #[serde(default)]
    request: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    web_search: Option<String>,
    #[serde(default)]
    internal_reasoning: Option<String>,
    #[serde(default)]
    input_cache_read: Option<String>,
    #[serde(default)]
    input_cache_write: Option<String>,
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
        pricing: model.pricing.map(|pricing| DiscoveredModelPricing {
            prompt: pricing.prompt,
            completion: pricing.completion,
            request: pricing.request,
            image: pricing.image,
            web_search: pricing.web_search,
            internal_reasoning: pricing.internal_reasoning,
            input_cache_read: pricing.input_cache_read,
            input_cache_write: pricing.input_cache_write,
        }),
        supported_parameters: model.supported_parameters,
        router: model.id == "openrouter/auto",
        architecture_modality: None,
    }
}

#[cfg(test)]
mod tests {
    use super::decode_models;

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
            model.pricing.as_ref().unwrap().prompt.as_deref(),
            Some("0.000003")
        );
        assert!(model.supported_parameters.contains(&"tools".to_string()));
    }
}
