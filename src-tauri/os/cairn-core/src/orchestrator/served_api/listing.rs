//! The served API's `GET /v1/models`, in OpenRouter's shape.
//!
//! Every model is listed once, under its real name (tenet 1336), with no alias
//! entries. Pure: what Cairn's deployments serve, the catalog, and what the
//! client may use are handed in.

use std::collections::BTreeSet;

use cairn_inference::served::{self, ServedModel};
use cairn_models::interface::Catalog;
use cairn_models::types::catalog::{Modalities, Modality};
use serde_json::{json, Value};

use super::{relays_to, LOCAL_PROVIDER};

/// The listing one client sees. Every entry carries
/// `architecture.{input_modalities, output_modalities}`.
///
/// 1. Each model Cairn's deployments run, loaded or not (asking loads one),
///    under the name of the model its engine loads, with its context clamped
///    to the engine's measured KV cache.
/// 2. The models of each remote provider the client may use, under the
///    provider's ids, except one a local deployment runs: a request for it
///    goes local, so listing it as the provider's would say otherwise.
///
/// An id is listed once; the first entry for it wins.
pub fn compose(served: &[ServedModel], catalog: &Catalog, may_use: impl Fn(&str) -> bool) -> Value {
    let mut entries: Vec<Value> = Vec::new();
    let mut listed: BTreeSet<String> = BTreeSet::new();
    let mut push = |entry: Value, entries: &mut Vec<Value>| {
        let id = entry["id"].as_str().unwrap_or_default().to_string();
        if !id.is_empty() && listed.insert(id) {
            entries.push(entry);
        }
    };

    let local = served::listing(served);
    for mut entry in local["data"].as_array().cloned().unwrap_or_default() {
        if let Some(object) = entry.as_object_mut() {
            object.remove("root");
        }
        push(entry, &mut entries);
    }

    let run_locally: BTreeSet<&str> = served
        .iter()
        .filter_map(|model| model.upstream_id.as_deref())
        .collect();
    for provider in catalog.providers.iter().filter(|provider| {
        let key = provider.descriptor.key.as_ref();
        provider.installed && key != LOCAL_PROVIDER && relays_to(key) && may_use(key)
    }) {
        let key = provider.descriptor.key.as_ref();
        let models = provider
            .models
            .as_ref()
            .map(|catalog| catalog.models.as_slice())
            .unwrap_or_default();
        for model in models.iter().filter(|model| {
            !model.hidden
                && !run_locally.contains(model.model.as_str())
                && !model
                    .canonical_slug
                    .as_deref()
                    .is_some_and(|slug| run_locally.contains(slug))
        }) {
            let modalities = model.modalities.clone().unwrap_or_else(text_to_text);
            let mut entry = json!({
                "id": model.model,
                "object": "model",
                "created": 0,
                "owned_by": key,
                "architecture": {
                    "input_modalities": modalities.input,
                    "output_modalities": modalities.output,
                },
            });
            if let Some(context) = model.context_window {
                entry["context_length"] = json!(context);
            }
            push(entry, &mut entries);
        }
    }

    json!({ "object": "list", "data": entries })
}

fn text_to_text() -> Modalities {
    Modalities {
        input: vec![Modality::Text],
        output: vec![Modality::Text],
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cairn_inference::{EngineMeasure, Inventory};
    use cairn_models::interface::CatalogProvider;
    use cairn_models::types::catalog::{
        claude_models, descriptor, ProviderModelCatalog, ServedVerb, Wire,
    };

    /// DeepSeek on the Sparks and the halo embedder, DeepSeek measured.
    pub(crate) fn lab_served() -> Vec<ServedModel> {
        let inventory = Inventory::from_yaml(
            r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
  - {name: qwen/qwen3-embedding-4b}
recipes:
  - id: dsv4f-tp2
    source: bginf
    model: {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
    servedName: deepseek-v4-flash-vision-exp
    upstreamId: deepseek/deepseek-v4-flash-vision-exp
    modalities: {input: [text, image], output: [text]}
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
  - id: qwen3-embed-4b
    source: bginf
    model: {name: qwen/qwen3-embedding-4b}
    servedName: qwen3-embedding-4b
    upstreamId: qwen/qwen3-embedding-4b
    verbs: [embed]
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
deployments:
  - {id: dsv4f-tp2, source: bginf, recipe: dsv4f-tp2, machines: [sp1], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, source: bginf, recipe: qwen3-embed-4b, machines: [halo], head: halo, apiPort: 8090, apiPath: /v1, healthPath: /health}
pools: []
"#,
        )
        .unwrap();
        served::served_models(
            &inventory,
            &std::collections::BTreeMap::from([(
                "dsv4f-tp2".into(),
                EngineMeasure {
                    max_model_len: Some(1_048_576),
                    kv_cache_tokens: Some(681_280),
                },
            )]),
        )
    }

    fn openrouter() -> Catalog {
        let mut model = claude_models().remove(0);
        model.id = "anthropic/claude-sonnet-4.5".into();
        model.model = model.id.clone();
        model.modalities = None;
        model.context_window = Some(200_000);
        // The same DeepSeek the Sparks run, which a request reaches locally.
        let mut dsv4 = model.clone();
        dsv4.id = "deepseek/deepseek-v4-flash-vision-exp".into();
        dsv4.model = dsv4.id.clone();
        Catalog {
            providers: vec![CatalogProvider {
                descriptor: descriptor("openrouter").unwrap().clone(),
                installed: true,
                models: Some(ProviderModelCatalog {
                    backend: "openrouter".into(),
                    models: vec![model, dsv4],
                    options: Vec::new(),
                    refreshed_at: Some(1),
                    error: None,
                }),
                tiers: Vec::new(),
                serves: ServedVerb::conversation(Wire::OpenAiChatCompletions),
                unable: Vec::new(),
            }],
        }
    }

    fn ids(listing: &Value) -> Vec<String> {
        listing["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["id"].as_str().unwrap().to_string())
            .collect()
    }

    fn entry<'a>(listing: &'a Value, id: &str) -> &'a Value {
        listing["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == id)
            .unwrap_or_else(|| panic!("{id} is not listed"))
    }

    #[test]
    fn a_local_only_client_sees_each_local_model_once_by_its_real_name() {
        let listing = compose(&lab_served(), &openrouter(), |_| false);
        assert_eq!(
            ids(&listing),
            [
                "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
                "qwen/qwen3-embedding-4b"
            ]
        );
        for entry in listing["data"].as_array().unwrap() {
            assert!(entry.get("alias_of").is_none(), "{entry}");
            assert!(
                entry["architecture"]["input_modalities"].is_array()
                    && entry["architecture"]["output_modalities"].is_array(),
                "{entry}"
            );
        }
        let dsv4 = entry(&listing, "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp");
        assert_eq!(
            dsv4["architecture"]["input_modalities"],
            json!(["text", "image"])
        );
        assert_eq!(dsv4["context_length"], 681_280);
        assert_eq!(
            entry(&listing, "qwen/qwen3-embedding-4b")["architecture"]["output_modalities"],
            json!(["embeddings"])
        );
    }

    #[test]
    fn remote_models_are_listed_only_for_a_client_that_may_use_them() {
        let granted = compose(&lab_served(), &openrouter(), |key| key == "openrouter");
        let sonnet = entry(&granted, "anthropic/claude-sonnet-4.5");
        assert_eq!(sonnet["owned_by"], "openrouter");
        assert_eq!(sonnet["context_length"], 200_000);
        assert_eq!(
            sonnet["architecture"],
            json!({"input_modalities": ["text"], "output_modalities": ["text"]})
        );
        // A model the Sparks run is not listed as OpenRouter's.
        assert!(!ids(&granted).contains(&"deepseek/deepseek-v4-flash-vision-exp".to_string()));
        let local = compose(&lab_served(), &openrouter(), |_| false);
        assert!(!ids(&local).contains(&"anthropic/claude-sonnet-4.5".to_string()));
    }
}
