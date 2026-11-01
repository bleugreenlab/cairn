//! What Cairn serves from its deployments, and what each model is.
//!
//! Every deployed model is served under its real name (tenet 1336), whether or
//! not it is loaded, because asking for it is what loads it (tenet 1321).
//! [`served_models`] is that list. [`listing`] renders it as an OpenAI
//! `/v1/models` body in OpenRouter's shape, so every entry says what its model
//! takes and gives in `architecture`. The models facet's offers are built from
//! the same list, so the two never disagree. The engine's own served name is
//! the engine boundary's detail: a request by real name reaches the engine
//! under it.
//!
//! A served name must mean exactly one deployment. A model with deployments in
//! several places needs a pool to say which, which a caller of the served API
//! cannot give, so it is not served there until placement chooses for it.
//!
//! The context a deployment advertises is the engine's own limit clamped to
//! the KV cache it measured at start ([`EngineMeasure`]). An advertised
//! context longer than the cache holds is the failure Cloudflare disclosed for
//! a served model in 2026-10: a request the engine could not hold attended over
//! stale cache pages from other requests.

use std::collections::{BTreeMap, BTreeSet};

use cairn_models_types::catalog::{Modalities, Modality, VerbKind};
use serde_json::{json, Value};

use crate::deployment::DeploymentId;
use crate::inventory::Inventory;
use crate::pool_control::deployments_serving;
use crate::recipe::{Recipe, RecipeId};

/// What an engine reported about itself once it was healthy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineMeasure {
    /// The longest context the engine accepts (vLLM's `max_model_len`).
    pub max_model_len: Option<u64>,
    /// How many tokens its KV cache holds (vLLM's `kv_cache_size_tokens`).
    pub kv_cache_tokens: Option<u64>,
}

impl EngineMeasure {
    /// The context to advertise: the engine's own limit, never more than its
    /// KV cache holds. With only one of the two known, that one; with neither,
    /// nothing is advertised.
    pub fn context_length(&self) -> Option<u64> {
        match (self.max_model_len, self.kv_cache_tokens) {
            (Some(limit), Some(pool)) => Some(limit.min(pool)),
            (limit, pool) => limit.or(pool),
        }
    }
}

/// The context limit in an engine's own `/v1/models` body (vLLM's
/// `max_model_len`): the entry for `served_name`, else the only entry.
pub fn max_model_len(models: &str, served_name: &str) -> Option<u64> {
    let body: Value = serde_json::from_str(models).ok()?;
    let data = body.get("data")?.as_array()?;
    let entry = data
        .iter()
        .find(|entry| entry.get("id").and_then(Value::as_str) == Some(served_name))
        .or(match data.as_slice() {
            [only] => Some(only),
            _ => None,
        })?;
    entry.get("max_model_len")?.as_u64()
}

/// One model Cairn serves, and the deployment that answers for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedModel {
    /// The model's real name, which a caller asks for and a tokenizer is
    /// looked up by.
    pub id: String,
    pub deployment: DeploymentId,
    pub recipe: RecipeId,
    /// The name the engine answers to: every request for `id` reaches the
    /// engine under this name.
    pub served_name: String,
    pub upstream_id: Option<String>,
    /// The verbs the recipe declares; `None` when it doesn't say.
    pub verbs: Option<BTreeSet<VerbKind>>,
    pub modalities: Modalities,
    /// The context to advertise, when the engine has been measured.
    pub context_length: Option<u64>,
}

/// Every model Cairn serves, by deployment id, each under its real name with
/// its deployment's measured context.
pub fn served_models(
    inventory: &Inventory,
    measures: &BTreeMap<DeploymentId, EngineMeasure>,
) -> Vec<ServedModel> {
    let mut served = Vec::new();
    let mut named = BTreeSet::new();
    for deployment in inventory.deployments.values() {
        let Some(recipe) = inventory.recipes.get(&deployment.recipe) else {
            continue;
        };
        let name = recipe.model.name.trim();
        if !named.insert(name.to_ascii_lowercase()) {
            continue;
        }
        if deployments_serving(inventory, name).as_slice() != std::slice::from_ref(&deployment.id) {
            continue;
        }
        served.push(ServedModel {
            id: name.to_string(),
            deployment: deployment.id.clone(),
            recipe: recipe.id.clone(),
            served_name: recipe.served_name.clone(),
            upstream_id: recipe.upstream_id.clone(),
            verbs: recipe.verbs.clone(),
            modalities: modalities(recipe),
            context_length: measures
                .get(&deployment.id)
                .and_then(EngineMeasure::context_length),
        });
    }
    served
}

/// What `recipe`'s model takes and gives: what it declares; else what its
/// declared verbs produce, from text; else text to text, the conversation an
/// OpenAI-compatible chat server is offered for when it says nothing.
pub fn modalities(recipe: &Recipe) -> Modalities {
    if let Some(declared) = &recipe.modalities {
        return declared.clone();
    }
    let Some(verbs) = &recipe.verbs else {
        return Modalities {
            input: vec![Modality::Text],
            output: vec![Modality::Text],
        };
    };
    let mut output = Vec::new();
    let mut gives = |modality: Modality| {
        if !output.contains(&modality) {
            output.push(modality);
        }
    };
    for verb in verbs {
        gives(match verb {
            VerbKind::Complete | VerbKind::Session | VerbKind::Decide => Modality::Text,
            VerbKind::Embed => Modality::Embeddings,
            VerbKind::Image => Modality::Image,
            VerbKind::Speak => Modality::Audio,
        });
    }
    Modalities {
        input: vec![Modality::Text],
        output,
    }
}

/// `models` as an OpenAI `/v1/models` body in OpenRouter's shape. Each entry
/// carries `architecture`, `root` (the loaded model, as vLLM reports it, which
/// is its real name), and the clamped context under both OpenRouter's
/// `context_length` and vLLM's `max_model_len` when measured. A served model
/// has no creation time, which `created: 0` says.
pub fn listing(models: &[ServedModel]) -> Value {
    json!({
        "object": "list",
        "data": models.iter().map(listing_entry).collect::<Vec<_>>(),
    })
}

fn listing_entry(model: &ServedModel) -> Value {
    let mut entry = json!({
        "id": model.id,
        "object": "model",
        "created": 0,
        "owned_by": "cairn-inference",
        "root": model.id,
        "architecture": {
            "input_modalities": model.modalities.input,
            "output_modalities": model.modalities.output,
        },
    });
    if let Some(context) = model.context_length {
        entry["context_length"] = json!(context);
        entry["max_model_len"] = json!(context);
    }
    entry
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{command_recipe, deployment, test_inventory};

    /// The same engine's own `/v1/models`, read the same day.
    const DSV4_MODELS: &str = r#"{"object":"list","data":[{"id":"deepseek-v4-flash-vision-exp","object":"model","created":1793470670,"owned_by":"vllm","root":"deepseek-ai/DeepSeek-V4-Flash-Vision-Exp","parent":null,"max_model_len":1048576,"permission":[]}]}"#;

    const DSV4: &str = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp";

    /// The test inventory, with DeepSeek's real and served names as its
    /// recipe has them.
    fn sparks() -> Inventory {
        let mut inventory = test_inventory();
        let recipe = inventory
            .recipes
            .get_mut(&RecipeId::from("dsv4f-tp2"))
            .unwrap();
        recipe.served_name = "deepseek-v4-flash-vision-exp".into();
        recipe.model.name = DSV4.into();
        inventory
    }

    fn ids(served: &[ServedModel]) -> Vec<&str> {
        served.iter().map(|s| s.id.as_str()).collect()
    }

    #[test]
    fn the_engine_limit_is_its_entry_for_the_served_name() {
        assert_eq!(
            max_model_len(DSV4_MODELS, "deepseek-v4-flash-vision-exp"),
            Some(1_048_576)
        );
        // The only entry answers for a name the engine renamed.
        assert_eq!(max_model_len(DSV4_MODELS, "other"), Some(1_048_576));
        assert_eq!(max_model_len("<html>", "x"), None);
    }

    #[test]
    fn the_advertised_context_never_exceeds_the_measured_cache() {
        let measure = |max_model_len, kv_cache_tokens| EngineMeasure {
            max_model_len,
            kv_cache_tokens,
        };
        // Today's DeepSeek: the cache is larger, so the clamp changes nothing.
        assert_eq!(
            measure(Some(1_048_576), Some(1_447_093)).context_length(),
            Some(1_048_576)
        );
        // Cloudflare's shape: a cache smaller than the advertised context.
        assert_eq!(
            measure(Some(1_048_576), Some(681_280)).context_length(),
            Some(681_280)
        );
        assert_eq!(measure(None, Some(681_280)).context_length(), Some(681_280));
        assert_eq!(measure(Some(8_192), None).context_length(), Some(8_192));
        assert_eq!(measure(None, None).context_length(), None);
    }

    #[test]
    fn every_deployed_model_is_served_by_its_real_name_loaded_or_not() {
        let measures = BTreeMap::from([(
            DeploymentId::from("dsv4f-tp2"),
            EngineMeasure {
                max_model_len: Some(1_048_576),
                kv_cache_tokens: Some(1_447_093),
            },
        )]);
        let served = served_models(&sparks(), &measures);
        assert_eq!(ids(&served), [DSV4, "qwen3-embed-4b", "qwen36-fast"]);
        let deepseek = &served[0];
        assert_eq!(deepseek.served_name, "deepseek-v4-flash-vision-exp");
        assert_eq!(deepseek.deployment.as_str(), "dsv4f-tp2");
        assert_eq!(deepseek.context_length, Some(1_048_576));
        // An unmeasured deployment advertises no context.
        let fast = served.iter().find(|s| s.id == "qwen36-fast").unwrap();
        assert_eq!(fast.context_length, None);
    }

    #[test]
    fn a_model_deployed_twice_is_not_served_until_something_chooses() {
        let mut inventory = sparks();
        let mut copy = deployment("dsv4f-copy", &["halo"]);
        copy.recipe = "dsv4f-tp2".into();
        inventory.insert_deployment(copy);
        let served = served_models(&inventory, &BTreeMap::new());
        assert_eq!(ids(&served), ["qwen3-embed-4b", "qwen36-fast"]);
        // Every listed name resolves to exactly the deployment it lists.
        for model in &served {
            assert_eq!(
                deployments_serving(&inventory, &model.id),
                vec![model.deployment.clone()],
                "{}",
                model.id
            );
        }
    }

    #[test]
    fn modalities_are_declared_else_produced_by_the_verbs_else_text() {
        let mut recipe = command_recipe("r", 1);
        assert_eq!(modalities(&recipe).to_string(), "text → text");
        recipe.verbs = Some(BTreeSet::from([VerbKind::Embed]));
        assert_eq!(modalities(&recipe).to_string(), "text → embeddings");
        recipe.verbs = Some(BTreeSet::from([VerbKind::Session, VerbKind::Decide]));
        assert_eq!(modalities(&recipe).to_string(), "text → text");
        recipe.modalities = Some(Modalities {
            input: vec![Modality::Text, Modality::Image],
            output: vec![Modality::Text],
        });
        assert_eq!(modalities(&recipe).to_string(), "text+image → text");
    }

    #[test]
    fn every_listing_entry_carries_architecture_under_the_real_name() {
        let mut inventory = sparks();
        inventory
            .recipes
            .get_mut(&RecipeId::from("dsv4f-tp2"))
            .unwrap()
            .modalities = Some(Modalities {
            input: vec![Modality::Text, Modality::Image],
            output: vec![Modality::Text],
        });
        let measures = BTreeMap::from([(
            DeploymentId::from("dsv4f-tp2"),
            EngineMeasure {
                max_model_len: Some(1_048_576),
                kv_cache_tokens: Some(1_447_093),
            },
        )]);
        let body = listing(&served_models(&inventory, &measures));
        let data = body["data"].as_array().unwrap();
        assert_eq!(data.len(), 3);
        for entry in data {
            assert!(
                entry["architecture"]["input_modalities"].is_array(),
                "{entry}"
            );
            assert!(
                entry["architecture"]["output_modalities"].is_array(),
                "{entry}"
            );
            assert_eq!(entry["object"], "model");
        }
        assert!(data.iter().all(|e| e.get("alias_of").is_none()));
        let deepseek = data.iter().find(|e| e["id"] == DSV4).unwrap();
        assert_eq!(
            deepseek,
            &json!({
                "id": DSV4,
                "object": "model",
                "created": 0,
                "owned_by": "cairn-inference",
                "root": DSV4,
                "context_length": 1_048_576,
                "max_model_len": 1_048_576,
                "architecture": {
                    "input_modalities": ["text", "image"],
                    "output_modalities": ["text"],
                },
            })
        );
        let fast = data.iter().find(|e| e["id"] == "qwen36-fast").unwrap();
        assert!(fast.get("context_length").is_none());
    }
}
