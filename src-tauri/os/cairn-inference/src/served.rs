//! What Cairn serves from its deployments, and what each served name says
//! about its model.
//!
//! Every deployed recipe is served under its served name and each of its
//! aliases, whether or not it is loaded, because asking for one is what loads
//! it (tenet 1321). [`served_models`] is that list. [`listing`] renders it as
//! an OpenAI `/v1/models` body in OpenRouter's shape, so every entry, aliases
//! included, says what its model takes and gives in `architecture`. The
//! models facet's offers are built from the same list, so the two never
//! disagree.
//!
//! A served name must mean exactly one deployment. A name that
//! [`deployments_serving`] would resolve to anything else (a second deployment
//! claiming it, or another deployment's id) is not served under that name; the
//! deployment is served under its own id instead, which always resolves to it.
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

/// The KV cache size in an engine's Prometheus metrics, which vLLM reports as
/// the `kv_cache_size_tokens` label of its `vllm:cache_config_info` gauge. With
/// several engines (one series per `engine` label) it is the smallest, since a
/// request lands on one of them. `None` when no series reports it.
///
/// Nothing is derived in its absence. `num_gpu_blocks` × `block_size` beside
/// it on the DeepSeek V4 Flash build gives 64,824 tokens against a reported
/// 1,447,093, so the product does not measure the cache on every engine.
pub fn kv_cache_tokens(metrics: &str) -> Option<u64> {
    metrics
        .lines()
        .filter_map(|line| line.strip_prefix("vllm:cache_config_info{"))
        .filter_map(|labels| label(labels, "kv_cache_size_tokens")?.parse::<u64>().ok())
        .min()
}

/// The value of `key` in a Prometheus label set (`a="1",b="2"}` and whatever
/// follows the closing brace), quotes and escapes respected.
fn label(labels: &str, key: &str) -> Option<String> {
    let mut rest = labels;
    loop {
        rest = rest.trim_start_matches([',', ' ']);
        if rest.starts_with('}') || rest.is_empty() {
            return None;
        }
        let (name, after) = rest.split_once('=')?;
        let after = after.strip_prefix('"')?;
        let mut value = String::new();
        let mut chars = after.char_indices();
        let end = loop {
            match chars.next()? {
                (_, '\\') => value.push(chars.next()?.1),
                (index, '"') => break index,
                (_, c) => value.push(c),
            }
        };
        if name.trim() == key {
            return Some(value);
        }
        rest = &after[end + 1..];
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

/// One name Cairn serves, and the deployment that answers to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedModel {
    /// The name a caller asks for.
    pub id: String,
    /// The served name this id stands for, when it is an alias.
    pub alias_of: Option<String>,
    pub deployment: DeploymentId,
    pub recipe: RecipeId,
    /// The name the engine answers to: every request for `id` reaches the
    /// engine under this name.
    pub served_name: String,
    /// The model the engine loaded (the recipe's model name, such as a
    /// repository id), which a tokenizer is looked up by.
    pub root: String,
    pub upstream_id: Option<String>,
    /// The verbs the recipe declares; `None` when it doesn't say.
    pub verbs: Option<BTreeSet<VerbKind>>,
    pub modalities: Modalities,
    /// The context to advertise, when the engine has been measured.
    pub context_length: Option<u64>,
}

/// Every name Cairn serves, by deployment id and then served name before
/// aliases, with each deployment's measured context.
pub fn served_models(
    inventory: &Inventory,
    measures: &BTreeMap<DeploymentId, EngineMeasure>,
) -> Vec<ServedModel> {
    let mut served = Vec::new();
    for deployment in inventory.deployments.values() {
        let Some(recipe) = inventory.recipes.get(&deployment.recipe) else {
            continue;
        };
        let resolves_here = |name: &str| {
            deployments_serving(inventory, name).as_slice() == std::slice::from_ref(&deployment.id)
        };
        let context_length = measures
            .get(&deployment.id)
            .and_then(EngineMeasure::context_length);
        let entry = |id: &str, alias_of: Option<&str>| ServedModel {
            id: id.to_string(),
            alias_of: alias_of.map(str::to_string),
            deployment: deployment.id.clone(),
            recipe: recipe.id.clone(),
            served_name: recipe.served_name.clone(),
            root: recipe.model.name.clone(),
            upstream_id: recipe.upstream_id.clone(),
            verbs: recipe.verbs.clone(),
            modalities: modalities(recipe),
            context_length,
        };
        let primary = if resolves_here(&recipe.served_name) {
            recipe.served_name.as_str()
        } else {
            deployment.id.as_str()
        };
        let mut named: BTreeSet<String> = BTreeSet::from([primary.to_ascii_lowercase()]);
        served.push(entry(primary, None));
        for alias in &recipe.aliases {
            if resolves_here(alias) && named.insert(alias.to_ascii_lowercase()) {
                served.push(entry(alias, Some(primary)));
            }
        }
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
/// carries `architecture`, `root` (the loaded model, as vLLM reports it),
/// `alias_of` on an alias (as bgllm did), and the clamped context under both
/// OpenRouter's `context_length` and vLLM's `max_model_len` when measured. A
/// served name has no creation time, which `created: 0` says.
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
        "root": model.root,
        "architecture": {
            "input_modalities": model.modalities.input,
            "output_modalities": model.modalities.output,
        },
    });
    if let Some(target) = &model.alias_of {
        entry["alias_of"] = json!(target);
    }
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

    /// The live `vllm:cache_config_info` series of DeepSeek V4 Flash on the
    /// Sparks, read 2026-10-31.
    const DSV4_METRICS: &str = r#"# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{_block_size_resolved="True",block_size="4",cache_dtype="nvfp4_ds_mla",calculate_kv_scales="False",enable_prefix_caching="True",engine="0",gpu_memory_utilization="0.835",hash_block_size="None",is_attention_free="False",kv_cache_dtype_skip_layers="[]",kv_cache_max_concurrency="1.3800562036958188",kv_cache_memory_bytes="None",kv_cache_size_tokens="1447093",kv_offloading_backend="native",kv_offloading_size="None",kv_sharing_fast_prefill="False",mamba_block_size="None",mamba_cache_dtype="auto",mamba_cache_mode="none",mamba_page_size_padded="None",mamba_ssm_cache_dtype="auto",num_cpu_blocks="None",num_gpu_blocks="16206",num_gpu_blocks_override="None",prefix_caching_hash_algo="sha256",skip_page_size_padded="None",sliding_window="128",user_specified_block_size="True",user_specified_mamba_block_size="False"} 1.0
vllm:num_requests_running{engine="0",model_name="deepseek-v4-flash-vision-exp"} 0.0
"#;

    /// The same engine's own `/v1/models`, read the same day.
    const DSV4_MODELS: &str = r#"{"object":"list","data":[{"id":"deepseek-v4-flash-vision-exp","object":"model","created":1793470670,"owned_by":"vllm","root":"deepseek-ai/DeepSeek-V4-Flash-Vision-Exp","parent":null,"max_model_len":1048576,"permission":[]}]}"#;

    /// The test inventory, with DeepSeek's served name and aliases as its
    /// recipe has them.
    fn sparks() -> Inventory {
        let mut inventory = test_inventory();
        let recipe = inventory
            .recipes
            .get_mut(&RecipeId::from("dsv4f-tp2"))
            .unwrap();
        recipe.served_name = "deepseek-v4-flash-vision-exp".into();
        recipe.model.name = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into();
        recipe.aliases = vec!["big".into(), "vision".into(), "dsv4-flash".into()];
        inventory
    }

    fn ids(served: &[ServedModel]) -> Vec<&str> {
        served.iter().map(|s| s.id.as_str()).collect()
    }

    #[test]
    fn the_kv_cache_is_the_reported_label_and_nothing_derived() {
        assert_eq!(kv_cache_tokens(DSV4_METRICS), Some(1_447_093));
        let derivable_only =
            "vllm:cache_config_info{block_size=\"4\",num_gpu_blocks=\"16206\"} 1.0\n";
        assert_eq!(kv_cache_tokens(derivable_only), None);
        assert_eq!(kv_cache_tokens(""), None);
        assert_eq!(kv_cache_tokens("not found"), None);
    }

    #[test]
    fn of_several_engines_the_smallest_cache_counts() {
        let two = "vllm:cache_config_info{engine=\"0\",kv_cache_size_tokens=\"900\"} 1.0\n\
                   vllm:cache_config_info{engine=\"1\",kv_cache_size_tokens=\"700\"} 1.0\n";
        assert_eq!(kv_cache_tokens(two), Some(700));
    }

    #[test]
    fn a_quoted_value_may_hold_separators_and_escapes() {
        let tricky =
            "vllm:cache_config_info{note=\"a,b}=c \\\"q\\\"\",kv_cache_size_tokens=\"42\"} 1.0";
        assert_eq!(kv_cache_tokens(tricky), Some(42));
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
    fn every_deployed_recipe_is_served_by_name_and_alias_loaded_or_not() {
        let measures = BTreeMap::from([(
            DeploymentId::from("dsv4f-tp2"),
            EngineMeasure {
                max_model_len: Some(1_048_576),
                kv_cache_tokens: Some(1_447_093),
            },
        )]);
        let served = served_models(&sparks(), &measures);
        assert_eq!(
            ids(&served),
            [
                "deepseek-v4-flash-vision-exp",
                "big",
                "vision",
                "dsv4-flash",
                "qwen3-embed-4b",
                "qwen36-fast",
            ]
        );
        let alias = served.iter().find(|s| s.id == "dsv4-flash").unwrap();
        assert_eq!(
            alias.alias_of.as_deref(),
            Some("deepseek-v4-flash-vision-exp")
        );
        assert_eq!(alias.served_name, "deepseek-v4-flash-vision-exp");
        assert_eq!(alias.deployment.as_str(), "dsv4f-tp2");
        assert_eq!(alias.context_length, Some(1_048_576));
        // An unmeasured deployment advertises no context.
        let fast = served.iter().find(|s| s.id == "qwen36-fast").unwrap();
        assert_eq!(fast.context_length, None);
    }

    #[test]
    fn a_name_two_deployments_claim_is_served_by_neither_and_each_keeps_its_id() {
        let mut inventory = sparks();
        for id in ["dsv4f-tp2", "qwen36-fast"] {
            inventory
                .recipes
                .get_mut(&RecipeId::from(id))
                .unwrap()
                .aliases
                .push("big".into());
        }
        // A second deployment of DeepSeek's recipe claims its served name.
        let mut copy = deployment("dsv4f-copy", &["halo"]);
        copy.recipe = "dsv4f-tp2".into();
        inventory.insert_deployment(copy);
        let served = served_models(&inventory, &BTreeMap::new());
        assert!(!ids(&served).contains(&"big"), "{:?}", ids(&served));
        assert!(!ids(&served).contains(&"deepseek-v4-flash-vision-exp"));
        assert!(ids(&served).contains(&"dsv4f-tp2"));
        assert!(ids(&served).contains(&"dsv4f-copy"));
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
    fn every_listing_entry_carries_architecture_and_an_alias_names_its_target() {
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
        assert_eq!(data.len(), 6);
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
        let alias = data.iter().find(|e| e["id"] == "dsv4-flash").unwrap();
        assert_eq!(
            alias,
            &json!({
                "id": "dsv4-flash",
                "object": "model",
                "created": 0,
                "owned_by": "cairn-inference",
                "root": "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp",
                "alias_of": "deepseek-v4-flash-vision-exp",
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
        assert!(fast.get("alias_of").is_none());
    }
}
