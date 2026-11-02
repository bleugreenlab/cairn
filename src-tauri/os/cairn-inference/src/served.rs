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
//! A model is one entry however many deployments it has. It serves the union
//! of what its deployments' profiles serve, each judged by one rule
//! ([`profile_verbs`]), and placement routes a request only to a deployment
//! whose profile serves the request's verb. Where deployments disagree, the
//! entry advertises the conservative fact: the inputs every deployment takes,
//! and the smallest context any measured.
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
use crate::profile::{InferenceProfile, InferenceProfileId, WeightFile};

/// The deployments of the model a caller names: every deployment of a profile
/// whose model has that real name, case-insensitively. A deployment id, profile
/// id or served name is an internal key and names no model (tenet 1336).
pub fn deployments_serving(inventory: &Inventory, name: &str) -> Vec<DeploymentId> {
    let name = name.trim();
    if name.is_empty() {
        return Vec::new();
    }
    inventory
        .deployments
        .values()
        .filter(|d| {
            inventory
                .profiles
                .get(&d.profile)
                .is_some_and(|r| r.model.name.trim().eq_ignore_ascii_case(name))
        })
        .map(|d| d.id.clone())
        .collect()
}

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

/// The verbs a conversation with a model is had through.
const CONVERSATION: [VerbKind; 3] = [VerbKind::Complete, VerbKind::Session, VerbKind::Decide];

/// The verbs `profile` serves: those it declares; else conversation and
/// decide for a model that outputs text, as an OpenAI-compatible chat server
/// is offered when it says nothing, embed for one that outputs only
/// embeddings, and nothing for any other. Nothing is inferred from a name.
/// Placement routes by this rule and the catalog offers by it.
pub fn profile_verbs(profile: &InferenceProfile) -> BTreeSet<VerbKind> {
    if let Some(verbs) = &profile.verbs {
        return verbs.clone();
    }
    let output = modalities(profile).output;
    if output.contains(&Modality::Text) {
        CONVERSATION.into()
    } else if output == [Modality::Embeddings] {
        BTreeSet::from([VerbKind::Embed])
    } else {
        BTreeSet::new()
    }
}

/// One deployment of a served model, as its profile describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedDeployment {
    pub deployment: DeploymentId,
    pub profile: InferenceProfileId,
    /// The name the engine answers to: a request routed here reaches the
    /// engine under this name.
    pub served_name: String,
    pub upstream_id: Option<String>,
    /// What the profile serves, by [`profile_verbs`].
    pub verbs: BTreeSet<VerbKind>,
    pub modalities: Modalities,
    /// The context the engine can hold, when it has been measured.
    pub context_length: Option<u64>,
    pub quantization: Option<String>,
    /// The weight files the profile pins; none for a profile that pins none.
    pub weights: Vec<WeightFile>,
}

impl ServedDeployment {
    /// Whether this deployment's vectors are the same weights' as `other`'s:
    /// the same files at the same revisions and the same quantization. A
    /// profile that pins no files is never shown to match, since what its
    /// engine loads is unknown.
    pub fn same_weights(&self, other: &ServedDeployment) -> bool {
        !self.weights.is_empty()
            && self.quantization == other.quantization
            && self.weights == other.weights
    }
}

/// One model Cairn serves, and every deployment that answers for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedModel {
    /// The model's real name, which a caller asks for and a tokenizer is
    /// looked up by.
    pub id: String,
    /// In deployment-id order; never empty.
    pub deployments: Vec<ServedDeployment>,
}

impl ServedModel {
    /// What the model serves: the union of its deployments'.
    pub fn verbs(&self) -> BTreeSet<VerbKind> {
        self.deployments
            .iter()
            .flat_map(|deployment| deployment.verbs.iter().copied())
            .collect()
    }

    /// The deployments a request for `verb` may be routed to.
    pub fn serving(&self, verb: VerbKind) -> impl Iterator<Item = &ServedDeployment> {
        self.deployments
            .iter()
            .filter(move |deployment| deployment.verbs.contains(&verb))
    }

    /// The OpenRouter slug catalog matching uses: the first deployment's
    /// profile that names one.
    pub fn upstream_id(&self) -> Option<&str> {
        self.deployments
            .iter()
            .find_map(|deployment| deployment.upstream_id.as_deref())
    }

    /// What the model takes and gives: the inputs every deployment takes, and
    /// the outputs any gives, since each output is served by the deployments
    /// that give it.
    pub fn modalities(&self) -> Modalities {
        let mut deployments = self.deployments.iter();
        let mut merged = deployments
            .next()
            .map(|first| first.modalities.clone())
            .unwrap_or_default();
        for deployment in deployments {
            merged
                .input
                .retain(|modality| deployment.modalities.input.contains(modality));
            for modality in &deployment.modalities.output {
                if !merged.output.contains(modality) {
                    merged.output.push(modality.clone());
                }
            }
        }
        merged
    }

    /// The smallest context any deployment serving `verb` measured.
    pub fn context_serving(&self, verb: VerbKind) -> Option<u64> {
        self.serving(verb)
            .filter_map(|deployment| deployment.context_length)
            .min()
    }

    /// The context to advertise: the smallest any deployment serving a
    /// conversation measured, or any deployment at all for a model that
    /// converses on none.
    pub fn context_length(&self) -> Option<u64> {
        let converses = |deployment: &&ServedDeployment| {
            CONVERSATION
                .iter()
                .any(|verb| deployment.verbs.contains(verb))
        };
        let conversing: Vec<&ServedDeployment> =
            self.deployments.iter().filter(converses).collect();
        let measured = if conversing.is_empty() {
            self.deployments.iter().collect()
        } else {
            conversing
        };
        measured
            .into_iter()
            .filter_map(|deployment| deployment.context_length)
            .min()
    }
}

/// Every model Cairn serves, once each under its real name (compared
/// case-insensitively), in the order of its first deployment's id, with every
/// deployment's measured context.
pub fn served_models(
    inventory: &Inventory,
    measures: &BTreeMap<DeploymentId, EngineMeasure>,
) -> Vec<ServedModel> {
    let mut served: Vec<ServedModel> = Vec::new();
    for deployment in inventory.deployments.values() {
        let Some(profile) = inventory.profiles.get(&deployment.profile) else {
            continue;
        };
        let name = profile.model.name.trim();
        let entry = ServedDeployment {
            deployment: deployment.id.clone(),
            profile: profile.id.clone(),
            served_name: profile.served_name.clone(),
            upstream_id: profile.upstream_id.clone(),
            verbs: profile_verbs(profile),
            modalities: modalities(profile),
            context_length: measures
                .get(&deployment.id)
                .and_then(EngineMeasure::context_length),
            quantization: profile.quantization.clone(),
            weights: profile.model.weights.clone(),
        };
        match served
            .iter_mut()
            .find(|model| model.id.eq_ignore_ascii_case(name))
        {
            Some(model) => model.deployments.push(entry),
            None => served.push(ServedModel {
                id: name.to_string(),
                deployments: vec![entry],
            }),
        }
    }
    served
}

/// What `profile`'s model takes and gives: what it declares; else what its
/// declared verbs produce, from text; else text to text, the conversation an
/// OpenAI-compatible chat server is offered for when it says nothing.
pub fn modalities(profile: &InferenceProfile) -> Modalities {
    if let Some(declared) = &profile.modalities {
        return declared.clone();
    }
    let Some(verbs) = &profile.verbs else {
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
    let modalities = model.modalities();
    let mut entry = json!({
        "id": model.id,
        "object": "model",
        "created": 0,
        "owned_by": "cairn-inference",
        "root": model.id,
        "architecture": {
            "input_modalities": modalities.input,
            "output_modalities": modalities.output,
        },
    });
    if let Some(context) = model.context_length() {
        entry["context_length"] = json!(context);
        entry["max_model_len"] = json!(context);
    }
    entry
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{command_profile, deployment, test_inventory};

    /// The same engine's own `/v1/models`, read the same day.
    const DSV4_MODELS: &str = r#"{"object":"list","data":[{"id":"deepseek-v4-flash-vision-exp","object":"model","created":1793470670,"owned_by":"vllm","root":"deepseek-ai/DeepSeek-V4-Flash-Vision-Exp","parent":null,"max_model_len":1048576,"permission":[]}]}"#;

    const DSV4: &str = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp";

    /// The test inventory, with DeepSeek's real and served names as its
    /// profile has them.
    fn sparks() -> Inventory {
        let mut inventory = test_inventory();
        let profile = inventory
            .profiles
            .get_mut(&InferenceProfileId::from("dsv4f-tp2"))
            .unwrap();
        profile.served_name = "deepseek-v4-flash-vision-exp".into();
        profile.model.name = DSV4.into();
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
        assert_eq!(deepseek.deployments.len(), 1);
        assert_eq!(
            deepseek.deployments[0].served_name,
            "deepseek-v4-flash-vision-exp"
        );
        assert_eq!(deepseek.deployments[0].deployment.as_str(), "dsv4f-tp2");
        assert_eq!(deepseek.context_length(), Some(1_048_576));
        // An unmeasured deployment advertises no context.
        let fast = served.iter().find(|s| s.id == "qwen36-fast").unwrap();
        assert_eq!(fast.context_length(), None);
    }

    /// `profile`'s model renamed `name`, its weights pinned to `quantization`.
    fn pinned(inventory: &mut Inventory, profile: &str, name: &str, quantization: &str) {
        let profile = inventory
            .profiles
            .get_mut(&InferenceProfileId::from(profile))
            .unwrap();
        profile.model.name = name.into();
        profile.quantization = Some(quantization.into());
        profile.model.weights = vec![WeightFile {
            repo: format!("{name}-GGUF"),
            revision: "f460253".into(),
            path: format!("model-{quantization}.gguf"),
            sha256: format!("sha-{quantization}"),
            size: None,
        }];
    }

    /// A model deployed twice is one entry, by its real name in any case,
    /// carrying both deployments, serving the union of what their profiles
    /// serve and advertising the conservative facts.
    #[test]
    fn a_model_deployed_twice_is_one_entry_serving_the_union() {
        let mut inventory = sparks();
        // Its name differs in case, and its deployment sorts after the
        // first, whose profile names the entry.
        let copy = deployment("dsv4f-zcopy", &["halo"]);
        let mut profile = command_profile("dsv4f-zcopy", 1);
        profile.model.name = DSV4.to_ascii_lowercase();
        profile.served_name = "copy".into();
        profile.upstream_id = Some("deepseek/deepseek-v4-flash-vision-exp".into());
        profile.verbs = Some(BTreeSet::from([VerbKind::Embed]));
        inventory.insert_profile(profile);
        inventory.insert_deployment(copy);
        inventory
            .profiles
            .get_mut(&InferenceProfileId::from("dsv4f-tp2"))
            .unwrap()
            .modalities = Some(Modalities {
            input: vec![Modality::Text, Modality::Image],
            output: vec![Modality::Text],
        });
        let measures = BTreeMap::from([
            (
                DeploymentId::from("dsv4f-tp2"),
                EngineMeasure {
                    max_model_len: Some(1_048_576),
                    kv_cache_tokens: None,
                },
            ),
            (
                DeploymentId::from("dsv4f-zcopy"),
                EngineMeasure {
                    max_model_len: Some(8_192),
                    kv_cache_tokens: None,
                },
            ),
        ]);
        let served = served_models(&inventory, &measures);
        assert_eq!(ids(&served), [DSV4, "qwen3-embed-4b", "qwen36-fast"]);
        let deepseek = &served[0];
        assert_eq!(
            deepseek
                .deployments
                .iter()
                .map(|d| d.deployment.as_str())
                .collect::<Vec<_>>(),
            ["dsv4f-tp2", "dsv4f-zcopy"]
        );
        assert_eq!(
            deepseek.verbs(),
            BTreeSet::from([
                VerbKind::Complete,
                VerbKind::Session,
                VerbKind::Decide,
                VerbKind::Embed
            ])
        );
        let embedders: Vec<_> = deepseek
            .serving(VerbKind::Embed)
            .map(|d| d.served_name.as_str())
            .collect();
        assert_eq!(embedders, ["copy"]);
        // Only text is taken by both; each output is given by one.
        assert_eq!(deepseek.modalities().to_string(), "text → text+embeddings");
        // The conversation's context is its own deployment's, the embedder's
        // its own.
        assert_eq!(deepseek.context_length(), Some(1_048_576));
        assert_eq!(deepseek.context_serving(VerbKind::Embed), Some(8_192));
        assert_eq!(
            deepseek.upstream_id(),
            Some("deepseek/deepseek-v4-flash-vision-exp")
        );
        // Every listed name resolves to every deployment it lists.
        for model in &served {
            let mut listed: Vec<_> = model
                .deployments
                .iter()
                .map(|d| d.deployment.clone())
                .collect();
            listed.sort();
            let mut serving = deployments_serving(&inventory, &model.id);
            serving.sort();
            assert_eq!(serving, listed, "{}", model.id);
        }
    }

    #[test]
    fn a_profile_serves_what_it_declares_else_what_its_output_says() {
        let mut profile = command_profile("r", 1);
        let conversation = BTreeSet::from(CONVERSATION);
        assert_eq!(profile_verbs(&profile), conversation, "says nothing: text");
        profile.modalities = Some(Modalities {
            input: vec![Modality::Text],
            output: vec![Modality::Embeddings],
        });
        assert_eq!(profile_verbs(&profile), BTreeSet::from([VerbKind::Embed]));
        profile.modalities = Some(Modalities {
            input: vec![Modality::Text],
            output: vec![Modality::Audio],
        });
        assert!(profile_verbs(&profile).is_empty());
        // A declaration is believed whatever the modalities say.
        profile.verbs = Some(BTreeSet::from([VerbKind::Decide]));
        assert_eq!(profile_verbs(&profile), BTreeSet::from([VerbKind::Decide]));
    }

    #[test]
    fn only_pinned_identical_weights_are_the_same_weights() {
        let mut inventory = sparks();
        pinned(
            &mut inventory,
            "qwen3-embed-4b",
            "Qwen/Qwen3-Embedding-4B",
            "q8_0",
        );
        inventory.insert_profile(command_profile("embed-twin", 1));
        inventory.insert_deployment(deployment("embed-twin", &["sp1"]));
        pinned(
            &mut inventory,
            "embed-twin",
            "Qwen/Qwen3-Embedding-4B",
            "q8_0",
        );
        let deployments = |inventory: &Inventory| {
            served_models(inventory, &BTreeMap::new())
                .into_iter()
                .find(|model| model.id == "Qwen/Qwen3-Embedding-4B")
                .unwrap()
                .deployments
        };
        let both = deployments(&inventory);
        assert!(both[0].same_weights(&both[1]));
        pinned(
            &mut inventory,
            "embed-twin",
            "Qwen/Qwen3-Embedding-4B",
            "q4_k_m",
        );
        let both = deployments(&inventory);
        assert!(!both[0].same_weights(&both[1]), "another quantization");
        let mut unpinned = both[0].clone();
        unpinned.weights.clear();
        assert!(!unpinned.same_weights(&unpinned.clone()), "nothing pinned");
    }

    #[test]
    fn modalities_are_declared_else_produced_by_the_verbs_else_text() {
        let mut profile = command_profile("r", 1);
        assert_eq!(modalities(&profile).to_string(), "text → text");
        profile.verbs = Some(BTreeSet::from([VerbKind::Embed]));
        assert_eq!(modalities(&profile).to_string(), "text → embeddings");
        profile.verbs = Some(BTreeSet::from([VerbKind::Session, VerbKind::Decide]));
        assert_eq!(modalities(&profile).to_string(), "text → text");
        profile.modalities = Some(Modalities {
            input: vec![Modality::Text, Modality::Image],
            output: vec![Modality::Text],
        });
        assert_eq!(modalities(&profile).to_string(), "text+image → text");
    }

    #[test]
    fn every_listing_entry_carries_architecture_under_the_real_name() {
        let mut inventory = sparks();
        inventory
            .profiles
            .get_mut(&InferenceProfileId::from("dsv4f-tp2"))
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
