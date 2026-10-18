use crate::credentials::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use cairn_models::embed::{families, local_server_limits};
use cairn_models::types::catalog::{DiscoveredModel, Modalities, Modality, ServedVerb, Verb, Wire};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(2);
#[derive(Debug, Deserialize)]
struct TagsResponse {
    #[serde(default)]
    models: Vec<TagModel>,
}

fn bounded_model_failures(errors: &[String]) -> String {
    const MAX_NAMES: usize = 5;
    let shown = errors
        .iter()
        .take(MAX_NAMES)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if errors.len() > MAX_NAMES {
        format!(
            "model details unavailable for {shown}, and {} more",
            errors.len() - MAX_NAMES
        )
    } else {
        format!("model details unavailable for {shown}")
    }
}
#[derive(Debug, Deserialize)]
struct TagModel {
    name: String,
}
#[derive(Debug, Default, Deserialize)]
struct ShowResponse {
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    model_info: serde_json::Map<String, serde_json::Value>,
}
#[derive(Debug)]
struct HostModels {
    account_id: String,
    models: Vec<(String, ShowResponse)>,
    errors: Vec<String>,
}

#[derive(Default)]
struct MergedModel {
    account_ids: Vec<String>,
    context_window: Option<i64>,
    supports_tools: bool,
    sees_images: bool,
    completes: bool,
    embeds: bool,
    /// The width an embedding model's vectors come back at.
    embedding_width: Option<u32>,
}
#[allow(dead_code)]
pub(crate) fn discover_models_blocking(
    orch: &Orchestrator,
) -> Result<Vec<DiscoveredModel>, String> {
    let (models, error) = discover_catalog_blocking(orch);
    if models.is_empty() {
        if let Some(error) = error {
            return Err(error);
        }
    }
    Ok(models)
}
pub(crate) fn discover_catalog_blocking(
    orch: &Orchestrator,
) -> (Vec<DiscoveredModel>, Option<String>) {
    let Some(store) = orch.get_credential_store() else {
        return (vec![], Some("No Ollama hosts configured".into()));
    };
    let hosts = store
        .accounts_for_provider(ApiProvider::Ollama, None)
        .into_iter()
        .filter_map(|a| match &a.auth {
            ProviderAuth::BaseUrl { url } => Some((a.id.clone(), a.label.clone(), url.clone())),
            _ => None,
        })
        .collect();
    discover_hosts_with_errors(hosts)
}
#[cfg(test)]
fn discover_hosts(hosts: Vec<(String, String, String)>) -> Result<Vec<DiscoveredModel>, String> {
    let (models, error) = discover_hosts_with_errors(hosts);
    if models.is_empty() {
        if let Some(error) = error {
            return Err(error);
        }
    }
    Ok(models)
}
fn discover_hosts_with_errors(
    hosts: Vec<(String, String, String)>,
) -> (Vec<DiscoveredModel>, Option<String>) {
    if hosts.is_empty() {
        return (vec![], Some("No Ollama hosts configured".into()));
    }
    let mut ok = Vec::new();
    let mut errors = Vec::new();
    for (id, label, url) in hosts {
        match discover_host(&id, &url) {
            Ok(v) => {
                if !v.errors.is_empty() {
                    errors.push(format!("{label}: {}", bounded_model_failures(&v.errors)));
                }
                ok.push(v)
            }
            Err(e) => errors.push(format!("{label}: {e}")),
        }
    }
    let error = (!errors.is_empty()).then(|| errors.join("; "));
    (merge_hosts(ok), error)
}
fn error_with_causes(context: &str, error: &dyn Error) -> String {
    let mut message = format!("{context}: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}

fn discover_host(account_id: &str, base_url: &str) -> Result<HostModels, String> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(DISCOVERY_TIMEOUT)
        .timeout(DISCOVERY_TIMEOUT)
        .build()
        .map_err(|e| error_with_causes("client failed", &e))?;
    let response = client
        .get(format!("{}/api/tags", base_url.trim_end_matches('/')))
        .send()
        .map_err(|e| error_with_causes("tags request failed", &e))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|e| error_with_causes("tags body failed", &e))?;
    if !status.is_success() {
        return Err(format!("tags returned HTTP {}: {}", status.as_u16(), body));
    }
    let tags: TagsResponse =
        serde_json::from_str(&body).map_err(|e| format!("tags JSON failed: {e}"))?;
    let mut models = Vec::new();
    let mut errors = Vec::new();
    for tag in tags.models {
        let result = (|| -> Result<ShowResponse, String> {
            let response = client
                .post(format!("{}/api/show", base_url.trim_end_matches('/')))
                .json(&serde_json::json!({"model": &tag.name}))
                .send()
                .map_err(|e| error_with_causes("request failed", &e))?;
            let status = response.status();
            let body = response
                .text()
                .map_err(|e| error_with_causes("body failed", &e))?;
            if !status.is_success() {
                return Err(format!("HTTP {}", status.as_u16()));
            }
            serde_json::from_str(&body).map_err(|e| format!("invalid JSON: {e}"))
        })();
        match result {
            Ok(show) => models.push((tag.name, show)),
            Err(_) => errors.push(tag.name),
        }
    }
    Ok(HostModels {
        account_id: account_id.into(),
        models,
        errors,
    })
}
fn context_length(info: &serde_json::Map<String, serde_json::Value>) -> Option<i64> {
    info.iter()
        .find_map(|(k, v)| k.ends_with(".context_length").then(|| v.as_i64()).flatten())
}
/// An embedding model's output width: `model_info["<arch>.embedding_length"]`,
/// where `<arch>` is its `general.architecture`. For a chat model the same key
/// is its hidden size, which is why it is read only for embedding models.
fn embedding_length(info: &serde_json::Map<String, serde_json::Value>) -> Option<u32> {
    let architecture = info.get("general.architecture")?.as_str()?;
    info.get(&format!("{architecture}.embedding_length"))?
        .as_u64()
        .and_then(|width| u32::try_from(width).ok())
}

/// What a model serves: nothing said for a chat model (the provider's
/// conversation claim applies), and its own claim for one that embeds, so an
/// embedding-only model never inherits a chat claim.
fn served(tag: &str, entry: &MergedModel) -> Option<Vec<ServedVerb>> {
    if !entry.embeds {
        return None;
    }
    let mut serves = if entry.completes {
        ServedVerb::conversation(Wire::OpenAiChatCompletions)
    } else {
        Vec::new()
    };
    // Ollama honours `dimensions` and re-normalises what it truncates, so a
    // Matryoshka family's shorter widths are offered.
    let limits = local_server_limits(
        entry
            .context_window
            .and_then(|window| u32::try_from(window).ok()),
    );
    serves.push(ServedVerb {
        verb: Verb::Embed(families::capabilities(
            tag,
            entry.embedding_width,
            true,
            limits,
        )),
        wire: Wire::OpenAiEmbeddings,
    });
    Some(serves)
}

fn merge_hosts(hosts: Vec<HostModels>) -> Vec<DiscoveredModel> {
    let mut merged: BTreeMap<String, MergedModel> = BTreeMap::new();
    for host in hosts {
        for (tag, show) in host.models {
            let completes = show.capabilities.iter().any(|c| c == "completion");
            let embeds = show.capabilities.iter().any(|c| c == "embedding");
            if !completes && !embeds {
                continue;
            }
            let entry = merged.entry(tag).or_default();
            let is_priority_host = entry.account_ids.is_empty();
            entry.account_ids.push(host.account_id.clone());
            if is_priority_host {
                // Discovery receives hosts in account priority order. Keep the
                // advertised runtime metadata aligned with the host routing will
                // select, while retaining every serving account for failover.
                entry.context_window = context_length(&show.model_info);
                entry.supports_tools = show.capabilities.iter().any(|c| c == "tools");
                entry.sees_images = show.capabilities.iter().any(|c| c == "vision");
                entry.completes = completes;
                entry.embeds = embeds;
                entry.embedding_width =
                    embeds.then(|| embedding_length(&show.model_info)).flatten();
            }
        }
    }
    merged
        .into_iter()
        .map(|(tag, entry)| DiscoveredModel {
            serves: served(&tag, &entry),
            // A GGUF conversion carries its own vocabulary, not guaranteed
            // identical to the repository's tokenizer.json, so no pin applies.
            tokenizer: None,
            id: tag.clone(),
            model: tag.clone(),
            display_name: tag,
            description: None,
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
            context_window: entry.context_window,
            canonical_slug: None,
            serving_account_ids: entry.account_ids,
            pricing: None,
            supported_parameters: if entry.supports_tools {
                vec!["tools".into()]
            } else {
                vec![]
            },
            router: false,
            // `vision` is Ollama's word for also reading images; a model gives
            // text when it completes and vectors when it embeds.
            modalities: Some(Modalities {
                input: if entry.sees_images {
                    vec![Modality::Text, Modality::Image]
                } else {
                    vec![Modality::Text]
                },
                output: [
                    entry.completes.then_some(Modality::Text),
                    entry.embeds.then_some(Modality::Embeddings),
                ]
                .into_iter()
                .flatten()
                .collect(),
            }),
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_partial_model_failure_notes() {
        let errors = (0..8).map(|i| format!("model-{i}")).collect::<Vec<_>>();
        let note = bounded_model_failures(&errors);
        assert!(note.contains("model-0, model-1, model-2, model-3, model-4"));
        assert!(note.contains("and 3 more"));
        assert!(!note.contains("model-5"));
    }

    fn show(w: i64, t: bool) -> ShowResponse {
        let mut capabilities = vec!["completion".into()];
        if t {
            capabilities.push("tools".into());
        }
        ShowResponse {
            capabilities,
            model_info: serde_json::from_value(serde_json::json!({"llama.context_length":w}))
                .unwrap(),
        }
    }

    /// `/api/show` for an embedding model, as Ollama 0.40 answers it.
    fn embedder(
        capabilities: &[&str],
        architecture: &str,
        width: u32,
        context: i64,
    ) -> ShowResponse {
        ShowResponse {
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            model_info: serde_json::from_value(serde_json::json!({
                "general.architecture": architecture,
                format!("{architecture}.embedding_length"): width,
                format!("{architecture}.context_length"): context,
            }))
            .unwrap(),
        }
    }

    fn embed_capabilities(
        model: &DiscoveredModel,
    ) -> cairn_models::types::embedding::EmbeddingCapabilities {
        model
            .serves
            .as_ref()
            .expect("an embedding model makes its own claim")
            .iter()
            .find_map(|served| match &served.verb {
                Verb::Embed(capabilities) => Some(capabilities.clone()),
                _ => None,
            })
            .expect("serves embed")
    }

    #[test]
    fn embedding_models_serve_only_embed_at_their_reported_width() {
        use cairn_models::types::catalog::VerbKind;
        use cairn_models::types::embedding::EmbeddingWidths;
        let models = merge_hosts(vec![HostModels {
            account_id: "host".into(),
            models: vec![
                ("chat".into(), show(8192, false)),
                (
                    "nomic-embed-text:latest".into(),
                    embedder(&["embedding"], "nomic-bert", 768, 2048),
                ),
                (
                    "qwen3-embedding:4b".into(),
                    embedder(&["tools", "embedding"], "qwen3", 2560, 40960),
                ),
                (
                    "neither".into(),
                    ShowResponse {
                        capabilities: vec!["insert".into()],
                        model_info: serde_json::Map::new(),
                    },
                ),
            ],
            errors: vec![],
        }]);
        assert_eq!(models.len(), 3);
        let find = |id: &str| models.iter().find(|model| model.id == id).unwrap();
        // A chat model is unchanged: no claim of its own.
        assert_eq!(find("chat").serves, None);
        assert_eq!(
            find("chat").modalities.as_ref().unwrap().output,
            vec![Modality::Text]
        );

        let nomic = find("nomic-embed-text:latest");
        let served = nomic.serves.as_ref().unwrap();
        assert_eq!(served.len(), 1);
        assert_eq!(served[0].wire, Wire::OpenAiEmbeddings);
        assert!(!served.iter().any(|s| s.verb.kind() == VerbKind::Session));
        assert_eq!(
            nomic.modalities.as_ref().unwrap().output,
            vec![Modality::Embeddings]
        );
        let capabilities = embed_capabilities(nomic);
        assert_eq!(capabilities.native_dims, Some(768));
        assert_eq!(capabilities.widths, EmbeddingWidths::UpTo { min: 64 });
        assert_eq!(capabilities.limits.max_input_tokens, Some(2048));

        let qwen = embed_capabilities(find("qwen3-embedding:4b"));
        assert_eq!(qwen.native_dims, Some(2560));
        assert_eq!(qwen.widths, EmbeddingWidths::UpTo { min: 32 });
    }

    /// What discovery makes of a real Ollama host's embedding models. Run with
    /// `--ignored --nocapture` beside a running Ollama.
    #[test]
    #[ignore = "reaches an Ollama host on localhost:11434"]
    fn live_embedding_discovery() {
        let models = discover_hosts(vec![(
            "local".into(),
            "Local".into(),
            "http://localhost:11434".into(),
        )])
        .unwrap();
        for model in &models {
            match model.serves.as_ref().and_then(|serves| {
                serves.iter().find_map(|served| match &served.verb {
                    Verb::Embed(capabilities) => Some(capabilities),
                    _ => None,
                })
            }) {
                Some(capabilities) => println!("embed {}: {}", model.id, capabilities.summary()),
                None => println!("chat  {}", model.id),
            }
        }
    }

    #[test]
    fn a_model_that_completes_and_embeds_serves_both() {
        let models = merge_hosts(vec![HostModels {
            account_id: "host".into(),
            models: vec![(
                "both".into(),
                embedder(&["completion", "embedding"], "llama", 4096, 8192),
            )],
            errors: vec![],
        }]);
        let served = models[0].serves.as_ref().unwrap();
        let kinds: Vec<_> = served.iter().map(|s| s.verb.kind().as_str()).collect();
        assert_eq!(kinds, ["complete", "session", "embed"]);
        // An unknown family takes Ollama's width and no shorter ones.
        assert_eq!(embed_capabilities(&models[0]).native_dims, Some(4096));
    }

    #[test]
    fn maps_capabilities_context_and_merges_hosts() {
        let m = merge_hosts(vec![
            HostModels {
                account_id: "a".into(),
                models: vec![("qwen".into(), show(32768, false))],
                errors: vec![],
            },
            HostModels {
                account_id: "b".into(),
                models: vec![
                    ("qwen".into(), show(131072, true)),
                    ("llama".into(), show(8192, false)),
                ],
                errors: vec![],
            },
        ]);
        // A tag installed on two hosts stays one selectable model, described by
        // the highest-priority host's runtime metadata.
        assert_eq!(m.len(), 2);
        let q = m.iter().find(|m| m.id == "qwen").unwrap();
        assert_eq!(q.context_window, Some(32768));
        assert!(q.supported_parameters.is_empty());
        assert_eq!(
            q.serving_account_ids,
            vec!["a".to_string(), "b".to_string()]
        );
        // Serving hosts are typed identity, never smuggled through canonical_slug.
        assert_eq!(q.canonical_slug, None);

        let llama = m.iter().find(|m| m.id == "llama").unwrap();
        assert_eq!(llama.serving_account_ids, vec!["b".to_string()]);
    }

    #[test]
    fn serving_hosts_follow_configured_account_priority() {
        let m = merge_hosts(vec![
            HostModels {
                account_id: "low".into(),
                models: vec![("qwen".into(), show(8192, false))],
                errors: vec![],
            },
            HostModels {
                account_id: "high".into(),
                models: vec![("qwen".into(), show(32768, false))],
                errors: vec![],
            },
        ]);
        let q = m.iter().find(|m| m.id == "qwen").unwrap();
        assert_eq!(
            q.serving_account_ids,
            vec!["low".to_string(), "high".to_string()]
        );
    }
    #[derive(Debug)]
    struct TestError {
        message: &'static str,
        source: Option<Box<TestError>>,
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl Error for TestError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source.as_deref().map(|error| error as &dyn Error)
        }
    }

    #[test]
    fn network_errors_include_the_underlying_cause() {
        let error = TestError {
            message: "error sending request",
            source: Some(Box::new(TestError {
                message: "tcp connect error",
                source: Some(Box::new(TestError {
                    message: "Connection refused",
                    source: None,
                })),
            })),
        };

        assert_eq!(
            error_with_causes("tags request failed", &error),
            "tags request failed: error sending request: tcp connect error: Connection refused"
        );
    }

    #[test]
    fn reports_empty_host_configuration() {
        assert!(discover_hosts(vec![])
            .unwrap_err()
            .contains("No Ollama hosts"));
    }

    #[test]
    fn aggregates_host_labels_for_discovery_errors() {
        let (models, error) = discover_hosts_with_errors(vec![
            ("a".into(), "Studio".into(), "://invalid-a".into()),
            ("b".into(), "Laptop".into(), "://invalid-b".into()),
        ]);
        assert!(models.is_empty());
        let error = error.expect("host errors");
        assert!(error.contains("Studio: tags request failed"));
        assert!(error.contains("Laptop: tags request failed"));
    }
}
