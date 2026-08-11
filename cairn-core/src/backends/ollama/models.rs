use crate::backends::DiscoveredModel;
use crate::identity::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
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
    let Some(store) = orch.get_identity_store() else {
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
fn merge_hosts(hosts: Vec<HostModels>) -> Vec<DiscoveredModel> {
    let mut merged: BTreeMap<String, MergedModel> = BTreeMap::new();
    for host in hosts {
        for (tag, show) in host.models {
            if !show
                .capabilities
                .iter()
                .any(|capability| capability == "completion")
            {
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
            }
        }
    }
    merged
        .into_iter()
        .map(|(tag, entry)| DiscoveredModel {
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
            architecture_modality: None,
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

    #[test]
    fn excludes_models_without_completion_capability() {
        let models = merge_hosts(vec![HostModels {
            account_id: "host".into(),
            models: vec![
                ("chat".into(), show(8192, false)),
                (
                    "embed".into(),
                    ShowResponse {
                        capabilities: vec!["embedding".into()],
                        model_info: serde_json::Map::new(),
                    },
                ),
            ],
            errors: vec![],
        }]);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model, "chat");
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
