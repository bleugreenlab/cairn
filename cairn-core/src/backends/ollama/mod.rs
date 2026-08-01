//! Native Ollama HTTP backend and local model discovery.

mod adapter;
pub(crate) mod models;

use crate::agent_process::process::BackendStdin;
use crate::backends::{AgentBackend, DiscoveredModel, ResolvedTools, SessionConfig};
use crate::identity::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use std::sync::atomic::Ordering;

pub(crate) const OLLAMA_BACKEND_KEY: &str = "ollama";
pub(crate) const OLLAMA_BACKEND_NAME: &str = "Ollama";

#[derive(Debug, Clone, Copy)]
pub struct OllamaBackend;

/// Resolve a model to the highest-priority configured host serving it. When the
/// catalog is cold, prefer the first configured host so first launch remains possible.
#[allow(dead_code)]
pub(crate) fn ollama_host_for_model(orch: &Orchestrator, model: &str) -> Option<(String, String)> {
    ollama_host_for_model_in_project(orch, model, None)
}

fn ollama_host_for_model_in_project(
    orch: &Orchestrator,
    model: &str,
    project_id: Option<&str>,
) -> Option<(String, String)> {
    let store = orch.get_identity_store()?;
    let accounts: Vec<_> = store
        .accounts_for_provider(ApiProvider::Ollama, project_id)
        .into_iter()
        .filter_map(|account| match &account.auth {
            ProviderAuth::BaseUrl { url } => Some((account.id.clone(), url.clone())),
            _ => None,
        })
        .collect();
    let served_ids = orch
        .get_model_catalog()
        .into_iter()
        .find(|c| c.backend == OLLAMA_BACKEND_KEY)
        .and_then(|catalog| {
            catalog
                .models
                .into_iter()
                .find(|entry| entry.model == model || entry.id == model)
        })
        .and_then(|entry| entry.canonical_slug);
    select_priority_host(accounts, served_ids.as_deref())
}

fn select_priority_host(
    accounts: Vec<(String, String)>,
    served_ids: Option<&str>,
) -> Option<(String, String)> {
    let first = accounts.first().cloned()?;
    let Some(served_ids) = served_ids else {
        return Some(first);
    };
    accounts
        .into_iter()
        .find(|(id, _)| served_ids.split(',').any(|served| served == id))
        .or(Some(first))
}

impl AgentBackend for OllamaBackend {
    fn name(&self) -> &str {
        OLLAMA_BACKEND_NAME
    }
    fn is_available(&self) -> Result<(), String> {
        Ok(())
    }
    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        Err("Ollama discovery requires configured hosts".to_string())
    }
    fn resolve_tools(&self, agent_tools: &[String], _agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;
        let mut allowed = toolkits::resolve_tools(agent_tools);
        toolkits::ensure_core_verbs(&mut allowed);
        allowed.retain(|tool| tool != "apply_patch");
        ResolvedTools {
            allowed,
            disallowed: Vec::new(),
        }
    }
    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
        let model = config
            .model
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let adapter = adapter::OllamaAdapter::new(orch, &model, Some(&config.project_id))?;
        crate::backends::http_loop::start_session(config, orch, adapter)
    }
    fn supports_resume(&self) -> bool {
        true
    }
    fn supports_warm_processes(&self) -> bool {
        false
    }
    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }
    fn send_user_message(
        &self,
        _: &mut dyn BackendStdin,
        _: &crate::agent_process::stdin::MessageContent,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<(), String> {
        Err("Ollama HTTP turns do not keep a warm stdin; start a new run/turn".to_string())
    }
    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String> {
        if let Some(s) = stdin
            .as_any_mut()
            .downcast_mut::<crate::backends::http_loop::HttpTurnStdin>()
        {
            s.cancel.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err("Ollama stdin unavailable".to_string())
        }
    }
    fn send_set_model(&self, _: &mut dyn BackendStdin, _: &str) -> Result<(), String> {
        Err("Ollama model changes apply to the next HTTP turn".to_string())
    }
    fn send_set_permission_mode(&self, _: &mut dyn BackendStdin, _: &str) -> Result<(), String> {
        Err("Ollama permission changes apply to the next HTTP turn".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::select_priority_host;

    fn hosts() -> Vec<(String, String)> {
        vec![
            ("high".into(), "http://high".into()),
            ("low".into(), "http://low".into()),
        ]
    }

    #[test]
    fn routing_uses_highest_priority_serving_host() {
        assert_eq!(
            select_priority_host(hosts(), Some("high,low")),
            Some(("high".into(), "http://high".into()))
        );
        assert_eq!(
            select_priority_host(hosts(), Some("low")),
            Some(("low".into(), "http://low".into()))
        );
    }

    #[test]
    fn routing_falls_back_to_first_host_when_catalog_is_cold_or_stale() {
        assert_eq!(
            select_priority_host(hosts(), None),
            Some(("high".into(), "http://high".into()))
        );
        assert_eq!(
            select_priority_host(hosts(), Some("removed")),
            Some(("high".into(), "http://high".into()))
        );
    }
}
