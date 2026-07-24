//! Native OpenRouter HTTP backend.
//!
//! OpenRouter exposes an OpenAI-compatible chat completions API. Cairn owns the
//! turn/tool loop for this backend so models receive Cairn's direct read/write/run
//! tools instead of Codex/Claude host tools. The generic turn/tool driver lives
//! in `backends/http_loop`; this module is its OpenRouter adapter (`adapter.rs`)
//! plus the OpenRouter wire submodules the adapter owns (`wire`, `http`,
//! `conversation`, `context`).

mod adapter;
mod context;
mod conversation;
mod http;
mod models;
mod usage;
mod wire;

#[cfg(test)]
mod tests;

pub use usage::collect_openrouter_usage_snapshot;

use crate::agent_process::process::BackendStdin;
use crate::backends::{
    AgentBackend, DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor,
    ProviderOptionKey, ResolvedTools, SessionConfig,
};
use crate::identity::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use std::sync::atomic::Ordering;

pub(crate) const OPENROUTER_BACKEND_NAME: &str = "OpenRouter";
pub(crate) const OPENROUTER_BACKEND_KEY: &str = "openrouter";

#[derive(Debug, Clone, Copy)]
pub struct OpenRouterBackend;

pub(crate) fn openrouter_api_key(orch: &Orchestrator) -> Option<String> {
    orch.get_identity_store().and_then(|store| {
        store
            .accounts_for_provider(ApiProvider::OpenRouter, None)
            .into_iter()
            .find(|account| {
                account
                    .compatible_backends()
                    .contains(&OPENROUTER_BACKEND_KEY)
            })
            .and_then(|account| match &account.auth {
                ProviderAuth::ApiKey { value } => Some(value.clone()),
                _ => None,
            })
    })
}

impl AgentBackend for OpenRouterBackend {
    fn name(&self) -> &str {
        OPENROUTER_BACKEND_NAME
    }

    fn is_available(&self) -> Result<(), String> {
        // Availability depends on a configured account in the owning orchestrator;
        // `start_session` performs the authoritative check and records transcript
        // errors. Keeping this optimistic lets Settings show the provider card and
        // unauthenticated model catalog fallback.
        Ok(())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        models::discover_models_blocking(None)
    }

    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        vec![ProviderOptionDescriptor {
            key: ProviderOptionKey::ReasoningEffort,
            label: "Effort".to_string(),
            kind: OptionKind::Enum,
            choices: ["low", "medium", "high"]
                .into_iter()
                .map(|value| OptionChoice {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
            default: None,
        }]
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
        crate::backends::http_loop::start_session(
            config,
            orch,
            adapter::OpenRouterAdapter::new(orch),
        )
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        false
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // OpenRouter runs the whole agentic loop in-process over async HTTP; a
        // call spawns no child process. Unbounded today.
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }

    fn send_user_message(
        &self,
        _stdin: &mut dyn BackendStdin,
        _content: &str,
        _session_id: &str,
        _parent_tool_use_id: Option<&str>,
        _working_dir: Option<&str>,
    ) -> Result<(), String> {
        Err("OpenRouter HTTP turns do not keep a warm stdin; start a new run/turn".to_string())
    }

    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String> {
        // Flip the cancel flag the streaming turn polls at SSE line boundaries.
        // Dropping the response there closes the connection and stops billing.
        if let Some(s) = stdin
            .as_any_mut()
            .downcast_mut::<crate::backends::http_loop::HttpTurnStdin>()
        {
            s.cancel.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err("OpenRouter stdin unavailable".to_string())
        }
    }

    fn send_set_model(&self, _stdin: &mut dyn BackendStdin, _model: &str) -> Result<(), String> {
        Err("OpenRouter model changes apply to the next HTTP turn".to_string())
    }

    fn send_set_permission_mode(
        &self,
        _stdin: &mut dyn BackendStdin,
        _mode: &str,
    ) -> Result<(), String> {
        Err("OpenRouter permission changes apply to the next HTTP turn".to_string())
    }
}
