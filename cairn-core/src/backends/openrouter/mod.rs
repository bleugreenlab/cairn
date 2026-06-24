//! Native OpenRouter HTTP backend.
//!
//! OpenRouter exposes an OpenAI-compatible chat completions API. Cairn owns the
//! turn/tool loop for this backend so models receive Cairn's direct read/write/run
//! tools instead of Codex/Claude host tools.

mod models;
mod repair;
mod runtime;
mod usage;

pub use usage::collect_openrouter_usage_snapshot;

use crate::agent_process::process::BackendStdin;
use crate::backends::{
    AgentBackend, DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor,
    ProviderOptionKey, ResolvedTools, SessionConfig,
};
use crate::identity::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use std::any::Any;
use std::io::{Result as IoResult, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const OPENROUTER_BACKEND_NAME: &str = "OpenRouter";
pub const OPENROUTER_BACKEND_KEY: &str = "openrouter";

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
        runtime::start_session(config, orch)
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        false
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
        if let Some(s) = stdin.as_any_mut().downcast_mut::<NoopOpenRouterStdin>() {
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

pub(super) struct NoopOpenRouterStdin {
    pub(super) cancel: Arc<AtomicBool>,
}

impl Write for NoopOpenRouterStdin {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl BackendStdin for NoopOpenRouterStdin {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_interrupt_sets_cancel_flag() {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut stdin = NoopOpenRouterStdin {
            cancel: cancel.clone(),
        };
        OpenRouterBackend
            .send_interrupt(&mut stdin as &mut dyn BackendStdin)
            .expect("interrupt accepted");
        assert!(cancel.load(Ordering::SeqCst));
    }
}
