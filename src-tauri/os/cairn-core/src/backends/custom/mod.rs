//! Named custom providers: OpenAI-compatible HTTP services an operator runs.
//!
//! This is the escape hatch between the providers Cairn ships an opinion about
//! and the raw transport underneath. A workspace can name any number of them;
//! each one is a `ProviderAccount` under [`ApiProvider::Custom`] holding a
//! [`ProviderAuth::CustomEndpoint`], and its backend key is `custom:<account
//! id>`. That account IS the provider: its id is the provider's identity, its
//! label is the provider's name, and its auth is everything needed to reach it.
//!
//! Two consequences follow from keying on the account rather than on the URL or
//! the name, and both are the point:
//!
//! - Renaming a provider or moving it to a new address leaves every tier
//!   default, recorded job model, and finished session still resolving to it.
//! - Two custom providers can serve the same model id without colliding,
//!   because a selection is the pair `{backend, model}` and the backend half
//!   distinguishes them. Their model ids never need mangling to stay apart,
//!   which is what lets ids be preserved exactly.
//!
//! What a custom provider is NOT is machine administration. It names
//! infrastructure the operator already runs; Cairn does not launch it, download
//! weights into it, schedule its GPUs, or know anything about the host beyond
//! the address it answers on.

mod adapter;
pub(crate) mod models;
#[cfg(test)]
mod tests;

use crate::backends::{
    AgentBackend, CompletionError, CompletionOutcome, CompletionRequest, CompletionShape,
    ResolvedTools, SessionConfig,
};
use crate::credentials::{
    ApiProvider, CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth,
};
use crate::orchestrator::Orchestrator;
use cairn_models::interface::decide::DecisionRoute;
use cairn_models::types::catalog::DiscoveredModel;
use cairn_models::types::catalog::{self, ServedVerb, Verb, Wire};
use cairn_models::types::image::ImageCapabilities;
use cairn_models::wire::decisions::http::DecisionsEndpoint;
use cairn_models::wire::openai_compat::http::Endpoint;

/// One custom provider, resolved from the account that defines it.
///
/// Held together rather than passed around in pieces for the same reason the
/// credential is: an address, a protocol, and a set of headers only mean
/// anything as one endpoint.
#[derive(Debug, Clone)]
pub(crate) struct CustomProviderConfig {
    pub(crate) account_id: String,
    /// The name the operator gave this provider. This is what appears in
    /// transcripts, logs, and error text, so a failure names the thing they
    /// think they configured rather than an opaque id.
    pub(crate) display_name: String,
    pub(crate) base_url: String,
    pub(crate) protocol: CustomProtocol,
    /// Every header the request actually carries, credentials included.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) models: CustomModelPolicy,
    pub(crate) sampling: CustomSampling,
    pub(crate) max_output_tokens: i64,
    /// What an OpenAI Images provider can draw; `None` for every other
    /// protocol.
    pub(crate) images: Option<ImageCapabilities>,
}

impl CustomProviderConfig {
    /// Resolve the custom provider held by `account_id`, as visible from
    /// `project_id`.
    ///
    /// Scope is enforced by asking the store rather than by filtering after the
    /// fact: a provider private to one project is simply not found from
    /// another, so a stray backend key cannot reach across projects.
    pub(crate) fn resolve(
        orch: &Orchestrator,
        account_id: &str,
        project_id: Option<&str>,
    ) -> Result<Self, String> {
        let store = orch
            .get_credential_store()
            .ok_or_else(|| "no identity store".to_string())?;
        let account = store
            .accounts_for_provider(ApiProvider::Custom, project_id)
            .into_iter()
            .find(|account| account.id == account_id)
            .ok_or_else(|| {
                "this custom provider is no longer configured; add it again in Settings → Providers"
                    .to_string()
            })?;
        let ProviderAuth::CustomEndpoint {
            base_url,
            protocol,
            api_key,
            headers,
            models,
            sampling,
            max_output_tokens,
            ..
        } = &account.auth
        else {
            return Err(format!("account {account_id} is not a custom endpoint"));
        };
        let mut request_headers =
            vec![("content-type".to_string(), "application/json".to_string())];
        if let Some(key) = api_key {
            request_headers.push(("authorization".to_string(), format!("Bearer {key}")));
        }
        request_headers.extend(
            headers
                .iter()
                .map(|header| (header.name.clone(), header.value.clone())),
        );
        Ok(Self {
            account_id: account.id.clone(),
            display_name: account.label.clone(),
            base_url: base_url.clone(),
            protocol: *protocol,
            headers: request_headers,
            models: models.clone(),
            sampling: sampling.clone(),
            max_output_tokens: *max_output_tokens,
            images: account.auth.image_capabilities(),
        })
    }

    pub(crate) fn backend_key(&self) -> String {
        catalog::custom_key(&self.account_id)
    }

    /// The chat-completions URL. `base_url` is already the API root, so this is
    /// a single join rather than a guess about layout.
    pub(crate) fn chat_url(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    pub(crate) fn models_url(&self) -> String {
        format!("{}/models", self.base_url)
    }

    pub(crate) fn decisions_url(&self) -> String {
        format!("{}/decisions", self.base_url)
    }

    /// Refuse what a Decisions or Images provider cannot do: each serves its
    /// one verb, and has no agent turn or text completion to offer.
    pub(crate) fn require_turns(&self) -> Result<(), String> {
        match self.protocol {
            CustomProtocol::OpenAiChatCompletions => Ok(()),
            CustomProtocol::Decisions => Err(format!(
                "{} speaks the Decisions protocol, which answers decisions only; it cannot run an agent or a response. Choose a chat provider for this.",
                self.display_name
            )),
            CustomProtocol::OpenAiImages => Err(format!(
                "{} speaks the OpenAI Images protocol, which draws images only; it cannot run an agent or a response. Choose a chat provider for this.",
                self.display_name
            )),
        }
    }

    /// What this provider serves, fixed by its protocol. A chat server is
    /// offered for decisions as a logprobs route; whether it actually returns
    /// logprobs is learned from its first reply, and one that does not is
    /// recorded unable, never guessed at.
    pub(crate) fn serves(&self) -> Vec<ServedVerb> {
        let wire = self.wire();
        let decide = ServedVerb {
            verb: Verb::Decide,
            wire,
        };
        match self.protocol {
            CustomProtocol::OpenAiChatCompletions => {
                let mut serves = ServedVerb::conversation(wire);
                serves.push(decide);
                serves
            }
            CustomProtocol::Decisions => vec![decide],
            CustomProtocol::OpenAiImages => vec![ServedVerb {
                verb: Verb::Image(
                    self.images
                        .clone()
                        .unwrap_or_else(ImageCapabilities::openai_images_minimal),
                ),
                wire,
            }],
        }
    }

    /// The catalog's name for this provider's protocol: the one place a
    /// [`CustomProtocol`] becomes a [`Wire`].
    pub(crate) fn wire(&self) -> Wire {
        match self.protocol {
            CustomProtocol::OpenAiChatCompletions => Wire::OpenAiChatCompletions,
            CustomProtocol::Decisions => Wire::OpenAiDecisions,
            CustomProtocol::OpenAiImages => Wire::OpenAiImages,
        }
    }

    /// The deployment that measures this provider's decisions.
    pub(crate) fn decision_route(&self) -> Result<DecisionRoute, String> {
        let provider_name = self.display_name.clone();
        let backend_key = self.backend_key();
        let headers = self.headers.clone();
        match self.protocol {
            CustomProtocol::OpenAiChatCompletions => Ok(DecisionRoute::Logprobs(Endpoint {
                provider_name,
                backend_key,
                chat_url: self.chat_url(),
                headers,
            })),
            CustomProtocol::Decisions => Ok(DecisionRoute::Native(DecisionsEndpoint {
                provider_name,
                backend_key,
                url: self.decisions_url(),
                headers,
            })),
            CustomProtocol::OpenAiImages => Err(format!(
                "{} speaks the OpenAI Images protocol, which cannot answer a decision",
                self.display_name
            )),
        }
    }
}

/// The backend for one custom provider.
///
/// Carries only the account id, because [`crate::backends::backend_for_name`]
/// resolves a backend from a key alone and has no workspace to consult. Every
/// method that needs the provider's name, address, or credential is handed the
/// orchestrator and resolves the whole endpoint at that point; a provider
/// removed in between therefore fails with a message that says so rather than
/// running against stale configuration.
#[derive(Debug, Clone)]
pub struct CustomBackend {
    account_id: String,
    /// The backend key, kept whole so `name()` can return a borrow.
    key: String,
}

impl CustomBackend {
    pub(crate) fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            key: catalog::custom_key(account_id),
        }
    }

    fn config(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
    ) -> Result<CustomProviderConfig, String> {
        CustomProviderConfig::resolve(orch, &self.account_id, project_id)
    }
}

impl AgentBackend for CustomBackend {
    /// The backend key, not the operator's name: this is answered without a
    /// workspace to ask, and an id is at least unambiguous. Surfaces that show
    /// a provider to a person read the name from the provider catalog, which is
    /// the one inventory that knows it.
    fn name(&self) -> &str {
        &self.key
    }

    fn is_available(&self) -> Result<(), String> {
        Ok(())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        Err("a custom provider's models come from its configured endpoint".to_string())
    }

    /// A provider removed in between serves nothing.
    fn serves(&self, orch: &Orchestrator, project_id: Option<&str>) -> Vec<ServedVerb> {
        self.config(orch, project_id)
            .map(|config| config.serves())
            .unwrap_or_default()
    }

    fn response_completion_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
    ) -> Result<(), String> {
        self.config(orch, project_id)?.require_turns()
    }

    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        model: &str,
    ) -> Result<(), String> {
        let config = self.config(orch, project_id)?;
        // An explicitly listed provider can answer this from configuration
        // alone, so it stays usable before the first discovery pass and while
        // the endpoint is down.
        if let CustomModelPolicy::Explicit { ids } = &config.models {
            return if ids.iter().any(|id| id == model) {
                Ok(())
            } else {
                Err(format!("{} does not list {model}", config.display_name))
            };
        }
        let key = config.backend_key();
        let discovered = orch
            .get_model_catalog()
            .into_iter()
            .find(|entry| entry.backend == key && entry.error.is_none())
            .map(|entry| {
                entry
                    .models
                    .iter()
                    .any(|entry| entry.model == model || entry.id == model)
            });
        match discovered {
            // A cold catalog is ignorance, not absence. Refusing here would
            // make a freshly added provider unusable until something happened
            // to refresh it.
            None => Ok(()),
            Some(true) => Ok(()),
            Some(false) => Err(format!("{} is not serving {model}", config.display_name)),
        }
    }

    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<CompletionOutcome, CompletionError> {
        let config = self
            .config(orch, request.project_id.as_deref())
            .map_err(|_| CompletionError::BackendUnavailable)?;
        adapter::CustomAdapter::new(config)
            .complete(orch.models_runtime().complete.as_ref(), request)
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::InProcess
    }

    fn resolve_tools(&self, agent_tools: &[String], _agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;
        let mut allowed = toolkits::resolve_tools(agent_tools);
        toolkits::ensure_core_verbs(&mut allowed);
        // `apply_patch` is a Codex-native affordance, not a Cairn verb, and an
        // OpenAI-compatible server has no way to service it.
        allowed.retain(|tool| tool != "apply_patch");
        ResolvedTools {
            allowed,
            disallowed: Vec::new(),
        }
    }

    fn start_session(
        &self,
        config: SessionConfig,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<(), String> {
        let provider = self.config(orch, Some(&config.project_id))?;
        provider.require_turns()?;
        crate::backends::http_loop::start_session(
            config,
            orch,
            adapter::CustomAdapter::new(provider),
        )
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        false
    }

    fn runtime_launch_capability(
        &self,
        _launch: &crate::backends::RuntimeLaunch,
    ) -> Result<crate::backends::RuntimeLaunchCapability, String> {
        // Lanes are per provider, not shared across all custom ones: two
        // operators' endpoints are different machines with different capacity,
        // and pooling them would let a busy one throttle an idle one.
        Ok(crate::backends::stateless_http_runtime_capability(
            &format!("custom-http:{}", self.account_id),
        ))
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }
}
