//! The wire adapter for Cairn's inference deployments: the shared
//! OpenAI-compatible chat transport, pointed at the deployment a model name
//! resolves to once that deployment is ready, under the engine's own name for
//! the model.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionReply, CompletionWire,
};
use cairn_models::turn::ChatFamily;
use serde_json::{json, Map};

use super::{INFERENCE_BACKEND_KEY, INFERENCE_BACKEND_NAME};
use crate::backends::http_loop::{Connection, WireAdapter};
use crate::backends::openai_compat::http::{ChatDialect, ChatReasoning, Endpoint};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage};
use crate::backends::openai_compat::{completion_body, conversation};
use crate::backends::{CompletionError, CompletionOutcome, CompletionRequest, SessionConfig};
use crate::credentials::DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS;
use crate::messages::transcript::insert_system_message_sync;
use crate::orchestrator::inference_route::{ready_blocking, Route};
use crate::orchestrator::Orchestrator;

/// The chat endpoint of `route`'s deployment. The engines take no credential:
/// they answer only on the lab network their pool's record names.
pub(super) fn endpoint(route: &Route) -> Endpoint {
    Endpoint {
        provider_name: INFERENCE_BACKEND_NAME.to_string(),
        backend_key: INFERENCE_BACKEND_KEY.to_string(),
        chat_url: route.chat_url(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
    }
}

/// The relay route to `route`'s deployment: its API base, no credential, and
/// the engine's own name for the model, whatever name the client asked by.
pub(super) fn relay_route(route: &Route) -> cairn_models::interface::relay::RelayRoute {
    cairn_models::interface::relay::RelayRoute {
        base_url: route.api_base.clone(),
        headers: Vec::new(),
        backend_key: INFERENCE_BACKEND_KEY.to_string(),
        provider_name: INFERENCE_BACKEND_NAME.to_string(),
        sent_model: route.served_name.clone(),
        account_id: None,
    }
}

/// The chat family every generation on `route` is built from, an agent turn
/// and a one-shot completion alike. The output cap is the one a custom
/// provider defaults to, for the same reason: without one an OpenAI-compatible
/// server generates up to the rest of the context.
fn chat_family(route: &Route) -> ChatFamily {
    ChatFamily {
        endpoint: endpoint(route),
        dialect: ChatDialect {
            max_tokens: Some(DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS),
            include_usage: true,
            reasoning: ChatReasoning::Effort,
            response_format: false,
        },
        extras: Map::new(),
    }
}

/// One completion on `route`. The engine is sent its own name for the model;
/// the outcome names the model as the caller did.
pub(super) fn complete(
    llm: &dyn Complete,
    route: &Route,
    mut request: CompletionRequest,
) -> Result<CompletionOutcome, CompletionError> {
    let requested = std::mem::replace(&mut request.model, route.served_name.clone());
    let family = chat_family(route);
    let body = completion_body(&request, &family)?;
    let started = Instant::now();
    let response = llm
        .complete(CompletionCall::whole(
            CompletionWire::Chat(&family.endpoint),
            body,
            request.timeout,
        ))
        .and_then(CompletionReply::into_chat)?;
    let text = response
        .choices
        .first()
        .and_then(|choice| choice.message.content.as_ref())
        .and_then(ChatContent::as_text)
        .ok_or_else(|| CompletionError::InvalidResponse("missing text choice".to_string()))?
        .to_string();
    Ok(CompletionOutcome {
        text,
        parsed: None,
        // Inference on hardware the operator owns has no per-token price.
        usage: response
            .usage
            .as_ref()
            .map(cairn_models::usage::metered)
            .unwrap_or_default()
            .with_cost(None),
        model: requested,
        latency_ms: started.elapsed().as_millis() as u64,
        account_id: None,
    })
}

/// One agent turn's adapter. Its route is found on the turn's own thread
/// ([`WireAdapter::prepare_turn`]), so a launch never blocks on a load.
pub(super) struct InferenceAdapter {
    /// The model as the run names it.
    model: String,
    /// The run's home, which the request queue names it by.
    requester: String,
    route: Option<Route>,
}

impl InferenceAdapter {
    pub(super) fn new(config: &SessionConfig) -> Self {
        Self {
            model: config
                .model
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            requester: config.home_uri.clone(),
            route: None,
        }
    }

    fn route(&self) -> Result<&Route, String> {
        self.route
            .as_ref()
            .ok_or_else(|| format!("{} was not readied for this turn", self.model))
    }
}

impl WireAdapter for InferenceAdapter {
    type Family = ChatFamily;

    fn backend_key(&self) -> &str {
        INFERENCE_BACKEND_KEY
    }

    fn backend_name(&self) -> &str {
        INFERENCE_BACKEND_NAME
    }

    /// No default: every model here is a deployment's, and the run names it.
    fn default_model(&self) -> &'static str {
        ""
    }

    fn connection(&self, _: &Orchestrator) -> Result<Connection, String> {
        Ok(Connection { api_key: None })
    }

    fn prepare_turn(
        &mut self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        cancel: &AtomicBool,
    ) -> Result<(), String> {
        if self.model.trim().is_empty() {
            return Err(
                "Cairn inference has no default model; name the model a deployment serves"
                    .to_string(),
            );
        }
        let model = self.model.clone();
        let tell = |content: &str, raw: serde_json::Value| {
            if let Err(error) = insert_system_message_sync(
                orch,
                &config.run_id,
                Some(session_id),
                None,
                content,
                raw,
            ) {
                log::warn!("inference: could not record the wait for {model}: {error}");
            }
        };
        let waited = Cell::new(false);
        let route = ready_blocking(
            orch,
            &model,
            &self.requester,
            None,
            || cancel.load(Ordering::SeqCst),
            |said| {
                waited.set(true);
                tell(
                    &format!("Waiting for {model}: {said}"),
                    json!({"inference": {"model": model, "waiting": said}}),
                );
            },
        )?;
        if waited.get() {
            tell(
                &format!("{model} is ready on deployment {}", route.deployment),
                json!({"inference": {"model": model, "ready": route.deployment.as_str()}}),
            );
        }
        self.route = Some(route);
        Ok(())
    }

    fn wire_model(&self, model: &str) -> String {
        self.route
            .as_ref()
            .map_or_else(|| model.to_string(), |route| route.served_name.clone())
    }

    fn family(&self, _: &Connection) -> Result<ChatFamily, String> {
        Ok(chat_family(self.route()?))
    }

    fn build_conversation(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        system_prompt: &str,
    ) -> Result<Vec<ChatMessage>, String> {
        conversation::build_conversation_messages(orch, config, session_id, system_prompt)
    }

    /// The engine's measured context when the route was measured, else what
    /// the catalog last recorded.
    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64> {
        self.route
            .as_ref()
            .and_then(|route| route.context_length)
            .and_then(|tokens| i64::try_from(tokens).ok())
            .or_else(|| orch.context_window_for_context_tokens(INFERENCE_BACKEND_KEY, Some(model)))
    }
}
