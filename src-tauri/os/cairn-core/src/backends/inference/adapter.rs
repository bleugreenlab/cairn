//! The wire adapter for Cairn's inference deployments: the shared
//! OpenAI-compatible chat transport, pointed at the deployment a model name
//! resolves to once that deployment is ready, under the engine's own name for
//! the model.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionReply, CompletionWire,
};
use cairn_models::turn::{ChatFamily, Reroute};
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

/// The `Authorization` header every request to `route`'s engine carries: the
/// deployment's own key, which Cairn minted when it started it.
fn authorization(route: &Route) -> Option<(String, String)> {
    route
        .api_key
        .as_ref()
        .map(|key| ("authorization".to_string(), key.bearer()))
}

/// The chat endpoint of `route`'s deployment, which a decision uses too.
pub(super) fn endpoint(route: &Route) -> Endpoint {
    let mut headers = vec![("content-type".to_string(), "application/json".to_string())];
    headers.extend(authorization(route));
    Endpoint {
        provider_name: INFERENCE_BACKEND_NAME.to_string(),
        backend_key: INFERENCE_BACKEND_KEY.to_string(),
        chat_url: route.chat_url(),
        headers,
        withdrawal: Some(route.withdrawal.clone()),
    }
}

/// The embeddings endpoint of `route`'s deployment.
pub(super) fn embed_route(route: &Route) -> cairn_models::interface::embed::EmbedRoute {
    let mut embed = cairn_models::embed::openai::route_for(
        INFERENCE_BACKEND_KEY,
        INFERENCE_BACKEND_NAME,
        &route.api_base,
        route.api_key.as_ref().map(|key| key.expose()),
        &[],
    );
    if let cairn_models::interface::embed::EmbedRoute::OpenAi(endpoint) = &mut embed {
        endpoint.withdrawal = Some(route.withdrawal.clone());
    }
    embed
}

/// The relay route to `route`'s deployment: its API base and key, and the
/// engine's own name for the model, whatever name the client asked by. The
/// outside client's own credential is Cairn's to check, never forwarded.
pub(super) fn relay_route(route: &Route) -> cairn_models::interface::relay::RelayRoute {
    cairn_models::interface::relay::RelayRoute {
        base_url: route.api_base.clone(),
        headers: authorization(route).into_iter().collect(),
        backend_key: INFERENCE_BACKEND_KEY.to_string(),
        provider_name: INFERENCE_BACKEND_NAME.to_string(),
        sent_model: route.served_name.clone(),
        account_id: None,
        withdrawal: Some(route.withdrawal.clone()),
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
        reasoning: cairn_models::interface::complete::chat_reasoning_text(&response),
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

/// Record in a session's transcript what its turn waits on.
fn tell(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    content: &str,
    raw: serde_json::Value,
) {
    if let Err(error) =
        insert_system_message_sync(orch, run_id, Some(session_id), None, content, raw)
    {
        log::warn!("inference: could not record a model wait in run {run_id}: {error}");
    }
}

/// Wait, on the turn's own thread and interruptibly, until `model` is ready
/// for a session turn, saying in the transcript what the wait is on and,
/// when there was one, that the model is ready.
fn ready_for_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    model: &str,
    requester: &str,
    cancel: &AtomicBool,
) -> Result<Route, String> {
    let waited = Cell::new(false);
    let route = ready_blocking(
        orch,
        model,
        cairn_models::types::catalog::VerbKind::Session,
        requester,
        None,
        || cancel.load(Ordering::SeqCst),
        |said| {
            waited.set(true);
            tell(
                orch,
                run_id,
                session_id,
                &format!("Waiting for {model}: {said}"),
                json!({"inference": {"model": model, "waiting": said}}),
            );
        },
    )?;
    if waited.get() {
        tell(
            orch,
            run_id,
            session_id,
            &format!("{model} is ready on deployment {}", route.deployment),
            json!({"inference": {"model": model, "ready": route.deployment.as_str()}}),
        );
    }
    Ok(route)
}

/// How long a turn waits before it resends a generation the engine refused
/// for want of capacity, by how many times that generation was already sent
/// again. A 507 passes as the generations holding the engine's memory end,
/// which can take minutes, so the waits grow.
const CAPACITY_BACKOFF: [Duration; 3] = [
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(60),
];

/// The longest an engine's own `Retry-After` is waited out.
const MAX_CAPACITY_WAIT: Duration = Duration::from_secs(5 * 60);

/// How often a capacity wait looks at the turn's interrupt flag.
const CAPACITY_POLL: Duration = Duration::from_millis(100);

/// Sleep for `wait` on the turn's own thread, ending early, as an error,
/// once the turn is interrupted.
fn wait_for_capacity(cancel: &AtomicBool, wait: Duration) -> Result<(), String> {
    let until = Instant::now() + wait;
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err("interrupted while waiting for capacity".to_string());
        }
        let now = Instant::now();
        if now >= until {
            return Ok(());
        }
        std::thread::sleep(CAPACITY_POLL.min(until - now));
    }
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
        self.route = Some(ready_for_turn(
            orch,
            &config.run_id,
            session_id,
            &self.model,
            &self.requester,
            cancel,
        )?);
        Ok(())
    }

    /// A generation cut off because its deployment is being stopped is sent
    /// again wherever the model is served next, after the same visible,
    /// interruptible wait a turn's first generation has. One the engine
    /// refused for want of capacity (a 507 when its GPU memory is full) waits
    /// first, as visibly and interruptibly, for the engine's `Retry-After` or
    /// else a growing backoff, and then goes the same way.
    fn rerouter(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        cancel: &Arc<AtomicBool>,
    ) -> Option<Reroute<ChatFamily>> {
        let orch = orch.clone();
        let model = self.model.clone();
        let requester = self.requester.clone();
        let run_id = config.run_id.clone();
        let session_id = session_id.to_string();
        let cancel = Arc::clone(cancel);
        Some(Box::new(move |error: &CompletionError, resent: usize| {
            match error {
                CompletionError::Http {
                    message,
                    retry_after,
                    ..
                } => {
                    let wait = retry_after.map_or_else(
                        || CAPACITY_BACKOFF[resent.min(CAPACITY_BACKOFF.len() - 1)],
                        |asked| asked.min(MAX_CAPACITY_WAIT),
                    );
                    let said = format!("{message}; sending again in {}s", wait.as_secs());
                    tell(
                        &orch,
                        &run_id,
                        &session_id,
                        &format!("Waiting for {model}: {said}"),
                        json!({"inference": {"model": model, "waiting": said}}),
                    );
                    wait_for_capacity(&cancel, wait)?;
                }
                withdrawn => {
                    let why = withdrawn.to_string();
                    tell(
                        &orch,
                        &run_id,
                        &session_id,
                        &format!("{why}. Asking for {model} again."),
                        json!({"inference": {"model": model, "withdrawn": why}}),
                    );
                }
            }
            let route = ready_for_turn(&orch, &run_id, &session_id, &model, &requester, &cancel)?;
            Ok((chat_family(&route), route.served_name))
        }))
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
