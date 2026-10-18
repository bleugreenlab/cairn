//! Native OpenCode Go HTTP backend.
//!
//! OpenCode Go is a set-price subscription ($10/month) over a curated line-up of
//! open coding models, metered against dollar-denominated rolling windows rather
//! than per-token billing. It sits beside OpenRouter as the cheap-faucet tier:
//! same shape (API key, discovered catalog, Cairn-owned turn/tool loop over
//! OpenAI-compatible HTTP), different economics.
//!
//! Go fronts THREE protocol families behind one base URL — `chat/completions`,
//! Anthropic-style `messages`, and OpenAI `responses` — and Cairn speaks all
//! three. Which one a model answers on is published per model as its AI SDK
//! package, recorded on the catalog entry by `models`, and routed on here before
//! any network I/O. That routing is an invariant rather than a convenience:
//! probed on 2026-08-16, a Responses model sent to `chat/completions` returns a
//! bare `503 Endpoint is unavailable`, and a Messages model sent there is
//! translated but leaks its raw `<think>` reasoning into the assistant's visible
//! text. Guessing the family produces a broken run either way.
//!
//! The generic turn/tool driver lives in `backends/http_loop`; each protocol
//! lives in its own module (`openai_compat`, `anthropic_messages`,
//! `openai_responses`). This module is Go's endpoints, its credential, and the
//! routing between them.

mod adapter;
mod http;
pub(crate) mod models;
mod usage;

#[cfg(test)]
mod tests;

pub use usage::collect_opencode_usage_snapshot;

use crate::backends::{
    AgentBackend, CompletionError, CompletionOutcome, CompletionRequest, CompletionRole,
    CompletionShape, ResolvedTools, SessionConfig,
};
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use cairn_models::types::catalog::{
    DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor, ProviderOptionKey,
    VerbKind, Wire,
};

/// The subscription is what Cairn serves, so the backend is named for it. The
/// credential is an OpenCode Zen account key, which is why the provider it
/// belongs to is [`ApiProvider::OpenCode`] rather than a Go-specific one.
pub(crate) use cairn_models::types::catalog::OPENCODE_BACKEND_KEY;
pub(crate) const OPENCODE_BACKEND_NAME: &str = "OpenCode Go";

/// The model a Go session falls back to when configuration names none. Go has
/// no auto-router to defer to, so this is a real choice: a mid-priced model with
/// a 1M context and the subscription's larger monthly allowance.
pub(crate) const DEFAULT_MODEL: &str = "glm-5.2";

#[derive(Debug, Clone, Copy)]
pub struct OpenCodeBackend;

pub(crate) fn opencode_api_key(orch: &Orchestrator) -> Option<String> {
    orch.get_credential_store().and_then(|store| {
        store
            .accounts_for_provider(ApiProvider::OpenCode, None)
            .into_iter()
            .find(|account| account.serves_backend(OPENCODE_BACKEND_KEY))
            .and_then(|account| match &account.auth {
                ProviderAuth::ApiKey { value } => Some(value.clone()),
                _ => None,
            })
    })
}

/// The discovered catalog's entry for a model, if discovery has one.
fn catalog_entry(orch: &Orchestrator, model: &str) -> Option<DiscoveredModel> {
    orch.get_model_catalog()
        .into_iter()
        .find(|catalog| catalog.backend == OPENCODE_BACKEND_KEY)
        .and_then(|catalog| {
            catalog
                .models
                .into_iter()
                .find(|entry| entry.model == model || entry.id == model)
        })
}

/// Why the catalog says a model cannot be served at all, if it says so.
///
/// The catalog already decided this — it marks such a model unselectable and
/// records the reason — so this reads that answer rather than deciding a second
/// time and risking a different one.
fn unservable_reason(entry: &DiscoveredModel, model: &str) -> Option<String> {
    if !entry.hidden {
        return None;
    }
    Some(
        entry
            .description
            .clone()
            .unwrap_or_else(|| format!("{model} is not served over an endpoint Cairn speaks yet.")),
    )
}

/// Why a model cannot run an agent SESSION, if it cannot.
///
/// Strictly stricter than [`unservable_reason`], because a session asks more of
/// a model than the catalog does. A Cairn agent drives its entire run through
/// the `read`/`write`/`run` tool calls, so a model whose published metadata says
/// it cannot call tools cannot run one — it would either be refused upstream or,
/// worse, complete a turn while unable to act.
///
/// This deliberately is NOT a catalog-level `hidden`, and NOT part of
/// `response_model_availability`: the same model remains genuinely useful for
/// the tool-free one-shot completions `complete` serves, and hiding it would
/// take that away to prevent a different mistake.
///
/// Metadata that does not positively claim tool calling counts as "cannot".
/// Every Go model published today claims it, so this only bites on an entry
/// whose metadata is absent or negative — and there the safe reading is the one
/// that fails before a run starts rather than during it.
fn session_blocker(entry: &DiscoveredModel, model: &str) -> Option<String> {
    if let Some(reason) = unservable_reason(entry, model) {
        return Some(reason);
    }
    if entry
        .supported_parameters
        .iter()
        .any(|parameter| parameter == "tools")
    {
        return None;
    }
    Some(format!(
        "{model} does not support tool calling, and a Cairn agent drives its whole run through the \
         read/write/run tools. Assign a tool-calling model to this tier."
    ))
}

/// The wire that serves `verb` for a model, resolved from the discovered
/// catalog BEFORE any network I/O.
///
/// Which endpoint a Go model answers on is not guessable from its id, and a
/// wrong guess is not a graceful failure: a Responses model sent to
/// `chat/completions` returns a bare 503, and a Messages model sent there
/// silently degrades. So a model Cairn cannot place is refused here, where the
/// message can say what to do about it, rather than at the first token.
///
/// Absent metadata is a refusal, never a default. OpenCode Go declares no
/// provider-wide wire, so reading "no wire recorded" as "chat completions" is
/// exactly the guess the per-model declaration exists to prevent.
fn model_wire(orch: &Orchestrator, model: &str, verb: VerbKind) -> Result<Wire, String> {
    let Some(entry) = catalog_entry(orch, model) else {
        return Err(format!(
            "{model} is not in the OpenCode Go model catalog, so Cairn cannot tell which endpoint \
             family serves it. Refresh the catalog in Settings → Providers, or pick a listed model."
        ));
    };
    let served = entry
        .serves
        .iter()
        .flatten()
        .find(|served| served.verb.kind() == verb);
    match served {
        Some(served) => Ok(served.wire),
        None => Err(unservable_reason(&entry, model).unwrap_or_else(|| {
            format!(
                "{model} does not publish an endpoint family Cairn has a mapping for, so Cairn \
                 cannot tell how to talk to it."
            )
        })),
    }
}

/// Merge a caller's `extras` object into a request body.
fn apply_extras(
    body: &mut serde_json::Value,
    extras: &serde_json::Value,
) -> Result<(), CompletionError> {
    if let Some(extras) = extras.as_object() {
        body.as_object_mut()
            .expect("completion body is an object")
            .extend(extras.clone());
        return Ok(());
    }
    if extras.is_null() {
        return Ok(());
    }
    Err(CompletionError::InvalidRequest(
        "extras must be an object or null".to_string(),
    ))
}

use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionReply, CompletionWire,
};

fn completion_role(role: &CompletionRole) -> &'static str {
    match role {
        CompletionRole::User => "user",
        CompletionRole::Assistant => "assistant",
    }
}

fn complete_chat(
    llm: &dyn Complete,
    api_key: &str,
    request: CompletionRequest,
    started: std::time::Instant,
) -> Result<CompletionOutcome, CompletionError> {
    use crate::backends::openai_compat::http::apply_output_schema;
    use serde_json::json;

    let requested_model = request.model.clone();
    let mut messages = Vec::with_capacity(request.messages.len() + 1);
    if let Some(system) = &request.system {
        messages.push(json!({"role": "system", "content": system}));
    }
    messages.extend(request.messages.iter().map(
        |message| json!({"role": completion_role(&message.role), "content": message.content}),
    ));
    let mut body = json!({
        "model": requested_model,
        "messages": messages,
        "stream": false,
    });
    apply_extras(&mut body, &request.extras)?;
    apply_output_schema(&mut body, request.output_schema.as_ref());

    let response = llm
        .complete(CompletionCall::whole(
            CompletionWire::Chat(&http::endpoint(api_key)),
            body,
            request.timeout,
        ))
        .and_then(CompletionReply::into_chat)?;
    let text = response
        .choices
        .first()
        .and_then(|choice| choice.message.content.as_ref())
        .and_then(|content| content.as_text())
        .ok_or_else(|| CompletionError::InvalidResponse("missing text choice".to_string()))?
        .to_string();
    Ok(CompletionOutcome {
        text,
        parsed: None,
        usage: response
            .usage
            .as_ref()
            .map(cairn_models::usage::metered)
            .unwrap_or_default(),
        model: response.model.unwrap_or(request.model),
        latency_ms: started.elapsed().as_millis() as u64,
        account_id: None,
    })
}

/// One-shot completion over Anthropic Messages.
///
/// The system prompt is a top-level field rather than a message, and
/// `max_tokens` is mandatory. This protocol has no native structured-output
/// constraint, so a schema-carrying request relies on the prompt plus Cairn's
/// own validation of the stored result — the same backstop the other families
/// depend on when a provider honors the constraint loosely.
fn complete_messages(
    llm: &dyn Complete,
    api_key: &str,
    request: CompletionRequest,
    started: std::time::Instant,
) -> Result<CompletionOutcome, CompletionError> {
    use crate::backends::anthropic_messages::wire::ContentBlock;
    use serde_json::json;

    let endpoint = http::messages_endpoint(api_key);
    let messages: Vec<serde_json::Value> = request
        .messages
        .iter()
        .map(|message| json!({"role": completion_role(&message.role), "content": message.content}))
        .collect();
    let mut body = json!({
        "model": request.model,
        "max_tokens": endpoint.max_output_tokens,
        "messages": messages,
        "stream": false,
    });
    if let Some(system) = &request.system {
        body["system"] = json!(system);
    }
    apply_extras(&mut body, &request.extras)?;

    let response = llm
        .complete(CompletionCall::whole(
            CompletionWire::Messages(&endpoint),
            body,
            request.timeout,
        ))
        .and_then(CompletionReply::into_messages)?;
    let text: String = response
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    if text.is_empty() {
        return Err(CompletionError::InvalidResponse(
            "response carried no text content".to_string(),
        ));
    }
    let cost = crate::backends::anthropic_messages::wire::parse_cost(response.cost.as_ref());
    Ok(CompletionOutcome {
        text,
        parsed: None,
        usage: response
            .usage
            .map(|usage| cairn_models::usage::metered(&usage.into_turn_usage(cost)))
            .unwrap_or_default()
            .with_cost(cost),
        model: response.model.unwrap_or(request.model),
        latency_ms: started.elapsed().as_millis() as u64,
        account_id: None,
    })
}

/// One-shot completion over OpenAI Responses.
fn complete_responses(
    llm: &dyn Complete,
    api_key: &str,
    request: CompletionRequest,
    started: std::time::Instant,
) -> Result<CompletionOutcome, CompletionError> {
    use crate::backends::openai_responses::http::apply_output_schema;
    use crate::backends::openai_responses::wire::{ContentPart, ResponsesItem};
    use serde_json::json;

    let input: Vec<serde_json::Value> = request
        .messages
        .iter()
        .map(|message| {
            // The input and output text spellings are distinct on this protocol
            // and are not interchangeable.
            let part_type = match message.role {
                CompletionRole::User => "input_text",
                CompletionRole::Assistant => "output_text",
            };
            json!({
                "type": "message",
                "role": completion_role(&message.role),
                "content": [{"type": part_type, "text": message.content}],
            })
        })
        .collect();
    let mut body = json!({
        "model": request.model,
        "input": input,
        "stream": false,
    });
    if let Some(system) = &request.system {
        body["instructions"] = json!(system);
    }
    apply_extras(&mut body, &request.extras)?;
    apply_output_schema(&mut body, request.output_schema.as_ref());

    let response = llm
        .complete(CompletionCall::whole(
            CompletionWire::Responses(&http::responses_endpoint(api_key)),
            body,
            request.timeout,
        ))
        .and_then(CompletionReply::into_responses)?;
    let text: String = response
        .output
        .iter()
        .filter_map(|item| match item {
            ResponsesItem::Message { content, .. } => Some(
                content
                    .iter()
                    .filter_map(ContentPart::text)
                    .collect::<String>(),
            ),
            _ => None,
        })
        .collect();
    if text.is_empty() {
        return Err(CompletionError::InvalidResponse(
            "response carried no text output".to_string(),
        ));
    }
    Ok(CompletionOutcome {
        text,
        parsed: None,
        usage: response
            .usage
            .map(|usage| cairn_models::usage::metered(&usage.into_turn_usage(response.cost)))
            .unwrap_or_default()
            .with_cost(response.cost),
        model: response.model.unwrap_or(request.model),
        latency_ms: started.elapsed().as_millis() as u64,
        account_id: None,
    })
}

impl AgentBackend for OpenCodeBackend {
    fn name(&self) -> &str {
        OPENCODE_BACKEND_NAME
    }

    fn response_completion_availability(
        &self,
        orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Result<(), String> {
        opencode_api_key(orch)
            .map(|_| ())
            .ok_or_else(|| "needs an OpenCode Go API key".to_string())
    }

    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        model: &str,
    ) -> Result<(), String> {
        self.response_completion_availability(orch, project_id)?;
        // A response is one tool-free completion, so this asks only whether the
        // model can be served at all — not whether it can call tools.
        match catalog_entry(orch, model).as_ref() {
            Some(entry) => match unservable_reason(entry, model) {
                Some(reason) => Err(reason),
                None => Ok(()),
            },
            None => Ok(()),
        }
    }

    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<CompletionOutcome, CompletionError> {
        if request.messages.is_empty() {
            return Err(CompletionError::InvalidRequest(
                "at least one message is required".to_string(),
            ));
        }
        let api_key = opencode_api_key(orch).ok_or(CompletionError::BackendUnavailable)?;
        // A one-shot completion reaches the same three endpoint families a
        // session does, so it routes the same way and for the same reason.
        let protocol = model_wire(orch, &request.model, VerbKind::Complete)
            .map_err(CompletionError::InvalidRequest)?;
        let started = std::time::Instant::now();
        let llm = orch.models_runtime().complete.as_ref();
        match protocol {
            Wire::OpenAiChatCompletions => complete_chat(llm, &api_key, request, started),
            Wire::AnthropicMessages => complete_messages(llm, &api_key, request, started),
            Wire::OpenAiResponses => complete_responses(llm, &api_key, request, started),
            _ => Err(CompletionError::InvalidRequest(format!(
                "{} is served over an endpoint family Cairn has no mapping for.",
                request.model
            ))),
        }
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::InProcess
    }

    fn is_available(&self) -> Result<(), String> {
        // Availability depends on a configured account in the owning
        // orchestrator; `start_session` performs the authoritative check. Being
        // optimistic here lets Settings show the provider card and its catalog
        // before a key is pasted.
        Ok(())
    }

    /// Go fronts several wires and names none by default, so a model
    /// discovery made no claim about serves nothing: absent metadata stays a
    /// refusal, never a guess at a family.
    fn serves(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Vec<cairn_models::types::catalog::ServedVerb> {
        Vec::new()
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        // Go answers its catalog unauthenticated, so the line-up is browsable
        // before anyone subscribes.
        models::discover_models_blocking(None)
    }

    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        // Go's models publish their own effort vocabularies, and the settings
        // surface prefers a selected model's discovered efforts over this list
        // (hiding the control entirely for models that expose none). This is the
        // fallback for a model with no catalog entry: the union Go publishes.
        vec![ProviderOptionDescriptor {
            key: ProviderOptionKey::ReasoningEffort,
            label: "Effort".to_string(),
            kind: OptionKind::Enum,
            choices: ["none", "low", "high", "max"]
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

    fn start_session(
        &self,
        config: SessionConfig,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<(), String> {
        // A model this session cannot actually run on fails here — naming the
        // missing tool calling, or the endpoint family Cairn cannot place —
        // rather than part-way through the first turn as an opaque upstream
        // error.
        let model = config
            .model
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        if let Some(entry) = catalog_entry(orch, &model) {
            if let Some(reason) = session_blocker(&entry, &model) {
                return Err(reason);
            }
        }
        // Each arm monomorphizes the generic turn loop with its own adapter, so
        // the protocol is settled once here and never re-decided per turn.
        match model_wire(orch, &model, VerbKind::Session)? {
            Wire::OpenAiChatCompletions => {
                crate::backends::http_loop::start_session(config, orch, adapter::OpenCodeAdapter)
            }
            Wire::AnthropicMessages => {
                crate::backends::http_loop::start_session(config, orch, adapter::messages_adapter())
            }
            Wire::OpenAiResponses => crate::backends::http_loop::start_session(
                config,
                orch,
                adapter::responses_adapter(),
            ),
            _ => Err(format!(
                "{model} is served over an endpoint family Cairn has no mapping for."
            )),
        }
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
        Ok(crate::backends::stateless_http_runtime_capability(
            "opencode-http",
        ))
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // Like OpenRouter, the whole agentic loop runs in-process over async
        // HTTP; a call spawns no child process.
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::InProcess,
            max_concurrency: None,
        }
    }
}
