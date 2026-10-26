//! Pure Response definition parsing, template rendering, and output validation.

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::backends::{
    backend_for_name, CompletionError, CompletionMessage, CompletionRequest, CompletionRole,
};
use crate::models::{Model, Preset, PresetOptionValue};
use crate::orchestrator::Orchestrator;
use crate::storage::NewResponseInvocation;
use cairn_models::types::catalog::VerbKind;

#[derive(Debug, Clone)]
pub enum ResponseCaller {
    Internal {
        label: String,
    },
    Agent {
        label: Option<String>,
        run_id: String,
        project_id: Option<String>,
        project_path: Option<std::path::PathBuf>,
    },
    Workflow {
        label: Option<String>,
        run_id: String,
        project_id: Option<String>,
        project_path: Option<std::path::PathBuf>,
    },
    /// A workflow AI condition node. It belongs to an execution, not a run.
    Condition {
        label: String,
        project_id: Option<String>,
        project_path: Option<std::path::PathBuf>,
    },
}

/// Invoke a built-in response definition without requiring a config file.
///
/// Built-ins use the same completion and response-history path as named
/// responses. The caller supplies a stable reserved id so their invocations
/// remain inspectable alongside user-authored responses.
pub async fn invoke_definition(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    response_id: &str,
    caller: ResponseCaller,
) -> Result<ResponseOutcome, ResponseError> {
    definition.validate().map_err(ResponseError::Upstream)?;
    let (caller_kind, caller_label, caller_run_id, project_id, project_path) = caller.fields();
    // Runtime eligibility is the backend's own account and model check, made
    // before anything launches, exactly as for a named response. The closed
    // catalog check (`validate_runnable_model`) is for authoring: a built-in's
    // exact pin need not be one the discovery catalog lists. A refusal is
    // journaled like any other failure, so history shows why it never ran.
    let attempt =
        complete_definition(orch, definition, args, project_id, project_path, None, None).await?;
    let now = chrono::Utc::now().timestamp_millis();
    let args_json = serde_json::to_string(args).ok();
    let scope_key = project_id
        .map(|id| format!("project:{id}"))
        .unwrap_or_else(|| "workspace".into());

    match attempt.result {
        Ok(outcome) => {
            let record = crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: response_id.to_string(),
                    scope_key,
                    project_id: project_id.map(str::to_owned),
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: attempt.rendered,
                    args_json,
                    status: "ok".into(),
                    output_text: Some(outcome.text.clone()),
                    error: None,
                    model: Some(outcome.model.clone()),
                    backend: Some(outcome.backend.clone()),
                    latency_ms: Some(outcome.latency_ms as i64),
                    input_tokens: outcome.input_tokens.map(|value| value as i64),
                    output_tokens: outcome.output_tokens.map(|value| value as i64),
                    cost: outcome.cost,
                    created_at: now,
                    account_id: outcome.account_id.clone(),
                },
            )
            .await
            .map_err(|error| {
                ResponseError::Upstream(format!("failed to persist response history: {error}"))
            })?;
            Ok(ResponseOutcome {
                seq: record.seq,
                text: outcome.text,
                parsed: outcome.parsed,
                model: outcome.model,
                backend: outcome.backend,
                latency_ms: outcome.latency_ms,
                cost: outcome.cost,
            })
        }
        Err(error) => {
            crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: response_id.to_string(),
                    scope_key,
                    project_id: project_id.map(str::to_owned),
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: attempt.rendered,
                    args_json,
                    status: "failed".into(),
                    output_text: None,
                    error: Some(error.to_string()),
                    model: Some(attempt.model.to_string()),
                    backend: Some(attempt.backend),
                    latency_ms: None,
                    input_tokens: None,
                    output_tokens: None,
                    cost: None,
                    created_at: now,
                    account_id: None,
                },
            )
            .await
            .map_err(|storage_error| {
                ResponseError::Upstream(format!(
                    "{error}; additionally failed to persist response history: {storage_error}"
                ))
            })?;
            Err(error)
        }
    }
}

fn saved_discovery_selection(
    backend: &str,
    model: &str,
    discovery_error: Option<&str>,
) -> Option<ResponseModelSelection> {
    discovery_error.map(|reason| ResponseModelSelection {
        kind: "model".into(),
        value: model.into(),
        label: format!("Exact · {model}"),
        backend: backend.into(),
        model: model.into(),
        legacy_values: Vec::new(),
        runnable: false,
        unavailable_reason: Some(format!("model discovery failed: {reason}")),
    })
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseModelCapabilities {
    pub backends: Vec<ResponseBackendCapability>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBackendCapability {
    pub backend: String,
    pub label: String,
    pub runnable: bool,
    pub unavailable_reason: Option<String>,
    pub selections: Vec<ResponseModelSelection>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseModelSelection {
    pub kind: String,
    pub value: String,
    pub label: String,
    pub backend: String,
    pub model: String,
    pub legacy_values: Vec<String>,
    pub runnable: bool,
    pub unavailable_reason: Option<String>,
}

/// Backend-owned response capability projected with the effective presets and
/// discovered models that can be authored in this scope.
pub fn model_capabilities(
    orch: &Orchestrator,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
    saved: Option<&ResponseDefinition>,
) -> ResponseModelCapabilities {
    let presets = crate::config::presets::load_effective_presets(&orch.config_dir, project_path);
    let catalog = orch.get_model_catalog();
    let mut backends = Vec::new();
    // Enabled, not supported: a response is authored to run next, so it may
    // only offer providers this workspace installed.
    let enabled = orch.enabled_providers();
    // Names come from the workspace's provider inventory rather than from the
    // backend adapter, which is the one place that knows what an operator
    // called a provider they defined themselves.
    let inventory = orch.provider_catalog();
    for backend_name in enabled.iter().map(String::as_str) {
        let backend = backend_for_name(Some(backend_name));
        // What each model serves, as the facet answers it at the launch door:
        // a response is a completion, so a model that does not serve one (an
        // image model, an embedder) is never a runnable choice.
        let provider = orch.catalog_provider(backend_name, project_id);
        let completes = |model: &str| provider.admit(model, VerbKind::Complete).map(|_| ());
        let availability = backend.response_completion_availability(orch, project_id);
        let mut selections = Vec::new();
        if let Some(backend_presets) = presets.backends.get(backend_name) {
            for tier in &presets.tiers {
                if let Some(preset) = backend_presets.get(tier) {
                    // A tier bound to a model that cannot complete stays
                    // listed, refused with why, rather than silently replaced.
                    let model_availability = completes(preset.model.as_str())
                        .map_err(|refusal| refusal.to_string())
                        .and_then(|()| {
                            backend.response_model_availability(
                                orch,
                                project_id,
                                preset.model.as_str(),
                            )
                        });
                    let legacy_values = crate::config::presets::resolve_preset(tier, &presets)
                        .ok()
                        .filter(|resolved| resolved.backend == backend_name)
                        .map(|_| vec![tier.clone()])
                        .unwrap_or_default();
                    selections.push(ResponseModelSelection {
                        kind: "tier".into(),
                        value: format!("{backend_name}/{tier}"),
                        label: format!("{} · {}", tier.to_uppercase(), preset.model),
                        backend: backend_name.into(),
                        model: preset.model.to_string(),
                        legacy_values,
                        runnable: availability.is_ok() && model_availability.is_ok(),
                        unavailable_reason: model_availability.err(),
                    });
                }
            }
        }
        if let Some(entry) = catalog
            .iter()
            .find(|entry| entry.backend == backend_name && entry.error.is_none())
        {
            for model in entry
                .models
                .iter()
                .filter(|model| !model.hidden && provider.serves(&model.model, VerbKind::Complete))
            {
                let model_availability =
                    backend.response_model_availability(orch, project_id, &model.model);
                selections.push(ResponseModelSelection {
                    kind: "model".into(),
                    value: model.model.clone(),
                    label: format!("Exact · {}", model.display_name),
                    backend: backend_name.into(),
                    model: model.model.clone(),
                    legacy_values: Vec::new(),
                    runnable: availability.is_ok() && model_availability.is_ok(),
                    unavailable_reason: model_availability.err(),
                });
            }
        }
        if let Some(saved_model) = saved
            .filter(|definition| definition.backend.as_deref() == Some(backend_name))
            .and_then(|definition| definition.model.as_deref())
        {
            let already_represented = selections
                .iter()
                .any(|selection| selection.kind == "model" && selection.model == saved_model);
            let discovery_error = catalog
                .iter()
                .find(|entry| entry.backend == backend_name)
                .and_then(|entry| entry.error.as_deref());
            if !already_represented {
                if let Some(selection) =
                    saved_discovery_selection(backend_name, saved_model, discovery_error)
                {
                    selections.push(selection);
                } else if let Err(refusal) = completes(saved_model) {
                    // Represented so validation can say why it cannot run,
                    // naming the model and what it serves instead.
                    selections.push(ResponseModelSelection {
                        kind: "model".into(),
                        value: saved_model.into(),
                        label: format!("Exact · {saved_model}"),
                        backend: backend_name.into(),
                        model: saved_model.into(),
                        legacy_values: Vec::new(),
                        runnable: false,
                        unavailable_reason: Some(refusal.to_string()),
                    });
                }
            }
        }
        backends.push(ResponseBackendCapability {
            backend: backend_name.into(),
            label: inventory
                .iter()
                .find(|entry| entry.key == backend_name)
                .map(|entry| entry.display_name.to_string())
                .unwrap_or_else(|| backend.name().to_string()),
            runnable: availability.is_ok(),
            unavailable_reason: availability.err(),
            selections,
        });
    }

    ResponseModelCapabilities { backends }
}

pub fn validate_runnable_model(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
) -> Result<(), String> {
    let capabilities = model_capabilities(orch, project_id, project_path, Some(definition));
    if is_offered_model_selection(definition, &capabilities) {
        Ok(())
    } else {
        Err(unrunnable_reason(definition, &capabilities)
            .unwrap_or_else(|| "Model configuration is not a runnable selection".into()))
    }
}

/// Whether `selection`, offered by `backend`, is the one `definition` authored.
fn selects(
    definition: &ResponseDefinition,
    backend: &str,
    selection: &ResponseModelSelection,
) -> bool {
    if let Some(model) = &definition.model {
        definition.backend.as_deref() == Some(backend)
            && selection.kind == "model"
            && selection.model == *model
    } else {
        let tier = definition.tier.as_deref().unwrap_or("sm");
        selection.kind == "tier"
            && (selection.value == tier
                || selection.legacy_values.iter().any(|legacy| legacy == tier))
    }
}

fn is_offered_model_selection(
    definition: &ResponseDefinition,
    capabilities: &ResponseModelCapabilities,
) -> bool {
    capabilities.backends.iter().any(|candidate| {
        candidate.runnable
            && candidate.selections.iter().any(|selection| {
                selection.runnable && selects(definition, &candidate.backend, selection)
            })
    })
}

/// Why the authored selection can't run, as its backend or the selection itself
/// reports it. `None` when no enabled backend offers the selection at all.
fn unrunnable_reason(
    definition: &ResponseDefinition,
    capabilities: &ResponseModelCapabilities,
) -> Option<String> {
    capabilities.backends.iter().find_map(|candidate| {
        candidate
            .selections
            .iter()
            .find(|selection| selects(definition, &candidate.backend, selection))
            .and_then(|selection| {
                candidate
                    .unavailable_reason
                    .clone()
                    .or_else(|| selection.unavailable_reason.clone())
            })
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseOutcome {
    pub seq: i64,
    pub text: String,
    pub parsed: Option<Value>,
    pub model: String,
    pub backend: String,
    pub latency_ms: u64,
    pub cost: Option<f64>,
}

/// A caller's bound on a whole model call, and what decided it.
///
/// A definition's `timeout` bounds each provider request. A caller that must
/// itself answer within some window (a `run` item executes on the host and
/// cannot suspend) bounds the whole call as well. When that bound is what ends
/// the call, the failure says so with the numbers, so neither the definition
/// nor the model is blamed for a limit neither of them set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationBound {
    pub limit: Duration,
    /// Present when a cap above the caller set `limit`, rather than the
    /// caller's own ask.
    pub cap: Option<BoundCap>,
}

/// A limit above the caller that overrode what it asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCap {
    /// What the cap is called where the reader meets it.
    pub name: &'static str,
    /// What the caller asked for: more than the cap, or `None` for nothing.
    pub requested: Option<Duration>,
    /// Why the cap holds, for a reader deciding whether to retry.
    pub why: &'static str,
}

impl InvocationBound {
    /// "timed out at ...": the bound, what set it, and each timeout the reader
    /// might have expected to govern instead.
    pub fn describe(&self, declared: Option<Duration>) -> String {
        let limit = seconds(self.limit);
        let mut context = Vec::new();
        let mut text = match &self.cap {
            Some(cap) => {
                context.push(cap.requested.map_or_else(
                    || "no timeout was requested".to_string(),
                    |requested| format!("requested {}", seconds(requested)),
                ));
                format!("timed out at the {limit} {}", cap.name)
            }
            None => format!("timed out at the {limit} bound its caller requested"),
        };
        if let Some(declared) = declared {
            context.push(format!("the definition declares {}", seconds(declared)));
        }
        if !context.is_empty() {
            text.push_str(&format!(" ({})", context.join("; ")));
        }
        text.push('.');
        if let Some(cap) = &self.cap {
            text.push(' ');
            text.push_str(cap.why);
        }
        text
    }
}

fn seconds(duration: Duration) -> String {
    if duration.subsec_millis() == 0 {
        format!("{} s", duration.as_secs())
    } else {
        format!("{:.1} s", duration.as_secs_f64())
    }
}

impl InvocationBound {
    /// What the caller itself asked for: the ask a cap overrode, or the limit
    /// when no cap did.
    pub(crate) fn requested(&self) -> Option<Duration> {
        match &self.cap {
            Some(cap) => cap.requested,
            None => Some(self.limit),
        }
    }
}

/// Why a caller ended its invocation before the invocation ended itself.
///
/// An [`InvocationBound`] is fixed when the call starts. Some callers only
/// learn mid-call that the answer is no longer wanted, or that they can no
/// longer carry it, and interrupt instead. Either way the invocation ends as
/// an outcome and is journaled as one. The provider request it abandons is
/// detached on its blocking thread, which cannot be cancelled, and ends
/// within its own timeout: the definition's, or what remained of the bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Interruption {
    /// Nobody is waiting for the answer any more. The reason is journaled.
    Cancelled(String),
    /// A window above the caller closed before the call's own bound did. The
    /// call ends exactly as if this cap had been its bound, so its failure
    /// reads like any other bound's and names what the caller had asked for.
    Capped {
        limit: Duration,
        name: &'static str,
        why: &'static str,
    },
}

impl Interruption {
    fn into_error(self, bound: Option<&InvocationBound>, declared: Duration) -> ResponseError {
        match self {
            Self::Cancelled(reason) => ResponseError::Cancelled(reason),
            Self::Capped { limit, name, why } => ResponseError::OutlivedBound {
                bound: InvocationBound {
                    limit,
                    cap: Some(BoundCap {
                        name,
                        requested: bound.and_then(InvocationBound::requested),
                        why,
                    }),
                },
                declared,
            },
        }
    }
}

/// The sending half of an interruption: fires once, for every invocation
/// holding one of its [`InvocationInterrupt`]s. A second interruption is
/// ignored, so the first reason is the one every invocation journals.
pub struct Interrupter(tokio::sync::watch::Sender<Option<Interruption>>);

/// The receiving half an invocation watches. Cloned into each invocation of
/// one caller; an interrupter dropped without firing never fires.
#[derive(Clone)]
pub struct InvocationInterrupt(tokio::sync::watch::Receiver<Option<Interruption>>);

pub fn interruption() -> (Interrupter, InvocationInterrupt) {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    (Interrupter(sender), InvocationInterrupt(receiver))
}

impl Interrupter {
    pub fn interrupt(&self, interruption: Interruption) {
        self.0.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(interruption);
            true
        });
    }
}

impl InvocationInterrupt {
    /// The interruption, once one has fired.
    pub fn fired(&self) -> Option<Interruption> {
        self.0.borrow().clone()
    }

    /// Resolve once an interruption fires; never, if its interrupter is
    /// dropped without firing.
    pub(crate) async fn wait(&mut self) -> Interruption {
        // The read guard is released before any further await, which keeps
        // this future `Send`.
        let fired = self
            .0
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|fired| fired.clone());
        match fired {
            Some(interruption) => interruption,
            None => std::future::pending().await,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResponseError {
    #[error("unknown response '{0}'")]
    UnknownResponse(String),
    #[error("missing required variable '{0}'")]
    MissingVariable(String),
    #[error("undeclared variable '{0}'")]
    UndeclaredVariable(String),
    #[error("response backend is unavailable: {0}")]
    BackendUnavailable(String),
    #[error("response timed out")]
    Timeout,
    /// The caller's bound on the whole call ran out before the definition's
    /// own timeout did.
    #[error("response {}", .bound.describe(Some(.declared.to_owned())))]
    OutlivedBound {
        bound: InvocationBound,
        declared: Duration,
    },
    /// Its caller interrupted it because nobody is waiting for the answer.
    #[error("response cancelled: {0}")]
    Cancelled(String),
    #[error("response output violated its contract: {0}")]
    ContractViolation(String),
    #[error("response failed: {0}")]
    Upstream(String),
    /// The model does not serve completions; the facet's refusal, verbatim.
    #[error("{0}")]
    Unserved(String),
}

impl ResponseCaller {
    fn fields(
        &self,
    ) -> (
        &'static str,
        Option<&str>,
        Option<&str>,
        Option<&str>,
        Option<&std::path::Path>,
    ) {
        match self {
            Self::Internal { label } => ("internal", Some(label.as_str()), None, None, None),
            Self::Agent {
                label,
                run_id,
                project_id,
                project_path,
            } => (
                "agent",
                label.as_deref(),
                Some(run_id.as_str()),
                project_id.as_deref(),
                project_path.as_deref(),
            ),
            Self::Workflow {
                label,
                run_id,
                project_id,
                project_path,
            } => (
                "workflow",
                label.as_deref(),
                Some(run_id.as_str()),
                project_id.as_deref(),
                project_path.as_deref(),
            ),
            // Journaled as a workflow caller: the workflow engine asked, and
            // the reserved response id already sets conditions apart.
            Self::Condition {
                label,
                project_id,
                project_path,
            } => (
                "workflow",
                Some(label.as_str()),
                None,
                project_id.as_deref(),
                project_path.as_deref(),
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTestOutcome {
    pub text: String,
    pub parsed: Option<Value>,
    pub model: String,
    pub backend: String,
    pub latency_ms: u64,
    pub cost: Option<f64>,
    #[serde(skip)]
    input_tokens: Option<u64>,
    #[serde(skip)]
    output_tokens: Option<u64>,
    /// The account that served it, for the journal.
    #[serde(skip)]
    account_id: Option<String>,
}

struct CompletionAttempt {
    rendered: String,
    model: Model,
    backend: String,
    result: Result<ResponseTestOutcome, ResponseError>,
    /// The provider request still running after its caller was released.
    /// Releasing the caller always ends the attempt in an error.
    detached: Option<DetachedRequest>,
}

/// Why a caller stopped waiting for its provider request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Release {
    /// Interrupted: nobody is waiting for the answer any more.
    Cancelled,
    /// Interrupted: a window above the caller closed.
    Capped,
    /// The caller's own bound ran out.
    Bound,
}

impl Release {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Capped => "capped",
            Self::Bound => "bound",
        }
    }
}

impl Interruption {
    fn release(&self) -> Release {
        match self {
            Self::Cancelled(_) => Release::Cancelled,
            Self::Capped { .. } => Release::Capped,
        }
    }
}

type CompletionJoin = tokio::task::JoinHandle<(
    Result<crate::backends::CompletionOutcome, ResponseError>,
    std::time::Instant,
)>;

/// A provider request its caller stopped waiting for, which runs on its
/// blocking thread until it returns or its own timeout fires.
struct DetachedRequest {
    /// Resolves to the request's outcome and the instant it returned.
    task: CompletionJoin,
    released_at: std::time::Instant,
    release: Release,
    backend: String,
    model: String,
    /// The request's own timeout: the definition's, or what remained of the
    /// bound when it was sent, whichever was shorter.
    held_to: Duration,
}

impl DetachedRequest {
    /// Log one line when the request returns: how long it kept the provider
    /// busy after its caller left, and how it ended. The journal records the
    /// release but never sees this end.
    fn watch(self, response_id: &str, journal_id: Option<&str>) {
        let response_id = response_id.to_owned();
        let journal_id = journal_id.unwrap_or("none").to_owned();
        tokio::spawn(async move {
            let (ended, returned_at) = match self.task.await {
                Ok((Ok(_), returned_at)) => ("result", returned_at),
                Ok((Err(ResponseError::Timeout), returned_at)) => ("timeout", returned_at),
                Ok((Err(_), returned_at)) => ("error", returned_at),
                // A panicked request stamped no instant of its own.
                Err(_) => ("error", std::time::Instant::now()),
            };
            log::info!(
                "detached response request returned response_id={response_id} journal_id={journal_id} backend={} model={} reason={} tail_ms={} ended={ended} timeout_ms={}",
                self.backend,
                self.model,
                self.release.as_str(),
                returned_at
                    .saturating_duration_since(self.released_at)
                    .as_millis(),
                self.held_to.as_millis(),
            );
        });
    }
}

/// Complete a supplied response definition without loading or persisting config,
/// response history, route state, or any other invocation record.
pub async fn test_definition(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
) -> Result<ResponseTestOutcome, ResponseError> {
    definition.validate().map_err(ResponseError::Upstream)?;
    validate_runnable_model(orch, definition, project_id, project_path)
        .map_err(ResponseError::BackendUnavailable)?;
    complete_definition(orch, definition, args, project_id, project_path, None, None)
        .await?
        .result
}

/// Invoke one named, tool-free model completion. Named invocations journal their
/// outcome after running through the same one-shot pipeline used by draft tests.
///
/// `bound` is the caller's limit on the whole call. It is enforced here rather
/// than around this future, because a call cut off from outside never reaches
/// its journal write: running out of `bound` is an outcome like any other, and
/// is recorded as one. `interrupt` ends the call early on the caller's say-so
/// and is journaled the same way.
pub async fn invoke(
    orch: &Orchestrator,
    id: &str,
    args: &Value,
    caller: ResponseCaller,
    bound: Option<&InvocationBound>,
    interrupt: Option<&InvocationInterrupt>,
) -> Result<ResponseOutcome, ResponseError> {
    let (caller_kind, caller_label, caller_run_id, project_id, project_path) = caller.fields();
    let file = crate::config::responses::get_response(&orch.config_dir, id, project_path)
        .map_err(ResponseError::Upstream)?
        .ok_or_else(|| ResponseError::UnknownResponse(id.to_string()))?;
    let definition_project_id = if file.is_project_scoped {
        Some(
            project_id
                .ok_or_else(|| {
                    ResponseError::Upstream(
                        "project-scoped response invoked without a project id".into(),
                    )
                })?
                .to_string(),
        )
    } else {
        None
    };
    let scope_key = definition_project_id
        .as_ref()
        .map(|id| format!("project:{id}"))
        .unwrap_or_else(|| "workspace".into());
    let attempt = complete_definition(
        orch,
        &file.definition,
        args,
        project_id,
        project_path,
        bound,
        interrupt,
    )
    .await?;
    let now = chrono::Utc::now().timestamp_millis();
    let args_json = serde_json::to_string(args).ok();

    match attempt.result {
        Ok(outcome) => {
            let record = crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: id.to_string(),
                    scope_key,
                    project_id: definition_project_id,
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: attempt.rendered,
                    args_json,
                    status: "ok".into(),
                    output_text: Some(outcome.text.clone()),
                    error: None,
                    model: Some(outcome.model.clone()),
                    backend: Some(outcome.backend.clone()),
                    latency_ms: Some(outcome.latency_ms as i64),
                    input_tokens: outcome.input_tokens.map(|value| value as i64),
                    output_tokens: outcome.output_tokens.map(|value| value as i64),
                    cost: outcome.cost,
                    created_at: now,
                    account_id: outcome.account_id.clone(),
                },
            )
            .await
            .map_err(|error| {
                ResponseError::Upstream(format!("failed to persist response history: {error}"))
            })?;
            Ok(ResponseOutcome {
                seq: record.seq,
                text: outcome.text,
                parsed: outcome.parsed,
                model: outcome.model,
                backend: outcome.backend,
                latency_ms: outcome.latency_ms,
                cost: outcome.cost,
            })
        }
        Err(error) => {
            let journaled = crate::storage::insert_response_invocation(
                &orch.db.local,
                NewResponseInvocation {
                    response_id: id.to_string(),
                    scope_key,
                    project_id: definition_project_id,
                    caller_kind: caller_kind.into(),
                    caller_label: caller_label.map(str::to_owned),
                    caller_run_id: caller_run_id.map(str::to_owned),
                    rendered_prompt: attempt.rendered,
                    args_json,
                    status: "failed".into(),
                    output_text: None,
                    error: Some(error.to_string()),
                    model: Some(attempt.model.to_string()),
                    backend: Some(attempt.backend),
                    latency_ms: None,
                    input_tokens: None,
                    output_tokens: None,
                    cost: None,
                    created_at: now,
                    account_id: None,
                },
            )
            .await;
            if let Some(detached) = attempt.detached {
                detached.watch(id, journaled.as_ref().ok().map(|row| row.id.as_str()));
            }
            journaled.map_err(|storage_error| {
                ResponseError::Upstream(format!(
                    "{error}; additionally failed to persist response history: {storage_error}"
                ))
            })?;
            Err(error)
        }
    }
}

async fn complete_definition(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
    bound: Option<&InvocationBound>,
    interrupt: Option<&InvocationInterrupt>,
) -> Result<CompletionAttempt, ResponseError> {
    #[cfg(test)]
    if let Some(executor) = test_completion::executor_for(&orch.config_dir) {
        return complete_definition_with_executor(
            orch,
            definition,
            args,
            project_id,
            project_path,
            bound,
            interrupt,
            executor,
        )
        .await;
    }
    complete_definition_with_executor(
        orch,
        definition,
        args,
        project_id,
        project_path,
        bound,
        interrupt,
        Arc::new(|backend_name, request, orch| {
            // Run-time eligibility, in order: the workspace has the provider
            // installed, an account can serve it, and (at the launch door) the
            // model serves completions. Catalog membership is an authoring
            // concern and is not checked.
            if !orch.provider_enabled(&backend_name) {
                return Err(ResponseError::BackendUnavailable(format!(
                    "the {backend_name} provider isn't enabled in this workspace; enable it in Settings → Providers"
                )));
            }
            let backend = backend_for_name(Some(&backend_name));
            backend
                .response_completion_availability(orch, request.project_id.as_deref())
                .map_err(ResponseError::BackendUnavailable)?;
            backend
                .response_model_availability(orch, request.project_id.as_deref(), &request.model)
                .map_err(ResponseError::BackendUnavailable)?;
            crate::backends::launch::complete(orch, &backend_name, backend.as_ref(), request)
                .map_err(|error| match error {
                    crate::backends::launch::LaunchError::Unserved(refusal) => {
                        ResponseError::Unserved(refusal.to_string())
                    }
                    crate::backends::launch::LaunchError::Completion(error) => {
                        map_completion_error(error)
                    }
                })
        }),
    )
    .await
}

type CompletionExecutor = Arc<
    dyn Fn(
            String,
            CompletionRequest,
            &Orchestrator,
        ) -> Result<crate::backends::CompletionOutcome, ResponseError>
        + Send
        + Sync,
>;

#[allow(clippy::too_many_arguments)]
async fn complete_definition_with_executor(
    orch: &Orchestrator,
    definition: &ResponseDefinition,
    args: &Value,
    project_id: Option<&str>,
    project_path: Option<&std::path::Path>,
    bound: Option<&InvocationBound>,
    interrupt: Option<&InvocationInterrupt>,
    executor: CompletionExecutor,
) -> Result<CompletionAttempt, ResponseError> {
    let deadline = bound.map(|bound| (tokio::time::Instant::now() + bound.limit, bound));
    let mut interrupt = interrupt.cloned();
    let interrupted =
        |interruption: Interruption| interruption.into_error(bound, definition.timeout);
    let outlived = |bound: &InvocationBound| ResponseError::OutlivedBound {
        bound: bound.clone(),
        declared: definition.timeout,
    };
    let rendered = definition.render(args).map_err(classify_render_error)?;
    let (model, extras, backend_name) = if let Some(model) = &definition.model {
        let preset = Preset {
            model: Model::new(model),
            options: definition.options.clone(),
        };
        (
            preset.model.clone(),
            preset.to_extras(),
            definition.backend.clone().expect("validated exact pin"),
        )
    } else {
        let presets =
            crate::config::presets::load_effective_presets(&orch.config_dir, project_path);
        let tier = definition.tier.as_deref().unwrap_or("sm");
        match crate::config::presets::resolve_preset(tier, &presets) {
            Ok(preset) => (preset.model, preset.extras, preset.backend),
            // No enabled provider serves the tier: a refusal, returned as an
            // attempt so the caller journals it under the authored tier.
            Err(error) => {
                return Ok(CompletionAttempt {
                    rendered,
                    model: Model::new(tier),
                    backend: crate::config::presets::parse_tier_ref(tier)
                        .0
                        .unwrap_or_default()
                        .to_string(),
                    result: Err(ResponseError::BackendUnavailable(format!(
                        "no enabled provider serves the '{tier}' tier ({error})"
                    ))),
                    detached: None,
                })
            }
        }
    };
    let output_schema = definition
        .output_schema()
        .map_err(ResponseError::Upstream)?;
    let mut messages = Vec::with_capacity(definition.examples.len() * 2 + 1);
    for example in &definition.examples {
        let prompt = definition
            .render(&Value::Object(example.input.clone()))
            .map_err(classify_render_error)?;
        messages.push(CompletionMessage {
            role: CompletionRole::User,
            content: prompt,
        });
        messages.push(CompletionMessage {
            role: CompletionRole::Assistant,
            content: example_output_text(&example.output, output_schema.as_ref())
                .map_err(ResponseError::Upstream)?,
        });
    }
    messages.push(CompletionMessage {
        role: CompletionRole::User,
        content: rendered.clone(),
    });
    let mut request = CompletionRequest {
        system: None,
        messages,
        model: model.to_string(),
        project_id: project_id.map(str::to_string),
        options: extras,
        output_schema: output_schema.clone(),
        timeout: definition.timeout,
    };
    let mut terminal_error = None;
    let mut accepted = None;
    let mut detached = None;
    for attempt in 0..=1 {
        if let Some(interruption) = interrupt.as_ref().and_then(InvocationInterrupt::fired) {
            terminal_error = Some(interrupted(interruption));
            break;
        }
        let mut request_for_attempt = request.clone();
        // A request handed to a blocking thread cannot be cancelled, so each one
        // is held to what remains of the caller's bound: abandoned at the
        // deadline, it ends on its own soon after instead of running out the
        // definition's full timeout.
        let mut shortened_by_bound = false;
        if let Some((deadline, _)) = deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining < request_for_attempt.timeout {
                request_for_attempt.timeout = remaining;
                shortened_by_bound = true;
            }
        }
        let backend_for_attempt = backend_name.clone();
        let orch_for_attempt = orch.clone();
        let executor_for_attempt = executor.clone();
        let held_to = request_for_attempt.timeout;
        // The request stamps when it returned, so a detached one's tail is
        // measured from its own end, not from whenever it is next awaited.
        let mut task = tokio::task::spawn_blocking(move || {
            let completion =
                executor_for_attempt(backend_for_attempt, request_for_attempt, &orch_for_attempt);
            (completion, std::time::Instant::now())
        });
        // Whichever ends the call first: the request, the caller's bound, or
        // the caller interrupting. A request that lands at the same instant
        // wins, because its answer is already paid for.
        let ended_early = async {
            tokio::select! {
                () = async {
                    match deadline {
                        Some((deadline, _)) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => (Release::Bound, outlived(deadline.expect("a deadline fired").1)),
                interruption = async {
                    match interrupt.as_mut() {
                        Some(interrupt) => interrupt.wait().await,
                        None => std::future::pending().await,
                    }
                } => (interruption.release(), interrupted(interruption)),
            }
        };
        let joined = tokio::select! {
            biased;
            joined = &mut task => joined,
            (release, error) = ended_early => {
                detached = Some(DetachedRequest {
                    task,
                    released_at: std::time::Instant::now(),
                    release,
                    backend: backend_name.clone(),
                    model: model.to_string(),
                    held_to,
                });
                terminal_error = Some(error);
                break;
            }
        };
        let completion = match joined {
            Ok((completion, _)) => completion,
            Err(error) => {
                terminal_error = Some(ResponseError::Upstream(format!(
                    "completion task failed: {error}"
                )));
                break;
            }
        };
        match completion {
            Ok(outcome) => {
                match definition.validate_output(&outcome.text, output_schema.as_ref()) {
                    Ok(validated) => {
                        let parsed = match validated {
                            ValidatedOutput::Text(_) => None,
                            ValidatedOutput::Json(value) => Some(value),
                        };
                        accepted = Some(ResponseTestOutcome {
                            text: outcome.text,
                            parsed,
                            model: outcome.model,
                            backend: backend_name.clone(),
                            latency_ms: outcome.latency_ms,
                            cost: outcome.usage.cost_usd,
                            input_tokens: outcome.usage.prompt_tokens(),
                            output_tokens: outcome.usage.quantity(
                                cairn_models::types::usage::Charge::Output,
                                cairn_models::types::usage::BillingUnit::Tokens,
                            ),
                            account_id: outcome.account_id,
                        });
                        break;
                    }
                    Err(error) if attempt == 0 => {
                        request.messages.push(CompletionMessage {
                            role: CompletionRole::Assistant,
                            content: outcome.text,
                        });
                        request.messages.push(CompletionMessage {
                        role: CompletionRole::User,
                        content: format!("Your previous answer violated the required output contract: {error}. Return a corrected answer only."),
                    });
                        terminal_error = Some(ResponseError::ContractViolation(error));
                    }
                    Err(error) => terminal_error = Some(ResponseError::ContractViolation(error)),
                }
            }
            // The provider's timeout fired, but it was the bound's, not the
            // definition's: name the bound, exactly as if its timer had won.
            Err(ResponseError::Timeout) if shortened_by_bound => {
                terminal_error = deadline.map(|(_, bound)| outlived(bound));
                break;
            }
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        }
    }
    Ok(CompletionAttempt {
        rendered,
        model,
        backend: backend_name,
        result: accepted.ok_or_else(|| {
            terminal_error
                .unwrap_or_else(|| ResponseError::Upstream("completion produced no result".into()))
        }),
        detached,
    })
}

fn classify_render_error(error: String) -> ResponseError {
    if let Some(name) = error
        .strip_prefix("Missing required variable '")
        .and_then(|s| s.strip_suffix('\''))
    {
        ResponseError::MissingVariable(name.into())
    } else if let Some(name) = error
        .strip_prefix("Undeclared variable '")
        .and_then(|s| s.strip_suffix('\''))
    {
        ResponseError::UndeclaredVariable(name.into())
    } else {
        ResponseError::Upstream(error)
    }
}

fn map_completion_error(error: CompletionError) -> ResponseError {
    match error {
        CompletionError::BackendUnavailable => {
            ResponseError::BackendUnavailable("one-shot completion is unsupported".into())
        }
        CompletionError::Timeout => ResponseError::Timeout,
        other => ResponseError::Upstream(other.to_string()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseDefinition {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, PresetOptionValue>,
    #[serde(default)]
    pub variables: Vec<ResponseVariable>,
    #[serde(default)]
    pub output: OutputContract,
    #[serde(default = "default_timeout", with = "duration_seconds")]
    pub timeout: Duration,
    #[serde(default)]
    pub examples: Vec<ResponseExample>,
    #[serde(skip)]
    pub template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseVariable {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponseExample {
    pub input: Map<String, Value>,
    pub output: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum OutputContract {
    Named(String),
    Schema(Value),
}

impl Default for OutputContract {
    fn default() -> Self {
        Self::Named("text".into())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidatedOutput {
    Text(String),
    Json(Value),
}

fn default_timeout() -> Duration {
    Duration::from_secs(15)
}

/// The longest timeout a definition may declare: the ceiling on one `run`
/// batch, which a response item can suspend all the way up to. Past it the
/// definition would promise `run` a bound the batch ceiling cuts first.
pub const MAX_RESPONSE_TIMEOUT: Duration =
    Duration::from_millis(cairn_common::run_contract::RUN_BATCH_CEILING_MS as u64);

mod duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}s", value.as_secs()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let raw = String::deserialize(deserializer)?;
        let seconds = raw
            .strip_suffix('s')
            .ok_or_else(|| {
                serde::de::Error::custom("timeout must be expressed in seconds, for example 15s")
            })?
            .parse::<u64>()
            .map_err(serde::de::Error::custom)?;
        Ok(Duration::from_secs(seconds))
    }
}

pub fn parse_definition(markdown: &str) -> Result<ResponseDefinition, String> {
    let (frontmatter, template) = crate::markdown_frontmatter::split_yaml_frontmatter(markdown)?;
    let mut definition: ResponseDefinition = serde_yaml::from_str(frontmatter)
        .map_err(|e| format!("Failed to parse frontmatter: {e}"))?;
    definition.template = template;
    definition.validate()?;
    Ok(definition)
}

impl ResponseDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("Response name cannot be empty".into());
        }
        if self.description.trim().is_empty() {
            return Err("Response description cannot be empty".into());
        }
        if self.template.trim().is_empty() {
            return Err("Response template cannot be empty".into());
        }
        if self.model.is_some() && self.tier.is_some() {
            return Err("Response must declare either 'model' or 'tier', not both".into());
        }
        if self.model.is_some() && self.backend.is_none() {
            return Err("Response with an exact 'model' pin must declare 'backend'".into());
        }
        if self.model.is_none() && self.backend.is_some() {
            return Err("Response 'backend' is only valid with an exact 'model' pin".into());
        }
        if self.timeout > MAX_RESPONSE_TIMEOUT {
            return Err(format!(
                "Response timeout {}s is past the {}s ceiling on a single `run` batch ({} hours), which no call of it from `run` could reach; declare at most {}s",
                self.timeout.as_secs(),
                MAX_RESPONSE_TIMEOUT.as_secs(),
                cairn_common::run_contract::RUN_BATCH_CEILING_HOURS,
                MAX_RESPONSE_TIMEOUT.as_secs()
            ));
        }

        let mut declared = HashSet::new();
        for variable in &self.variables {
            if variable.name.is_empty()
                || !variable
                    .name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(format!("Invalid variable name '{}'", variable.name));
            }
            if !declared.insert(variable.name.as_str()) {
                return Err(format!("Duplicate variable '{}'", variable.name));
            }
            if variable.required && variable.default.is_some() {
                return Err(format!(
                    "Required variable '{}' cannot have a default",
                    variable.name
                ));
            }
            if let Some(default) = &variable.default {
                if !variable.values.is_empty() && !variable.values.contains(default) {
                    return Err(format!(
                        "Variable '{}' default is not one of its declared values",
                        variable.name
                    ));
                }
            }
        }
        for referenced in template_variables(&self.template) {
            if !declared.contains(referenced.as_str()) {
                return Err(format!(
                    "Template references undeclared variable '{referenced}'"
                ));
            }
        }
        if let OutputContract::Named(name) = &self.output {
            if name != "text" && !crate::output_schemas::is_preset_schema(name) {
                return Err(format!("Unknown output contract '{name}'"));
            }
        } else if let OutputContract::Schema(schema) = &self.output {
            jsonschema::validator_for(schema)
                .map_err(|e| format!("Invalid output JSON Schema: {e}"))?;
        }
        let output_schema = self.output_schema()?;
        for (index, example) in self.examples.iter().enumerate() {
            self.render(&Value::Object(example.input.clone()))?;
            example_output_text(&example.output, output_schema.as_ref())
                .map_err(|e| format!("Example {}: {e}", index + 1))?;
        }
        Ok(())
    }

    /// The JSON Schema this definition's output must satisfy, or `None` for a
    /// plain-text contract. Presets resolve from the embedded registry.
    pub fn output_schema(&self) -> Result<Option<Value>, String> {
        match &self.output {
            OutputContract::Named(name) if name == "text" => Ok(None),
            OutputContract::Named(name) => crate::output_schemas::resolve_output_schema(
                None,
                &crate::models::OutputSchema::Preset(name.clone()),
            )
            .map(Some),
            OutputContract::Schema(schema) => Ok(Some(schema.clone())),
        }
    }

    pub fn render(&self, args: &Value) -> Result<String, String> {
        let args = args
            .as_object()
            .ok_or_else(|| "Response arguments must be a JSON object".to_string())?;
        let variables: HashMap<&str, &ResponseVariable> = self
            .variables
            .iter()
            .map(|v| (v.name.as_str(), v))
            .collect();
        for name in args.keys() {
            if !variables.contains_key(name.as_str()) {
                return Err(format!("Undeclared variable '{name}'"));
            }
        }
        let mut values = HashMap::new();
        for variable in &self.variables {
            match args.get(&variable.name).or(variable.default.as_ref()) {
                Some(value) => {
                    if !variable.values.is_empty() && !variable.values.contains(value) {
                        return Err(format!(
                            "Variable '{}' must be one of its declared values",
                            variable.name
                        ));
                    }
                    values.insert(variable.name.as_str(), render_value(value));
                }
                None if variable.required => {
                    return Err(format!("Missing required variable '{}'", variable.name))
                }
                None => {
                    values.insert(variable.name.as_str(), String::new());
                }
            }
        }
        Ok(template_regex()
            .replace_all(&self.template, |captures: &regex::Captures<'_>| {
                values
                    .get(captures.get(1).unwrap().as_str())
                    .cloned()
                    .unwrap_or_default()
            })
            .into_owned())
    }

    pub fn validate_output(
        &self,
        text: &str,
        preset_schema: Option<&Value>,
    ) -> Result<ValidatedOutput, String> {
        let schema = match &self.output {
            OutputContract::Named(name) if name == "text" => {
                return Ok(ValidatedOutput::Text(text.to_string()))
            }
            OutputContract::Named(_) => {
                preset_schema.ok_or_else(|| "Preset output schema was not resolved".to_string())?
            }
            OutputContract::Schema(schema) => schema,
        };
        let value: Value =
            serde_json::from_str(text).map_err(|e| format!("Output is not valid JSON: {e}"))?;
        let validator = jsonschema::validator_for(schema)
            .map_err(|e| format!("Invalid output JSON Schema: {e}"))?;
        validator
            .validate(&value)
            .map_err(|e| format!("Output contract violation: {e}"))?;
        Ok(ValidatedOutput::Json(value))
    }
}

/// The assistant turn a few-shot example contributes.
///
/// Under a JSON contract the example must be a JSON instance of the schema
/// (written inline or as a JSON string), and it is written in the schema-order
/// contract, the order a grammar-constrained provider enforces. An example in
/// any other key order would teach the model a sequence the grammar truncates,
/// silently dropping the optional keys listed before the one it wrote; one that
/// violates the schema teaches output the contract refuses. Both are rejected
/// or corrected here rather than left for output validation, which cannot see
/// a foreclosed optional key.
fn example_output_text(output: &Value, schema: Option<&Value>) -> Result<String, String> {
    let Some(schema) = schema else {
        return Ok(output
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| output.to_string()));
    };
    let parsed;
    let instance = match output {
        Value::String(text) => {
            parsed = serde_json::from_str::<Value>(text)
                .map_err(|e| format!("output must be JSON under a JSON contract: {e}"))?;
            &parsed
        }
        other => other,
    };
    jsonschema::validator_for(schema)
        .map_err(|e| format!("Invalid output JSON Schema: {e}"))?
        .validate(instance)
        .map_err(|e| format!("output violates the output contract: {e}"))?;
    cairn_models::wire::schema_order::ordered_instance_json(instance, schema)
        .map_err(|e| format!("output has no key order the output contract enforces: {e}"))
}

fn template_regex() -> Regex {
    Regex::new(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}").unwrap()
}
fn template_variables(template: &str) -> Vec<String> {
    template_regex()
        .captures_iter(template)
        .map(|c| c[1].to_string())
        .collect()
}
fn render_value(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

/// A stand-in provider for tests that drive [`invoke`] from outside this
/// module, keyed by the orchestrator's config directory so concurrent tests
/// cannot answer for each other. Keyed rather than task-local because callers
/// such as a suspended `run` batch invoke from a task of their own.
#[cfg(test)]
pub(crate) mod test_completion {
    use super::CompletionExecutor;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{LazyLock, Mutex};

    static EXECUTORS: LazyLock<Mutex<HashMap<PathBuf, CompletionExecutor>>> =
        LazyLock::new(Default::default);

    pub(crate) fn install(config_dir: &Path, executor: CompletionExecutor) {
        EXECUTORS
            .lock()
            .unwrap()
            .insert(config_dir.to_path_buf(), executor);
    }

    pub(super) fn executor_for(config_dir: &Path) -> Option<CompletionExecutor> {
        EXECUTORS.lock().unwrap().get(config_dir).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::CompletionOutcome;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use serde_json::json;
    use std::sync::Mutex;

    async fn test_orchestrator() -> Orchestrator {
        let temp = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open_migrated_for_test(temp.join("responses.db"))
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.join("config"),
        )
        .build()
    }

    #[tokio::test]
    async fn capabilities_keep_canonical_tiers_when_providers_are_unavailable() {
        let orch = test_orchestrator().await;
        let capabilities = model_capabilities(&orch, None, None, None);
        let selections: Vec<_> = capabilities
            .backends
            .iter()
            .flat_map(|backend| backend.selections.iter())
            .collect();

        assert!(selections
            .iter()
            .any(|selection| selection.value == "openrouter/md"));
        assert!(selections.iter().any(|selection| {
            selection.kind == "tier" && selection.legacy_values.iter().any(|value| value == "md")
        }));
        assert!(capabilities
            .backends
            .iter()
            .filter(|backend| !backend.runnable)
            .flat_map(|backend| backend.selections.iter())
            .all(|selection| !selection.runnable));
    }

    /// A response is a completion: a model that serves only `image` (Codex's
    /// image offer, an OpenRouter image model) is never one of its choices.
    #[tokio::test]
    async fn capabilities_offer_no_image_only_model() {
        use cairn_models::types::catalog::{DiscoveredModel, ProviderModelCatalog};
        let orch = test_orchestrator().await;
        let chat = |id: &str| {
            let mut model: DiscoveredModel = cairn_models::image::codex::discovered_model();
            model.id = id.into();
            model.model = id.into();
            model.display_name = id.into();
            model.modalities = None;
            model.serves = None;
            model
        };
        let mut openrouter_image = cairn_models::image::codex::discovered_model();
        openrouter_image.id = "bfl/flux".into();
        openrouter_image.model = "bfl/flux".into();
        openrouter_image.serves.as_mut().unwrap()[0].wire =
            cairn_models::types::catalog::Wire::OpenRouterImages;
        for (backend, models) in [
            (
                "codex",
                vec![
                    chat("gpt-coverage"),
                    cairn_models::image::codex::discovered_model(),
                ],
            ),
            ("openrouter", vec![chat("chat/model"), openrouter_image]),
        ] {
            orch.seed_model_catalog_for_test(ProviderModelCatalog {
                backend: backend.to_string(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            });
        }
        let capabilities = model_capabilities(&orch, None, None, None);
        let exact = |backend: &str| -> Vec<String> {
            capabilities
                .backends
                .iter()
                .filter(|capability| capability.backend == backend)
                .flat_map(|capability| capability.selections.iter())
                .filter(|selection| selection.kind == "model")
                .map(|selection| selection.model.clone())
                .collect()
        };
        let seen: Vec<_> = capabilities
            .backends
            .iter()
            .map(|b| b.backend.clone())
            .collect();
        assert!(seen.contains(&"codex".to_string()), "{seen:?}");
        assert_eq!(exact("codex"), ["gpt-coverage"]);
        assert_eq!(exact("openrouter"), ["chat/model"]);
    }

    /// An orchestrator whose Codex is signed in with an API key and whose
    /// discovery listed its image model, with a definition pinned to it.
    async fn codex_image_orchestrator() -> Orchestrator {
        codex_image_orchestrator_discovered(true).await
    }

    /// As above; `discovered` false leaves the catalog cold, as at startup.
    async fn codex_image_orchestrator_discovered(discovered: bool) -> Orchestrator {
        use cairn_models::types::catalog::ProviderModelCatalog;
        let orch = test_orchestrator().await;
        assert!(orch.provider_enabled("codex"));
        // Saved through the store rather than `add_account`, which also starts
        // a detached rediscovery of every provider. That refresh would race the
        // catalog seeded below (a failed Codex discovery marks the entry
        // errored, hiding its models) and fetch real provider listings.
        let mut store = orch.get_credential_store().unwrap_or_else(|| {
            crate::identity::local::credential_store_from_git_config(&orch.config_dir)
        });
        store.add_account(
            crate::credentials::ApiProvider::OpenAI,
            "OpenAI key".into(),
            crate::credentials::ProviderAuth::ApiKey {
                value: "sk-responses-test".into(),
            },
            None,
            0,
        );
        orch.save_credential_store(store).unwrap();
        if !discovered {
            return orch;
        }
        orch.seed_model_catalog_for_test(ProviderModelCatalog {
            backend: "codex".to_string(),
            models: vec![cairn_models::image::codex::discovered_model()],
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        });
        orch
    }

    fn image_pinned() -> ResponseDefinition {
        parse_definition(&format!(
            "---\nname: Image pinned\ndescription: pinned to an image model\nmodel: {}\nbackend: codex\n---\nSay hi",
            cairn_models::image::codex::CODEX_IMAGE_MODEL
        ))
        .unwrap()
    }

    fn assert_names_the_image_model(text: &str) {
        let model = cairn_models::image::codex::CODEX_IMAGE_MODEL;
        assert!(
            text.contains(&format!("'{model}' on codex serves only image")),
            "{text}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_tier_bound_to_an_image_model_is_listed_unrunnable_with_why() {
        for discovered in [true, false] {
            a_tier_bound_to_an_image_model_is_unrunnable(discovered).await;
        }
    }

    async fn a_tier_bound_to_an_image_model_is_unrunnable(discovered: bool) {
        let orch = codex_image_orchestrator_discovered(discovered).await;
        let mut backends = orch.get_settings().backends;
        backends.entry("codex".to_string()).or_default().insert(
            "lg".to_string(),
            Preset {
                model: Model::new(cairn_models::image::codex::CODEX_IMAGE_MODEL),
                options: Default::default(),
            },
        );
        orch.update_settings(crate::models::UpdateSettings {
            backends: Some(backends),
            ..Default::default()
        })
        .unwrap();

        let capabilities = model_capabilities(&orch, None, None, None);
        let codex = capabilities
            .backends
            .iter()
            .find(|backend| backend.backend == "codex")
            .unwrap();
        assert!(codex.runnable, "{:?}", codex.unavailable_reason);
        let lg = codex
            .selections
            .iter()
            .find(|selection| selection.value == "codex/lg")
            .unwrap();
        assert!(!lg.runnable);
        assert_names_the_image_model(lg.unavailable_reason.as_deref().unwrap());
        assert!(codex
            .selections
            .iter()
            .all(|selection| selection.kind != "model"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_saved_pin_to_an_image_model_fails_validation_and_invocation_naming_it() {
        let orch = codex_image_orchestrator().await;
        let definition = image_pinned();

        let invalid = validate_runnable_model(&orch, &definition, None, None).unwrap_err();
        assert_names_the_image_model(&invalid);

        let refused = invoke_definition(
            &orch,
            &definition,
            &json!({}),
            "image-pinned",
            ResponseCaller::Internal {
                label: "test".into(),
            },
        )
        .await
        .unwrap_err();
        match &refused {
            ResponseError::Unserved(text) => {
                assert!(
                    text.starts_with("No completion-capable model serves"),
                    "{text}"
                );
                assert_names_the_image_model(text);
            }
            other => panic!("expected the launch door's refusal, got {other:?}"),
        }
        let journal = crate::storage::list_response_invocations(
            &orch.db.local,
            "workspace",
            "image-pinned",
            10,
        )
        .await
        .unwrap();
        assert_eq!(journal.len(), 1, "the refusal is journaled");
    }

    #[test]
    fn discovery_failure_represents_saved_exact_pin_as_recovery_only() {
        let selection =
            saved_discovery_selection("openrouter", "saved/model", Some("catalog offline"))
                .unwrap();
        assert_eq!(selection.value, "saved/model");
        assert!(!selection.runnable);
        assert_eq!(
            selection.unavailable_reason.as_deref(),
            Some("model discovery failed: catalog offline")
        );
        assert!(saved_discovery_selection("openrouter", "removed/model", None).is_none());
    }

    fn definition() -> ResponseDefinition {
        parse_definition("---\nname: Test\ndescription: test response\nvariables:\n  - name: required\n    required: true\n  - name: optional\n    default: fallback\noutput: text\n---\n{{required}} / {{ optional }}").unwrap()
    }

    /// A definition whose JSON contract lists `title` and `content` as required
    /// and `summary` as optional, with one example.
    fn plan_definition(example_output: &str) -> Result<ResponseDefinition, String> {
        parse_definition(&format!(
            "---\nname: Plan\ndescription: plan response\nvariables:\n  - name: subject\n    required: true\noutput: plan\nexamples:\n  - input:\n      subject: parser\n    output: {example_output}\n---\nPlan {{{{subject}}}}"
        ))
    }

    #[test]
    fn a_json_example_is_shown_in_the_order_the_grammar_enforces() {
        // Authored summary-first, and inline (which YAML then sorts): either way
        // the model must see required keys first, or the example teaches it to
        // write a key that forecloses `summary` under a constrained decoder.
        for authored in [
            r#"'{"summary":"s","content":"c","title":"t"}'"#,
            "{ summary: s, content: c, title: t }",
        ] {
            let definition = plan_definition(authored).unwrap();
            let schema = definition.output_schema().unwrap();
            let shown =
                example_output_text(&definition.examples[0].output, schema.as_ref()).unwrap();
            assert_eq!(
                shown, r#"{"title":"t","content":"c","summary":"s"}"#,
                "{authored}"
            );
        }
    }

    /// A definition whose inline JSON contract is `output_schema`, with one
    /// example whose output is `example_output`.
    fn schema_definition(
        output_schema: &Value,
        example_output: &Value,
    ) -> Result<ResponseDefinition, String> {
        parse_definition(&format!(
            "---\nname: T\ndescription: t\noutput: {output_schema}\nexamples:\n  - input: {{}}\n    output: '{}'\n---\nWrite",
            example_output.to_string().replace('\'', "''")
        ))
    }

    #[test]
    fn an_example_under_a_nullable_object_is_shown_in_its_object_branchs_order() {
        // `anyOf: [object, null]` is how an optional nested object is usually
        // written. The wire orders the object branch required-first, so the
        // example must too, even though sorting puts `summary` first.
        let schema = json!({
            "type": "object",
            "required": ["result"],
            "properties": {
                "result": {
                    "anyOf": [
                        {
                            "type": "object",
                            "required": ["title"],
                            "properties": {
                                "title": { "type": "string" },
                                "summary": { "type": "string" }
                            }
                        },
                        { "type": "null" }
                    ]
                }
            }
        });
        let definition = schema_definition(
            &schema,
            &json!({ "result": { "title": "t", "summary": "s" } }),
        )
        .unwrap();
        let shown = example_output_text(
            &definition.examples[0].output,
            definition.output_schema().unwrap().as_ref(),
        )
        .unwrap();
        assert_eq!(shown, r#"{"result":{"title":"t","summary":"s"}}"#);
    }

    #[test]
    fn an_example_whose_branches_disagree_on_key_order_is_refused_at_load() {
        let branch = |required: &str| {
            json!({
                "type": "object",
                "required": [required],
                "properties": { "alpha": { "type": "string" }, "beta": { "type": "string" } }
            })
        };
        let schema = json!({
            "type": "object",
            "properties": { "pick": { "anyOf": [branch("alpha"), branch("beta")] } }
        });
        let refused = schema_definition(&schema, &json!({ "pick": { "alpha": "a", "beta": "b" } }))
            .unwrap_err();
        assert!(
            refused.contains("Example 1")
                && refused.contains("no key order")
                && refused.contains("`/pick`"),
            "{refused}"
        );
    }

    #[test]
    fn an_example_that_cannot_teach_its_json_contract_is_refused() {
        let not_json = plan_definition("'Title: t'").unwrap_err();
        assert!(
            not_json.contains("Example 1") && not_json.contains("JSON"),
            "{not_json}"
        );
        let missing_required = plan_definition("{ title: t }").unwrap_err();
        assert!(
            missing_required.contains("violates the output contract"),
            "{missing_required}"
        );
        // A text contract takes its example verbatim.
        let text = parse_definition(
            "---\nname: T\ndescription: t\noutput: text\nexamples:\n  - input: {}\n    output: 'Title: t'\n---\nWrite",
        )
        .unwrap();
        assert_eq!(
            example_output_text(&text.examples[0].output, None).unwrap(),
            "Title: t"
        );
    }

    #[test]
    fn omits_absent_variable_defaults_when_serializing() {
        let serialized = serde_yaml::to_string(&definition()).unwrap();

        assert!(!serialized.contains("default: null"));
        assert!(serialized.contains("default: fallback"));
    }

    #[tokio::test]
    async fn draft_completion_uses_declared_request_without_persisting_history() {
        let orch = test_orchestrator().await;
        let captured = Arc::new(Mutex::new(None));
        let captured_for_backend = captured.clone();
        let definition = parse_definition(
            "---\nname: Draft\ndescription: draft response\nmodel: test-model\nbackend: openrouter\ntimeout: 7s\nvariables:\n  - name: subject\n    required: true\noutput:\n  type: object\n  required: [answer]\n  properties:\n    answer: { type: string }\n---\nDraft {{subject}}",
        )
        .unwrap();

        let attempt = complete_definition_with_executor(
            &orch,
            &definition,
            &json!({"subject": "reply"}),
            None,
            None,
            None,
            None,
            Arc::new(move |backend, request, _orch| {
                *captured_for_backend.lock().unwrap() = Some((backend, request));
                Ok(CompletionOutcome {
                    text: r#"{"answer":"done"}"#.into(),
                    parsed: None,
                    model: "test-model".into(),
                    usage: Default::default(),
                    latency_ms: 3,
                    account_id: Some("acct-journal".into()),
                })
            }),
        )
        .await
        .unwrap();

        assert!(
            attempt.detached.is_none(),
            "an answered caller detaches nothing"
        );
        let outcome = attempt.result.unwrap();
        assert_eq!(outcome.text, r#"{"answer":"done"}"#);
        assert_eq!(outcome.parsed, Some(json!({"answer": "done"})));
        let (backend, request) = captured.lock().unwrap().take().unwrap();
        assert_eq!(backend, "openrouter");
        assert_eq!(request.model, "test-model");
        assert_eq!(request.timeout, Duration::from_secs(7));
        assert_eq!(request.messages.last().unwrap().content, "Draft reply");
        assert_eq!(
            request.output_schema,
            Some(json!({
                "type": "object",
                "required": ["answer"],
                "properties": {"answer": {"type": "string"}}
            }))
        );

        assert!(crate::storage::list_response_invocations(
            &orch.db.local,
            "workspace",
            "draft",
            10,
        )
        .await
        .unwrap()
        .is_empty());
        assert!(
            crate::storage::list_route_firings(&orch.db.local, "workspace", "draft", 10,)
                .await
                .unwrap()
                .is_empty()
        );
    }

    fn pinned(timeout_secs: u64) -> ResponseDefinition {
        parse_definition(&format!(
            "---\nname: Pinned\ndescription: pinned response\nmodel: test-model\nbackend: openrouter\ntimeout: {timeout_secs}s\n---\nAnswer"
        ))
        .unwrap()
    }

    /// The caller's bound releases it while the provider is still working, as
    /// an attempt (so `invoke` journals it), and the request it abandoned was
    /// held to the bound rather than to the definition's longer timeout.
    #[tokio::test]
    async fn a_callers_bound_ends_the_attempt_and_shortens_the_abandoned_request() {
        let orch = test_orchestrator().await;
        let sent_timeout = Arc::new(Mutex::new(None));
        let sent_for_backend = sent_timeout.clone();
        let bound = InvocationBound {
            limit: Duration::from_millis(200),
            cap: None,
        };
        let started = std::time::Instant::now();
        let attempt = complete_definition_with_executor(
            &orch,
            &pinned(300),
            &json!({}),
            None,
            None,
            Some(&bound),
            None,
            Arc::new(move |_, request, _| {
                *sent_for_backend.lock().unwrap() = Some(request.timeout);
                std::thread::sleep(Duration::from_secs(2));
                Err(ResponseError::Timeout)
            }),
        )
        .await
        .expect("a bound that runs out is an attempt, so invoke journals it");

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(attempt.model.as_str(), "test-model");
        assert_eq!(attempt.rendered, "Answer");
        assert_eq!(
            attempt.result,
            Err(ResponseError::OutlivedBound {
                bound,
                declared: Duration::from_secs(300),
            })
        );
        let sent = sent_timeout.lock().unwrap().expect("the request was sent");
        assert!(sent <= Duration::from_millis(200), "{sent:?}");
        let detached = attempt.detached.expect("the abandoned request is detached");
        assert_eq!(detached.release, Release::Bound);
        assert_eq!(detached.held_to, sent);
    }

    /// A definition may declare up to the run-batch ceiling, which a `run`
    /// call reaches by suspending; one past it is refused where it is written
    /// (both write paths parse through here), naming the ceiling.
    #[test]
    fn a_definition_timeout_past_the_run_batch_ceiling_is_refused() {
        let ceiling = MAX_RESPONSE_TIMEOUT.as_secs();
        assert_eq!(ceiling, 6 * 60 * 60);
        assert_eq!(pinned(ceiling).timeout, MAX_RESPONSE_TIMEOUT);
        let error = parse_definition(&format!(
            "---\nname: Pinned\ndescription: pinned response\nmodel: test-model\nbackend: openrouter\ntimeout: {}s\n---\nAnswer",
            ceiling + 1
        ))
        .unwrap_err();
        assert!(
            error.starts_with("Response timeout 21601s is past the 21600s ceiling"),
            "{error}"
        );
    }

    /// An interruption releases the caller while the provider is still working,
    /// as an attempt, so `invoke` journals it. A cancellation names its reason;
    /// a cap reads like any bound and carries the caller's own ask.
    #[tokio::test]
    async fn an_interruption_ends_the_attempt_with_its_cause() {
        let orch = test_orchestrator().await;
        let bound = InvocationBound {
            limit: Duration::from_secs(600),
            cap: None,
        };
        let (interrupter, interrupt) = interruption();
        let (definition, args) = (pinned(300), json!({}));
        let started = std::time::Instant::now();
        let (attempt, ()) = tokio::join!(
            complete_definition_with_executor(
                &orch,
                &definition,
                &args,
                None,
                None,
                Some(&bound),
                Some(&interrupt),
                Arc::new(|_, _, _| {
                    std::thread::sleep(Duration::from_secs(2));
                    Err(ResponseError::Timeout)
                }),
            ),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                interrupter.interrupt(Interruption::Cancelled("stopped".into()));
                interrupter.interrupt(Interruption::Cancelled("second".into()));
            }
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        let attempt = attempt.unwrap();
        assert_eq!(
            attempt.result,
            Err(ResponseError::Cancelled("stopped".into())),
            "the first interruption is the one journaled"
        );
        assert_eq!(
            attempt.detached.map(|detached| detached.release),
            Some(Release::Cancelled)
        );

        let (interrupter, interrupt) = interruption();
        interrupter.interrupt(Interruption::Capped {
            limit: Duration::from_secs(120),
            name: "host-item cap",
            why: "Why.",
        });
        let capped = complete_definition_with_executor(
            &orch,
            &pinned(300),
            &json!({}),
            None,
            None,
            Some(&bound),
            Some(&interrupt),
            Arc::new(|_, _, _| panic!("an interrupted call starts no request")),
        )
        .await
        .unwrap();
        assert!(
            capped.detached.is_none(),
            "no request was started to detach"
        );
        assert_eq!(
            capped.result.unwrap_err().to_string(),
            "response timed out at the 120 s host-item cap (requested 600 s; the definition declares 300 s). Why."
        );
    }

    /// A request that outlives its released caller logs one line when it
    /// finally returns, naming the invocation's journal row, why the caller
    /// left, and how long the provider stayed busy after. A request that
    /// answers its caller logs nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_detached_request_logs_its_tail_once_it_returns() {
        const TARGET: &str = "cairn_core::responses";
        crate::test_support::capture_log(TARGET);
        let orch = test_orchestrator().await;
        let responses = orch.config_dir.join("responses");
        std::fs::create_dir_all(&responses).unwrap();
        for (id, prompt) in [("tail-answered", "Answer"), ("tail-detached", "Detach")] {
            std::fs::write(
                responses.join(format!("{id}.md")),
                format!("---\nname: Tail\ndescription: tail\nmodel: test-model\nbackend: openrouter\ntimeout: 300s\n---\n{prompt}"),
            )
            .unwrap();
        }
        // The detached request returns only when the test opens the gate,
        // well after its caller was released.
        let (open_gate, gate) = std::sync::mpsc::channel::<()>();
        let gate = Mutex::new(gate);
        test_completion::install(
            &orch.config_dir,
            Arc::new(move |_, request: CompletionRequest, _| {
                if request.messages.last().unwrap().content == "Detach" {
                    gate.lock().unwrap().recv().unwrap();
                }
                Ok(CompletionOutcome {
                    text: "answer".into(),
                    parsed: None,
                    model: "test-model".into(),
                    usage: Default::default(),
                    latency_ms: 1,
                    account_id: None,
                })
            }),
        );
        let caller = || ResponseCaller::Internal {
            label: "test".into(),
        };
        let lines_for = |id: &str| {
            let marker = format!(" response_id={id} ");
            crate::test_support::captured_log(TARGET)
                .into_iter()
                .filter(|line| line.contains(&marker))
                .collect::<Vec<_>>()
        };

        invoke(&orch, "tail-answered", &json!({}), caller(), None, None)
            .await
            .expect("the normal path answers its caller");

        let bound = InvocationBound {
            limit: Duration::from_secs(60),
            cap: None,
        };
        let (interrupter, interrupt) = interruption();
        let args = json!({});
        let (released, ()) = tokio::join!(
            invoke(
                &orch,
                "tail-detached",
                &args,
                caller(),
                Some(&bound),
                Some(&interrupt),
            ),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                interrupter.interrupt(Interruption::Cancelled("stopped".into()));
            }
        );
        assert_eq!(released, Err(ResponseError::Cancelled("stopped".into())));
        assert!(
            lines_for("tail-detached").is_empty(),
            "nothing logs until the detached request returns"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        open_gate.send(()).unwrap();

        let waited = std::time::Instant::now();
        let line = loop {
            if let Some(line) = lines_for("tail-detached").pop() {
                break line;
            }
            assert!(
                waited.elapsed() < Duration::from_secs(10),
                "the detached request never logged its return"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(lines_for("tail-detached").len(), 1, "exactly one line");
        let journal = crate::storage::list_response_invocations(
            &orch.db.local,
            "workspace",
            "tail-detached",
            10,
        )
        .await
        .unwrap();
        let field = |key: &str| {
            line.split(' ')
                .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
                .unwrap_or_else(|| panic!("no {key} in {line}"))
                .to_string()
        };
        assert!(
            line.starts_with("detached response request returned "),
            "{line}"
        );
        assert_eq!(field("journal_id"), journal[0].id);
        assert_eq!(field("backend"), "openrouter");
        assert_eq!(field("model"), "test-model");
        assert_eq!(field("reason"), "cancelled");
        assert_eq!(field("ended"), "result");
        // Held to what remained of the 60 s bound, not the definition's 300 s.
        let timeout_ms: u64 = field("timeout_ms").parse().unwrap();
        assert!((55_000..=60_000).contains(&timeout_ms), "{line}");
        let tail_ms: u64 = field("tail_ms").parse().unwrap();
        assert!(
            tail_ms >= 150,
            "the tail runs from release to return: {line}"
        );

        assert!(
            lines_for("tail-answered").is_empty(),
            "a request that answered its caller logs nothing"
        );
    }

    /// A provider timeout that fires because the bound shortened the request
    /// is the bound's doing, and is named as such; one the definition's own
    /// timeout governed stays a plain timeout.
    #[tokio::test]
    async fn a_provider_timeout_is_attributed_to_whichever_bound_set_it() {
        let orch = test_orchestrator().await;
        let bound = InvocationBound {
            limit: Duration::from_secs(60),
            cap: None,
        };
        let timed_out: CompletionExecutor = Arc::new(|_, _, _| Err(ResponseError::Timeout));

        let shortened = complete_definition_with_executor(
            &orch,
            &pinned(300),
            &json!({}),
            None,
            None,
            Some(&bound),
            None,
            timed_out.clone(),
        )
        .await
        .unwrap();
        assert!(
            matches!(&shortened.result, Err(ResponseError::OutlivedBound { .. })),
            "{:?}",
            shortened.result
        );

        let declared = complete_definition_with_executor(
            &orch,
            &pinned(15),
            &json!({}),
            None,
            None,
            Some(&bound),
            None,
            timed_out,
        )
        .await
        .unwrap();
        assert_eq!(declared.result, Err(ResponseError::Timeout));
    }

    #[tokio::test]
    async fn an_unresolvable_tier_is_a_journalable_refusal() {
        let orch = test_orchestrator().await;
        let mut definition = definition();
        definition.tier = Some("ghost/sm".into());
        let attempt = complete_definition_with_executor(
            &orch,
            &definition,
            &json!({"required": "value"}),
            None,
            None,
            None,
            None,
            Arc::new(|_, _, _| panic!("an unresolved tier must not reach a backend")),
        )
        .await
        .expect("a refusal is an attempt, so invoke journals it");
        assert_eq!(attempt.rendered, "value / fallback");
        assert_eq!(attempt.model.as_str(), "ghost/sm");
        assert_eq!(attempt.backend, "ghost");
        assert!(matches!(
            attempt.result,
            Err(ResponseError::BackendUnavailable(reason)) if reason.contains("'ghost/sm' tier")
        ));
    }

    #[test]
    fn renders_substitutions_and_defaults() {
        assert_eq!(
            definition().render(&json!({"required": "value"})).unwrap(),
            "value / fallback"
        );
    }

    #[test]
    fn rejects_missing_and_undeclared_arguments() {
        assert!(definition()
            .render(&json!({}))
            .unwrap_err()
            .contains("Missing required"));
        assert!(definition()
            .render(&json!({"required": "ok", "extra": true}))
            .unwrap_err()
            .contains("Undeclared"));
    }

    #[test]
    fn validates_declared_variable_values() {
        let response = parse_definition(
            "---\nname: Test\ndescription: test\nvariables:\n  - name: surface\n    default: imessage\n    values: [imessage, discord]\n---\n{{surface}}",
        )
        .unwrap();
        assert_eq!(response.render(&json!({})).unwrap(), "imessage");
        assert_eq!(
            response.render(&json!({"surface": "discord"})).unwrap(),
            "discord"
        );
        assert!(response
            .render(&json!({"surface": "email"}))
            .unwrap_err()
            .contains("declared values"));
        assert!(parse_definition(
            "---\nname: Test\ndescription: test\nvariables:\n  - name: surface\n    default: email\n    values: [imessage, discord]\n---\n{{surface}}",
        )
        .unwrap_err()
        .contains("default is not one"));
    }

    #[test]
    fn rejects_undeclared_template_variable() {
        let error =
            parse_definition("---\nname: Test\ndescription: test\n---\n{{missing}}").unwrap_err();
        assert!(error.contains("undeclared variable 'missing'"));
    }

    #[test]
    fn validates_text_and_json_schema_outputs() {
        assert_eq!(
            definition().validate_output("plain", None).unwrap(),
            ValidatedOutput::Text("plain".into())
        );
        let mut structured = definition();
        structured.output = OutputContract::Schema(
            json!({"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}),
        );
        assert!(matches!(
            structured.validate_output(r#"{"answer":"yes"}"#, None),
            Ok(ValidatedOutput::Json(_))
        ));
        assert!(structured
            .validate_output(r#"{"answer":1}"#, None)
            .unwrap_err()
            .contains("violation"));
    }

    #[test]
    fn validates_exact_model_pins_and_defaults_to_sm_tier() {
        let defaulted = definition();
        assert_eq!(defaulted.tier.as_deref().unwrap_or("sm"), "sm");
        assert!(parse_definition(
            "---\nname: Test\ndescription: test\nmodel: exact\ntier: sm\nbackend: openrouter\n---\nprompt"
        ).unwrap_err().contains("either 'model' or 'tier'"));
        assert!(
            parse_definition("---\nname: Test\ndescription: test\nmodel: exact\n---\nprompt")
                .unwrap_err()
                .contains("must declare 'backend'")
        );
        let pinned = parse_definition(
            "---\nname: Test\ndescription: test\nmodel: exact\nbackend: openrouter\noptions:\n  reasoningEffort: low\n---\nprompt"
        ).unwrap();
        let preset = Preset {
            model: Model::new(pinned.model.unwrap()),
            options: pinned.options,
        };
        assert_eq!(preset.to_extras().reasoning_effort.as_deref(), Some("low"));
    }

    #[test]
    fn closed_model_validation_accepts_only_offered_selection_identities() {
        let capabilities = ResponseModelCapabilities {
            backends: vec![ResponseBackendCapability {
                backend: "openrouter".into(),
                label: "OpenRouter".into(),
                runnable: true,
                unavailable_reason: None,
                selections: vec![
                    ResponseModelSelection {
                        kind: "tier".into(),
                        value: "openrouter/sm".into(),
                        label: "SM".into(),
                        backend: "openrouter".into(),
                        model: "openrouter/auto".into(),
                        legacy_values: vec!["sm".into()],
                        runnable: true,
                        unavailable_reason: None,
                    },
                    ResponseModelSelection {
                        kind: "model".into(),
                        value: "openrouter/auto".into(),
                        label: "Exact".into(),
                        backend: "openrouter".into(),
                        model: "openrouter/auto".into(),
                        legacy_values: vec![],
                        runnable: true,
                        unavailable_reason: None,
                    },
                ],
            }],
        };
        let mut authored = definition();
        authored.tier = Some("sm".into());
        assert!(is_offered_model_selection(&authored, &capabilities));
        authored.tier = Some("ghost/sm".into());
        assert!(!is_offered_model_selection(&authored, &capabilities));
        authored.tier = None;
        authored.backend = Some("openrouter".into());
        authored.model = Some("openrouter/auto".into());
        assert!(is_offered_model_selection(&authored, &capabilities));
        let mut unavailable = capabilities.clone();
        unavailable.backends[0].selections[1].runnable = false;
        assert!(!is_offered_model_selection(&authored, &unavailable));
    }
}
