//! Agent execution backends.
//!
//! This module defines the `AgentBackend` trait that abstracts over different
//! agent runtimes (Claude, Codex, etc.). The orchestrator resolves session
//! configuration (tools, model, prompt, MCP config) and then delegates to a
//! backend for process spawning and event streaming.
//!
//! ## Permission model
//!
//! [`AgentPermissions`] bundles [`ApprovalPolicy`] and [`FilesystemScope`]
//! (defined in `models/permissions.rs`). Agent configs store the two enum
//! fields directly; each backend translates them into its own CLI flags
//! or protocol fields.
//!
//! ## Tool resolution
//!
//! [`ResolvedTools`] is produced by [`AgentBackend::resolve_tools`] — each
//! backend maps agent-declared tool names into backend-specific allowed and
//! disallowed lists.

pub mod claude;
mod claude_transcript;
pub mod claude_usage;
pub mod codex;
pub mod context_window;
mod http_loop;
pub mod ollama;
mod openai_compat;
pub mod opencode;
pub mod openrouter;
mod run_state;
pub mod stdin;
pub(crate) mod workflow;

pub use claude_usage::collect_claude_usage_snapshot;

use crate::agent_process::process::BackendStdin;
use crate::models::{Fence, Model};
use crate::orchestrator::Orchestrator;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// One conversational message in a backend-neutral completion request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionMessage {
    pub role: CompletionRole,
    pub content: String,
}

/// A single model round-trip. Completion requests never expose tools or create a
/// session, run, transcript, or branch.
#[derive(Debug, Clone)]
pub struct CompletionRequest {
    pub system: Option<String>,
    pub messages: Vec<CompletionMessage>,
    pub model: String,
    /// Project whose provider-account scope should govern backend routing.
    pub project_id: Option<String>,
    pub extras: Value,
    pub output_schema: Option<Value>,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletionTokens {
    pub input: Option<u64>,
    pub output: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionOutcome {
    pub text: String,
    pub parsed: Option<Value>,
    pub model: String,
    pub tokens: CompletionTokens,
    pub cost: Option<f64>,
    pub latency_ms: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompletionError {
    #[error("backend does not support one-shot completions")]
    BackendUnavailable,
    #[error("completion timed out")]
    Timeout,
    #[error("completion request is invalid: {0}")]
    InvalidRequest(String),
    #[error("completion provider failed: {0}")]
    Upstream(String),
    #[error("completion response was invalid: {0}")]
    InvalidResponse(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionShape {
    InProcess,
    PooledSessions,
    DedicatedProcess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStart {
    New {
        session_id: String,
    },
    Resume {
        session_id: String,
        backend_id: String,
    },
    Fork {
        session_id: String,
        source_backend_id: String,
    },
}

impl SessionStart {
    pub(crate) fn session_id(&self) -> &str {
        match self {
            SessionStart::New { session_id }
            | SessionStart::Resume { session_id, .. }
            | SessionStart::Fork { session_id, .. } => session_id,
        }
    }

    pub fn backend_id(&self) -> Option<&str> {
        match self {
            SessionStart::Resume { backend_id, .. } => Some(backend_id),
            _ => None,
        }
    }

    pub fn source_backend_id(&self) -> Option<&str> {
        match self {
            SessionStart::Fork {
                source_backend_id, ..
            } => Some(source_backend_id),
            _ => None,
        }
    }

    /// The provider-side conversation this start replays, if any: a resume
    /// continues its own, a fork replays its source's. A new session replays
    /// nothing.
    pub fn replayed_backend_id(&self) -> Option<&str> {
        self.backend_id().or_else(|| self.source_backend_id())
    }
}

/// A backend-diagnosed failure the lifecycle layer reacts to structurally.
///
/// Recorded on the run as `runs.exit_reason` before finalization, so the
/// reaction reads a typed fact rather than re-parsing provider CLI text. The
/// only layer that matches provider wording is the backend that emitted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendFailure {
    /// The backend could not resolve the native conversation handle it was
    /// asked to resume. Cairn's own transcript is intact; only the
    /// provider-side conversation is unreachable.
    SessionUnresolvable,
}

impl BackendFailure {
    /// The token written to `runs.exit_reason`.
    pub fn exit_reason(self) -> &'static str {
        match self {
            BackendFailure::SessionUnresolvable => "session_unresolvable",
        }
    }

    /// Inverse of [`BackendFailure::exit_reason`]. Returns `None` for the exit
    /// reasons written by other paths (`capacity_retry`, `turn_failed`, …).
    pub fn from_exit_reason(value: &str) -> Option<Self> {
        match value {
            "session_unresolvable" => Some(BackendFailure::SessionUnresolvable),
            _ => None,
        }
    }
}

/// Backend-resolved agent permissions.
///
/// Each backend translates the canonical [`Fence`] into its own CLI flags or
/// protocol fields. Actual enforcement lives in Cairn's verb handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPermissions {
    fence: Fence,
}

impl AgentPermissions {
    pub(crate) fn new(fence: Fence) -> Self {
        Self { fence }
    }

    /// Convert to a legacy permission mode string for the runtime stdin protocol.
    pub(crate) fn to_legacy_str(&self) -> &'static str {
        self.fence.to_legacy_permission_mode()
    }
}

// ============================================================================
// Tool resolution
// ============================================================================

/// Backend-resolved tool configuration.
#[derive(Debug, Clone)]
pub struct ResolvedTools {
    /// Tools the backend's native runtime should allow.
    pub(crate) allowed: Vec<String>,
    /// Tools the backend's native runtime should disallow.
    pub(crate) disallowed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredReasoningEffort {
    reasoning_effort: String,
    description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredModel {
    pub(crate) id: String,
    pub(crate) model: String,
    pub(crate) display_name: String,
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) hidden: bool,
    #[serde(default)]
    pub(crate) is_default: bool,
    pub(crate) default_reasoning_effort: Option<String>,
    #[serde(default)]
    pub(crate) supported_reasoning_efforts: Vec<DiscoveredReasoningEffort>,
    #[serde(default)]
    pub(crate) context_window: Option<i64>,
    #[serde(default)]
    pub(crate) canonical_slug: Option<String>,
    /// Identity of every configured account whose host serves this model, in
    /// account priority order. Providers that serve every model from a single
    /// endpoint leave this empty; Ollama populates it so routing and the
    /// settings inventory agree on which hosts have which models.
    #[serde(default)]
    pub(crate) serving_account_ids: Vec<String>,
    #[serde(default)]
    pub(crate) pricing: Option<DiscoveredModelPricing>,
    #[serde(default)]
    pub(crate) supported_parameters: Vec<String>,
    #[serde(default)]
    pub(crate) router: bool,
    #[serde(default)]
    pub(crate) architecture_modality: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredModelPricing {
    prompt: Option<String>,
    completion: Option<String>,
    request: Option<String>,
    image: Option<String>,
    web_search: Option<String>,
    internal_reasoning: Option<String>,
    input_cache_read: Option<String>,
    input_cache_write: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOptionDescriptor {
    /// Runtime-supported option key. Add variants here only when preset
    /// resolution and backend launch paths also carry the option end-to-end.
    key: ProviderOptionKey,
    label: String,
    kind: OptionKind,
    #[serde(default)]
    choices: Vec<OptionChoice>,
    default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OptionChoice {
    value: String,
    label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProviderOptionKey {
    ReasoningEffort,
    FastMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OptionKind {
    Enum,
    Boolean,
    String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelCatalog {
    pub(crate) backend: String,
    pub(crate) models: Vec<DiscoveredModel>,
    #[serde(default)]
    pub(crate) options: Vec<ProviderOptionDescriptor>,
    pub(crate) refreshed_at: Option<i64>,
    pub(crate) error: Option<String>,
}

// ============================================================================
// SessionConfig
// ============================================================================

/// Configuration for starting a session (backend-agnostic).
///
/// All complex resolution (tools, model, prompt, MCP config) is done by the
/// caller in `session.rs`. The backend receives a clean config struct with
/// everything pre-resolved.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Run ID for this session
    pub(crate) run_id: String,
    /// Working directory for the agent process
    pub(crate) working_dir: String,
    /// Independently established owning project authority for durable resources.
    pub(crate) project_id: String,
    pub(crate) project_key: String,
    /// The user prompt to send, including any natively delivered images.
    pub(crate) prompt: String,
    pub(crate) message_content: crate::agent_process::stdin::MessageContent,
    /// Agent role instructions (injected as system prompt content)
    pub(crate) system_prompt_content: Option<String>,
    /// The trailing per-run dynamic suffix of `system_prompt_content` (the
    /// orientation block + `</agent_role>` close). A suffix of
    /// `system_prompt_content`; used only to split the agent content into a static
    /// head and the inlined dynamic tail when recording the segment boundary map.
    pub(crate) system_prompt_dynamic_tail: Option<String>,
    /// Resolved model (job > agent > workspace default)
    pub(crate) model: Option<Model>,
    /// Explicit session start semantics for this backend invocation.
    pub(crate) session_start: SessionStart,
    /// Resolved allowed tools list
    pub(crate) allowed_tools: Vec<String>,
    /// Resolved disallowed tools list
    pub(crate) disallowed_tools: Vec<String>,
    /// The MCP config as a self-contained JSON string (built per run, never
    /// shared on disk). Claude passes it inline via `--mcp-config <json>`; Codex
    /// parses it to extract the cairn-cmd args.
    pub(crate) mcp_config_json: String,
    /// Stable home URI for this run (full node URI). Forwarded to the MCP child
    /// as `CAIRN_HOME_URI` so `cairn:~/...` shorthand resolves. Claude bakes this
    /// into its inline MCP config JSON env; Codex inherits it via the process env.
    pub(crate) home_uri: String,
    /// Max thinking tokens (None = disabled)
    pub(crate) max_thinking_tokens: Option<i32>,
    /// Codex: reasoning effort level ("low", "medium", "high", "xhigh")
    pub(crate) reasoning_effort: Option<String>,
    /// Service tier request id, if the backend supports one.
    pub(crate) service_tier: Option<String>,
    /// Canonical agent permissions (replaces opaque permission_mode string).
    pub(crate) permissions: AgentPermissions,
    /// Enable stdin streaming (bidirectional mode)
    pub(crate) bidirectional: bool,
    /// Pre-resolved identity for this session (includes project overrides).
    /// If set, backends use this instead of calling `orch.get_identity()`.
    pub(crate) identity: Option<crate::identity::UserIdentity>,
    /// Resolved JSON Schema to constrain the model's output natively (CAIRN-2505).
    /// Set only for node-less ephemeral calls that carry an output contract, so
    /// each backend passes the provider's native output constraint (Claude
    /// `--json-schema`, OpenRouter `response_format`, Codex per-turn
    /// `outputSchema`). `None` for ordinary agent sessions, which are unchanged.
    pub(crate) output_schema: Option<serde_json::Value>,
    /// Ambient (no-worktree) run: selects the ambient tier variant of the shared
    /// CAIRN system-prompt segment (`cairn_system_prompt(ambient)`) rather than
    /// the authoring one. Set from `is_ambient_run` at session assembly.
    pub(crate) ambient: bool,
    /// True only for a node-less EPHEMERAL CALL (CAIRN-2549). The calls path is
    /// the single source of truth: `start_agent_session` derives this from
    /// `constrain_output_natively`, which ONLY the calls path passes `true`.
    /// Backends that pool ephemeral calls (Codex) branch on this to run the call
    /// as a lightweight thread on a shared app-server; ordinary node/task
    /// sessions leave it `false` and keep the process-per-session shape. A future
    /// non-call constrained session must NOT set this without owning the pooled
    /// lifecycle.
    pub(crate) is_ephemeral_call: bool,
}

impl SessionConfig {}

/// Trait for agent execution backends.
///
/// Backends are responsible for spawning the agent process, registering it
/// in process state, and starting the reader thread that streams events
/// into the database.
/// Which process-minimization shape a backend offers for ephemeral (call)
/// fan-out. This is descriptive metadata the per-backend enforcement branches
/// on; the generic admission machinery only reads [`CallBatchCapability::max_concurrency`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallBatchShape {
    /// Whole agentic loop runs inside the runner; no child process per call
    /// (OpenRouter's async HTTP turn loop).
    InProcess,
    /// One long-lived pooled process; each call is a lightweight session on it
    /// (Codex app-server `thread/start` per call).
    PooledSessions,
    /// Each call is a dedicated backend process, admitted under a concurrency cap
    /// (Claude CLI, permanently CLI-bound).
    DedicatedProcess,
}

/// A backend's ephemeral-call batching capability: the shape it offers plus the
/// max number of concurrent ephemeral calls it will admit at once. `None` =
/// unbounded (pure passthrough — no admission bookkeeping).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallBatchCapability {
    pub(crate) shape: CallBatchShape,
    pub(crate) max_concurrency: Option<usize>,
}

pub trait AgentBackend: Send + Sync {
    /// Human-readable name (e.g. "Claude", "Codex")
    fn name(&self) -> &str;

    /// Check if this backend is available (binary exists, API key set, etc.)
    fn is_available(&self) -> Result<(), String>;

    /// Discover currently available model options for this backend.
    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String>;

    /// Whether this backend can execute a response's tool-free, one-shot
    /// completion with the credentials or host configuration currently in
    /// scope. This is the canonical capability check used by both authoring and
    /// invocation; backends that only support sessions keep the honest default.
    fn response_completion_availability(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Result<(), String> {
        Err(format!(
            "{} sessions don't support one-shot completions",
            self.name()
        ))
    }

    /// Whether one concrete model is runnable for a response in this scope.
    /// Providers with host-local inventories override this to keep discovery,
    /// authoring, and routing on the same account boundary.
    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        _model: &str,
    ) -> Result<(), String> {
        self.response_completion_availability(orch, project_id)
    }

    /// Backend-published preset option descriptors.
    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        Vec::new()
    }

    /// Resolve agent tool names into backend-specific allowed/disallowed lists.
    /// Called after backend selection, before prompt building.
    fn resolve_tools(&self, agent_tools: &[String], agent_disallowed: &[String]) -> ResolvedTools;

    /// Start a new session. Spawns the process and event reader thread.
    /// Returns immediately — events flow through the Orchestrator's DB/emitter.
    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String>;

    /// Execute one tool-free request without creating agent/session state.
    fn complete(
        &self,
        _request: CompletionRequest,
        _orch: &Orchestrator,
    ) -> Result<CompletionOutcome, CompletionError> {
        Err(CompletionError::BackendUnavailable)
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::DedicatedProcess
    }

    /// Whether this backend supports session resume (--resume)
    fn supports_resume(&self) -> bool;

    /// Whether this backend supports warm process retention
    fn supports_warm_processes(&self) -> bool;

    /// Ephemeral-call batching capability. Default is the behavior-preserving
    /// dedicated-process, unbounded shape. The calls path (only) consults this;
    /// ordinary node/task sessions never reach it.
    fn call_batch_capability(&self) -> CallBatchCapability {
        CallBatchCapability {
            shape: CallBatchShape::DedicatedProcess,
            max_concurrency: None,
        }
    }

    /// Send a user message to a running process via stdin.
    fn send_user_message(
        &self,
        stdin: &mut dyn BackendStdin,
        content: &crate::agent_process::stdin::MessageContent,
        session_id: &str,
        parent_tool_use_id: Option<&str>,
        working_dir: Option<&str>,
    ) -> Result<(), String>;

    /// Send an interrupt to a running process via stdin.
    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String>;

    /// Send a model change to a running process via stdin.
    fn send_set_model(&self, stdin: &mut dyn BackendStdin, model: &str) -> Result<(), String>;

    /// Send a permission mode change to a running process via stdin.
    fn send_set_permission_mode(
        &self,
        stdin: &mut dyn BackendStdin,
        mode: &str,
    ) -> Result<(), String>;
}

/// Every backend Cairn ships, in product order.
///
/// The model-catalog refresh, the responses surface, and the settings provider
/// list each need the same answer to "which providers exist". They read it here
/// instead of each carrying a copy, so adding a provider cannot leave one
/// surface quietly behind.
pub(crate) const KNOWN_BACKENDS: &[&str] = &[
    "claude",
    "codex",
    "openrouter",
    opencode::OPENCODE_BACKEND_KEY,
    "ollama",
];

/// Create an AgentBackend for the given backend name.
/// Returns ClaudeBackend for None or "claude", CodexBackend for "codex".
pub(crate) fn backend_for_name(name: Option<&str>) -> Box<dyn AgentBackend> {
    match name {
        Some("codex") => Box::new(codex::CodexBackend),
        Some("openrouter") => Box::new(openrouter::OpenRouterBackend),
        Some(opencode::OPENCODE_BACKEND_KEY) => Box::new(opencode::OpenCodeBackend),
        Some("ollama") => Box::new(ollama::OllamaBackend),
        _ => Box::new(claude::ClaudeBackend),
    }
}

/// The provider [`backend_for_model`]'s `None` stands for.
///
/// This is the Claude FAMILY, not a configurable default, and the distinction
/// matters: `backend_for_model` answers `None` for `opus`/`sonnet`/`haiku` and
/// every other Claude tier alias, so substituting a workspace's configured
/// `active_backend` here would route `opus` to whatever that workspace happens
/// to have selected. `None` means "this name belongs to Claude", and the only
/// honest way to spell it is `claude`.
///
/// The real limit this exposes is that inference from a bare model NAME cannot
/// express a provider that lives only in configuration — an Ollama tag, a custom
/// backend's model. Those are not representable as a job's persisted model
/// string at all, which is why the surfaces that offer a model to run filter to
/// what this resolution can honor (see `isRuntimeRepresentable`) rather than
/// pretending the name carries the provider.
pub(crate) const CLAUDE_FAMILY_BACKEND: &str = "claude";

/// The backend a model will actually run on, spelled.
///
/// [`backend_for_model`] answers `None` for every Claude model, and
/// [`backend_for_name`] then totalizes that `None` to Claude when the process is
/// spawned. A *comparison* site cannot use the partial answer: `None` reads as
/// "no opinion" there, so comparing against it can only ever detect a move
/// toward an inferable provider (Claude -> Codex) and never the move back
/// (Codex -> Claude), leaving the session recorded on a provider the process is
/// not running. Anything deciding whether a session still matches its model must
/// ask this, so the decision and the spawn resolve the same question one way.
pub(crate) fn resolved_backend_for_model(model: &str) -> &'static str {
    backend_for_model(model).unwrap_or(CLAUDE_FAMILY_BACKEND)
}

/// The backend a session will actually run on, given the agent's resolved
/// selection and the model persisted on the job.
///
/// A model and the provider that serves it are ONE fact. Every site that needs
/// the provider asks here — the spawn in `start_agent_session`, the rotation
/// comparison in `continue_job_impl`, call admission, the session row a new job
/// is created with — so none of them can answer differently from the process
/// that actually starts.
///
/// `jobs.model` is a concrete model: written from the atomic selection the job
/// launched with, rewritten only by an explicit model edit, and handed verbatim
/// to the CLI at spawn. So when a model is present it decides the provider, with
/// one exception — a selection naming that SAME model speaks for it, because an
/// atomic `{ backend, model }` pair is the only thing able to name a provider a
/// model name cannot carry (an Ollama tag, a custom backend; see
/// [`CLAUDE_FAMILY_BACKEND`]).
///
/// The exception is deliberately narrow. An agent config resolved against
/// configuration that has since moved — a retuned tier preset, a changed
/// `active_backend` — carries a selection for a DIFFERENT model than the job
/// persisted, and letting it name the provider splits the pair: that is
/// CAIRN-3798, where live Claude `fable` threads were rotated onto Codex, which
/// then rejected the model outright. An agent's default may choose a model for a
/// job that has none; it may not reinterpret one the job already has.
///
/// With no job model the ladder is the agent's own: its atomic selection, then
/// its authored `backend_preference`. `None` collapses to Claude in
/// [`backend_for_name`].
pub(crate) fn effective_backend_name(
    selection: Option<&crate::models::ModelSelection>,
    backend_preference: Option<&str>,
    model: Option<&str>,
) -> Option<String> {
    let Some(model) = model else {
        return selection
            .map(|selection| selection.backend.clone())
            .or_else(|| backend_preference.map(str::to_string));
    };
    match selection.filter(|selection| selection.model.as_str() == model) {
        Some(selection) => Some(selection.backend.clone()),
        None => Some(resolved_backend_for_model(model).to_string()),
    }
}

/// Known Codex model prefixes — prefix matching covers versioned variants
/// (e.g. "gpt-5" matches "gpt-5.4", "gpt-5.3-codex", etc.).
const CODEX_MODEL_PREFIXES: &[&str] = &["codex-mini", "gpt-5"];

/// Infer backend from a model identifier.
/// Returns "codex" for known Codex/OpenAI models, None (= Claude) otherwise.
///
/// **Deprecated path:** The primary path now is preset resolution via
/// `config::presets::resolve_preset()`. This function remains as a fallback
/// for legacy data (snapshots, concrete model strings not yet migrated to tiers).
pub(crate) fn backend_for_model(model: &str) -> Option<&'static str> {
    let lower = model.to_lowercase();
    // Legacy inference cannot distinguish slash-containing Ollama tags (for
    // example hf.co/user/model). Atomic { backend, model } selections bypass it.
    if lower == "openrouter/auto" || lower.starts_with('~') || lower.contains('/') {
        return Some("openrouter");
    }
    for prefix in CODEX_MODEL_PREFIXES {
        if lower.starts_with(prefix) {
            return Some("codex");
        }
    }
    None
}

#[cfg(test)]
mod backend_failure_tests {
    use super::BackendFailure;

    #[test]
    fn exit_reason_round_trips() {
        let failure = BackendFailure::SessionUnresolvable;
        assert_eq!(failure.exit_reason(), "session_unresolvable");
        assert_eq!(
            BackendFailure::from_exit_reason(failure.exit_reason()),
            Some(failure)
        );
    }

    #[test]
    fn existing_exit_reasons_are_not_backend_failures() {
        // `runs.exit_reason` is a shared column; the tokens other paths write
        // must never be read back as a backend diagnosis.
        for reason in ["capacity_retry", "turn_failed", "", "session unresolvable"] {
            assert_eq!(
                BackendFailure::from_exit_reason(reason),
                None,
                "{reason:?} must not decode as a backend failure"
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    // =========================================================================
    // AgentPermissions::to_legacy_str
    // =========================================================================

    #[test]
    fn to_legacy_str_allow_is_accept_edits() {
        let perms = AgentPermissions::new(Fence::Allow);
        assert_eq!(perms.to_legacy_str(), "acceptEdits");
    }

    #[test]
    fn to_legacy_str_ask_is_default() {
        let perms = AgentPermissions::new(Fence::Ask);
        assert_eq!(perms.to_legacy_str(), "default");
    }

    #[test]
    fn to_legacy_str_deny_is_default() {
        let perms = AgentPermissions::new(Fence::Deny);
        assert_eq!(perms.to_legacy_str(), "default");
    }

    // =========================================================================
    // ResolvedTools
    // =========================================================================

    #[test]
    fn resolved_tools_construction() {
        let rt = ResolvedTools {
            allowed: vec!["mcp__cairn__read".into(), "mcp__cairn__write".into()],
            disallowed: vec!["Read".into(), "Write".into()],
        };
        assert_eq!(rt.allowed.len(), 2);
        assert_eq!(rt.disallowed.len(), 2);
    }

    // =========================================================================
    // Backend trait properties
    // =========================================================================

    #[test]
    fn claude_backend_trait_properties() {
        let backend = claude::ClaudeBackend;
        assert_eq!(backend.name(), "Claude");
        assert!(backend.supports_resume());
        assert!(backend.supports_warm_processes());
        assert_eq!(
            backend.completion_shape(),
            CompletionShape::DedicatedProcess
        );
        // Claude enforces a bounded ephemeral-call ceiling (CAIRN-2557): each
        // call is a dedicated ~450 MB `claude` process, so fan-out is capped at
        // a RAM-derived bound. The descriptor reports exactly the computed
        // startup value.
        assert_eq!(
            backend.call_batch_capability(),
            CallBatchCapability {
                shape: CallBatchShape::DedicatedProcess,
                max_concurrency: Some(*claude::CLAUDE_CALL_MAX_CONCURRENCY),
            }
        );
    }

    /// The RAM-derived ceiling formula (CAIRN-2557) clamps at its documented
    /// boundaries and scales proportionally between them. Exercised against
    /// injected RAM values — never the host's real memory — so the boundaries
    /// are deterministic. Budget is 25% of RAM, per-process estimate 450 MB,
    /// clamped into [4, 64].
    #[test]
    fn claude_call_ceiling_clamps_at_boundaries() {
        use claude::claude_call_concurrency_ceiling as ceiling;
        const GIB: u64 = 1024 * 1024 * 1024;

        // Tiny RAM → FLOOR: 1 GB budgets 256 MB / 450 MB ≈ 0, clamped up to 4;
        // 8 GB budgets 2 GB / 450 MB ≈ 4, already at the floor.
        assert_eq!(ceiling(GIB), 4);
        assert_eq!(ceiling(8 * GIB), 4);

        // Huge RAM → CAP: 128 GB budgets 32 GB / 450 MB ≈ 72, clamped to 64.
        assert_eq!(ceiling(128 * GIB), 64);
        assert_eq!(ceiling(1024 * GIB), 64);

        // Mid RAM → proportional: 32 GB budgets 8 GB / 450 MB ≈ 18.
        assert_eq!(ceiling(32 * GIB), 18);

        // Acceptance anchors (CAIRN-2557): a 128 GB machine runs a 40-call
        // fan-out fully parallel, and an 8–16 GB machine stays single-digit.
        assert!(ceiling(128 * GIB) >= 40);
        assert!(ceiling(8 * GIB) < 10);
        assert!(ceiling(16 * GIB) < 10);
    }

    #[test]
    fn codex_backend_trait_properties() {
        let backend = codex::CodexBackend;
        assert_eq!(backend.name(), "Codex");
        assert!(backend.supports_resume());
        assert!(backend.supports_warm_processes());
        assert_eq!(
            backend.call_batch_capability(),
            CallBatchCapability {
                shape: CallBatchShape::PooledSessions,
                max_concurrency: None,
            }
        );
    }

    #[test]
    fn openrouter_backend_call_batch_capability() {
        let backend = openrouter::OpenRouterBackend;
        assert_eq!(
            backend.call_batch_capability(),
            CallBatchCapability {
                shape: CallBatchShape::InProcess,
                max_concurrency: None,
            }
        );
    }

    /// The trait default surfaces the behavior-preserving unbounded shape, and a
    /// backend that overrides `max_concurrency` surfaces its ceiling through the
    /// same descriptor the admission machinery reads.
    #[test]
    fn call_batch_capability_default_and_override_surface() {
        struct DefaultBackend;
        impl AgentBackend for DefaultBackend {
            fn name(&self) -> &str {
                "Default"
            }
            fn is_available(&self) -> Result<(), String> {
                Ok(())
            }
            fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
                Ok(Vec::new())
            }
            fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
                ResolvedTools {
                    allowed: Vec::new(),
                    disallowed: Vec::new(),
                }
            }
            fn start_session(&self, _: SessionConfig, _: &Orchestrator) -> Result<(), String> {
                Ok(())
            }
            fn supports_resume(&self) -> bool {
                false
            }
            fn supports_warm_processes(&self) -> bool {
                false
            }
            fn send_user_message(
                &self,
                _: &mut dyn BackendStdin,
                _: &crate::agent_process::stdin::MessageContent,
                _: &str,
                _: Option<&str>,
                _: Option<&str>,
            ) -> Result<(), String> {
                Ok(())
            }
            fn send_interrupt(&self, _: &mut dyn BackendStdin) -> Result<(), String> {
                Ok(())
            }
            fn send_set_model(&self, _: &mut dyn BackendStdin, _: &str) -> Result<(), String> {
                Ok(())
            }
            fn send_set_permission_mode(
                &self,
                _: &mut dyn BackendStdin,
                _: &str,
            ) -> Result<(), String> {
                Ok(())
            }
        }
        // Default: unbounded dedicated-process passthrough.
        assert_eq!(
            DefaultBackend.call_batch_capability(),
            CallBatchCapability {
                shape: CallBatchShape::DedicatedProcess,
                max_concurrency: None,
            }
        );

        struct CappedBackend;
        impl AgentBackend for CappedBackend {
            fn name(&self) -> &str {
                "Capped"
            }
            fn is_available(&self) -> Result<(), String> {
                Ok(())
            }
            fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
                Ok(Vec::new())
            }
            fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
                ResolvedTools {
                    allowed: Vec::new(),
                    disallowed: Vec::new(),
                }
            }
            fn start_session(&self, _: SessionConfig, _: &Orchestrator) -> Result<(), String> {
                Ok(())
            }
            fn supports_resume(&self) -> bool {
                false
            }
            fn supports_warm_processes(&self) -> bool {
                false
            }
            fn call_batch_capability(&self) -> CallBatchCapability {
                CallBatchCapability {
                    shape: CallBatchShape::DedicatedProcess,
                    max_concurrency: Some(1),
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
                Ok(())
            }
            fn send_interrupt(&self, _: &mut dyn BackendStdin) -> Result<(), String> {
                Ok(())
            }
            fn send_set_model(&self, _: &mut dyn BackendStdin, _: &str) -> Result<(), String> {
                Ok(())
            }
            fn send_set_permission_mode(
                &self,
                _: &mut dyn BackendStdin,
                _: &str,
            ) -> Result<(), String> {
                Ok(())
            }
        }
        assert_eq!(
            CappedBackend.call_batch_capability().max_concurrency,
            Some(1)
        );
    }

    // =========================================================================
    // Backend factory
    // =========================================================================

    #[test]
    fn backend_factory_defaults_to_claude() {
        let backend = backend_for_name(None);
        assert_eq!(backend.name(), "Claude");
    }

    #[test]
    fn backend_factory_returns_claude_for_claude() {
        let backend = backend_for_name(Some("claude"));
        assert_eq!(backend.name(), "Claude");
    }

    #[test]
    fn backend_factory_returns_codex() {
        let backend = backend_for_name(Some("codex"));
        assert_eq!(backend.name(), "Codex");
    }

    #[test]
    fn backend_factory_unknown_falls_back_to_claude() {
        let backend = backend_for_name(Some("unknown"));
        assert_eq!(backend.name(), "Claude");
    }

    #[test]
    fn session_start_helpers_cover_all_modes() {
        let new_start = SessionStart::New {
            session_id: "session-new".into(),
        };
        assert_eq!(new_start.session_id(), "session-new");
        assert_eq!(new_start.backend_id(), None);
        assert_eq!(new_start.source_backend_id(), None);

        let resume_start = SessionStart::Resume {
            session_id: "session-resume".into(),
            backend_id: "backend-resume".into(),
        };
        assert_eq!(resume_start.session_id(), "session-resume");
        assert_eq!(resume_start.backend_id(), Some("backend-resume"));
        assert_eq!(resume_start.source_backend_id(), None);

        let fork_start = SessionStart::Fork {
            session_id: "session-fork".into(),
            source_backend_id: "backend-source".into(),
        };
        assert_eq!(fork_start.session_id(), "session-fork");
        assert_eq!(fork_start.backend_id(), None);
        assert_eq!(fork_start.source_backend_id(), Some("backend-source"));
    }

    // =========================================================================
    // backend_for_model
    // =========================================================================

    #[test]
    fn backend_for_model_claude_models() {
        assert_eq!(backend_for_model("sonnet"), None);
        assert_eq!(backend_for_model("opus"), None);
        assert_eq!(backend_for_model("haiku"), None);
        assert_eq!(backend_for_model("fable"), None);
    }

    #[test]
    fn backend_for_model_codex_models() {
        assert_eq!(backend_for_model("gpt-5.4-mini"), Some("codex"));
        assert_eq!(backend_for_model("codex-mini"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.4"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.3-codex"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.3-codex-spark"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.2-codex"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.2"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.1-codex-max"), Some("codex"));
        assert_eq!(backend_for_model("gpt-5.1-codex-mini"), Some("codex"));
    }

    // =========================================================================
    // effective_backend_name
    // =========================================================================

    fn selection(backend: &str, model: &str) -> crate::models::ModelSelection {
        crate::models::ModelSelection::new(backend, Model::new(model))
    }

    /// The CAIRN-3798 regression, stated as the resolution that produces it: a
    /// thread persisted on `fable` whose agent default has since resolved to a
    /// Codex model. The selection describes a different model, so it does not
    /// get to name the provider for this one — `fable` runs on Claude, which is
    /// what both the rotation check and the spawn must see.
    #[test]
    fn an_agent_default_never_reinterprets_the_job_model() {
        assert_eq!(
            effective_backend_name(
                Some(&selection("codex", "gpt-5.6-sol")),
                Some("codex"),
                Some("fable")
            )
            .as_deref(),
            Some("claude")
        );
        assert_eq!(
            backend_for_name(
                effective_backend_name(
                    Some(&selection("codex", "gpt-5.6-sol")),
                    Some("codex"),
                    Some("fable")
                )
                .as_deref()
            )
            .name(),
            "Claude"
        );
    }

    /// The deliberate switch, which is the same question answered the other way:
    /// the job's model IS the Codex one, so Codex is what serves it.
    #[test]
    fn an_explicit_codex_model_resolves_to_codex() {
        assert_eq!(
            effective_backend_name(
                Some(&selection("claude", "fable")),
                None,
                Some("gpt-5.6-sol")
            )
            .as_deref(),
            Some("codex")
        );
    }

    /// A selection naming the job's own model is the atomic pair, and it is the
    /// only authority that can spell a provider the model name cannot carry.
    #[test]
    fn a_matching_selection_speaks_for_its_model() {
        assert_eq!(
            effective_backend_name(
                Some(&selection("ollama", "gemma4:e4b")),
                None,
                Some("gemma4:e4b")
            )
            .as_deref(),
            Some("ollama")
        );
        // An OpenRouter reroute persists the OpenRouter model id, so the pair
        // survives a restart rebuilt from `jobs.model` alone.
        assert_eq!(
            effective_backend_name(
                Some(&selection("openrouter", "~anthropic/claude-sonnet-latest")),
                None,
                Some("~anthropic/claude-sonnet-latest")
            )
            .as_deref(),
            Some("openrouter")
        );
    }

    /// With no model persisted there is nothing to reinterpret, so the agent's
    /// own ladder answers: its atomic selection, then its authored preference.
    #[test]
    fn without_a_job_model_the_agent_ladder_answers() {
        assert_eq!(
            effective_backend_name(Some(&selection("codex", "gpt-5.6-sol")), None, None).as_deref(),
            Some("codex")
        );
        assert_eq!(
            effective_backend_name(None, Some("codex"), None).as_deref(),
            Some("codex")
        );
        assert_eq!(effective_backend_name(None, None, None), None);
    }

    // =========================================================================
    // resolve_tools (backend-level)
    // =========================================================================

    #[test]
    fn claude_resolve_tools_basic() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(&["Read".into(), "Bash".into()], &[]);
        // Should contain Cairn versions
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__run".into()));
        // Native versions in disallowed
        assert!(rt.disallowed.contains(&"Read".into()));
        assert!(rt.disallowed.contains(&"Bash".into()));
    }

    #[test]
    fn claude_resolve_tools_aliases_all_three_verbs() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(
            &["Read".into(), "Write".into(), "Edit".into(), "Bash".into()],
            &[],
        );
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__write".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__run".into()));
        // Friendly names never survive into allowed.
        assert!(!rt.allowed.contains(&"Read".into()));
        assert!(!rt.allowed.contains(&"Write".into()));
        assert!(!rt.allowed.contains(&"Edit".into()));
        assert!(!rt.allowed.contains(&"Bash".into()));
    }

    #[test]
    fn claude_resolve_tools_drops_and_disallows_all_native() {
        let backend = claude::ClaudeBackend;
        let native = [
            "Task",
            "TaskOutput",
            "AskUserQuestion",
            "WebFetch",
            "WebSearch",
            "Glob",
            "Grep",
            "LSP",
            "Skill",
            "NotebookEdit",
        ];
        let input: Vec<String> = std::iter::once("Read".to_string())
            .chain(native.iter().map(|t| t.to_string()))
            .collect();
        let rt = backend.resolve_tools(&input, &[]);
        for tool in native {
            assert!(
                !rt.allowed.contains(&tool.to_string()),
                "{tool} must be dropped from allowed"
            );
            assert!(
                rt.disallowed.contains(&tool.to_string()),
                "{tool} must be in Claude's disallowed list"
            );
        }
        // Only the aliased read verb survives (the dead `return` tool is no
        // longer auto-added — CAIRN-2505).
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
        assert!(!rt.allowed.contains(&"mcp__cairn__return".into()));
    }

    /// Every name on the roster, driven from the roster itself.
    ///
    /// Each entry is there because Claude Code declares that built-in to the
    /// model unless `--disallowedTools` names it, so the guarantee has to hold
    /// for the whole list rather than for whichever names a test once copied.
    /// The roster is fed in as agent config too: a stale config listing
    /// `TodoWrite` or `EnterPlanMode` must still be stripped from `allowed`
    /// (native `TodoWrite` would silently store nothing) rather than honoured.
    #[test]
    fn claude_resolve_tools_blocks_every_always_disallowed_tool() {
        let backend = claude::ClaudeBackend;
        let agent_tools: Vec<String> = std::iter::once("Read".to_string())
            .chain(
                crate::models::ALWAYS_DISALLOWED_TOOLS
                    .iter()
                    .map(|t| t.to_string()),
            )
            .collect();
        let rt = backend.resolve_tools(&agent_tools, &[]);
        for tool in crate::models::ALWAYS_DISALLOWED_TOOLS {
            assert!(
                rt.disallowed.contains(&tool.to_string()),
                "{tool} must be in Claude's disallowed list"
            );
            assert!(
                !rt.allowed.contains(&tool.to_string()),
                "{tool} must never be allowed, even when agent config lists it"
            );
        }
    }

    #[test]
    fn claude_resolve_tools_drops_dead_cairn_names() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(
            &[
                "mcp__cairn__task".into(),
                "mcp__cairn__batch_tasks".into(),
                "mcp__cairn__ask_user".into(),
                "mcp__cairn__web_fetch".into(),
                "mcp__cairn__web_search".into(),
            ],
            &[],
        );
        // None of the dead names survive; only the always-on core-verb floor
        // (CAIRN-1172) remains (the dead `return` tool is no longer auto-added
        // — CAIRN-2505).
        assert_eq!(
            rt.allowed,
            vec![
                "mcp__cairn__read".to_string(),
                "mcp__cairn__write".to_string(),
                "mcp__cairn__run".to_string(),
            ]
        );
    }

    #[test]
    fn claude_resolve_tools_keeps_corpus_tools() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(
            &["mcp__cairn__create_pr".into(), "mcp__cairn__read".into()],
            &[],
        );
        assert!(rt.allowed.contains(&"mcp__cairn__create_pr".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
    }

    #[test]
    fn claude_resolve_tools_includes_agent_disallowed() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(
            &["Read".into(), "mcp__cairn__update_issue".into()],
            &["mcp__cairn__update_issue".into()],
        );
        assert!(rt.disallowed.contains(&"mcp__cairn__update_issue".into()));
    }

    #[test]
    fn codex_resolve_tools_empty_disallowed() {
        let backend = codex::CodexBackend;
        let rt = backend.resolve_tools(&["Read".into(), "Bash".into()], &[]);
        // Codex should have tools in allowed
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__run".into()));
        // Codex disallowed is empty (Codex ignores it)
        assert!(rt.disallowed.is_empty());
    }

    // =========================================================================
    // to_legacy: non-canonical combinations (lossy by design)
    // =========================================================================

    #[test]
    fn to_legacy_fence_determines_legacy() {
        let allow = AgentPermissions {
            fence: Fence::Allow,
        };
        assert_eq!(allow.to_legacy_str(), "acceptEdits");

        let ask = AgentPermissions { fence: Fence::Ask };
        assert_eq!(ask.to_legacy_str(), "default");
    }

    // =========================================================================
    // resolve_tools: auto-added tools
    // =========================================================================

    #[test]
    fn claude_resolve_tools_does_not_add_dead_return() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(&["Read".into()], &[]);
        // The `return` tool was retired (return is now `write cairn:~/return`);
        // it must not be injected into the allow-list (CAIRN-2505).
        assert!(
            !rt.allowed.contains(&"mcp__cairn__return".into()),
            "dead return tool must not be auto-added"
        );
        assert!(
            !rt.allowed.contains(&"mcp__cairn__skill".into()),
            "skill tool was removed and must not be auto-added"
        );
    }

    #[test]
    fn codex_resolve_tools_does_not_add_dead_return() {
        let backend = codex::CodexBackend;
        let rt = backend.resolve_tools(&["Read".into()], &[]);
        // The `return` tool was retired; it must not be injected (CAIRN-2505).
        assert!(
            !rt.allowed.contains(&"mcp__cairn__return".into()),
            "dead return tool must not be auto-added"
        );
        assert!(
            !rt.allowed.contains(&"mcp__cairn__skill".into()),
            "skill tool was removed and must not be auto-added"
        );
    }

    #[test]
    fn codex_resolve_tools_ignores_agent_disallowed() {
        let backend = codex::CodexBackend;
        let rt =
            backend.resolve_tools(&["Read".into(), "Bash".into()], &["mcp__cairn__run".into()]);
        // Codex should still have empty disallowed — agent_disallowed is ignored
        assert!(rt.disallowed.is_empty());
        // And the tool should still be in allowed
        assert!(rt.allowed.contains(&"mcp__cairn__run".into()));
    }
}
