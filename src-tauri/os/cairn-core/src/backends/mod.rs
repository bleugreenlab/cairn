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

pub mod actual;
mod anthropic_messages;
use cairn_models::types::catalog;
pub mod claude;
mod claude_transcript;
pub mod codex;
pub mod custom;
mod http_loop;
pub(crate) mod launch;
pub use launch::Admitted;
pub mod ollama;
mod one_shot;
mod openai_compat;
mod openai_responses;
pub mod opencode;
pub mod openrouter;
mod run_state;
pub(crate) use run_state::is_task_spawned_run;
pub mod stdin;
pub(crate) mod workflow;

#[cfg(feature = "test-utils")]
pub fn replayed_anthropic_context_for_test(
    orch: &crate::orchestrator::Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<serde_json::Value, String> {
    serde_json::to_value(anthropic_messages::conversation::load_prior_messages(
        orch,
        session_id,
        current_run_id,
        project_id,
        project_key,
    )?)
    .map_err(|error| error.to_string())
}

#[cfg(test)]
use crate::models::Fence;
use crate::models::Model;
use crate::orchestrator::Orchestrator;
pub use cairn_models::wire::CompletionError;

impl From<&SessionConfig> for cairn_models::wire::GenerationConfig {
    fn from(config: &SessionConfig) -> Self {
        Self {
            reasoning_effort: config.reasoning_effort.clone(),
            max_thinking_tokens: config.max_thinking_tokens,
            output_schema: config.output_schema.clone(),
            tools: cairn_models::wire::cairn_tool_definitions(),
        }
    }
}

/// The generation knobs of a one-shot completion: the caller's reasoning and
/// output schema, and no tools. The tool-free twin of the session conversion
/// above, so an HTTP provider builds a completion with the same wire builder,
/// and maps the caller's options the same way, as it builds a session turn.
pub(crate) fn completion_generation(
    request: &CompletionRequest,
) -> cairn_models::wire::GenerationConfig {
    cairn_models::wire::GenerationConfig {
        reasoning_effort: request.options.reasoning_effort.clone(),
        max_thinking_tokens: request.options.max_thinking_tokens,
        output_schema: request.output_schema.clone(),
        tools: Vec::new(),
    }
}

pub use cairn_models::interface::session::SessionStart;
use cairn_models::session::mcp::{McpServer, McpServers};

/// Cairn's one MCP server as the set a session or a Codex app-server is given:
/// every agent Cairn launches is given `cairn` and nothing else.
pub(crate) fn cairn_servers(server: &McpServer) -> McpServers {
    McpServers::new(vec![server.clone()]).expect("Cairn's own MCP server name is valid")
}
use cairn_models::interface::decide::DecisionRoute;
use cairn_models::interface::SelectedOffer;
use cairn_models::types::catalog::{DiscoveredModel, ProviderOptionDescriptor, ServedVerb};
pub use cairn_session::config::*;

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
    /// Stable assigned work for the provider's durable continuation goal.
    pub(crate) goal_objective: Option<String>,
    /// Ensure this provider thread has no native goal. Used by standing project
    /// threads, whose perpetual jurisdiction is driven only by admitted attention.
    pub(crate) clear_native_goal: bool,
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
    /// How the agent's CLI reaches Cairn's MCP server: the stdio `cairn-cmd`
    /// shim or the streamable-HTTP endpoint. Built per run, never shared on
    /// disk; each backend gives it as the session's one server
    /// ([`cairn_servers`]).
    pub(crate) mcp: McpServer,
    /// The run bearer an HTTP attachment's agent presents. `None` for stdio,
    /// which authenticates with the machine secret instead.
    pub(crate) mcp_bearer: Option<crate::mcp::run_bearer::RunBearer>,
    /// Stable home URI for this run (full node URI). The stdio attachment
    /// forwards it to `cairn-cmd` as `CAIRN_HOME_URI`.
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
    /// Durable account pin resolved before backend launch. This is metadata,
    /// never a credential, and records which auth was frozen into the process.
    pub(crate) account_id: Option<String>,
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

/// The facet's reading of a fence: how much a session may do without asking.
pub(crate) fn approval_mode(
    fence: crate::models::Fence,
) -> cairn_models::interface::session::ApprovalMode {
    use cairn_models::interface::session::ApprovalMode;
    match fence {
        crate::models::Fence::Allow => ApprovalMode::Allow,
        crate::models::Fence::Ask => ApprovalMode::Ask,
        crate::models::Fence::Deny => ApprovalMode::Deny,
    }
}

pub trait AgentBackend: Send + Sync {
    /// Human-readable name (e.g. "Claude", "Codex")
    fn name(&self) -> &str;

    /// Check if this backend is available (binary exists, API key set, etc.)
    fn is_available(&self) -> Result<(), String>;

    /// Discover currently available model options for this backend.
    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String>;

    /// What this backend serves for any model it routes in `project_id`'s
    /// scope, each verb with the wire that carries it. A model whose discovery
    /// declares what it serves answers for itself ([`DiscoveredModel::serves`]);
    /// this is what every other model serves, including one a tier is bound to
    /// that discovery has not reported.
    fn serves(&self, orch: &Orchestrator, project_id: Option<&str>) -> Vec<ServedVerb>;

    /// The deployment that measures `offer`'s decisions: the catalog chose
    /// `offer` because this backend's catalog said it serves `decide` over
    /// `offer.served.wire`, and the backend knows where that wire is reached
    /// and with what credentials. A backend that serves no `decide` keeps the
    /// refusal.
    fn decision_route(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
        offer: &SelectedOffer,
    ) -> Result<DecisionRoute, String> {
        Err(format!("{} cannot answer a decision", offer.provider))
    }

    /// Models Cairn itself declares this backend serves, whatever discovery
    /// finds or whether it has run: an offer the facet owns rather than one
    /// the provider lists (Codex's image tool). The provider's catalog record
    /// always carries them, so what they serve is known before discovery
    /// completes and after it fails; discovery includes them too.
    fn declared_models(&self) -> Vec<DiscoveredModel> {
        Vec::new()
    }

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
    /// Only [`launch::start_session`] can supply `admitted`, so every session
    /// passes the verb gate first.
    fn start_session(
        &self,
        config: SessionConfig,
        orch: &Orchestrator,
        admitted: Admitted,
    ) -> Result<(), String>;

    /// Execute one tool-free request without creating agent/session state.
    /// Only [`launch::complete`] can supply the admission, so every
    /// completion passes the verb gate first.
    fn complete(
        &self,
        _request: CompletionRequest,
        _orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<CompletionOutcome, CompletionError> {
        Err(CompletionError::BackendUnavailable)
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::DedicatedProcess
    }

    /// Whether this backend supports session resume (--resume)
    fn supports_resume(&self) -> bool;

    /// Whether a fresh start hands the provider Cairn's own session id as the
    /// conversation's id. Such a provider creates the conversation under that
    /// id at launch, before Cairn confirms a handle, and refuses the same id on
    /// a second fresh start — so an unconfirmed launch cannot simply be retried.
    fn fresh_start_claims_session_id(&self) -> bool {
        false
    }

    /// Whether this backend supports warm process retention
    fn supports_warm_processes(&self) -> bool;

    /// Resolve provider-cost admission metadata for a concrete launch.
    /// Backends with multiple launch shapes must override this method.
    fn runtime_launch_capability(
        &self,
        _launch: &RuntimeLaunch,
    ) -> Result<RuntimeLaunchCapability, String> {
        Ok(RuntimeLaunchCapability {
            class: RuntimeClass::DedicatedProcess,
            lane: RuntimeLane::new("default-dedicated-process"),
            host_requirement: RuntimeHostRequirement::ResidentOn(RuntimeHostKind::Runner),
            claim: RuntimeResourceClaim {
                resident_process_units: 1,
                server_instance_key: None,
                logical_stream_units: 1,
                estimated_memory_bytes: None,
            },
            supports_warm_reuse: self.supports_warm_processes(),
        })
    }

    /// Ephemeral-call batching capability. Default is the behavior-preserving
    /// dedicated-process, unbounded shape. The calls path (only) consults this;
    /// ordinary node/task sessions never reach it.
    fn call_batch_capability(&self) -> CallBatchCapability {
        CallBatchCapability {
            shape: CallBatchShape::DedicatedProcess,
            max_concurrency: None,
        }
    }
}

/// Create an AgentBackend for the given backend name.
/// Returns ClaudeBackend for None or "claude", CodexBackend for "codex".
pub(crate) fn backend_for_name(name: Option<&str>) -> Box<dyn AgentBackend> {
    match name {
        Some("codex") => Box::new(codex::CodexBackend),
        Some("openrouter") => Box::new(openrouter::OpenRouterBackend),
        Some(opencode::OPENCODE_BACKEND_KEY) => Box::new(opencode::OpenCodeBackend),
        Some("ollama") => Box::new(ollama::OllamaBackend),
        Some(actual::ACTUAL_BACKEND_KEY) => Box::new(actual::ActualBackend),
        // A custom provider's key names the account that defines it. Resolving
        // the account itself needs a workspace, which this has none of, so the
        // backend carries the id and looks the rest up when it is handed an
        // orchestrator.
        Some(key) if catalog::custom_account_id(key).is_some() => {
            Box::new(custom::CustomBackend::new(
                catalog::custom_account_id(key).expect("guarded by the match"),
            ))
        }
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
pub(crate) use cairn_models::types::catalog::{backend_for_model, CLAUDE_FAMILY_BACKEND};

#[cfg(test)]
mod backend_failure_tests {
    use super::BackendFailure;

    #[test]
    fn exit_reason_round_trips() {
        for (failure, token) in [
            (BackendFailure::SessionUnresolvable, "session_unresolvable"),
            (BackendFailure::SessionIdInUse, "session_id_in_use"),
            (BackendFailure::StartupRejected, "session_start_rejected"),
        ] {
            assert_eq!(failure.exit_reason(), token);
            assert_eq!(
                BackendFailure::from_exit_reason(failure.exit_reason()),
                Some(failure)
            );
        }
    }

    #[test]
    fn existing_exit_reasons_are_not_backend_failures() {
        // `runs.exit_reason` is a shared column; the tokens other paths write
        // must never be read back as a backend diagnosis.
        for reason in [
            "capacity_retry",
            "turn_failed",
            "crash",
            "",
            "session unresolvable",
        ] {
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

    #[test]
    fn backend_runtime_capabilities_describe_physical_launch_cost() {
        let ordinary = RuntimeLaunch::default();

        let claude = claude::ClaudeBackend
            .runtime_launch_capability(&ordinary)
            .unwrap();
        assert_eq!(claude.class, RuntimeClass::DedicatedProcess);
        assert_eq!(claude.lane, RuntimeLane::new("claude-cli"));
        assert_eq!(claude.claim.resident_process_units, 1);
        assert!(claude.claim.estimated_memory_bytes.is_some());
        assert!(claude.supports_warm_reuse);

        for capability in [
            openrouter::OpenRouterBackend
                .runtime_launch_capability(&ordinary)
                .unwrap(),
            opencode::OpenCodeBackend
                .runtime_launch_capability(&ordinary)
                .unwrap(),
        ] {
            assert_eq!(capability.class, RuntimeClass::StatelessHttp);
            assert_eq!(capability.host_requirement, RuntimeHostRequirement::None);
            assert_eq!(capability.claim.resident_process_units, 0);
            assert_eq!(capability.claim.logical_stream_units, 1);
        }
    }

    #[test]
    fn codex_capability_distinguishes_dedicated_and_pooled_launches() {
        let backend = codex::CodexBackend;
        let dedicated = backend
            .runtime_launch_capability(&RuntimeLaunch::default())
            .unwrap();
        assert_eq!(dedicated.class, RuntimeClass::DedicatedProcess);
        assert_eq!(dedicated.claim.resident_process_units, 1);
        assert!(dedicated.claim.server_instance_key.is_none());

        let pooled = backend
            .runtime_launch_capability(&RuntimeLaunch {
                is_ephemeral_call: true,
                server_instance_key: Some("codex-oauth:account-a".to_string()),
            })
            .unwrap();
        assert_eq!(pooled.class, RuntimeClass::MultiplexedServer);
        assert_eq!(pooled.claim.resident_process_units, 1);
        assert_eq!(pooled.claim.logical_stream_units, 1);
        assert_eq!(
            pooled.claim.server_instance_key.as_deref(),
            Some("codex-oauth:account-a")
        );

        // The call funnel classifies runtime cost before Codex auth resolves,
        // so a keyless ephemeral call is a provisional claim, not a launch
        // failure. Durable admission replaces the key before leasing.
        let provisional = backend
            .runtime_launch_capability(&RuntimeLaunch {
                is_ephemeral_call: true,
                server_instance_key: None,
            })
            .expect("a keyless ephemeral call classifies provisionally");
        assert_eq!(provisional.class, RuntimeClass::MultiplexedServer);
        assert!(provisional.claim.server_instance_key.is_none());
    }

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
            fn serves(&self, _: &Orchestrator, _: Option<&str>) -> Vec<ServedVerb> {
                Vec::new()
            }
            fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
                ResolvedTools {
                    allowed: Vec::new(),
                    disallowed: Vec::new(),
                }
            }
            fn start_session(
                &self,
                _: SessionConfig,
                _: &Orchestrator,
                _admitted: crate::backends::Admitted,
            ) -> Result<(), String> {
                Ok(())
            }
            fn supports_resume(&self) -> bool {
                false
            }
            fn supports_warm_processes(&self) -> bool {
                false
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
            fn serves(&self, _: &Orchestrator, _: Option<&str>) -> Vec<ServedVerb> {
                Vec::new()
            }
            fn resolve_tools(&self, _: &[String], _: &[String]) -> ResolvedTools {
                ResolvedTools {
                    allowed: Vec::new(),
                    disallowed: Vec::new(),
                }
            }
            fn start_session(
                &self,
                _: SessionConfig,
                _: &Orchestrator,
                _admitted: crate::backends::Admitted,
            ) -> Result<(), String> {
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
    fn backend_factory_resolves_a_custom_key_to_its_own_provider() {
        // Two custom keys are two different backends, each naming its account.
        // Falling back to Claude here would run a job on a provider the user
        // never chose.
        let first = backend_for_name(Some("custom:acc_1"));
        let second = backend_for_name(Some("custom:acc_2"));
        assert_eq!(first.name(), "custom:acc_1");
        assert_eq!(second.name(), "custom:acc_2");
        assert!(first.supports_resume());
        assert!(!first.supports_warm_processes());
    }

    #[test]
    fn backend_factory_does_not_mistake_a_malformed_custom_key_for_a_provider() {
        assert_eq!(backend_for_name(Some("custom:")).name(), "Claude");
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
        assert_eq!(backend_for_model("gpt-6-astra"), Some("codex"));
        assert_eq!(backend_for_model("GPT-6-ASTRA"), Some("codex"));
        assert_eq!(resolved_backend_for_model("gpt-6-astra"), "codex");
        assert_eq!(backend_for_model("openai/gpt-6-astra"), Some("openrouter"));
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

    #[test]
    fn stale_agent_selection_still_constructs_the_claude_runtime() {
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
        // Native names are aliased away, and `--tools ""` keeps the built-ins
        // off, so nothing needs disallowing by name.
        assert!(!rt.allowed.contains(&"Read".into()));
        assert!(!rt.allowed.contains(&"Bash".into()));
        assert!(rt.disallowed.is_empty(), "{:?}", rt.disallowed);
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
    fn claude_resolve_tools_drops_every_native_name() {
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
            "FutureBuiltin",
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
        }
        assert!(rt.disallowed.is_empty(), "{:?}", rt.disallowed);
        // Only the aliased read verb survives (the dead `return` tool is no
        // longer auto-added — CAIRN-2505).
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
        assert!(!rt.allowed.contains(&"mcp__cairn__return".into()));
    }

    /// No bare name reaches `allowed`, whether a planning or todo tool a stale
    /// config still lists (native `TodoWrite` would silently store nothing), a
    /// known host-harness built-in, or one no list has ever named. The
    /// disallow list is exactly the agent's own MCP denials, sorted and
    /// deduped, since `--tools ""` already keeps every built-in off.
    #[test]
    fn claude_resolve_tools_never_allows_a_bare_name_and_disallows_only_the_agents_list() {
        let backend = claude::ClaudeBackend;
        let agent_tools: Vec<String> = [
            "Read",
            "TodoWrite",
            "EnterPlanMode",
            "Monitor",
            "SendMessage",
            "FutureBuiltin",
            "mcp__cairn__create_pr",
        ]
        .map(String::from)
        .to_vec();
        let agent_disallowed: Vec<String> = [
            "mcp__cairn__update_issue",
            "mcp__axon__look",
            "mcp__cairn__update_issue",
        ]
        .map(String::from)
        .to_vec();
        let rt = backend.resolve_tools(&agent_tools, &agent_disallowed);
        assert!(
            rt.allowed.iter().all(|tool| tool.starts_with("mcp__")),
            "{:?}",
            rt.allowed
        );
        assert!(rt.allowed.contains(&"mcp__cairn__create_pr".into()));
        assert_eq!(
            rt.disallowed,
            vec![
                "mcp__axon__look".to_string(),
                "mcp__cairn__update_issue".to_string()
            ]
        );
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

    #[test]
    fn codex_call_cost_classification_does_not_require_auth_resolution() {
        let backend = codex::CodexBackend;
        let capability = backend
            .runtime_launch_capability(&RuntimeLaunch {
                is_ephemeral_call: true,
                server_instance_key: None,
            })
            .expect("the pre-auth call funnel must remain launchable");

        assert_eq!(capability.class, RuntimeClass::MultiplexedServer);
        assert_eq!(capability.claim.resident_process_units, 1);
        assert_eq!(capability.claim.logical_stream_units, 1);
        assert_eq!(capability.claim.server_instance_key, None);
    }

    // =========================================================================
    // to_legacy: non-canonical combinations (lossy by design)
    // =========================================================================

    #[test]
    fn to_legacy_fence_determines_legacy() {
        let allow = AgentPermissions::new(Fence::Allow);
        assert_eq!(allow.to_legacy_str(), "acceptEdits");

        let ask = AgentPermissions::new(Fence::Ask);
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

mod transcript_host;

/// Compare `rendered` with the checked-in golden at `path`, or rewrite the
/// golden when `CAIRN_UPDATE_GOLDENS=1`. A golden pins observable output across
/// a refactor, so regenerating one is a claim that the change was intended.
#[cfg(test)]
pub(crate) fn assert_golden_file(path: &str, rendered: &str) {
    if std::env::var("CAIRN_UPDATE_GOLDENS").as_deref() == Ok("1") {
        let path = std::path::Path::new(path);
        std::fs::create_dir_all(path.parent().expect("a golden lives in a directory")).unwrap();
        std::fs::write(path, rendered).unwrap();
        return;
    }
    let golden = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("golden {path} is unreadable: {error}"));
    assert!(
        golden == rendered,
        "{path} drifted (rerun with CAIRN_UPDATE_GOLDENS=1 only for an intended change)\n--- golden\n{golden}\n--- rendered\n{rendered}"
    );
}

pub(crate) use cairn_models::types::catalog::resolved_backend_for_model;

pub(crate) use cairn_config::providers::effective_backend_name;

#[cfg(test)]
mod catalog_coverage_tests {
    use cairn_models::types::catalog::{CLAUDE_FAMILY_BACKEND, PROVIDER_CATALOG};

    #[test]
    fn a_backend_exists_for_every_catalog_entry() {
        // `backend_for_name` totalizes an unknown name to Claude, so a
        // mistyped key would silently resolve to the wrong provider rather
        // than fail. Only the Claude entry may land on that default.
        let default_backend = super::backend_for_name(None);
        for entry in PROVIDER_CATALOG {
            let backend = super::backend_for_name(Some(entry.key.as_ref()));
            if entry.key == CLAUDE_FAMILY_BACKEND {
                continue;
            }
            assert_ne!(
                backend.name(),
                default_backend.name(),
                "catalog entry {} has no backend of its own",
                entry.key
            );
        }
    }
}
