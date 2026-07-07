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
pub mod claude_usage;
pub mod codex;
pub mod context_window;
pub mod openrouter;
mod run_state;
pub mod stdin;
pub(crate) mod workflow;

pub use claude_usage::collect_claude_usage_snapshot;

use crate::agent_process::process::BackendStdin;
use crate::models::{Fence, Model};
use crate::orchestrator::Orchestrator;
use serde::{Deserialize, Serialize};

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
    pub fn session_id(&self) -> &str {
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
}

/// Backend-resolved agent permissions.
///
/// Each backend translates the canonical [`Fence`] into its own CLI flags or
/// protocol fields. Actual enforcement lives in Cairn's verb handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPermissions {
    pub fence: Fence,
}

impl AgentPermissions {
    pub fn new(fence: Fence) -> Self {
        Self { fence }
    }

    /// Convert to a legacy permission mode string for the runtime stdin protocol.
    pub fn to_legacy_str(&self) -> &'static str {
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
    pub allowed: Vec<String>,
    /// Tools the backend's native runtime should disallow.
    pub disallowed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredReasoningEffort {
    pub reasoning_effort: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredModel {
    pub id: String,
    pub model: String,
    pub display_name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub is_default: bool,
    pub default_reasoning_effort: Option<String>,
    #[serde(default)]
    pub supported_reasoning_efforts: Vec<DiscoveredReasoningEffort>,
    #[serde(default)]
    pub context_window: Option<i64>,
    #[serde(default)]
    pub canonical_slug: Option<String>,
    #[serde(default)]
    pub pricing: Option<DiscoveredModelPricing>,
    #[serde(default)]
    pub supported_parameters: Vec<String>,
    #[serde(default)]
    pub router: bool,
    #[serde(default)]
    pub architecture_modality: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredModelPricing {
    pub prompt: Option<String>,
    pub completion: Option<String>,
    pub request: Option<String>,
    pub image: Option<String>,
    pub web_search: Option<String>,
    pub internal_reasoning: Option<String>,
    pub input_cache_read: Option<String>,
    pub input_cache_write: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOptionDescriptor {
    /// Runtime-supported option key. Add variants here only when preset
    /// resolution and backend launch paths also carry the option end-to-end.
    pub key: ProviderOptionKey,
    pub label: String,
    pub kind: OptionKind,
    #[serde(default)]
    pub choices: Vec<OptionChoice>,
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OptionChoice {
    pub value: String,
    pub label: String,
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
    pub backend: String,
    pub models: Vec<DiscoveredModel>,
    #[serde(default)]
    pub options: Vec<ProviderOptionDescriptor>,
    pub refreshed_at: Option<i64>,
    pub error: Option<String>,
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
    pub run_id: String,
    /// Working directory for the agent process
    pub working_dir: String,
    /// The user prompt to send
    pub prompt: String,
    /// Agent role instructions (injected as system prompt content)
    pub system_prompt_content: Option<String>,
    /// The trailing per-run dynamic suffix of `system_prompt_content` (the
    /// orientation block + `</agent_role>` close). A suffix of
    /// `system_prompt_content`; used only to split the agent content into a static
    /// head and the inlined dynamic tail when recording the segment boundary map.
    pub system_prompt_dynamic_tail: Option<String>,
    /// Resolved model (job > agent > workspace default)
    pub model: Option<Model>,
    /// Explicit session start semantics for this backend invocation.
    pub session_start: SessionStart,
    /// Resolved allowed tools list
    pub allowed_tools: Vec<String>,
    /// Resolved disallowed tools list
    pub disallowed_tools: Vec<String>,
    /// The MCP config as a self-contained JSON string (built per run, never
    /// shared on disk). Claude passes it inline via `--mcp-config <json>`; Codex
    /// parses it to extract the cairn-cmd args.
    pub mcp_config_json: String,
    /// Stable home URI for this run (full node URI). Forwarded to the MCP child
    /// as `CAIRN_HOME_URI` so `cairn:~/...` shorthand resolves. Claude bakes this
    /// into its inline MCP config JSON env; Codex inherits it via the process env.
    pub home_uri: String,
    /// Max thinking tokens (None = disabled)
    pub max_thinking_tokens: Option<i32>,
    /// Codex: reasoning effort level ("low", "medium", "high", "xhigh")
    pub reasoning_effort: Option<String>,
    /// Service tier request id, if the backend supports one.
    pub service_tier: Option<String>,
    /// Canonical agent permissions (replaces opaque permission_mode string).
    pub permissions: AgentPermissions,
    /// Enable stdin streaming (bidirectional mode)
    pub bidirectional: bool,
    /// Pre-resolved identity for this session (includes project overrides).
    /// If set, backends use this instead of calling `orch.get_identity()`.
    pub identity: Option<crate::identity::UserIdentity>,
    /// Resolved JSON Schema to constrain the model's output natively (CAIRN-2505).
    /// Set only for node-less ephemeral calls that carry an output contract, so
    /// each backend passes the provider's native output constraint (Claude
    /// `--json-schema`, OpenRouter `response_format`, Codex per-turn
    /// `outputSchema`). `None` for ordinary agent sessions, which are unchanged.
    pub output_schema: Option<serde_json::Value>,
}

impl SessionConfig {}

/// Trait for agent execution backends.
///
/// Backends are responsible for spawning the agent process, registering it
/// in process state, and starting the reader thread that streams events
/// into the database.
pub trait AgentBackend: Send + Sync {
    /// Human-readable name (e.g. "Claude", "Codex")
    fn name(&self) -> &str;

    /// Check if this backend is available (binary exists, API key set, etc.)
    fn is_available(&self) -> Result<(), String>;

    /// Discover currently available model options for this backend.
    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String>;

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

    /// Whether this backend supports session resume (--resume)
    fn supports_resume(&self) -> bool;

    /// Whether this backend supports warm process retention
    fn supports_warm_processes(&self) -> bool;

    /// Send a user message to a running process via stdin.
    fn send_user_message(
        &self,
        stdin: &mut dyn BackendStdin,
        content: &str,
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

/// Create an AgentBackend for the given backend name.
/// Returns ClaudeBackend for None or "claude", CodexBackend for "codex".
pub fn backend_for_name(name: Option<&str>) -> Box<dyn AgentBackend> {
    match name {
        Some("codex") => Box::new(codex::CodexBackend),
        Some("openrouter") => Box::new(openrouter::OpenRouterBackend),
        _ => Box::new(claude::ClaudeBackend),
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
pub fn backend_for_model(model: &str) -> Option<&'static str> {
    let lower = model.to_lowercase();
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
    }

    #[test]
    fn codex_backend_trait_properties() {
        let backend = codex::CodexBackend;
        assert_eq!(backend.name(), "Codex");
        assert!(backend.supports_resume());
        assert!(backend.supports_warm_processes());
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
    fn claude_resolve_tools_blocks_always_disallowed() {
        let backend = claude::ClaudeBackend;
        // Even if agent config includes planning tools, they must be blocked
        let rt = backend.resolve_tools(
            &["Read".into(), "EnterPlanMode".into(), "ExitPlanMode".into()],
            &[],
        );
        assert!(
            rt.disallowed.contains(&"EnterPlanMode".into()),
            "EnterPlanMode must be disallowed"
        );
        assert!(
            rt.disallowed.contains(&"ExitPlanMode".into()),
            "ExitPlanMode must be disallowed"
        );
        assert!(
            !rt.allowed.contains(&"EnterPlanMode".into()),
            "EnterPlanMode must not be in allowed"
        );
        assert!(
            !rt.allowed.contains(&"ExitPlanMode".into()),
            "ExitPlanMode must not be in allowed"
        );
    }

    #[test]
    fn claude_resolve_tools_blocks_host_harness_tools() {
        let backend = claude::ClaudeBackend;
        // Claude Code (the host harness) declares its built-in tools to the
        // model unless they are named in --disallowedTools. None has a Cairn
        // equivalent, so even if an agent config lists one it must be stripped
        // from `allowed` and present in `disallowed`.
        let harness_tools = [
            "CronCreate",
            "CronDelete",
            "CronList",
            "ScheduleWakeup",
            "RemoteTrigger",
            "EnterWorktree",
            "ExitWorktree",
            "ListMcpResourcesTool",
            "ReadMcpResourceTool",
            "Monitor",
            "TaskStop",
            "PushNotification",
            "DesignSync",
        ];
        let agent_tools: Vec<String> = harness_tools.iter().map(|t| t.to_string()).collect();
        let rt = backend.resolve_tools(&agent_tools, &[]);
        for tool in harness_tools {
            assert!(
                rt.disallowed.contains(&tool.to_string()),
                "{tool} must be disallowed"
            );
            assert!(
                !rt.allowed.contains(&tool.to_string()),
                "{tool} must not be in allowed"
            );
        }
    }

    #[test]
    fn claude_resolve_tools_blocks_native_todo_write() {
        let backend = claude::ClaudeBackend;
        // Agents previously carried `TodoWrite`; todos now go through `write`.
        // Native TodoWrite must be disallowed, never silently enabled (it would
        // store nothing).
        let rt = backend.resolve_tools(&["Read".into(), "TodoWrite".into()], &[]);
        assert!(
            rt.disallowed.contains(&"TodoWrite".into()),
            "TodoWrite must be disallowed"
        );
        assert!(
            !rt.allowed.contains(&"TodoWrite".into()),
            "TodoWrite must not be in allowed"
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
