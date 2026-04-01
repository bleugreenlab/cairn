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
pub mod codex;
pub mod stdin;

use crate::agent_process::process::BackendStdin;
use crate::models::{ApprovalPolicy, FilesystemScope, Model};
use crate::orchestrator::Orchestrator;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
/// Constructed from the two enum fields on agent configs / snapshots.
/// Each backend translates these into its own CLI flags or protocol fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPermissions {
    pub approval: ApprovalPolicy,
    pub filesystem: FilesystemScope,
}

impl AgentPermissions {
    pub fn new(approval: ApprovalPolicy, filesystem: FilesystemScope) -> Self {
        Self {
            approval,
            filesystem,
        }
    }

    /// Convert to a legacy permission mode string for the runtime stdin protocol.
    ///
    /// The stdin protocol (`send_set_permission_mode`) still speaks legacy strings.
    /// This is lossy for novel combinations — falls back by approval policy.
    pub fn to_legacy_str(&self) -> &'static str {
        match (self.approval, self.filesystem) {
            (ApprovalPolicy::AcceptAll, FilesystemScope::FullAccess) => "bypassPermissions",
            (ApprovalPolicy::AcceptAll, _) => "acceptEdits",
            (_, FilesystemScope::ReadOnly) => "plan",
            _ => "default",
        }
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModelCatalog {
    pub backend: String,
    pub models: Vec<DiscoveredModel>,
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
    /// Resolved model (job > agent > workspace default)
    pub model: Option<Model>,
    /// Explicit session start semantics for this backend invocation.
    pub session_start: SessionStart,
    /// Resolved allowed tools list
    pub allowed_tools: Vec<String>,
    /// Resolved disallowed tools list
    pub disallowed_tools: Vec<String>,
    /// Path to the MCP config file (already generated)
    pub mcp_config_path: PathBuf,
    /// Max thinking tokens (None = disabled)
    pub max_thinking_tokens: Option<i32>,
    /// Codex: reasoning effort level ("low", "medium", "high")
    pub reasoning_effort: Option<String>,
    /// Canonical agent permissions (replaces opaque permission_mode string).
    pub permissions: AgentPermissions,
    /// Enable stdin streaming (bidirectional mode)
    pub bidirectional: bool,
    /// Pre-resolved identity for this session (includes project overrides).
    /// If set, backends use this instead of calling `orch.get_identity()`.
    pub identity: Option<crate::identity::UserIdentity>,
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

    pub(crate) fn make_config(session_start: SessionStart) -> SessionConfig {
        SessionConfig {
            run_id: "run-1".into(),
            working_dir: "/tmp".into(),
            prompt: "hello".into(),
            system_prompt_content: None,
            model: None,
            session_start,
            allowed_tools: vec![],
            disallowed_tools: vec![],
            mcp_config_path: PathBuf::from("/tmp/mcp.json"),
            max_thinking_tokens: None,
            reasoning_effort: None,
            permissions: AgentPermissions::new(
                ApprovalPolicy::default(),
                FilesystemScope::default(),
            ),
            bidirectional: false,
            identity: None,
        }
    }

    // =========================================================================
    // AgentPermissions::to_legacy_str
    // =========================================================================

    #[test]
    fn to_legacy_str_bypass() {
        let perms = AgentPermissions::new(ApprovalPolicy::AcceptAll, FilesystemScope::FullAccess);
        assert_eq!(perms.to_legacy_str(), "bypassPermissions");
    }

    #[test]
    fn to_legacy_str_accept_edits() {
        let perms = AgentPermissions::new(ApprovalPolicy::AcceptAll, FilesystemScope::CwdOnly);
        assert_eq!(perms.to_legacy_str(), "acceptEdits");
    }

    #[test]
    fn to_legacy_str_ask_is_default() {
        let perms = AgentPermissions::new(ApprovalPolicy::Ask, FilesystemScope::CwdOnly);
        assert_eq!(perms.to_legacy_str(), "default");
    }

    #[test]
    fn to_legacy_str_reject_all_is_default() {
        let perms = AgentPermissions::new(ApprovalPolicy::RejectAll, FilesystemScope::CwdOnly);
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
        assert!(rt.allowed.contains(&"mcp__cairn__bash".into()));
        // Native versions in disallowed
        assert!(rt.disallowed.contains(&"Read".into()));
        assert!(rt.disallowed.contains(&"Bash".into()));
    }

    #[test]
    fn claude_resolve_tools_auto_adds_glob_grep() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(&["Read".into()], &[]);
        assert!(rt.allowed.contains(&"mcp__cairn__glob".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__grep".into()));
    }

    #[test]
    fn claude_resolve_tools_includes_agent_disallowed() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(
            &["Read".into(), "mcp__cairn__create_issue".into()],
            &["mcp__cairn__create_issue".into()],
        );
        assert!(rt.disallowed.contains(&"mcp__cairn__create_issue".into()));
    }

    #[test]
    fn codex_resolve_tools_empty_disallowed() {
        let backend = codex::CodexBackend;
        let rt = backend.resolve_tools(&["Read".into(), "Bash".into()], &[]);
        // Codex should have tools in allowed
        assert!(rt.allowed.contains(&"mcp__cairn__read".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__bash".into()));
        // Codex disallowed is empty (Codex ignores it)
        assert!(rt.disallowed.is_empty());
    }

    // =========================================================================
    // to_legacy: non-canonical combinations (lossy by design)
    // =========================================================================

    #[test]
    fn to_legacy_accept_all_filesystem_scope_determines_legacy() {
        // AcceptAll + CwdOnly → "acceptEdits"; AcceptAll + FullAccess → "bypassPermissions"
        let perms_cwd = AgentPermissions {
            approval: ApprovalPolicy::AcceptAll,
            filesystem: FilesystemScope::CwdOnly,
        };
        assert_eq!(perms_cwd.to_legacy_str(), "acceptEdits");

        let perms_full = AgentPermissions {
            approval: ApprovalPolicy::AcceptAll,
            filesystem: FilesystemScope::FullAccess,
        };
        assert_eq!(perms_full.to_legacy_str(), "bypassPermissions");
    }

    #[test]
    fn to_legacy_ask_full_access_falls_to_default() {
        let perms = AgentPermissions {
            approval: ApprovalPolicy::Ask,
            filesystem: FilesystemScope::FullAccess,
        };
        assert_eq!(perms.to_legacy_str(), "default");
    }

    // =========================================================================
    // resolve_tools: auto-added tools
    // =========================================================================

    #[test]
    fn claude_resolve_tools_auto_adds_return_and_skill() {
        let backend = claude::ClaudeBackend;
        let rt = backend.resolve_tools(&["Read".into()], &[]);
        assert!(
            rt.allowed.contains(&"mcp__cairn__return".into()),
            "return tool should be auto-added"
        );
        assert!(
            rt.allowed.contains(&"mcp__cairn__skill".into()),
            "skill tool should be auto-added"
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
    fn codex_resolve_tools_auto_adds_return_and_skill() {
        let backend = codex::CodexBackend;
        let rt = backend.resolve_tools(&["Read".into()], &[]);
        assert!(
            rt.allowed.contains(&"mcp__cairn__return".into()),
            "return tool should be auto-added"
        );
        assert!(
            rt.allowed.contains(&"mcp__cairn__skill".into()),
            "skill tool should be auto-added"
        );
    }

    #[test]
    fn codex_resolve_tools_ignores_agent_disallowed() {
        let backend = codex::CodexBackend;
        let rt = backend.resolve_tools(
            &["Read".into(), "Bash".into()],
            &["mcp__cairn__bash".into()],
        );
        // Codex should still have empty disallowed — agent_disallowed is ignored
        assert!(rt.disallowed.is_empty());
        // And the tool should still be in allowed
        assert!(rt.allowed.contains(&"mcp__cairn__bash".into()));
    }

    #[test]
    fn codex_resolve_tools_auto_adds_glob_grep() {
        let backend = codex::CodexBackend;
        let rt = backend.resolve_tools(&["Read".into()], &[]);
        assert!(rt.allowed.contains(&"mcp__cairn__glob".into()));
        assert!(rt.allowed.contains(&"mcp__cairn__grep".into()));
    }
}
