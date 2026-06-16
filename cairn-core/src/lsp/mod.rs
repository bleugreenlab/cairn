//! Cairn-side LSP engine: spawn, drive, and pool Language Server Protocol
//! servers so the read surface can answer semantic code-navigation questions
//! (definition, references, hover, implementations, callers, subtypes).
//!
//! This is the **engine layer** (Phase 2): a JSON-RPC transport ([`client`]),
//! a per-worktree instance pool ([`manager`]), deterministic extension/root
//! routing ([`routing`]), the name-to-position resolution chain ([`queries`]),
//! and read-style rendering ([`render`]). The `read`-dispatcher wiring, Tauri
//! commands, and settings UI sit on top of this in later phases.
//!
//! The transport is a **blocking reader thread per instance** (not an async
//! task): the [`crate::services::ProcessSpawner`] abstraction exposes only
//! blocking `std::io` child stdio, and the engine reuses it so the client is
//! unit-testable against the existing mock spawner. See [`client`] for the
//! threading model and [`manager`] for the pool and idle eviction.

pub mod client;
pub mod edit;
pub mod manager;
pub mod queries;
pub mod render;
pub mod routing;

use std::path::PathBuf;

/// The semantic code-navigation operations the engine exposes. Each maps to one
/// or more LSP requests (see [`queries`]) and to a single server-capability key
/// used for honest capability gating ([`LspOp::capability_key`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LspOp {
    /// `textDocument/definition`.
    Definition,
    /// `textDocument/references`.
    References,
    /// `textDocument/hover`.
    Hover,
    /// `textDocument/implementation`.
    Implementations,
    /// `textDocument/prepareCallHierarchy` + `callHierarchy/incomingCalls`.
    Callers,
    /// `textDocument/prepareTypeHierarchy` + `typeHierarchy/subtypes`.
    Subtypes,
    /// `textDocument/rename` (optionally gated by `textDocument/prepareRename`).
    /// The engine's one write op: it computes a `WorkspaceEdit` rather than a
    /// read-style location result, so it is driven through
    /// [`queries::compute_rename`], never [`queries::execute_op_at`].
    Rename,
}

impl LspOp {
    /// The stable lowercase op name used in descriptors and errors.
    pub fn as_str(&self) -> &'static str {
        match self {
            LspOp::Definition => "definition",
            LspOp::References => "references",
            LspOp::Hover => "hover",
            LspOp::Implementations => "implementations",
            LspOp::Callers => "callers",
            LspOp::Subtypes => "subtypes",
            LspOp::Rename => "rename",
        }
    }

    /// Parse an op from its [`LspOp::as_str`] name.
    pub fn from_name(s: &str) -> Option<LspOp> {
        Some(match s {
            "definition" => LspOp::Definition,
            "references" => LspOp::References,
            "hover" => LspOp::Hover,
            "implementations" => LspOp::Implementations,
            "callers" => LspOp::Callers,
            "subtypes" => LspOp::Subtypes,
            "rename" => LspOp::Rename,
            _ => return None,
        })
    }

    /// The `ServerCapabilities` JSON key whose truthy presence (`true` or an
    /// options object — not `false`/`null`/absent) means the server can answer
    /// this op. Drives honest capability degradation: an op the server does not
    /// advertise returns an explicit "unsupported" result instead of hanging.
    pub fn capability_key(&self) -> &'static str {
        match self {
            LspOp::Definition => "definitionProvider",
            LspOp::References => "referencesProvider",
            LspOp::Hover => "hoverProvider",
            LspOp::Implementations => "implementationProvider",
            LspOp::Callers => "callHierarchyProvider",
            LspOp::Subtypes => "typeHierarchyProvider",
            LspOp::Rename => "renameProvider",
        }
    }

    /// Whether executing this op queries the whole project (so it must wait on
    /// indexing readiness before issuing). `references`, `callers`, and
    /// `subtypes` fan out across the index; `definition`, `hover`, and
    /// `implementations` are localized position lookups. (Name resolution via
    /// `workspace/symbol` always waits on readiness regardless of the op.)
    // Readiness gating is currently driven inside `execute_op_at` per op; this
    // classifier is retained for the Phase-4 status/scheduling surface.
    #[allow(dead_code)]
    pub fn is_project_wide(&self) -> bool {
        matches!(
            self,
            LspOp::References | LspOp::Callers | LspOp::Subtypes | LspOp::Rename
        )
    }
}

/// The engine declined to spawn or reuse a server for a structural reason
/// (missing indexing root, no OS sandbox, empty command, spawn failure). This
/// is distinct from a per-request [`LspError`]: the server was never reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unavailable {
    pub reason: String,
}

impl Unavailable {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LSP unavailable: {}", self.reason)
    }
}

impl std::error::Error for Unavailable {}

/// A per-request failure once a server is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LspError {
    /// Transport/protocol failure: the spawn died, a frame was malformed, or a
    /// write failed. The reader thread closing unblocks pending requests with
    /// this rather than a timeout.
    Transport(String),
    /// A request exceeded its timeout with no response.
    Timeout(String),
    /// The server does not advertise support for this op (capability gating).
    Unsupported { op: LspOp, language: String },
}

impl std::fmt::Display for LspError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LspError::Transport(m) => write!(f, "lsp transport error: {m}"),
            LspError::Timeout(m) => write!(f, "lsp timeout: {m}"),
            LspError::Unsupported { op, language } => {
                write!(
                    f,
                    "{} is unsupported by the {language} language server",
                    op.as_str()
                )
            }
        }
    }
}

impl std::error::Error for LspError {}

/// Pool key: one running server per `(language, resolved indexing root)`. The
/// root is resolved by [`routing::resolve_root`] (nearest ancestor with a root
/// marker, clamped to the worktree, falling back to the worktree root), so a
/// monorepo with sub-roots gets one server per sub-root while a single-root
/// worktree collapses to one server keyed on the worktree root.
///
/// The root may resolve to the project's **main checkout** rather than the
/// worktree: when a worktree subroot is byte-identical to the main checkout's
/// (both clean, equal HEAD tree SHA — see [`routing::subroot_equivalent`]),
/// content-equivalent worktrees collapse onto one shared instance keyed on the
/// base root. This is the pragmatic form of content-identity keying. Full
/// tree-SHA keying — sharing across worktrees that match *each other* but not
/// main — is a future refinement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstanceKey {
    pub language: String,
    pub root: PathBuf,
}

impl InstanceKey {
    pub fn new(language: impl Into<String>, root: PathBuf) -> Self {
        Self {
            language: language.into(),
            root,
        }
    }
}
