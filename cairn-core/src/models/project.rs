//! Project types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    pub context: String,
    pub docs_enabled: bool,
    pub default_branch: String,
    pub next_issue_number: i32,
    pub setup_commands: Option<String>,
    pub terminal_commands: Option<String>,
    /// Worktree populate config as JSON (copy/symlink pattern lists).
    pub worktree_populate: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// When set, this project is a remote bookmark pointing to a cairn-server instance.
    pub remote_url: Option<String>,
    /// Whether this project is hidden from the sidebar.
    pub hidden: bool,
    /// Server this project belongs to (for remote projects).
    pub server_id: Option<String>,
    /// Whether this project represents the Cairn workspace config root.
    pub is_workspace: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRemoteStatus {
    pub has_remote: bool,
    pub remote_url: Option<String>,
    pub is_workspace: bool,
}

/// Terminal shortcut command configuration.
///
/// A blessed, named command for this project. An optional `write` carveout lets a
/// fenced agent run this command with the out-of-worktree write scopes it needs
/// (e.g. a dev launcher writing a per-instance state dir) without parking on a
/// worktree-fence prompt. Because this lives in repo-committed `.cairn/config.yaml`,
/// the scopes are bounded: they may never intersect the secret-store read
/// denylist. See `crate::config::dev_commands` and `docs/worktree-fence.md`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TerminalCommand {
    pub name: String,
    pub command: String,
    /// Writable (and readable) glob scopes pre-approved for this command when it
    /// runs under the worktree fence. `**` spans path segments, `*` stays within
    /// one; `~`/`{home}`/`{cairnHome}`/`{worktrees}`/`{worktree}` expand.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProject {
    /// Optional ID to use instead of generating a new UUID.
    /// Used when creating a local bookmark for a remote project so the local
    /// entry shares the same UUID as the server's project.
    pub id: Option<String>,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    /// When set, creates a remote project bookmark instead of a local project.
    pub remote_url: Option<String>,
    /// Server ID for remote projects (replaces remote_url for routing).
    pub server_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProject {
    pub id: String,
    pub setup_commands: Option<Vec<String>>,
    pub terminal_commands: Option<Vec<TerminalCommand>>,
    pub worktree_populate: Option<crate::config::project_settings::PopulateConfig>,
    pub default_branch: Option<String>,
}
