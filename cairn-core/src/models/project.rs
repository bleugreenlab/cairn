//! Project types.

use crate::config::project_settings::CheckCommand;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Background-testing checks as JSON (map of check name → CheckCommand).
    pub checks: Option<String>,
    /// Worktree populate config as JSON (copy/symlink pattern lists).
    pub worktree_populate: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// Whether this project is hidden from the sidebar.
    pub hidden: bool,
    /// Whether this project lives in a team replica.
    pub is_team: bool,
    /// Whether this machine has a usable local git repository for the project.
    pub repo_cloned: bool,
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
    /// Optional ID to use instead of generating a new UUID. Set when a project's
    /// row must adopt a specific UUID (e.g. a team-synced project created from an
    /// existing record).
    pub id: Option<String>,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    /// Routes the project to a team's synced database (CAIRN-2132). `None` (the
    /// default for every existing caller) creates a local project whose data
    /// lives in the private database; `Some(team_id)` writes the `projects` row
    /// to that team's already-open replica and records a `project_routes` stub.
    /// Seeded by tests/settings this slice; the api/ control plane populates it
    /// later.
    #[serde(default)]
    pub team_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProject {
    pub id: String,
    pub setup_commands: Option<Vec<String>>,
    pub terminal_commands: Option<Vec<TerminalCommand>>,
    /// Background-testing checks keyed by name. An empty map clears all checks
    /// from `.cairn/config.yaml`; `None` leaves the existing checks untouched.
    pub checks: Option<HashMap<String, CheckCommand>>,
    pub worktree_populate: Option<crate::config::project_settings::PopulateConfig>,
    pub default_branch: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveProject {
    pub project_id: String,
    pub team_id: String,
}
