//! Project types.

use cairn_common::executor_protocol::PlacementConstraints;
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
    /// Human-readable name of the owning team, resolved from the private team
    /// registry. `None` for local projects or an unregistered team.
    pub team_name: Option<String>,
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
/// denylist. The dev-command denylist lives in cairn-core's config layer; see
/// `docs/worktree-fence.md`.
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
    pub worktree_populate: Option<PopulateConfig>,
    pub default_branch: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveProject {
    pub project_id: String,
    pub team_id: String,
}

/// Configuration for how gitignored paths are populated into new worktrees.
///
/// Paths matching `copy` patterns are copied from the main repo (isolated per worktree).
/// Paths matching `symlink` patterns are symlinked to the main repo.
/// Unmatched paths are skipped — new worktrees start clean by default.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PopulateConfig {
    /// Patterns whose matching paths are copied into the worktree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copy: Vec<String>,
    /// Patterns whose matching paths are symlinked to the main repo.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub symlink: Vec<String>,
}

impl PopulateConfig {
    pub fn is_empty(&self) -> bool {
        self.copy.is_empty() && self.symlink.is_empty()
    }
}

/// One project check: a single command run at one cadence. Selectivity is
/// expressed by a `{changedFiles}` or `{targets}` placeholder inside the
/// command — the placeholder *is* the selector, and a placeholder-less command
/// runs as-is (a full run).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CheckCommand {
    /// The command to run. A `{changedFiles}` or `{targets}` placeholder makes
    /// it selective; a placeholder-less command runs as-is.
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impact: Option<Vec<String>>,
    #[serde(
        default = "default_check_policy",
        skip_serializing_if = "is_default_check_policy"
    )]
    pub policy: CheckPolicy,
    #[serde(
        default = "default_check_when",
        skip_serializing_if = "is_default_check_when"
    )]
    pub when: CheckWhen,
    /// Host resource admission class. Shared checks consume one global permit;
    /// exclusive checks consume the controller's full capacity.
    #[serde(
        default = "default_check_resource_class",
        skip_serializing_if = "is_default_check_resource_class"
    )]
    pub resource_class: CheckResourceClass,
    /// Maximum wall-clock SECONDS this check may run before it is killed at its
    /// budget and recorded AS a timeout (not an opaque failure). `None` uses the
    /// cadence default — 10 min for `write`, 30 min for `review`. Clamped to a
    /// 60-minute hard ceiling. See docs/checks.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    /// Hard executor placement requirements for this check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<PlacementConstraints>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckPolicy {
    Advisory,
    Gate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckResourceClass {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckWhen {
    Write,
    /// Turn-end cadence: runs the fuller suites at every turn-end. `idle` is
    /// accepted as a legacy alias for un-migrated project configs — it used to
    /// be a separate every-turn-end cadence that `review` (once PR-gated) now
    /// subsumes — so an old `when: idle` still parses to this cadence instead of
    /// silently disabling every check for that project.
    #[serde(alias = "idle")]
    Review,
}

fn default_check_policy() -> CheckPolicy {
    CheckPolicy::Advisory
}

fn is_default_check_policy(policy: &CheckPolicy) -> bool {
    *policy == default_check_policy()
}

fn default_check_resource_class() -> CheckResourceClass {
    CheckResourceClass::Shared
}

fn is_default_check_resource_class(resource_class: &CheckResourceClass) -> bool {
    *resource_class == default_check_resource_class()
}

fn default_check_when() -> CheckWhen {
    CheckWhen::Write
}

fn is_default_check_when(when: &CheckWhen) -> bool {
    *when == default_check_when()
}

impl CheckPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckPolicy::Advisory => "advisory",
            CheckPolicy::Gate => "gate",
        }
    }
}

impl CheckResourceClass {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckResourceClass::Shared => "shared",
            CheckResourceClass::Exclusive => "exclusive",
        }
    }
}

impl CheckWhen {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckWhen::Write => "write",
            CheckWhen::Review => "review",
        }
    }
}
