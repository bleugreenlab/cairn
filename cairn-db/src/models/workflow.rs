//! Workflow configuration types (settings surface).
//!
//! A workflow is a file-backed package (`workflow.yaml` + a script entry) loaded
//! by `cairn_core::config::workflows`. These types are the API shape the Settings
//! Workflows page reads and writes: a list/scope row ([`WorkflowConfig`]), the
//! editor payload that also carries the script text ([`WorkflowConfigDetail`]),
//! and the create/update input ([`SaveWorkflowConfig`]).
//!
//! Unlike agents/skills/recipes, workflows have NO team-workspace authoring home
//! (they surface on the personal workspace and real projects only), so the scope
//! ids here are only ever `"default"` / a project id.

use serde::{Deserialize, Serialize};

use super::agent::OutputSchema;

/// A workflow package as a Settings list/scope row. `worktree` is the manifest's
/// binding mode serialized as a lowercase string (`"none"` | `"inherit"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    /// The resolved script entry filename (`main.ts` when the manifest omits it).
    pub script: String,
    /// Worktree binding: `"none"` (scratch, default) or `"inherit"` (project tree).
    pub worktree: String,
    /// The args JSON Schema, if the workflow declares one.
    pub args_schema: Option<serde_json::Value>,
    /// The output schema (preset name or inline JSON Schema), if declared.
    pub output: Option<OutputSchema>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
}

/// The editor payload for one workflow: its manifest row plus the full script
/// source, so the in-app editor can round-trip both `workflow.yaml` and the
/// script entry (`main.ts`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowConfigDetail {
    pub config: WorkflowConfig,
    /// The full text of the resolved script entry.
    pub script_content: String,
}

/// Create-or-update input for a workflow package. `id` present targets an
/// existing package (update); absent derives the id from `name` (create).
/// Exactly one of `workspace_id` / `project_id` selects the write scope.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveWorkflowConfig {
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Script entry filename; defaults to `main.ts` when absent/blank.
    #[serde(default)]
    pub script: Option<String>,
    /// `"none"` | `"inherit"`; defaults to `"none"` when absent.
    #[serde(default)]
    pub worktree: Option<String>,
    /// The args JSON Schema, or `None` for a workflow that takes no args.
    #[serde(default)]
    pub args_schema: Option<serde_json::Value>,
    /// The output schema (preset name or inline JSON Schema), or `None` for the
    /// default `return` contract.
    #[serde(default)]
    pub output: Option<OutputSchema>,
    /// The full text to write to the script entry.
    #[serde(default)]
    pub script_content: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
}
