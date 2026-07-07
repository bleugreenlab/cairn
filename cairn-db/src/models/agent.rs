//! Agent configuration types.

use serde::{Deserialize, Serialize};

use super::common::{Model, ModelSelection, RuntimeExtras};
use super::permissions::{Fence, LegacyOnEscape, LegacySandbox};
use super::recipe::ConfirmPolicy;

/// Output schema for an agent - either a preset name or custom JSON Schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum OutputSchema {
    /// Preset schema name: "document", "plan", "review", "checklist"
    Preset(String),
    /// Custom JSON Schema object
    Custom(serde_json::Value),
}

/// Output schema with optional custom tool name and description.
/// Bundles the schema definition with metadata for the output tool.
#[derive(Debug, Clone)]
pub struct OutputSchemaInfo {
    /// The schema defining the output structure
    pub schema: OutputSchema,
    /// The artifact's canonical name — the agent writes its output to
    /// `cairn:~/<artifact_name>`. Taken from the producing node's own output
    /// schema name (independent of where the field shape is inherited from).
    pub artifact_name: Option<String>,
    /// Whether the produced artifact auto-confirms or waits for a human. Drives
    /// the gating language injected into the agent's prompt.
    pub confirm_policy: ConfirmPolicy,
    /// Custom tool name (e.g., "write_plan", "create_pr") - defaults to "return" if None
    pub tool_name: Option<String>,
    /// Tool description shown to the agent
    pub description: Option<String>,
}

/// Agent configuration for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub tools: Vec<String>,
    #[serde(alias = "model")]
    pub tier: Option<Model>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    /// Tools to disallow (added to blocked list)
    pub disallowed_tools: Option<Vec<String>>,
    /// Skills to inject into the prompt
    pub skills: Option<Vec<String>>,
    /// Worktree fence behavior for sandbox escapes.
    pub fence: Option<Fence>,
    /// Preferred backend to use when multiple providers are available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
    /// Resolved atomic backend+model selection carried from the execution
    /// snapshot to session start. When set, the runtime uses it verbatim and
    /// never re-resolves a tier. `None` only for authoring/display configs that
    /// never run a session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<ModelSelection>,
    /// Backend-specific runtime parameters (effort, thinking) resolved early and
    /// carried to session start instead of being recomputed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extras: Option<RuntimeExtras>,
}

/// Input for creating an agent config
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAgentConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub tools: Vec<String>,
    #[serde(alias = "model")]
    pub tier: Option<Model>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub fence: Option<Fence>,
    #[serde(default, skip_serializing)]
    pub sandbox: Option<LegacySandbox>,
    #[serde(default, skip_serializing)]
    pub on_escape: Option<LegacyOnEscape>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
}

/// Input for updating an agent config
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAgentConfig {
    pub name: Option<String>,
    pub description: Option<String>,
    pub prompt: Option<String>,
    pub tools: Option<Vec<String>>,
    #[serde(alias = "model")]
    pub tier: Option<Option<Model>>,
    pub disallowed_tools: Option<Option<Vec<String>>>,
    pub skills: Option<Option<Vec<String>>>,
    /// New scope: workspace_id = Some("default") for workspace, None for project
    pub workspace_id: Option<Option<String>>,
    /// New scope: project_id = Some(id) for project, None for workspace
    pub project_id: Option<Option<String>>,
    pub fence: Option<Option<Fence>>,
    #[serde(rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<Option<String>>,
}
