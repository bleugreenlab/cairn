//! Agent configuration types.

use serde::{Deserialize, Serialize};

use super::common::Model;

/// Output schema for an agent - either a preset name or custom JSON Schema
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub model: Option<Model>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    /// Tools to disallow (added to blocked list)
    pub disallowed_tools: Option<Vec<String>>,
    /// Skills to inject into the prompt
    pub skills: Option<Vec<String>>,
    /// Permission mode: "default", "acceptEdits", "bypassPermissions", etc.
    pub permission_mode: Option<String>,
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
    pub model: Option<Model>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub permission_mode: Option<String>,
}

/// Input for updating an agent config
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAgentConfig {
    pub name: Option<String>,
    pub description: Option<String>,
    pub prompt: Option<String>,
    pub tools: Option<Vec<String>>,
    pub model: Option<Option<Model>>,
    pub disallowed_tools: Option<Option<Vec<String>>>,
    pub skills: Option<Option<Vec<String>>>,
    /// New scope: workspace_id = Some("default") for workspace, None for project
    pub workspace_id: Option<Option<String>>,
    /// New scope: project_id = Some(id) for project, None for workspace
    pub project_id: Option<Option<String>>,
    /// Permission mode: "default", "acceptEdits", "bypassPermissions", etc.
    pub permission_mode: Option<Option<String>>,
}
