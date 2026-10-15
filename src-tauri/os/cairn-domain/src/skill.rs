//! Skill configuration types.
//!
//! Skills are prompt injections that augment an agent's behavior within the same
//! conversation context. They can be attached to agents for automatic injection
//! or invoked dynamically via the skill tool.

use serde::{Deserialize, Serialize};

/// Skill configuration for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
}

/// Input for creating a skill config
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSkillConfig {
    pub id: Option<String>, // Auto-generated from name if not provided
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
}

/// Input for updating a skill config
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSkillConfig {
    pub name: Option<String>,
    pub description: Option<String>,
    pub prompt: Option<String>,
    pub allowed_tools: Option<Option<Vec<String>>>,
    pub workspace_id: Option<Option<String>>,
    pub project_id: Option<Option<String>>,
}
