//! Execution snapshot types for self-contained runtime state.
//!
//! When an execution starts, we snapshot the entire configuration so that:
//! - Recipe/agent/skill file deletion doesn't break running executions
//! - Execution can be resumed even if config files changed
//! - Full audit trail of what configuration was used

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::common::Model;
use super::execution::TriggerType;
use super::recipe::{RecipeContext, RecipeEdge, RecipeNode, RecipeTrigger};

/// Snapshot of a recipe at execution time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeSnapshot {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub trigger: RecipeTrigger,
    pub context: RecipeContext,
    pub nodes: Vec<RecipeNode>,
    pub edges: Vec<RecipeEdge>,
}

/// Snapshot of an agent configuration at execution time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSnapshot {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub tools: Vec<String>,
    pub model: Option<Model>,
    pub disallowed_tools: Option<Vec<String>>,
    /// Skill IDs attached to this agent (None = inherit all available skills)
    pub skills: Option<Vec<String>>,
    /// Permission mode for tool approval (approve_all, approve_once, prompt)
    pub permission_mode: Option<String>,
}

/// Snapshot of a skill configuration at execution time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSnapshot {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub model: Option<Model>,
}

/// Snapshot of a custom tool at execution time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolSnapshot {
    pub id: String,
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub code: String,
    pub required_tools: Vec<String>,
}

/// Context about what triggered the execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerContext {
    /// Issue ID if triggered by an issue
    pub issue_id: Option<String>,
    /// Project ID
    pub project_id: String,
    /// How the execution was triggered
    pub trigger_type: TriggerType,
    /// Skill IDs attached to the issue (if any)
    #[serde(default)]
    pub issue_skills: Vec<String>,
}

/// Complete execution snapshot - everything needed to run/display an execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSnapshot {
    /// The recipe configuration at execution time
    pub recipe: RecipeSnapshot,
    /// All agents referenced by the recipe (keyed by agent ID)
    pub agents: HashMap<String, AgentSnapshot>,
    /// All skills referenced by agents (keyed by skill ID)
    pub skills: HashMap<String, SkillSnapshot>,
    /// All custom tools available at execution time (keyed by tool ID)
    #[serde(default)]
    pub tools: HashMap<String, ToolSnapshot>,
    /// Context about what triggered this execution
    pub trigger_context: TriggerContext,
    /// When the snapshot was created
    pub created_at: i64,
}

/// Overrides to apply when creating an execution snapshot
///
/// Used by start_recipe_execution to customize the snapshot before execution starts.
/// This allows editing the recipe graph and agent assignments at issue creation time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotOverrides {
    /// Override the recipe (nodes/edges) from the template
    pub recipe: Option<RecipeSnapshot>,
    /// Override or add agents (keyed by agent ID)
    pub agents: Option<HashMap<String, AgentSnapshot>>,
}

impl ExecutionSnapshot {
    /// Create a new execution snapshot
    pub fn new(
        recipe: RecipeSnapshot,
        agents: HashMap<String, AgentSnapshot>,
        skills: HashMap<String, SkillSnapshot>,
        tools: HashMap<String, ToolSnapshot>,
        trigger_context: TriggerContext,
    ) -> Self {
        Self {
            recipe,
            agents,
            skills,
            tools,
            trigger_context,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    /// Serialize to JSON string for storage
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|e| format!("Failed to serialize snapshot: {}", e))
    }

    /// Deserialize from JSON string
    #[allow(dead_code)]
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("Failed to deserialize snapshot: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::RecipeContext;

    #[test]
    fn test_snapshot_serialization_roundtrip() {
        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Test Recipe".to_string(),
                description: Some("A test recipe".to_string()),
                trigger: RecipeTrigger::Issue,
                context: RecipeContext::Issue,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::from([(
                "build".to_string(),
                AgentSnapshot {
                    id: "build".to_string(),
                    name: "Build Agent".to_string(),
                    description: "Builds things".to_string(),
                    prompt: "You are a build agent".to_string(),
                    tools: vec!["Read".to_string(), "Write".to_string()],
                    model: Some(Model::Sonnet),
                    disallowed_tools: Some(vec!["Bash".to_string()]),
                    skills: Some(vec!["data-fetching".to_string()]),
                    permission_mode: Some("approve_once".to_string()),
                },
            )]),
            skills: HashMap::from([(
                "data-fetching".to_string(),
                SkillSnapshot {
                    id: "data-fetching".to_string(),
                    name: "Data Fetching".to_string(),
                    description: "React Query patterns".to_string(),
                    prompt: "Use React Query...".to_string(),
                    allowed_tools: Some(vec!["Read".to_string()]),
                    model: None,
                },
            )]),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Issue,
                issue_skills: vec![],
            },
            created_at: 1234567890,
        };

        let json = snapshot.to_json().unwrap();
        let restored = ExecutionSnapshot::from_json(&json).unwrap();

        assert_eq!(restored.recipe.id, "recipe-1");
        assert_eq!(restored.agents.len(), 1);
        assert_eq!(restored.skills.len(), 1);
        assert_eq!(restored.trigger_context.project_id, "project-1");
    }
}
