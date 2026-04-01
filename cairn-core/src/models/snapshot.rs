//! Execution snapshot types for self-contained runtime state.
//!
//! When an execution starts, we snapshot the entire configuration so that:
//! - Recipe/agent/skill file deletion doesn't break running executions
//! - Execution can be resumed even if config files changed
//! - Full audit trail of what configuration was used

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::common::{Model, Preset, RuntimeExtras};
use super::execution::TriggerType;
use super::permissions::{ApprovalPolicy, FilesystemScope};
use super::recipe::{RecipeEdge, RecipeNode, RecipeTrigger};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DelegatedSessionMode {
    #[default]
    New,
    Fork,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DelegatedSessionStrategy {
    #[serde(default)]
    pub mode: DelegatedSessionMode,
}

/// Durable record of a delegated task lowered into the execution snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegatedWorkPacket {
    pub id: String,
    pub parent_job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_tool_use_id: Option<String>,
    pub origin: DelegationOrigin,
    pub title: String,
    pub problem_statement: String,
    pub agent_config_id: String,
    pub ownership: DelegatedOwnershipScope,
    #[serde(default)]
    pub session: DelegatedSessionStrategy,
    #[serde(default)]
    pub acceptance: Vec<String>,
    pub output_contract: DelegatedOutputContract,
    pub status: DelegatedStatus,
    #[serde(default)]
    pub materialized_node_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_artifact_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_index: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier_override: Option<String>,
    #[serde(rename = "backend", alias = "backendPreference")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_preference: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationOrigin {
    TaskTool,
    Executor,
    Manager,
    Planner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedStatus {
    Pending,
    Materialized,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegatedOwnershipScope {
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filesystem_scope: Option<FilesystemScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegatedOutputContract {
    pub schema_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Snapshot of a recipe at execution time
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeSnapshot {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub trigger: RecipeTrigger,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<Model>,
    #[serde(rename = "backend", alias = "backendPreference")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_preference: Option<String>,
    pub model: Option<Model>,
    pub disallowed_tools: Option<Vec<String>>,
    /// Skill IDs attached to this agent (None = inherit all available skills)
    pub skills: Option<Vec<String>>,
    /// How tool invocations are approved
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    /// What the agent can read/write on the filesystem
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filesystem_scope: Option<FilesystemScope>,
    /// Concrete backend selected after resolving tier + authored backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub resolved_backend: Option<String>,
    /// Backend-specific runtime parameters (per-agent overrides)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub extras: Option<RuntimeExtras>,
}

/// Frozen preset matrix captured with an execution snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPresets {
    pub active_backend: String,
    pub default_tier: String,
    pub tiers: Vec<String>,
    pub backends: HashMap<String, HashMap<String, Preset>>,
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
    /// Event payload for event-driven triggers (JobEnded, SkillCalled).
    /// Flows to downstream agents via context edges from the trigger node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_payload: Option<serde_json::Value>,
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
    /// Frozen presets used to resolve tier/backend selections for this execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presets: Option<SnapshotPresets>,
    /// Durable task delegations awaiting or resulting from materialization into the DAG
    #[serde(default)]
    pub delegated_packets: Vec<DelegatedWorkPacket>,
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
            presets: None,
            delegated_packets: Vec::new(),
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
    #[test]
    fn test_snapshot_serialization_roundtrip() {
        let snapshot = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Test Recipe".to_string(),
                description: Some("A test recipe".to_string()),
                trigger: RecipeTrigger::Manual,
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
                    tier: Some(Model::new(Model::SONNET)),
                    backend_preference: None,
                    model: Some(Model::new(Model::SONNET)),
                    disallowed_tools: Some(vec!["Bash".to_string()]),
                    skills: Some(vec!["data-fetching".to_string()]),
                    approval_policy: None,
                    filesystem_scope: None,
                    resolved_backend: None,
                    extras: None,
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
                },
            )]),
            tools: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Manual,

                event_payload: None,
            },
            presets: None,
            delegated_packets: vec![DelegatedWorkPacket {
                id: "pkt-1".to_string(),
                parent_job_id: "job-1".to_string(),
                parent_turn_id: Some("turn-1".to_string()),
                parent_tool_use_id: Some("tool-1".to_string()),
                origin: DelegationOrigin::TaskTool,
                title: "Explore".to_string(),
                problem_statement: "Investigate the bug".to_string(),
                agent_config_id: "explore".to_string(),
                ownership: DelegatedOwnershipScope {
                    cwd: "/tmp/test".to_string(),
                    filesystem_scope: None,
                    approval_policy: None,
                },
                session: DelegatedSessionStrategy::default(),
                acceptance: vec!["Return findings".to_string()],
                output_contract: DelegatedOutputContract {
                    schema_type: "return".to_string(),
                    tool_name: None,
                    description: Some("Submit the task result".to_string()),
                },
                status: DelegatedStatus::Pending,
                materialized_node_ids: vec![],
                result_artifact_job_id: None,
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                created_at: 1234567890,
            }],
            created_at: 1234567890,
        };

        let json = snapshot.to_json().unwrap();
        let restored = ExecutionSnapshot::from_json(&json).unwrap();

        assert_eq!(restored.recipe.id, "recipe-1");
        assert_eq!(restored.agents.len(), 1);
        assert_eq!(restored.skills.len(), 1);
        assert_eq!(restored.trigger_context.project_id, "project-1");
        assert_eq!(restored.delegated_packets.len(), 1);
        assert_eq!(
            restored.delegated_packets[0].output_contract.schema_type,
            "return"
        );
    }

    /// Old snapshots (pre-event-payload) must deserialize without error.
    #[test]
    fn trigger_context_backward_compat_no_event_payload() {
        let json = r#"{
            "issueId": "iss-1",
            "projectId": "proj-1",
            "triggerType": "manual",
            "issueSkills": []
        }"#;
        let ctx: TriggerContext = serde_json::from_str(json).unwrap();
        assert_eq!(ctx.project_id, "proj-1");
        assert!(ctx.event_payload.is_none());
    }

    /// Snapshots with event_payload round-trip correctly.
    #[test]
    fn trigger_context_with_event_payload_roundtrip() {
        let ctx = TriggerContext {
            issue_id: None,
            project_id: "proj-1".to_string(),
            trigger_type: TriggerType::JobEnded,
            event_payload: Some(serde_json::json!({
                "jobId": "j1",
                "status": "complete"
            })),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let restored: TriggerContext = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.event_payload.unwrap()["status"], "complete");
    }

    /// Old snapshots without extras field deserialize cleanly (backward compat).
    #[test]
    fn agent_snapshot_backward_compat_no_extras() {
        let json = r#"{
            "id": "build",
            "name": "Build Agent",
            "description": "Builds things",
            "prompt": "You are a builder",
            "tools": ["Read"],
            "model": "sonnet",
            "disallowedTools": null,
            "skills": null,
            "permissionMode": null,
            "backend": null
        }"#;
        let snap: AgentSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snap.id, "build");
        assert!(snap.extras.is_none());
    }

    /// AgentSnapshot with extras round-trips correctly.
    #[test]
    fn agent_snapshot_with_extras_roundtrip() {
        let snap = AgentSnapshot {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "desc".to_string(),
            prompt: "prompt".to_string(),
            tools: vec![],
            tier: None,
            backend_preference: None,
            model: None,
            disallowed_tools: None,
            skills: None,
            approval_policy: None,
            filesystem_scope: None,
            resolved_backend: Some("codex".to_string()),
            extras: Some(RuntimeExtras {
                max_thinking_tokens: None,
                reasoning_effort: Some("high".to_string()),
            }),
        };
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("reasoningEffort"));
        let restored: AgentSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(
            restored.extras.unwrap().reasoning_effort.as_deref(),
            Some("high")
        );
    }

    /// extras is omitted from JSON when None (skip_serializing_if).
    #[test]
    fn agent_snapshot_skips_null_extras() {
        let snap = AgentSnapshot {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "desc".to_string(),
            prompt: "prompt".to_string(),
            tools: vec![],
            tier: None,
            backend_preference: None,
            model: None,
            disallowed_tools: None,
            skills: None,
            approval_policy: None,
            filesystem_scope: None,
            resolved_backend: None,
            extras: None,
        };
        let json = serde_json::to_string(&snap).unwrap();
        assert!(!json.contains("extras"));
    }

    /// event_payload is omitted from JSON when None (skip_serializing_if).
    #[test]
    fn trigger_context_skips_null_event_payload() {
        let ctx = TriggerContext {
            issue_id: None,
            project_id: "proj-1".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
        };
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(!json.contains("eventPayload"));
    }

    #[test]
    fn delegated_packet_defaults_session_to_new() {
        let json = r#"{
            "id": "pkt-1",
            "parentJobId": "job-1",
            "origin": "task_tool",
            "title": "Explore",
            "problemStatement": "Investigate",
            "agentConfigId": "Explore",
            "ownership": {"cwd": "/tmp/test"},
            "acceptance": [],
            "outputContract": {"schemaType": "return"},
            "status": "pending",
            "createdAt": 123
        }"#;

        let packet: DelegatedWorkPacket = serde_json::from_str(json).unwrap();
        assert_eq!(packet.session.mode, DelegatedSessionMode::New);
    }

    #[test]
    fn snapshot_backward_compat_without_delegated_packets() {
        let json = r#"{
            "recipe": {
                "id": "recipe-1",
                "name": "Test Recipe",
                "description": null,
                "trigger": "manual",
                "nodes": [],
                "edges": []
            },
            "agents": {},
            "skills": {},
            "tools": {},
            "triggerContext": {
                "issueId": null,
                "projectId": "project-1",
                "triggerType": "manual"
            },
            "createdAt": 123
        }"#;

        let restored: ExecutionSnapshot = serde_json::from_str(json).unwrap();
        assert!(restored.delegated_packets.is_empty());
    }
}
