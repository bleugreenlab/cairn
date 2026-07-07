//! Execution snapshot types for self-contained runtime state.
//!
//! When an execution starts, we snapshot the entire configuration so that:
//! - Recipe/agent/skill file deletion doesn't break running executions
//! - Execution can be resumed even if config files changed
//! - Full audit trail of what configuration was used

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::agent::OutputSchema;
use super::common::{Model, ModelSelection, Preset, RuntimeExtras};
use super::execution::TriggerType;
use super::permissions::{Fence, LegacyOnEscape, LegacySandbox};
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
// No `Eq`: `output_contract` can carry an inline custom JSON Schema
// (`serde_json::Value`), which is `PartialEq` but not `Eq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    /// Whether the spawning agent requested this packet run in the background
    /// (fire-and-forget). Persisted so the finalize-time resume handler can tell
    /// an intentional background batch (expected: no successor turn) from a
    /// genuinely broken blocking parent. `#[serde(default)]` keeps pre-field
    /// snapshots deserializable as non-background.
    #[serde(default)]
    pub background: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationOrigin {
    TaskTool,
    Manager,
    Planner,
    /// An ephemeral agent call (CAIRN-2481): a node-less run created directly
    /// with a pre-materialized packet, never expanded into a recipe node.
    CallTool,
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
    pub fence: Option<Fence>,
    #[serde(default, skip_serializing)]
    pub sandbox: Option<LegacySandbox>,
    #[serde(default, skip_serializing)]
    pub on_escape: Option<LegacyOnEscape>,
}

// No `Eq`: `schema_type` may carry an inline custom JSON Schema
// (`serde_json::Value`), which is `PartialEq` but not `Eq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DelegatedOutputContract {
    /// The delegated task's output schema: a preset name (serialized as a bare
    /// string, e.g. `"return"`) or an inline custom JSON Schema (serialized as an
    /// object). `OutputSchema` is untagged, so the on-disk `schemaType` key stays
    /// back-compatible with snapshots written before custom schemas existed.
    pub schema_type: OutputSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl DelegatedOutputContract {
    /// The artifact name / URI segment the delegated child writes its result to.
    /// A preset uses its own name (defaulting to `return` when empty); a custom
    /// inline schema always writes to the canonical `return` artifact.
    pub fn artifact_name(&self) -> String {
        match &self.schema_type {
            OutputSchema::Preset(name) if !name.is_empty() => name.clone(),
            _ => "return".to_string(),
        }
    }
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
    /// Authored tier default — a pre-fill for edit/re-resolution, not a runtime
    /// input. The runtime reads `selection`, never this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<Model>,
    /// Authored backend preference default (pre-fill, not a runtime input).
    #[serde(rename = "backend", alias = "backendPreference")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_preference: Option<String>,
    /// Resolved atomic backend+model selection — the single runtime source of
    /// truth for what runs where. Populated at resolve-early time and migrated
    /// on read from the legacy flat `model`/`resolved_backend` for old snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<ModelSelection>,
    pub disallowed_tools: Option<Vec<String>>,
    /// Skill IDs attached to this agent (None = inherit all available skills)
    pub skills: Option<Vec<String>>,
    /// Worktree fence behavior for sandbox escapes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<Fence>,
    /// Legacy permission fields, accepted on read and collapsed into `fence`.
    #[serde(default, skip_serializing)]
    pub sandbox: Option<LegacySandbox>,
    #[serde(default, skip_serializing)]
    pub on_escape: Option<LegacyOnEscape>,
    /// Backend-specific runtime parameters (effort, thinking) resolved early.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extras: Option<RuntimeExtras>,
    /// Legacy flat resolved model — deserialize-only input consumed by
    /// `migrate_on_read` to build `selection`; never serialized.
    #[serde(default, skip_serializing)]
    pub model: Option<Model>,
    /// Legacy flat resolved backend — deserialize-only, see `model`.
    #[serde(default, skip_serializing)]
    pub resolved_backend: Option<String>,
}

/// Frozen preset matrix captured with an execution snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPresets {
    pub active_backend: String,
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
    /// Attribution for who initiated this execution: `Some("external")` when
    /// started by an authenticated external caller (e.g. a driving CLI session),
    /// `None`/absent for the default user-initiated start. Display and audit
    /// only — it does not gate anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiated_via: Option<String>,
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
    /// Context about what triggered this execution
    pub trigger_context: TriggerContext,
    /// Legacy frozen preset matrix — deserialize-only input consumed by
    /// `migrate_on_read` to recover `extras` for pre-resolve-early snapshots.
    /// Never serialized; no runtime path reads it.
    #[serde(default, skip_serializing)]
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
        trigger_context: TriggerContext,
    ) -> Self {
        Self {
            recipe,
            agents,
            skills,
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

    /// Re-key every routable id embedded in the snapshot for a private-to-team
    /// project move. Provider ids and backend session ids are intentionally not
    /// present in this pass.
    pub fn rekey_to_team(&mut self, team_id: &str) -> Result<(), String> {
        self.trigger_context.project_id =
            cairn_common::ids::rekey_to_team(&self.trigger_context.project_id, team_id)
                .map_err(|e| format!("re-keying snapshot project_id: {e}"))?;
        if let Some(issue_id) = &mut self.trigger_context.issue_id {
            *issue_id = cairn_common::ids::rekey_to_team(issue_id, team_id)
                .map_err(|e| format!("re-keying snapshot issue_id: {e}"))?;
        }
        for packet in &mut self.delegated_packets {
            packet.id = cairn_common::ids::rekey_to_team(&packet.id, team_id)
                .map_err(|e| format!("re-keying delegated packet id: {e}"))?;
            packet.parent_job_id = cairn_common::ids::rekey_to_team(&packet.parent_job_id, team_id)
                .map_err(|e| format!("re-keying delegated packet parent_job_id: {e}"))?;
            if let Some(parent_turn_id) = &mut packet.parent_turn_id {
                *parent_turn_id = cairn_common::ids::rekey_to_team(parent_turn_id, team_id)
                    .map_err(|e| format!("re-keying delegated packet parent_turn_id: {e}"))?;
            }
            if let Some(result_artifact_job_id) = &mut packet.result_artifact_job_id {
                *result_artifact_job_id =
                    cairn_common::ids::rekey_to_team(result_artifact_job_id, team_id).map_err(
                        |e| format!("re-keying delegated packet result_artifact_job_id: {e}"),
                    )?;
            }
        }
        Ok(())
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
                    selection: None,
                    model: Some(Model::new(Model::SONNET)),
                    disallowed_tools: Some(vec!["Bash".to_string()]),
                    skills: Some(vec!["data-fetching".to_string()]),
                    fence: None,
                    sandbox: None,
                    on_escape: None,
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
            trigger_context: TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "project-1".to_string(),
                trigger_type: TriggerType::Manual,

                event_payload: None,
                initiated_via: None,
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
                    fence: None,
                    sandbox: None,
                    on_escape: None,
                },
                session: DelegatedSessionStrategy::default(),
                acceptance: vec!["Return findings".to_string()],
                output_contract: DelegatedOutputContract {
                    schema_type: OutputSchema::Preset("return".to_string()),
                    tool_name: None,
                    description: Some("Submit the task result".to_string()),
                },
                status: DelegatedStatus::Pending,
                materialized_node_ids: vec![],
                result_artifact_job_id: None,
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                background: false,
                created_at: 1234567890,
            }],
            created_at: 1234567890,
        };

        let json = snapshot.to_json().unwrap();
        let restored: ExecutionSnapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.recipe.id, "recipe-1");
        assert_eq!(restored.agents.len(), 1);
        assert_eq!(restored.skills.len(), 1);
        assert_eq!(restored.trigger_context.project_id, "project-1");
        assert_eq!(restored.delegated_packets.len(), 1);
        assert_eq!(
            restored.delegated_packets[0]
                .output_contract
                .artifact_name(),
            "return"
        );
    }

    #[test]
    fn output_contract_backcompat_string_and_custom_schema() {
        // Old snapshots persisted the preset as a bare string under `schemaType`.
        let preset: DelegatedOutputContract =
            serde_json::from_str(r#"{"schemaType":"return"}"#).unwrap();
        assert_eq!(
            preset.schema_type,
            OutputSchema::Preset("return".to_string())
        );
        // A custom inline schema persists as an object.
        let custom: DelegatedOutputContract =
            serde_json::from_str(r#"{"schemaType":{"type":"object","required":["score"]}}"#)
                .unwrap();
        assert!(matches!(custom.schema_type, OutputSchema::Custom(_)));
        assert_eq!(custom.artifact_name(), "return");
        // A preset round-trips back to a bare string.
        let reser = serde_json::to_string(&preset).unwrap();
        assert!(reser.contains(r#""schemaType":"return""#), "{reser}");
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
            initiated_via: None,
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
            selection: None,
            model: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            sandbox: None,
            on_escape: None,
            resolved_backend: Some("codex".to_string()),
            extras: Some(RuntimeExtras {
                max_thinking_tokens: None,
                reasoning_effort: Some("high".to_string()),
                service_tier: Some("priority".to_string()),
            }),
        };
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("reasoningEffort"));
        assert!(json.contains("serviceTier"));
        let restored: AgentSnapshot = serde_json::from_str(&json).unwrap();
        let extras = restored.extras.unwrap();
        assert_eq!(extras.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(extras.service_tier.as_deref(), Some("priority"));
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
            selection: None,
            model: None,
            disallowed_tools: None,
            skills: None,
            fence: None,
            sandbox: None,
            on_escape: None,
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
            initiated_via: None,
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

    /// A snapshot serialized before the `background` field deserializes as
    /// non-background, so old executions resume on the blocking path unchanged.
    #[test]
    fn delegated_packet_defaults_background_to_false() {
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
        assert!(!packet.background);
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
