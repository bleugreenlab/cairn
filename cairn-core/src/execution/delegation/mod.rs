//! DAG-native delegated work packets.
//!
//! Delegated task requests record durable packets in the execution snapshot. The
//! DAG effect pipeline materializes pending packets into ordinary nodes/jobs.

pub mod runtime;

pub(crate) use runtime::lookup_caller_job_id;
pub use runtime::{
    is_call_child, resume_suspended_parent_after_task_completion, spawn_call_packets,
    spawn_task_packets, spawn_workflow_packets,
};

use cairn_common::ids;

use crate::models::{
    DelegatedOutputContract, DelegatedSessionMode, DelegatedSessionStrategy, DelegatedStatus,
    DelegatedWorkPacket, ExecutionSnapshot, OutputSchema,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegatedTaskSessionMode {
    New,
    Fork,
}

impl From<DelegatedTaskSessionMode> for DelegatedSessionStrategy {
    fn from(mode: DelegatedTaskSessionMode) -> Self {
        let mode = match mode {
            DelegatedTaskSessionMode::New => DelegatedSessionMode::New,
            DelegatedTaskSessionMode::Fork => DelegatedSessionMode::Fork,
        };
        Self { mode }
    }
}

#[derive(Debug, Clone)]
pub struct DelegatedTaskPayload {
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    pub tier: Option<String>,
    pub backend_preference: Option<String>,
    pub session: DelegatedTaskSessionMode,
    pub task_index: Option<i32>,
    /// Optional per-task output schema: a preset name or an inline custom JSON
    /// Schema. `None` preserves the default `return` contract.
    pub output_schema: Option<OutputSchema>,
}

/// Build a delegated task's output contract from an optional caller-supplied
/// schema, rejecting a bad schema up front so the error surfaces at task-append
/// time rather than when the child writes its result.
///
/// `None` preserves today's default `return` preset. A preset name is checked
/// against the known presets; an inline custom JSON Schema is compiled to confirm
/// it is a valid schema. Either failure returns a caller-facing error.
pub(crate) fn resolve_delegated_output_contract(
    output_schema: Option<&OutputSchema>,
) -> Result<DelegatedOutputContract, String> {
    let schema_type = match output_schema {
        None => OutputSchema::Preset("return".to_string()),
        Some(OutputSchema::Preset(name)) => {
            if !crate::output_schemas::is_preset_schema(name) {
                return Err(format!(
                    "Unknown output_schema preset '{}'. Valid presets: {:?}",
                    name,
                    crate::output_schemas::PRESET_SCHEMAS
                ));
            }
            OutputSchema::Preset(name.clone())
        }
        Some(OutputSchema::Custom(schema)) => {
            jsonschema::validator_for(schema)
                .map_err(|e| format!("Invalid output_schema (not a valid JSON Schema): {e}"))?;
            OutputSchema::Custom(schema.clone())
        }
    };
    Ok(DelegatedOutputContract {
        schema_type,
        tool_name: None,
        description: Some("Submit the task result".to_string()),
    })
}

/// A resolved ephemeral-call request (CAIRN-2481), the call sibling of
/// [`DelegatedTaskPayload`]. A call is one prompt in, schema-validated JSON out,
/// run as a node-less job.
#[derive(Debug, Clone)]
pub struct DelegatedCallPayload {
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    pub tier: Option<String>,
    pub backend_preference: Option<String>,
    /// Optional per-call output schema: a preset name or an inline custom JSON
    /// Schema. `None` preserves the default `return` contract.
    pub output_schema: Option<OutputSchema>,
    pub worktree: crate::execution::jobs::CallWorktree,
    pub label: Option<String>,
    pub phase: Option<String>,
    pub task_index: Option<i32>,
    /// The harness dispatch ordinal (CAIRN-2498), threaded from `agent()` so a
    /// workflow-parented call can be journaled and replayed. `None` for an
    /// ordinary call.
    pub ordinal: Option<i64>,
}

pub struct SpawnCallPacketsInput<'a> {
    pub run_id: Option<&'a str>,
    pub cwd: &'a str,
    pub payloads: &'a [DelegatedCallPayload],
    /// Synthetic batch id, the resume-group fallback when no tool-use id is
    /// available.
    pub group_id: &'a str,
    pub parent_tool_use_id: Option<&'a str>,
    pub background: bool,
}

/// A resolved workflow invocation (CAIRN-2487): a `run` item whose target is a
/// workflow URI. Unlike a call it has no agent/prompt — its process is a
/// supervised `bun <script>` — but it reuses the call-packet suspend/resume tail
/// to wake the caller with the workflow's output artifact.
pub struct SpawnWorkflowPacketsInput<'a> {
    pub run_id: Option<&'a str>,
    pub cwd: &'a str,
    pub workflow_id: &'a str,
    /// Absolute path to the workflow's resolved script entry.
    pub script_path: std::path::PathBuf,
    /// The workflow's declared output schema (preset or inline), or `None` for
    /// the default `return` contract.
    pub output_schema: Option<OutputSchema>,
    /// The validated named args, forwarded to the script as `CAIRN_WORKFLOW_ARGS`.
    pub args_json: String,
    pub worktree: crate::execution::jobs::CallWorktree,
    pub group_id: &'a str,
    pub parent_tool_use_id: Option<&'a str>,
    pub background: bool,
}

pub struct SpawnTaskPacketsInput<'a> {
    pub run_id: Option<&'a str>,
    pub cwd: &'a str,
    pub payloads: &'a [DelegatedTaskPayload],
    /// Synthetic batch id, used as the resume-group fallback when the originating
    /// tool-use id is unavailable.
    pub group_id: &'a str,
    /// The originating `write` tool-use id. When present it is persisted as each
    /// packet's (and child job's) `parent_tool_use_id`, which the transcript uses
    /// to locate the spawned child jobs. Falls back to `group_id` when absent.
    pub parent_tool_use_id: Option<&'a str>,
    pub background: bool,
}

pub struct CreateDelegatedPacketInput<'a> {
    pub parent_job_id: &'a str,
    pub parent_turn_id: Option<&'a str>,
    pub parent_tool_use_id: Option<&'a str>,
    pub title: &'a str,
    pub problem_statement: &'a str,
    pub agent_config_id: &'a str,
    pub cwd: &'a str,
    pub fence: Option<crate::models::Fence>,
    pub acceptance: Vec<String>,
    pub output_contract: DelegatedOutputContract,
    pub session: DelegatedSessionStrategy,
    pub task_index: Option<i32>,
    pub tier_override: Option<&'a str>,
    #[allow(dead_code)]
    pub backend_preference: Option<&'a str>,
    /// Fire-and-forget request: the spawning parent does not block or suspend on
    /// this packet. Persisted so the completion-resume handler notifies the
    /// spawner via a push instead of looking for a (nonexistent) successor turn.
    pub background: bool,
}

pub fn create_or_reuse_task_packet(
    snapshot: &mut ExecutionSnapshot,
    input: CreateDelegatedPacketInput<'_>,
) -> DelegatedWorkPacket {
    if let Some(existing) = snapshot
        .delegated_packets
        .iter()
        .find(|packet| {
            packet.parent_job_id == input.parent_job_id
                && packet.parent_tool_use_id.as_deref() == input.parent_tool_use_id
                && packet.agent_config_id == input.agent_config_id
                && packet.title == input.title
                && packet.problem_statement == input.problem_statement
                && packet.task_index == input.task_index
                && packet.tier_override.as_deref() == input.tier_override
                && packet.backend_preference.as_deref() == input.backend_preference
                && packet.session == input.session
        })
        .cloned()
    {
        return existing;
    }

    let packet = DelegatedWorkPacket {
        id: ids::mint_child(input.parent_job_id),
        parent_job_id: input.parent_job_id.to_string(),
        parent_turn_id: input.parent_turn_id.map(str::to_string),
        parent_tool_use_id: input.parent_tool_use_id.map(str::to_string),
        origin: crate::models::DelegationOrigin::TaskTool,
        title: input.title.to_string(),
        problem_statement: input.problem_statement.to_string(),
        agent_config_id: input.agent_config_id.to_string(),
        ownership: crate::models::DelegatedOwnershipScope {
            cwd: input.cwd.to_string(),
            fence: input.fence,
            sandbox: None,
            on_escape: None,
        },
        session: input.session,
        acceptance: input.acceptance,
        output_contract: input.output_contract,
        status: DelegatedStatus::Pending,
        materialized_node_ids: vec![],
        result_artifact_job_id: None,
        task_index: input.task_index,
        tier_override: input.tier_override.map(str::to_string),
        backend_preference: input.backend_preference.map(str::to_string),
        background: input.background,
        created_at: chrono::Utc::now().timestamp(),
    };

    snapshot.delegated_packets.push(packet.clone());
    packet
}

pub struct CreateCallPacketInput<'a> {
    pub parent_job_id: &'a str,
    pub parent_turn_id: Option<&'a str>,
    pub parent_tool_use_id: Option<&'a str>,
    pub title: &'a str,
    pub problem_statement: &'a str,
    pub agent_config_id: &'a str,
    pub cwd: &'a str,
    pub output_contract: DelegatedOutputContract,
    /// The call's own job id: the completion-resume path finds this packet by it.
    pub result_artifact_job_id: &'a str,
    pub task_index: Option<i32>,
    pub tier_override: Option<&'a str>,
    pub backend_preference: Option<&'a str>,
    pub background: bool,
}

/// Persist a pre-materialized call packet (CAIRN-2481).
///
/// Unlike a task packet it is born `Materialized` with its
/// `result_artifact_job_id` already set, so `expand_delegated_packets` (which
/// only expands `Pending` packets) skips it — the call never becomes a recipe
/// node and never advances the DAG — while the completion-resume path finds it by
/// `result_artifact_job_id` and drives the parent exactly as it does for a task.
pub fn create_call_packet(
    snapshot: &mut ExecutionSnapshot,
    input: CreateCallPacketInput<'_>,
) -> DelegatedWorkPacket {
    let packet = DelegatedWorkPacket {
        id: ids::mint_child(input.parent_job_id),
        parent_job_id: input.parent_job_id.to_string(),
        parent_turn_id: input.parent_turn_id.map(str::to_string),
        parent_tool_use_id: input.parent_tool_use_id.map(str::to_string),
        origin: crate::models::DelegationOrigin::CallTool,
        title: input.title.to_string(),
        problem_statement: input.problem_statement.to_string(),
        agent_config_id: input.agent_config_id.to_string(),
        ownership: crate::models::DelegatedOwnershipScope {
            cwd: input.cwd.to_string(),
            fence: None,
            sandbox: None,
            on_escape: None,
        },
        session: DelegatedSessionStrategy::default(),
        acceptance: vec![],
        output_contract: input.output_contract,
        status: DelegatedStatus::Materialized,
        materialized_node_ids: vec![],
        result_artifact_job_id: Some(input.result_artifact_job_id.to_string()),
        task_index: input.task_index,
        tier_override: input.tier_override.map(str::to_string),
        backend_preference: input.backend_preference.map(str::to_string),
        background: input.background,
        created_at: chrono::Utc::now().timestamp(),
    };

    snapshot.delegated_packets.push(packet.clone());
    packet
}

#[cfg(test)]
mod call_packet_tests {
    use super::*;
    use crate::models::{RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType};

    fn empty_snapshot() -> ExecutionSnapshot {
        ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "r".to_string(),
                name: "n".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: std::collections::HashMap::new(),
            skills: std::collections::HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "p".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 0,
        }
    }

    /// A call packet is born Materialized with its result job set, and the DAG
    /// expander (which only expands Pending packets) never lowers it into a node.
    #[test]
    fn call_packet_is_materialized_and_never_expands() {
        let mut snapshot = empty_snapshot();
        let contract = resolve_delegated_output_contract(None).unwrap();
        let packet = create_call_packet(
            &mut snapshot,
            CreateCallPacketInput {
                parent_job_id: "parent",
                parent_turn_id: Some("T1"),
                parent_tool_use_id: Some("tool-1"),
                title: "call",
                problem_statement: "do it",
                agent_config_id: "Explore",
                cwd: "/tmp",
                output_contract: contract,
                result_artifact_job_id: "call-job",
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                background: false,
            },
        );

        assert_eq!(packet.origin, crate::models::DelegationOrigin::CallTool);
        assert_eq!(packet.status, DelegatedStatus::Materialized);
        assert_eq!(packet.result_artifact_job_id.as_deref(), Some("call-job"));
        assert!(packet.materialized_node_ids.is_empty());

        // The DAG expander only expands Pending packets, so a call is never
        // lowered into a recipe node and never advances the DAG.
        assert!(snapshot
            .delegated_packets
            .iter()
            .all(|p| p.status != DelegatedStatus::Pending));
        assert!(snapshot.recipe.nodes.is_empty());
        assert!(snapshot.recipe.edges.is_empty());
    }

    /// A batch of N calls creates N Materialized CallTool packets and zero
    /// recipe nodes / edges.
    #[test]
    fn batch_of_calls_adds_packets_but_no_nodes() {
        let mut snapshot = empty_snapshot();
        for i in 0..5 {
            create_call_packet(
                &mut snapshot,
                CreateCallPacketInput {
                    parent_job_id: "parent",
                    parent_turn_id: Some("T1"),
                    parent_tool_use_id: Some("tool-1"),
                    title: "call",
                    problem_statement: "do it",
                    agent_config_id: "Explore",
                    cwd: "/tmp",
                    output_contract: resolve_delegated_output_contract(None).unwrap(),
                    result_artifact_job_id: "call-job",
                    task_index: Some(i),
                    tier_override: None,
                    backend_preference: None,
                    background: false,
                },
            );
        }
        assert_eq!(snapshot.delegated_packets.len(), 5);
        assert!(snapshot
            .delegated_packets
            .iter()
            .all(|p| p.origin == crate::models::DelegationOrigin::CallTool
                && p.status == DelegatedStatus::Materialized));
        assert!(snapshot.recipe.nodes.is_empty());
        assert!(snapshot.recipe.edges.is_empty());
    }
}

#[cfg(test)]
mod output_contract_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn omitting_output_schema_yields_return_preset() {
        let contract = resolve_delegated_output_contract(None).unwrap();
        assert_eq!(
            contract.schema_type,
            OutputSchema::Preset("return".to_string())
        );
        assert_eq!(contract.artifact_name(), "return");
    }

    #[test]
    fn preset_name_is_carried_through() {
        let schema = OutputSchema::Preset("review".to_string());
        let contract = resolve_delegated_output_contract(Some(&schema)).unwrap();
        assert_eq!(
            contract.schema_type,
            OutputSchema::Preset("review".to_string())
        );
        assert_eq!(contract.artifact_name(), "review");
    }

    #[test]
    fn unknown_preset_is_rejected() {
        let schema = OutputSchema::Preset("nonexistent".to_string());
        let err = resolve_delegated_output_contract(Some(&schema)).unwrap_err();
        assert!(err.contains("Unknown output_schema preset"), "{err}");
    }

    #[test]
    fn inline_custom_schema_is_carried_through() {
        let inline = json!({
            "type": "object",
            "properties": {"score": {"type": "number"}},
            "required": ["score"]
        });
        let schema = OutputSchema::Custom(inline.clone());
        let contract = resolve_delegated_output_contract(Some(&schema)).unwrap();
        match &contract.schema_type {
            OutputSchema::Custom(v) => assert_eq!(v, &inline),
            other => panic!("expected custom schema, got {other:?}"),
        }
        // A custom inline schema still writes to the canonical `return` artifact.
        assert_eq!(contract.artifact_name(), "return");
    }

    #[test]
    fn malformed_inline_schema_is_rejected() {
        // An unclosed regex group is an invalid `pattern`, so schema compilation
        // fails rather than deferring the error to child-write time.
        let schema = OutputSchema::Custom(json!({"type": "string", "pattern": "("}));
        let err = resolve_delegated_output_contract(Some(&schema)).unwrap_err();
        assert!(err.contains("Invalid output_schema"), "{err}");
    }
}
