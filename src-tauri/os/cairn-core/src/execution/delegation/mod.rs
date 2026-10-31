//! DAG-native delegated work packets.
//!
//! Delegated task requests record durable packets in `delegated_packets`, one
//! row per packet (`cairn_workflow::delegated_packets`). The DAG effect pipeline
//! materializes pending packets into ordinary nodes/jobs.

pub mod runtime;

pub(crate) use runtime::lookup_caller_job_id;
pub(crate) use runtime::{
    is_call_child, resume_parked_parent_after_repair,
    resume_suspended_parent_after_task_completion, spawn_call_packets, spawn_task_packets,
    spawn_workflow_packets, TaskSpawnOutcome,
};

use cairn_common::ids;

use crate::models::{
    DelegatedOutputContract, DelegatedSessionMode, DelegatedSessionStrategy, DelegatedStatus,
    DelegatedWorkPacket, OutputSchema,
};
use cairn_workflow::delegated_packets::prompt_digest;

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
    pub(crate) description: String,
    pub(crate) prompt: String,
    pub(crate) subagent_type: String,
    pub(crate) tier: Option<String>,
    pub(crate) backend_preference: Option<String>,
    pub(crate) session: DelegatedTaskSessionMode,
    pub(crate) task_index: Option<i32>,
    /// Optional per-task output schema: a preset name or an inline custom JSON
    /// Schema. `None` preserves the default `return` contract.
    pub(crate) output_schema: Option<OutputSchema>,
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
    pub(crate) description: String,
    pub(crate) prompt: String,
    pub(crate) subagent_type: String,
    pub(crate) tier: Option<String>,
    pub(crate) backend_preference: Option<String>,
    /// Optional per-call output schema: a preset name or an inline custom JSON
    /// Schema. `None` preserves the default `return` contract.
    pub(crate) output_schema: Option<OutputSchema>,
    pub(crate) branch_policy: crate::execution::jobs::CallBranchPolicy,
    pub(crate) label: Option<String>,
    pub(crate) phase: Option<String>,
    pub(crate) task_index: Option<i32>,
    /// The harness dispatch ordinal (CAIRN-2498), threaded from `agent()` so a
    /// workflow-parented call can be journaled and replayed. `None` for an
    /// ordinary call.
    pub(crate) ordinal: Option<i64>,
}

pub(crate) struct SpawnCallPacketsInput<'a> {
    pub(crate) run_id: Option<&'a str>,
    pub(crate) payloads: &'a [DelegatedCallPayload],
    pub(crate) parent_tool_use_id: Option<&'a str>,
    pub(crate) background: bool,
}

/// A resolved workflow invocation (CAIRN-2487): a `run` item whose target is a
/// workflow URI. Unlike a call it has no agent/prompt — its process is a
/// supervised `bun <script>` — but it reuses the call-packet suspend/resume tail
/// to wake the caller with the workflow's output artifact.
pub(crate) struct SpawnWorkflowPacketsInput<'a> {
    pub(crate) run_id: Option<&'a str>,
    pub(crate) workflow_id: &'a str,
    /// Absolute path to the workflow's resolved script entry.
    pub(crate) script_path: std::path::PathBuf,
    /// The workflow's declared output schema (preset or inline), or `None` for
    /// the default `return` contract.
    pub(crate) output_schema: Option<OutputSchema>,
    /// The validated named args, forwarded to the script as `CAIRN_WORKFLOW_ARGS`.
    pub(crate) args_json: String,
    pub(crate) branch_policy: crate::execution::jobs::CallBranchPolicy,
    pub(crate) parent_tool_use_id: Option<&'a str>,
    pub(crate) background: bool,
}

pub(crate) struct SpawnTaskPacketsInput<'a> {
    pub(crate) run_id: Option<&'a str>,
    pub(crate) cwd: &'a str,
    pub(crate) payloads: &'a [DelegatedTaskPayload],
    /// The originating tool call's provider id, persisted as each packet's (and
    /// child job's) `parent_tool_use_id`. It is what the transcript locates the
    /// spawned children by and what the completion resume binds the synthetic
    /// `tool_result` to, so only a real provider id belongs here: `None` means
    /// there is no call to bind to, and the result resumes as a visible
    /// continuation event instead (CAIRN-3230).
    pub(crate) parent_tool_use_id: Option<&'a str>,
    pub(crate) background: bool,
}

pub(crate) struct CreateDelegatedPacketInput<'a> {
    parent_job_id: &'a str,
    parent_turn_id: Option<&'a str>,
    parent_tool_use_id: Option<&'a str>,
    title: &'a str,
    problem_statement: &'a str,
    agent_config_id: &'a str,
    cwd: &'a str,
    fence: Option<crate::models::Fence>,
    acceptance: Vec<String>,
    output_contract: DelegatedOutputContract,
    session: DelegatedSessionStrategy,
    task_index: Option<i32>,
    tier_override: Option<&'a str>,
    #[allow(dead_code)]
    pub backend_preference: Option<&'a str>,
    /// Fire-and-forget request: the spawning parent does not block or suspend on
    /// this packet. Persisted so the completion-resume handler notifies the
    /// spawner via a push instead of looking for a (nonexistent) successor turn.
    background: bool,
}

/// A task packet for `input`: the one already among `existing` that it
/// retries (`.1` is `false`), or a new pending one for the caller to store.
pub(crate) fn create_or_reuse_task_packet(
    existing: &[DelegatedWorkPacket],
    input: CreateDelegatedPacketInput<'_>,
) -> (DelegatedWorkPacket, bool) {
    let digest = prompt_digest(input.problem_statement);
    if let Some(existing) = existing
        .iter()
        .find(|packet| {
            packet.parent_job_id == input.parent_job_id
                && packet.parent_tool_use_id.as_deref() == input.parent_tool_use_id
                && packet.agent_config_id == input.agent_config_id
                && packet.title == input.title
                // A packet past pending keeps only its prompt's digest; one
                // written before digests existed keeps the prompt itself.
                && (packet.prompt_digest.as_deref() == Some(digest.as_str())
                    || packet.problem_statement.as_deref() == Some(input.problem_statement))
                && packet.task_index == input.task_index
                && packet.tier_override.as_deref() == input.tier_override
                && packet.backend_preference.as_deref() == input.backend_preference
                && packet.session == input.session
        })
        .cloned()
    {
        return (existing, false);
    }

    let packet = DelegatedWorkPacket {
        id: ids::mint_child(input.parent_job_id),
        parent_job_id: input.parent_job_id.to_string(),
        parent_turn_id: input.parent_turn_id.map(str::to_string),
        parent_tool_use_id: input.parent_tool_use_id.map(str::to_string),
        origin: crate::models::DelegationOrigin::TaskTool,
        title: input.title.to_string(),
        problem_statement: Some(input.problem_statement.to_string()),
        prompt_digest: Some(digest),
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
    (packet, true)
}

pub(crate) struct CreateCallPacketInput<'a> {
    pub(crate) parent_job_id: &'a str,
    pub(crate) parent_turn_id: Option<&'a str>,
    pub(crate) parent_tool_use_id: Option<&'a str>,
    pub(crate) title: &'a str,
    pub(crate) problem_statement: &'a str,
    pub(crate) agent_config_id: &'a str,
    pub(crate) cwd: &'a str,
    pub(crate) output_contract: DelegatedOutputContract,
    /// The call's own job id: the completion-resume path finds this packet by it.
    pub(crate) result_artifact_job_id: &'a str,
    pub(crate) task_index: Option<i32>,
    pub(crate) tier_override: Option<&'a str>,
    pub(crate) backend_preference: Option<&'a str>,
    pub(crate) background: bool,
}

/// A pre-materialized call packet (CAIRN-2481).
///
/// It keeps its prompt's digest but not the prompt: the call's own launch
/// carries the prompt, and nothing reads a call packet's.
///
/// Unlike a task packet it is born `Materialized` with its
/// `result_artifact_job_id` already set, so `expand_delegated_packets` (which
/// only expands `Pending` packets) skips it — the call never becomes a recipe
/// node and never advances the DAG — while the completion-resume path finds it by
/// `result_artifact_job_id` and drives the parent exactly as it does for a task.
pub(crate) fn create_call_packet(input: CreateCallPacketInput<'_>) -> DelegatedWorkPacket {
    DelegatedWorkPacket {
        id: ids::mint_child(input.parent_job_id),
        parent_job_id: input.parent_job_id.to_string(),
        parent_turn_id: input.parent_turn_id.map(str::to_string),
        parent_tool_use_id: input.parent_tool_use_id.map(str::to_string),
        origin: crate::models::DelegationOrigin::CallTool,
        title: input.title.to_string(),
        problem_statement: None,
        prompt_digest: Some(prompt_digest(input.problem_statement)),
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
    }
}

#[cfg(test)]
mod call_packet_tests {
    use super::*;

    /// A call packet is born Materialized with its result job set, so the DAG
    /// expander (which only expands Pending packets) never lowers it into a
    /// node, and it holds no prompt.
    #[test]
    fn call_packet_is_materialized_and_never_expands() {
        let contract = resolve_delegated_output_contract(None).unwrap();
        let packet = create_call_packet(CreateCallPacketInput {
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
        });

        assert_eq!(packet.origin, crate::models::DelegationOrigin::CallTool);
        assert_eq!(packet.status, DelegatedStatus::Materialized);
        assert_eq!(packet.result_artifact_job_id.as_deref(), Some("call-job"));
        assert!(packet.materialized_node_ids.is_empty());
        assert_eq!(packet.problem_statement, None);
        assert_eq!(packet.prompt_digest, Some(prompt_digest("do it")));
    }

    fn task_input<'a>(prompt: &'a str) -> CreateDelegatedPacketInput<'a> {
        CreateDelegatedPacketInput {
            parent_job_id: "parent",
            parent_turn_id: Some("T1"),
            parent_tool_use_id: Some("tool-1"),
            title: "task",
            problem_statement: prompt,
            agent_config_id: "Explore",
            cwd: "/tmp",
            fence: None,
            acceptance: vec![],
            output_contract: resolve_delegated_output_contract(None).unwrap(),
            session: DelegatedSessionStrategy::default(),
            task_index: Some(0),
            tier_override: None,
            backend_preference: None,
            background: false,
        }
    }

    /// A retried spawn reuses its packet whether that packet still holds the
    /// prompt, holds only its digest (past pending), or predates digests.
    #[test]
    fn a_retried_task_matches_its_packet_by_prompt_or_digest() {
        let (pending, created) = create_or_reuse_task_packet(&[], task_input("do it"));
        assert!(created);
        assert_eq!(pending.problem_statement.as_deref(), Some("do it"));

        let past_pending = DelegatedWorkPacket {
            problem_statement: None,
            status: DelegatedStatus::Running,
            ..pending.clone()
        };
        let legacy = DelegatedWorkPacket {
            prompt_digest: None,
            ..pending.clone()
        };
        for existing in [pending.clone(), past_pending, legacy] {
            let (reused, created) =
                create_or_reuse_task_packet(std::slice::from_ref(&existing), task_input("do it"));
            assert!(!created);
            assert_eq!(reused.id, existing.id);
        }
        let (_, created) = create_or_reuse_task_packet(&[pending], task_input("do else"));
        assert!(created, "a different prompt is a different task");
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
