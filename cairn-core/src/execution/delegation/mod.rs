//! DAG-native delegated work packets.
//!
//! Delegated task requests record durable packets in the execution snapshot. The
//! DAG effect pipeline materializes pending packets into ordinary nodes/jobs.

pub mod runtime;

pub(crate) use runtime::lookup_caller_job_id;
pub use runtime::{resume_suspended_parent_after_task_completion, spawn_task_packets};

use uuid::Uuid;

use crate::models::{
    DelegatedOutputContract, DelegatedSessionMode, DelegatedSessionStrategy, DelegatedStatus,
    DelegatedWorkPacket, ExecutionSnapshot,
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
        id: Uuid::new_v4().to_string(),
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
        created_at: chrono::Utc::now().timestamp(),
    };

    snapshot.delegated_packets.push(packet.clone());
    packet
}
