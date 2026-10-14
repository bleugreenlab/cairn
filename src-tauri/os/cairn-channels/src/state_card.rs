//! Snapshot data rendered by channel providers; derived from application state by the caller.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCardState {
    pub subject_uri: String,
    pub agent_name: String,
    pub phase: AgentPhase,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub active_todo: Option<String>,
    pub todos: TodoProgress,
    pub pull_request: Option<PullRequestState>,
    pub checks: Vec<CheckState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AgentPhase {
    Running,
    WaitingForUser,
    Succeeded,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TodoProgress {
    pub completed: u32,
    pub total: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequestState {
    pub number: i64,
    pub status: String,
    pub additions: i64,
    pub deletions: i64,
    pub review_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckState {
    pub name: String,
    pub verdict: CheckVerdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CheckVerdict {
    Pending,
    Passed,
    Unverified,
    Failed,
}
