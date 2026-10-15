//! Job types - the unit of agent work.
//!
//! Jobs replace timeline_nodes as the core execution unit. A job owns:
//! - durable branch and logical-head coordinates
//! - backend session (conversation state)
//! - Artifacts (outputs)
//! - Runs (execution attempts)

use serde::{Deserialize, Serialize};

/// Job status in the execution lifecycle.
///
/// Transitions are validated by `transitions::transition_job`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    #[default]
    Pending, // Not started: deps unmet, or deps met but not yet claimed by advancement
    Running,  // Started/claimed: a turn is attached or being spun up (also crash-in-flight)
    Complete, // Finished with artifacts
    Failed,   // Error occurred or cascaded from upstream
    Blocked,  // Checkpoint awaiting approval
    // Idle: a long-running node ended its turn with no unmet output contract.
    // Non-terminal and resumable — it fires no JobEnded, advances no DAG, and
    // keeps taking wakes until the issue is closed. Only nodes a recipe's
    // topology derives as long-running (a contract-less control-terminal in a
    // recipe with no terminal action node) reach this; every other node still
    // completes at turn-end.
    Idle,
    // Archived: the job's recipe node was removed from the execution snapshot
    // mid-flight. Not derived from facts — it is an explicit override the status
    // projection treats as sticky (see `execution::advancement::recompute`). The
    // transcript (runs/events/turns/sessions/artifacts) is preserved; the job is
    // excluded from DAG readiness/advancement and never counts toward an
    // execution's running/failed status.
    Cancelled,
}

impl JobStatus {
    /// Whether the job is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Complete | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending => write!(f, "pending"),
            JobStatus::Running => write!(f, "running"),
            JobStatus::Complete => write!(f, "complete"),
            JobStatus::Failed => write!(f, "failed"),
            JobStatus::Blocked => write!(f, "blocked"),
            JobStatus::Idle => write!(f, "idle"),
            JobStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(JobStatus::Pending),
            // `ready` is a removed status; any lingering rows read back as Pending
            // (the migration converts them, this guards in-flight reads).
            "ready" => Ok(JobStatus::Pending),
            "running" => Ok(JobStatus::Running),
            "complete" => Ok(JobStatus::Complete),
            "failed" => Ok(JobStatus::Failed),
            "blocked" => Ok(JobStatus::Blocked),
            "idle" => Ok(JobStatus::Idle),
            "cancelled" => Ok(JobStatus::Cancelled),
            _ => Err(format!("Unknown job status: {}", s)),
        }
    }
}

/// A job - the unit of agent work within an execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: String,
    pub execution_id: Option<String>,
    pub recipe_node_id: Option<String>,
    pub parent_job_id: Option<String>,

    // Durable repository coordinate.
    //
    // The branch is the whole of a job's live coordinate: a base commit is
    // resolved from the store at the moment it is needed, never carried on this
    // model. `jobs.base_commit` is deliberately absent here — it is an archival
    // record of where the branch was last anchored, and every surface that
    // presents or acts on a base coordinate derives it live (CAIRN-3224). A
    // plausible-looking commit id on the model a UI receives is exactly how the
    // CAIRN-3094 / 3108 / 3150 stale-coordinate family started, so it does not
    // cross this boundary (CAIRN-3226).
    pub branch: Option<String>,
    /// Nearest durable ancestor commit reachable from the project default
    /// branch. Archival: it selects the range a pack is built against and is
    /// never a placement coordinate.
    pub pack_anchor: Option<String>,
    pub current_session_id: Option<String>,

    pub status: JobStatus,

    // Job metadata
    pub agent_config_id: Option<String>,
    pub issue_id: Option<String>,
    pub thread_id: Option<String>,
    pub project_id: String,
    pub task_description: Option<String>,
    pub model: Option<crate::Model>,

    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
    pub started_at: Option<i64>,

    /// Available tabs for this job: ["chat"] or ["chat", "artifact"]
    #[serde(default = "Job::default_available_tabs")]
    pub available_tabs: Vec<String>,

    /// Initial tab to show: "chat" or "artifact" based on status and artifact presence
    #[serde(default = "Job::default_initial_tab")]
    pub initial_tab: String,

    /// Parent tool use ID for batch_tasks - links child jobs to their parent batch
    pub parent_tool_use_id: Option<String>,
    /// Task index within a batch_tasks call (0, 1, 2...)
    pub task_index: Option<i32>,
    /// Human-readable node name from recipe (e.g., "builder-1")
    pub node_name: Option<String>,
    /// Execution sequence number (1-indexed) for URI routing
    pub exec_seq: Option<i32>,
    /// Base branch for logical branch creation and PR targeting (None = repository default).
    pub base_branch: Option<String>,
    /// Stable URI segment assigned at creation for addressable job resources.
    pub uri_segment: Option<String>,
}

impl Job {
    pub fn default_available_tabs() -> Vec<String> {
        vec!["chat".to_string()]
    }

    pub fn default_initial_tab() -> String {
        "chat".to_string()
    }
}

/// Input for creating a job
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct CreateJob {
    pub execution_id: Option<String>,
    pub parent_job_id: Option<String>,
    pub agent_config_id: Option<String>,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub task_description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_jobs_default_tabs_and_authored_tabs_survive_deserialization() {
        let mut value = serde_json::json!({
            "id": "job", "projectId": "project", "status": "running",
            "createdAt": 1, "updatedAt": 2
        });
        let legacy: Job = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(legacy.available_tabs, ["chat"]);
        assert_eq!(legacy.initial_tab, "chat");
        value["availableTabs"] = serde_json::json!(["chat", "artifact"]);
        value["initialTab"] = serde_json::json!("artifact");
        let authored: Job = serde_json::from_value(value).unwrap();
        assert_eq!(authored.available_tabs, ["chat", "artifact"]);
        assert_eq!(authored.initial_tab, "artifact");
    }
}
