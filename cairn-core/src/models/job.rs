//! Job types - the unit of agent work.
//!
//! Jobs replace timeline_nodes as the core execution unit. A job owns:
//! - Worktree and branch (execution environment)
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
    // Archived: the job's recipe node was removed from the execution snapshot
    // mid-flight. Not derived from facts — it is an explicit override the status
    // projection treats as sticky (see `execution::advancement::recompute`). The
    // transcript (runs/events/turns/sessions/artifacts) is preserved; the job is
    // excluded from DAG readiness/advancement and never counts toward an
    // execution's running/failed status.
    Cancelled,
}

/// One attempt in a recipe node's job lineage (oldest→newest), including
/// archived (`cancelled`) attempts. Restart-node archives the prior job and
/// creates a fresh one, so a node can own several attempts; this is the
/// “Attempt N of M” view, with each attempt's transcript reachable via its
/// session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAttempt {
    pub id: String,
    pub status: String,
    pub created_at: i32,
    pub current_session_id: Option<String>,
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

    // Execution environment
    pub worktree_path: Option<String>,
    pub branch: Option<String>,
    pub base_commit: Option<String>,
    /// Nearest durable ancestor commit (reachable from the project default
    /// branch), captured alongside `base_commit` at worktree creation.
    pub pack_anchor: Option<String>,
    pub current_session_id: Option<String>,

    pub status: JobStatus,

    // Job metadata
    pub agent_config_id: Option<String>,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub task_description: Option<String>,
    pub model: Option<crate::models::Model>,

    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
    pub started_at: Option<i64>,

    /// Available tabs for this job: ["chat"] or ["chat", "artifact"]
    #[serde(default = "default_available_tabs")]
    pub available_tabs: Vec<String>,

    /// Initial tab to show: "chat" or "artifact" based on status and artifact presence
    #[serde(default = "default_initial_tab")]
    pub initial_tab: String,

    /// Parent tool use ID for batch_tasks - links child jobs to their parent batch
    pub parent_tool_use_id: Option<String>,
    /// Task index within a batch_tasks call (0, 1, 2...)
    pub task_index: Option<i32>,
    /// Human-readable node name from recipe (e.g., "builder-1")
    pub node_name: Option<String>,
    /// Execution sequence number (1-indexed) for URI routing
    pub exec_seq: Option<i32>,
    /// Base branch for worktree creation and PR targeting (None = use HEAD / repo default)
    pub base_branch: Option<String>,
    /// Stable URI segment assigned at creation for addressable job resources.
    pub uri_segment: Option<String>,
}

fn default_available_tabs() -> Vec<String> {
    vec!["chat".to_string()]
}

fn default_initial_tab() -> String {
    "chat".to_string()
}

/// Convert DbJob to Job
impl TryFrom<crate::db_records::DbJob> for Job {
    type Error = String;

    fn try_from(db: crate::db_records::DbJob) -> Result<Self, Self::Error> {
        let status: JobStatus = db
            .status
            .parse()
            .map_err(|e: String| format!("Invalid job status: {}", e))?;

        let model = db.model.as_ref().map(crate::models::Model::new);

        Ok(Job {
            id: db.id,
            execution_id: db.execution_id,
            recipe_node_id: db.recipe_node_id,
            parent_job_id: db.parent_job_id,
            worktree_path: db.worktree_path,
            branch: db.branch,
            base_commit: db.base_commit,
            pack_anchor: db.pack_anchor,
            current_session_id: db.current_session_id,
            status,
            agent_config_id: db.agent_config_id,
            issue_id: db.issue_id,
            project_id: db.project_id,
            task_description: db.task_description,
            model,
            created_at: db.created_at as i64,
            updated_at: db.updated_at as i64,
            completed_at: db.completed_at.map(|t| t as i64),
            started_at: db.started_at.map(|t| t as i64),
            available_tabs: default_available_tabs(),
            initial_tab: default_initial_tab(),
            parent_tool_use_id: db.parent_tool_use_id,
            task_index: db.task_index,
            node_name: db.node_name,
            exec_seq: None, // Populated by query functions via execution join
            base_branch: db.base_branch,
            uri_segment: db.uri_segment,
        })
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
