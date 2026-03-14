//! Execution types - instances of recipes running for issues/projects.

use serde::{Deserialize, Serialize};

use crate::models::{ActionRun, Job};

/// Execution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionStatus {
    #[default]
    Running,
    Paused, // Waiting at checkpoint
    Complete,
    Failed,
}

impl std::fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStatus::Running => write!(f, "running"),
            ExecutionStatus::Paused => write!(f, "paused"),
            ExecutionStatus::Complete => write!(f, "complete"),
            ExecutionStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ExecutionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "running" => Ok(ExecutionStatus::Running),
            "paused" => Ok(ExecutionStatus::Paused),
            "complete" => Ok(ExecutionStatus::Complete),
            "failed" => Ok(ExecutionStatus::Failed),
            _ => Err(format!("Unknown execution status: {}", s)),
        }
    }
}

/// An execution - an instance of a recipe running for an issue or project.
///
/// The relationship is like model:series - the recipe is the template,
/// executions are instances that follow it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Execution {
    pub id: String,
    pub recipe_id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub status: ExecutionStatus,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    /// 1-indexed sequence number within an issue (1, 2, 3...).
    /// None for executions not associated with an issue.
    pub seq: Option<i32>,
}

/// Convert DbExecution to Execution
impl TryFrom<crate::diesel_models::DbExecution> for Execution {
    type Error = String;

    fn try_from(db: crate::diesel_models::DbExecution) -> Result<Self, Self::Error> {
        let status: ExecutionStatus = db
            .status
            .parse()
            .map_err(|e: String| format!("Invalid execution status: {}", e))?;

        Ok(Execution {
            id: db.id,
            recipe_id: db.recipe_id,
            issue_id: db.issue_id,
            project_id: db.project_id,
            status,
            started_at: db.started_at as i64,
            completed_at: db.completed_at.map(|t| t as i64),
            seq: db.seq,
        })
    }
}

/// A condition evaluation - result of evaluating a condition node during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionEvaluation {
    pub id: String,
    pub execution_id: String,
    pub recipe_node_id: String,
    pub result_port: String,
    pub raw_result: Option<String>,
    pub error_message: Option<String>,
    pub evaluated_at: i64,
}

/// Convert DbConditionEvaluation to ConditionEvaluation
impl From<crate::diesel_models::DbConditionEvaluation> for ConditionEvaluation {
    fn from(db: crate::diesel_models::DbConditionEvaluation) -> Self {
        ConditionEvaluation {
            id: db.id,
            execution_id: db.execution_id,
            recipe_node_id: db.recipe_node_id,
            result_port: db.result_port,
            raw_result: db.raw_result,
            error_message: db.error_message,
            evaluated_at: db.evaluated_at as i64,
        }
    }
}

/// Trigger type for executions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TriggerType {
    #[default]
    Manual,
    Issue,
    Schedule,
    Webhook,
}

impl std::fmt::Display for TriggerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerType::Manual => write!(f, "manual"),
            TriggerType::Issue => write!(f, "issue"),
            TriggerType::Schedule => write!(f, "schedule"),
            TriggerType::Webhook => write!(f, "webhook"),
        }
    }
}

impl std::str::FromStr for TriggerType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "manual" => Ok(TriggerType::Manual),
            "issue" => Ok(TriggerType::Issue),
            "schedule" => Ok(TriggerType::Schedule),
            "webhook" => Ok(TriggerType::Webhook),
            _ => Err(format!("Unknown trigger type: {}", s)),
        }
    }
}

/// Execution list item for the Executions view
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionListItem {
    pub id: String,
    pub recipe_name: String,
    pub issue_id: Option<String>,
    pub issue_number: Option<i32>,
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub triggered_by: TriggerType,
    pub status: ExecutionStatus,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub jobs_total: i64,
    pub jobs_complete: i64,
    /// 1-indexed sequence number within an issue (1, 2, 3...).
    /// None for executions not associated with an issue.
    pub seq: Option<i32>,
}

/// Filters for listing executions
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionFilters {
    pub status: Option<String>,
    pub triggered_by: Option<String>,
    pub project_id: Option<String>,
}

/// Paginated result for executions
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionListResult {
    pub items: Vec<ExecutionListItem>,
    pub total: i64,
    pub has_more: bool,
}

/// Full execution detail with related data
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionDetail {
    pub execution: Execution,
    pub recipe_name: String,
    pub issue_id: Option<String>,
    pub issue_number: Option<i32>,
    pub issue_title: Option<String>,
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub jobs: Vec<Job>,
    pub action_runs: Vec<ActionRun>,
}
