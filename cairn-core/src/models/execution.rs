//! Execution types - instances of recipes running for issues/projects.

use serde::{Deserialize, Serialize};

use crate::execution::Initiator;
use crate::models::{ActionRun, Job};

/// Execution status (recomputed from job states, stored for query efficiency).
///
/// The canonical derivation logic is in `transitions::recompute_execution_status`.
pub use crate::transitions::ExecutionStatus;

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
    /// Resolver key for the user who initiated this execution.
    /// Used to re-resolve credentials for auto-started DAG jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initiator: Option<Initiator>,
    /// How this execution was triggered (manual, schedule, job_ended, skill_called).
    pub triggered_by: TriggerType,
}

/// Convert DbExecution to Execution
impl TryFrom<crate::db_records::DbExecution> for Execution {
    type Error = String;

    fn try_from(db: crate::db_records::DbExecution) -> Result<Self, Self::Error> {
        let status: ExecutionStatus = db
            .status
            .parse()
            .map_err(|e: String| format!("Invalid execution status: {}", e))?;

        // Reconstruct Initiator from nullable columns (all-or-nothing).
        let initiator = match (
            db.initiator_sub,
            db.initiator_auth_mode,
            db.initiator_org_id,
        ) {
            (Some(sub), Some(auth_mode), Some(org_id)) => Some(Initiator {
                sub,
                auth_mode,
                org_id,
            }),
            _ => None,
        };

        let triggered_by: TriggerType = db.triggered_by.parse().unwrap_or_default();

        Ok(Execution {
            id: db.id,
            recipe_id: db.recipe_id,
            issue_id: db.issue_id,
            project_id: db.project_id,
            status,
            started_at: db.started_at as i64,
            completed_at: db.completed_at.map(|t| t as i64),
            seq: db.seq,
            initiator,
            triggered_by,
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
impl From<crate::db_records::DbConditionEvaluation> for ConditionEvaluation {
    fn from(db: crate::db_records::DbConditionEvaluation) -> Self {
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
#[serde(rename_all = "snake_case")]
pub enum TriggerType {
    #[default]
    #[serde(alias = "issue")]
    Manual,
    Schedule,
    JobEnded,
    SkillCalled,
}

impl std::fmt::Display for TriggerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerType::Manual => write!(f, "manual"),
            TriggerType::Schedule => write!(f, "schedule"),
            TriggerType::JobEnded => write!(f, "job_ended"),
            TriggerType::SkillCalled => write!(f, "skill_called"),
        }
    }
}

impl std::str::FromStr for TriggerType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "manual" | "issue" => Ok(TriggerType::Manual),
            "schedule" => Ok(TriggerType::Schedule),
            "job_ended" => Ok(TriggerType::JobEnded),
            "skill_called" => Ok(TriggerType::SkillCalled),
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

/// A lightweight execution record for the triggered-execution indicator on job rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggeredExecution {
    pub id: String,
    pub recipe_name: String,
    pub status: ExecutionStatus,
    pub triggered_by: TriggerType,
    pub started_at: i64,
    pub completed_at: Option<i64>,
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

#[cfg(test)]
mod tests {
    use crate::db_records::DbExecution;

    fn base_db_execution() -> DbExecution {
        DbExecution {
            id: "exec-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            issue_id: None,
            project_id: Some("proj-1".to_string()),
            status: "running".to_string(),
            started_at: 1000,
            completed_at: None,
            snapshot: None,
            seq: Some(1),
            initiator_sub: None,
            initiator_auth_mode: None,
            initiator_org_id: None,
            triggered_by: "manual".to_string(),
        }
    }

    #[test]
    fn try_from_db_execution_all_initiator_columns_present() {
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_auth_mode: Some("byot".to_string()),
            initiator_org_id: Some("org-456".to_string()),
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        let initiator = exec.initiator.expect("should have initiator");
        assert_eq!(initiator.sub, "user-123");
        assert_eq!(initiator.auth_mode, "byot");
        assert_eq!(initiator.org_id, "org-456");
    }

    #[test]
    fn try_from_db_execution_no_initiator_columns() {
        let db = base_db_execution();
        let exec: super::Execution = db.try_into().unwrap();
        assert!(exec.initiator.is_none());
    }

    #[test]
    fn try_from_db_execution_partial_initiator_is_none() {
        // Only sub present, auth_mode and org_id missing → None (all-or-nothing)
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_auth_mode: None,
            initiator_org_id: None,
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        assert!(
            exec.initiator.is_none(),
            "partial initiator columns should produce None"
        );
    }

    #[test]
    fn try_from_db_execution_two_of_three_initiator_is_none() {
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_auth_mode: Some("shared".to_string()),
            initiator_org_id: None,
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        assert!(
            exec.initiator.is_none(),
            "two of three initiator columns should produce None"
        );
    }

    #[test]
    fn try_from_db_execution_empty_org_id_is_valid_initiator() {
        // Empty string org_id (personal account) is valid — it's Some(""), not None
        let db = DbExecution {
            initiator_sub: Some("user-123".to_string()),
            initiator_auth_mode: Some("byot".to_string()),
            initiator_org_id: Some("".to_string()),
            ..base_db_execution()
        };
        let exec: super::Execution = db.try_into().unwrap();
        let initiator = exec
            .initiator
            .expect("empty org_id should still produce Some");
        assert_eq!(initiator.org_id, "");
    }

    // =========================================================================
    // TriggerType tests
    // =========================================================================
    use super::TriggerType;

    #[test]
    fn trigger_type_display() {
        assert_eq!(TriggerType::Manual.to_string(), "manual");
        assert_eq!(TriggerType::Schedule.to_string(), "schedule");
        assert_eq!(TriggerType::JobEnded.to_string(), "job_ended");
        assert_eq!(TriggerType::SkillCalled.to_string(), "skill_called");
    }

    #[test]
    fn trigger_type_from_str() {
        assert_eq!(
            "manual".parse::<TriggerType>().unwrap(),
            TriggerType::Manual
        );
        assert_eq!(
            "schedule".parse::<TriggerType>().unwrap(),
            TriggerType::Schedule
        );
        assert_eq!(
            "job_ended".parse::<TriggerType>().unwrap(),
            TriggerType::JobEnded
        );
        assert_eq!(
            "skill_called".parse::<TriggerType>().unwrap(),
            TriggerType::SkillCalled
        );
    }

    #[test]
    fn trigger_type_from_str_legacy_issue_maps_to_manual() {
        assert_eq!("issue".parse::<TriggerType>().unwrap(), TriggerType::Manual);
        assert_eq!("ISSUE".parse::<TriggerType>().unwrap(), TriggerType::Manual);
    }

    #[test]
    fn trigger_type_from_str_invalid() {
        let result = "webhook".parse::<TriggerType>();
        assert!(result.is_err());
    }

    #[test]
    fn trigger_type_default() {
        assert_eq!(TriggerType::default(), TriggerType::Manual);
    }

    #[test]
    fn trigger_type_serde_alias_issue() {
        let t: TriggerType = serde_json::from_str("\"issue\"").unwrap();
        assert_eq!(t, TriggerType::Manual);
    }

    #[test]
    fn trigger_type_serde_roundtrip_new_variants() {
        let t: TriggerType = serde_json::from_str("\"job_ended\"").unwrap();
        assert_eq!(t, TriggerType::JobEnded);
        assert_eq!(serde_json::to_string(&t).unwrap(), "\"job_ended\"");

        let t: TriggerType = serde_json::from_str("\"skill_called\"").unwrap();
        assert_eq!(t, TriggerType::SkillCalled);
        assert_eq!(serde_json::to_string(&t).unwrap(), "\"skill_called\"");
    }
}
