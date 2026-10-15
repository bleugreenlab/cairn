//! Execution types - instances of recipes running for issues/projects.

use serde::{Deserialize, Serialize};

use crate::{ActionRun, Job};

/// Execution lifecycle status (recomputed from job states, stored for query
/// efficiency).
///
/// The canonical derivation logic is in `transitions::recompute_execution_status`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionStatus {
    #[default]
    Running,
    Complete,
    Failed,
}

impl std::fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStatus::Running => write!(f, "running"),
            ExecutionStatus::Complete => write!(f, "complete"),
            ExecutionStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ExecutionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "running" => Ok(ExecutionStatus::Running),
            "complete" => Ok(ExecutionStatus::Complete),
            "failed" => Ok(ExecutionStatus::Failed),
            "paused" => Ok(ExecutionStatus::Running),
            other => Err(format!("unknown execution status: {}", other)),
        }
    }
}

/// Whether an execution owns its issue's current attempt.
///
/// Deliberately separate from [`ExecutionStatus`], which answers "how did this
/// attempt turn out" and is RECOMPUTED from job states. Lifecycle answers "does
/// this attempt own the issue" and is written only by the two supersession
/// operations in `execution::canonical`; nothing derives it. Sharing one column
/// is exactly how cairn/4658's superseded execution came back reading `running`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionLifecycle {
    /// Owns the issue's current attempt. At most one per issue, enforced by a
    /// partial unique index.
    #[default]
    Canonical,
    /// Fenced: no turn starts, no tool effect lands, no durable output.
    Superseded,
    /// A declared alternative. Runs freely, never claims to be the issue's
    /// implementation, and is never fenced by a successor.
    Parallel,
}

impl ExecutionLifecycle {
    /// Whether this lifecycle refuses turns and effects.
    pub fn is_fenced(self) -> bool {
        matches!(self, ExecutionLifecycle::Superseded)
    }
}

impl std::fmt::Display for ExecutionLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionLifecycle::Canonical => write!(f, "canonical"),
            ExecutionLifecycle::Superseded => write!(f, "superseded"),
            ExecutionLifecycle::Parallel => write!(f, "parallel"),
        }
    }
}

impl std::str::FromStr for ExecutionLifecycle {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "canonical" => Ok(ExecutionLifecycle::Canonical),
            "superseded" => Ok(ExecutionLifecycle::Superseded),
            "parallel" => Ok(ExecutionLifecycle::Parallel),
            other => Err(format!("unknown execution lifecycle: {}", other)),
        }
    }
}

/// Resolver key stored on the execution record. Captures the initiating user
/// so auto-started DAG jobs and cold resumes can re-resolve credentials from
/// the local identity store.
///
/// Not identity persistence — just a `(sub, org_id)` key.
///
/// Note: the `executions.initiator_auth_mode` column that once backed a removed
/// `auth_mode` field is intentionally left dormant (nullable, unwritten) rather
/// than dropped. `executions` is a ProjectScoped synced replica table, and
/// dropping a column from one is forbidden by sync-safety canon (see the
/// `shared_tail_jobs_child_base` precedent in cairn-core's migrations).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Initiator {
    /// JWT `sub` claim — the user who initiated this execution.
    pub sub: String,
    /// Organization ID (empty string for personal accounts).
    pub org_id: String,
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
    /// Resolver key for the user who initiated this execution.
    /// Used to re-resolve credentials for auto-started DAG jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initiator: Option<Initiator>,
    /// How this execution was triggered (manual, schedule, job_ended, skill_called).
    pub triggered_by: TriggerType,
    /// Stable per-machine device id that OWNS this execution (CAIRN-2629). Only
    /// the owning machine claims and runs its jobs; the UI shows it as the runner
    /// and renders "waiting for <device>" when it is a peer. `None` for legacy
    /// executions, which the ownership guard treats as "anyone may run".
    pub runner_device_id: Option<String>,
    /// Whether this execution owns its issue's current attempt (CAIRN-4665).
    /// Written only by `execution::canonical`; never recomputed.
    #[serde(default)]
    pub lifecycle: ExecutionLifecycle,
}

/// What a condition evaluation did.
///
/// Stored as free text in `condition_evaluations.outcome` with no CHECK, so a
/// new outcome is a new variant here rather than a table rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConditionOutcome {
    /// Routed to a port; downstream edges on that port may fire.
    #[default]
    Routed,
    /// Routed nowhere. The node stays parked until it is re-evaluated.
    Blocked,
}

impl ConditionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            ConditionOutcome::Routed => "routed",
            ConditionOutcome::Blocked => "blocked",
        }
    }
}

impl std::fmt::Display for ConditionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ConditionOutcome {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "routed" => Ok(ConditionOutcome::Routed),
            "blocked" => Ok(ConditionOutcome::Blocked),
            other => Err(format!("unknown condition outcome: {other}")),
        }
    }
}

/// A condition evaluation - result of evaluating a condition node during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionEvaluation {
    pub id: String,
    pub execution_id: String,
    pub recipe_node_id: String,
    pub outcome: ConditionOutcome,
    /// The port that fired. `None` unless `outcome` is `Routed`.
    pub result_port: Option<String>,
    pub raw_result: Option<String>,
    pub error_message: Option<String>,
    pub evaluated_at: i64,
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
    /// Owning machine's device id (CAIRN-2629); drives the owner/waiting badge.
    pub runner_device_id: Option<String>,
    /// Canonical/superseded/parallel standing (CAIRN-4665).
    #[serde(default)]
    pub lifecycle: ExecutionLifecycle,
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
    #[test]
    fn initiator_serde_roundtrip() {
        let initiator = super::Initiator {
            sub: "user-123".to_string(),
            org_id: "org-456".to_string(),
        };
        let json = serde_json::to_string(&initiator).unwrap();
        let parsed: super::Initiator = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sub, "user-123");
        assert_eq!(parsed.org_id, "org-456");
    }

    #[test]
    fn initiator_serde_empty_org_id() {
        let initiator = super::Initiator {
            sub: "user-789".to_string(),
            org_id: "".to_string(),
        };
        let json = serde_json::to_string(&initiator).unwrap();
        let parsed: super::Initiator = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sub, "user-789");
        assert_eq!(parsed.org_id, "");
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
