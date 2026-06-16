//! PR (Pull Request) related types.

use serde::{Deserialize, Serialize};

/// PR cache for GitHub API data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrCache {
    pub id: String,
    pub job_id: Option<String>,
    pub pr_number: i32,
    pub pr_url: String,
    pub title: Option<String>,
    pub body: Option<String>,
    pub state: PrState,
    pub is_draft: bool,
    pub review_decision: Option<ReviewDecision>,
    pub mergeable: MergeableState,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub checks_status: Option<ChecksStatus>,
    pub checks: Vec<Check>,
    pub fetched_at: i64,
    pub updated_at: i64,
    pub is_local: bool,
    pub source_branch: Option<String>,
    pub target_branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrState {
    #[default]
    Open,
    Closed,
    Merged,
}

impl std::fmt::Display for PrState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrState::Open => write!(f, "OPEN"),
            PrState::Closed => write!(f, "CLOSED"),
            PrState::Merged => write!(f, "MERGED"),
        }
    }
}

impl std::str::FromStr for PrState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "OPEN" => Ok(PrState::Open),
            "CLOSED" => Ok(PrState::Closed),
            "MERGED" => Ok(PrState::Merged),
            _ => Err(format!("Unknown PR state: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewDecision {
    Approved,
    ChangesRequested,
    ReviewRequired,
}

impl std::fmt::Display for ReviewDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReviewDecision::Approved => write!(f, "APPROVED"),
            ReviewDecision::ChangesRequested => write!(f, "CHANGES_REQUESTED"),
            ReviewDecision::ReviewRequired => write!(f, "REVIEW_REQUIRED"),
        }
    }
}

impl std::str::FromStr for ReviewDecision {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "APPROVED" => Ok(ReviewDecision::Approved),
            "CHANGES_REQUESTED" => Ok(ReviewDecision::ChangesRequested),
            "REVIEW_REQUIRED" => Ok(ReviewDecision::ReviewRequired),
            _ => Err(format!("Unknown review decision: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MergeableState {
    Mergeable,
    Conflicting,
    #[default]
    Unknown,
}

impl std::fmt::Display for MergeableState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeableState::Mergeable => write!(f, "MERGEABLE"),
            MergeableState::Conflicting => write!(f, "CONFLICTING"),
            MergeableState::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl std::str::FromStr for MergeableState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "MERGEABLE" => Ok(MergeableState::Mergeable),
            "CONFLICTING" => Ok(MergeableState::Conflicting),
            "UNKNOWN" | "" => Ok(MergeableState::Unknown),
            _ => Err(format!("Unknown mergeable state: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChecksStatus {
    Success,
    Failure,
    Pending,
    Error,
}

impl std::fmt::Display for ChecksStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChecksStatus::Success => write!(f, "SUCCESS"),
            ChecksStatus::Failure => write!(f, "FAILURE"),
            ChecksStatus::Pending => write!(f, "PENDING"),
            ChecksStatus::Error => write!(f, "ERROR"),
        }
    }
}

impl std::str::FromStr for ChecksStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SUCCESS" => Ok(ChecksStatus::Success),
            "FAILURE" => Ok(ChecksStatus::Failure),
            "PENDING" => Ok(ChecksStatus::Pending),
            "ERROR" => Ok(ChecksStatus::Error),
            _ => Err(format!("Unknown checks status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Check {
    pub name: String,
    pub state: CheckState,
    pub description: Option<String>,
    pub workflow_name: Option<String>,
    pub link: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckState {
    Success,
    Failure,
    Pending,
    Skipped,
    Cancelled,
}

impl std::fmt::Display for CheckState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckState::Success => write!(f, "SUCCESS"),
            CheckState::Failure => write!(f, "FAILURE"),
            CheckState::Pending => write!(f, "PENDING"),
            CheckState::Skipped => write!(f, "SKIPPED"),
            CheckState::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl std::str::FromStr for CheckState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SUCCESS" | "PASS" => Ok(CheckState::Success),
            "FAILURE" | "FAIL" => Ok(CheckState::Failure),
            "PENDING" | "QUEUED" | "IN_PROGRESS" | "WAITING" => Ok(CheckState::Pending),
            "SKIPPED" | "NEUTRAL" => Ok(CheckState::Skipped),
            "CANCELLED" | "CANCELED" | "TIMED_OUT" | "STALE" => Ok(CheckState::Cancelled),
            _ => Err(format!("Unknown check state: {}", s)),
        }
    }
}

/// Details about a failed CI check, including error logs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckFailureDetails {
    pub failed_step: Option<String>,
    pub log_excerpt: String,
    pub full_log_available: bool,
}

/// Lightweight PR data for sidebar display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrDataSummary {
    pub id: String,
    pub action_run_id: Option<String>,
    pub pr_number: i32,
    pub pr_url: String,
    pub pr_status: String,
}
