//! Issue and comment types.

use serde::{Deserialize, Serialize};

use super::common::Model;

/// Issue with stored lifecycle status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Issue {
    pub id: String,
    pub project_id: String,
    pub number: i32,
    pub title: String,
    pub description: String,
    pub status: IssueStatus,
    pub priority: i32,
    pub wait_state: Option<WaitState>,
    pub completed_at: Option<i64>,
    pub dismissed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub model: Option<Model>,
    /// Skill IDs attached to this issue
    pub skills: Vec<String>,
}

/// Issue lifecycle status (stored, not computed)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IssueStatus {
    #[default]
    Backlog,
    Active,
    Waiting,
    Merged,
    Closed,
}

impl std::fmt::Display for IssueStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueStatus::Backlog => write!(f, "backlog"),
            IssueStatus::Active => write!(f, "active"),
            IssueStatus::Waiting => write!(f, "waiting"),
            IssueStatus::Merged => write!(f, "merged"),
            IssueStatus::Closed => write!(f, "closed"),
        }
    }
}

impl std::str::FromStr for IssueStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "backlog" => Ok(IssueStatus::Backlog),
            "active" => Ok(IssueStatus::Active),
            "waiting" => Ok(IssueStatus::Waiting),
            "merged" => Ok(IssueStatus::Merged),
            "closed" => Ok(IssueStatus::Closed),
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

/// Wait state - why an issue is waiting
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WaitState {
    Prompt,
    Checkpoint,
    PrReview,
}

impl std::fmt::Display for WaitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WaitState::Prompt => write!(f, "prompt"),
            WaitState::Checkpoint => write!(f, "checkpoint"),
            WaitState::PrReview => write!(f, "pr_review"),
        }
    }
}

impl std::str::FromStr for WaitState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "prompt" => Ok(WaitState::Prompt),
            "checkpoint" => Ok(WaitState::Checkpoint),
            "pr_review" => Ok(WaitState::PrReview),
            _ => Err(format!("Unknown wait state: {}", s)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIssue {
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    pub model: Option<Model>,
    /// Skill IDs to attach to this issue
    pub skills: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIssue {
    pub id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub model: Option<Option<Model>>, // Nested Option to support clearing
    /// Skill IDs to attach to this issue (replaces existing skills)
    pub skills: Option<Vec<String>>,
}

// Comment types

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    pub id: String,
    pub issue_id: String,
    pub content: String,
    pub source: CommentSource,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CommentSource {
    User,
    Agent,
}

impl std::fmt::Display for CommentSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommentSource::User => write!(f, "user"),
            CommentSource::Agent => write!(f, "agent"),
        }
    }
}

impl std::str::FromStr for CommentSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "user" => Ok(CommentSource::User),
            "agent" => Ok(CommentSource::Agent),
            _ => Err(format!("Unknown comment source: {}", s)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateComment {
    pub issue_id: String,
    pub content: String,
    pub source: CommentSource,
}
