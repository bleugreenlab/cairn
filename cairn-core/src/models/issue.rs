//! Issue and comment types.

use serde::{Deserialize, Serialize};

/// Issue with lifecycle status derived from executions + resolution timestamps.
///
/// Status is stored for query efficiency but recomputed deterministically:
/// - If `merged_at` is set → Merged
/// - If `closed_at` is set → Closed
/// - Else derived from execution states (Backlog, Active, Complete, Failed)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Issue {
    pub id: String,
    pub project_id: String,
    pub number: i32,
    pub title: String,
    pub description: String,
    pub status: IssueStatus,
    pub progress: IssueProgress,
    pub attention: IssueAttention,
    pub priority: i32,
    pub completed_at: Option<i64>,
    pub dismissed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub backend_override: Option<String>,
    /// Timestamp when the issue's PR was merged (resolution)
    pub merged_at: Option<i64>,
    /// Timestamp when the issue was closed (resolution)
    pub closed_at: Option<i64>,
    /// Manager that owns this issue
    pub manager_id: Option<String>,
}

/// Issue lifecycle status.
///
/// Stored but deterministically recomputed — the `recompute_issue_status`
/// function is the ONLY writer. Do not set this directly via SQL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IssueStatus {
    #[default]
    Backlog,
    Active,
    Waiting,
    Complete,
    Failed,
    Merged,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IssueProgress {
    #[default]
    Backlog,
    Active,
    Complete,
    Failed,
    Merged,
    Closed,
}

impl std::fmt::Display for IssueProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueProgress::Backlog => write!(f, "backlog"),
            IssueProgress::Active => write!(f, "active"),
            IssueProgress::Complete => write!(f, "complete"),
            IssueProgress::Failed => write!(f, "failed"),
            IssueProgress::Merged => write!(f, "merged"),
            IssueProgress::Closed => write!(f, "closed"),
        }
    }
}

impl std::str::FromStr for IssueProgress {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "backlog" => Ok(IssueProgress::Backlog),
            "active" => Ok(IssueProgress::Active),
            "complete" => Ok(IssueProgress::Complete),
            "failed" => Ok(IssueProgress::Failed),
            "merged" => Ok(IssueProgress::Merged),
            "closed" => Ok(IssueProgress::Closed),
            _ => Err(format!("Unknown progress: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IssueAttention {
    #[default]
    None,
    NeedsInput,
    NeedsAuthorization,
    NeedsApproval,
    NeedsConflictResolution,
    NeedsReview,
    NeedsMerge,
}

impl IssueAttention {
    pub fn blocks_status_projection(&self) -> bool {
        !matches!(self, IssueAttention::None)
    }
}

impl std::fmt::Display for IssueAttention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueAttention::None => write!(f, "none"),
            IssueAttention::NeedsInput => write!(f, "needs_input"),
            IssueAttention::NeedsAuthorization => write!(f, "needs_authorization"),
            IssueAttention::NeedsApproval => write!(f, "needs_approval"),
            IssueAttention::NeedsConflictResolution => write!(f, "needs_conflict_resolution"),
            IssueAttention::NeedsReview => write!(f, "needs_review"),
            IssueAttention::NeedsMerge => write!(f, "needs_merge"),
        }
    }
}

impl std::str::FromStr for IssueAttention {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(IssueAttention::None),
            "needs_input" => Ok(IssueAttention::NeedsInput),
            "needs_authorization" => Ok(IssueAttention::NeedsAuthorization),
            "needs_approval" => Ok(IssueAttention::NeedsApproval),
            "needs_conflict_resolution" => Ok(IssueAttention::NeedsConflictResolution),
            "needs_review" => Ok(IssueAttention::NeedsReview),
            "needs_merge" => Ok(IssueAttention::NeedsMerge),
            _ => Err(format!("Unknown attention: {}", s)),
        }
    }
}

impl std::fmt::Display for IssueStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueStatus::Backlog => write!(f, "backlog"),
            IssueStatus::Active => write!(f, "active"),
            IssueStatus::Waiting => write!(f, "waiting"),
            IssueStatus::Complete => write!(f, "complete"),
            IssueStatus::Failed => write!(f, "failed"),
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
            "complete" => Ok(IssueStatus::Complete),
            "failed" => Ok(IssueStatus::Failed),
            "merged" => Ok(IssueStatus::Merged),
            "closed" => Ok(IssueStatus::Closed),
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIssue {
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    #[serde(alias = "model")]
    pub backend_override: Option<String>,
    /// Manager that owns this issue
    pub manager_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIssue {
    pub id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "model")]
    pub backend_override: Option<Option<String>>, // Nested Option to support clearing
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_progress_display_fromstr_round_trip() {
        let variants = [
            IssueProgress::Backlog,
            IssueProgress::Active,
            IssueProgress::Complete,
            IssueProgress::Failed,
            IssueProgress::Merged,
            IssueProgress::Closed,
        ];
        for v in &variants {
            let s = v.to_string();
            let parsed: IssueProgress = s.parse().unwrap();
            assert_eq!(&parsed, v, "round-trip failed for {s}");
        }
    }

    #[test]
    fn issue_progress_fromstr_rejects_unknown() {
        assert!("garbage".parse::<IssueProgress>().is_err());
    }

    #[test]
    fn issue_attention_display_fromstr_round_trip() {
        let variants = [
            IssueAttention::None,
            IssueAttention::NeedsInput,
            IssueAttention::NeedsAuthorization,
            IssueAttention::NeedsApproval,
            IssueAttention::NeedsConflictResolution,
            IssueAttention::NeedsReview,
            IssueAttention::NeedsMerge,
        ];
        for v in &variants {
            let s = v.to_string();
            let parsed: IssueAttention = s.parse().unwrap();
            assert_eq!(&parsed, v, "round-trip failed for {s}");
        }
    }

    #[test]
    fn issue_attention_fromstr_rejects_unknown() {
        assert!("garbage".parse::<IssueAttention>().is_err());
    }

    #[test]
    fn blocks_status_projection_none_returns_false() {
        assert!(!IssueAttention::None.blocks_status_projection());
    }

    #[test]
    fn blocks_status_projection_all_others_return_true() {
        let blocking = [
            IssueAttention::NeedsInput,
            IssueAttention::NeedsAuthorization,
            IssueAttention::NeedsApproval,
            IssueAttention::NeedsConflictResolution,
            IssueAttention::NeedsReview,
            IssueAttention::NeedsMerge,
        ];
        for v in &blocking {
            assert!(
                v.blocks_status_projection(),
                "{v} should block status projection"
            );
        }
    }
}
