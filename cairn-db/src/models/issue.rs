//! Issue and comment types.

use serde::{Deserialize, Serialize};

use super::Label;

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
    /// Number of dependencies that have not reached Merged or Closed.
    #[serde(default)]
    pub unmet_dependency_count: i64,
    /// Canonical issue URIs this issue depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Canonical issue URIs of dependencies that have not yet reached Merged or
    /// Closed — what this issue is currently blocked on.
    #[serde(default)]
    pub unmet_depends_on: Vec<String>,
    /// Parent issue this issue branches from and wakes on attention.
    #[serde(default)]
    pub parent_issue_id: Option<String>,
    /// Workspace labels attached to this issue.
    #[serde(default)]
    pub labels: Vec<Label>,
    /// What this row IS: an ordinary issue or a thread. Defaulted so a payload
    /// written before the discriminator existed reads as an ordinary issue
    /// rather than failing to deserialize.
    #[serde(default)]
    pub kind: IssueKind,
}

/// What a row in `issues` is.
///
/// An `Issue` is a unit of work: it branches, produces a pull request, and
/// terminates by merging. A `Thread` is a durable, objective-free session
/// anchor: it owns no branch, its children merge to the project base branch,
/// and it never terminates via a PR — so it refuses a `merged` resolution.
/// Both share identity (the project-scoped issue number), children, comments,
/// and executions; this is what tells them apart.
///
/// An enum rather than a raw string on purpose: branch derivation, attention
/// projection, and the issue table all key on this, and an exhaustive match is
/// what makes a future kind a compile error at every one of those sites instead
/// of a silent fallthrough.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum IssueKind {
    #[default]
    Issue,
    Thread,
}

impl IssueKind {
    /// The accepted values, for a refusal message that tells the caller what it
    /// may write instead. Derived from the same variants `FromStr` parses.
    pub const ACCEPTED_VALUES: &'static str = "issue, thread";

    /// Whether an agent session resting Idle on a row of this kind is an
    /// attention-worthy fact.
    ///
    /// For an issue it is: an issue is a unit of work driving toward a merge, so
    /// a session that stopped short of one is stalled and asking for eyes. For a
    /// thread rest is the normal state — a thread is a durable session anchor
    /// that can sit resumable for weeks with nothing wrong — so rest is not
    /// attention. A resting thread projects [`IssueAttention::None`] and reads
    /// `active`: alive and resumable, not stalled.
    ///
    /// This gates only the *resting* presentation. A thread that genuinely needs
    /// a human — a pending question, a permission request, a blocked job or open
    /// merge request — reaches `NeedsInput` / `NeedsAuthorization` /
    /// `NeedsApproval` through exactly the projection an issue uses, and
    /// surfaces with the same urgency. There is no thread-specific attention
    /// channel.
    ///
    /// Exhaustive on purpose: a future kind must answer this question
    /// explicitly rather than inherit an answer.
    pub fn idle_session_needs_attention(self) -> bool {
        match self {
            IssueKind::Issue => true,
            IssueKind::Thread => false,
        }
    }
}

impl std::fmt::Display for IssueKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueKind::Issue => write!(f, "issue"),
            IssueKind::Thread => write!(f, "thread"),
        }
    }
}

impl std::str::FromStr for IssueKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "issue" => Ok(IssueKind::Issue),
            "thread" => Ok(IssueKind::Thread),
            _ => Err(format!(
                "Unknown issue kind: {s}. Accepted values: {}",
                IssueKind::ACCEPTED_VALUES
            )),
        }
    }
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
    /// A long-running agent node is resting Idle: non-terminal, resumable, and
    /// awaiting a wake. Blocks the status projection (the issue reads `waiting`)
    /// but is distinct from the human-decision attentions above.
    ///
    /// Only ever projected for [`IssueKind::Issue`]. A thread at rest is not
    /// stalled work asking for eyes — see
    /// [`IssueKind::idle_session_needs_attention`].
    Idle,
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
            IssueAttention::Idle => write!(f, "idle"),
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
            "idle" => Ok(IssueAttention::Idle),
            _ => Err(format!("Unknown attention: {}", s)),
        }
    }
}

impl IssueStatus {
    /// A terminal status is a stable end-state: no further automated progress
    /// will happen without an external action. `watch` returns on these so an
    /// external driver stops waiting instead of long-polling forever on a done
    /// issue. Distinct from cairn-core's issue-relations completeness check
    /// (dependency satisfaction = Merged | Closed only): `Failed` is terminal
    /// for a watcher's purposes too. `Complete` is intentionally *not* terminal
    /// — it is a transient successful state that typically advances to a PR /
    /// merge, so treating it as terminal would return early before the work is
    /// actually done. (Interrupts never reach `Failed`, so an interrupted issue
    /// stays watchable — see the interrupt-not-failure projection rule.)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            IssueStatus::Merged | IssueStatus::Closed | IssueStatus::Failed
        )
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
    #[serde(default)]
    pub label_ids: Option<Vec<String>>,
    /// What to create. Omitted means an ordinary issue.
    #[serde(default)]
    pub kind: IssueKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIssue {
    pub id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "model")]
    pub backend_override: Option<Option<String>>, // Nested Option to support clearing
    /// Full replacement dependency list when provided.
    pub depends_on: Option<Vec<String>>,
    /// Full replacement label list when provided.
    #[serde(default)]
    pub label_ids: Option<Vec<String>>,
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
    /// Stable, 1-based per-issue sequence. This is the identifier surfaced in
    /// the comment URI (`cairn://p/PROJECT/NUMBER/comments/{seq}`); the `id`
    /// UUID stays the internal primary key.
    pub seq: i64,
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
            IssueAttention::Idle,
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
    fn issue_kind_display_fromstr_round_trip() {
        for v in &[IssueKind::Issue, IssueKind::Thread] {
            let s = v.to_string();
            let parsed: IssueKind = s.parse().unwrap();
            assert_eq!(&parsed, v, "round-trip failed for {s}");
        }
    }

    #[test]
    fn issue_kind_fromstr_rejects_unknown_and_names_the_accepted_values() {
        let error = "discussion".parse::<IssueKind>().unwrap_err();
        assert!(error.contains("issue, thread"), "{error}");
    }

    #[test]
    fn issue_kind_defaults_to_issue() {
        assert_eq!(IssueKind::default(), IssueKind::Issue);
    }

    /// A serialized issue written before the discriminator existed carries no
    /// `kind` at all. It must read as an ordinary issue, not fail to parse —
    /// this is the same guarantee the column default gives a pre-migration row.
    #[test]
    fn issue_without_kind_deserializes_as_an_ordinary_issue() {
        let issue: Issue = serde_json::from_value(serde_json::json!({
            "id": "i-1",
            "projectId": "p-1",
            "number": 7,
            "title": "Legacy",
            "description": "",
            "status": "backlog",
            "progress": "backlog",
            "attention": "none",
            "priority": 0,
            "completedAt": null,
            "dismissedAt": null,
            "createdAt": 1,
            "updatedAt": 1,
            "backendOverride": null,
            "mergedAt": null,
            "closedAt": null,
        }))
        .expect("a payload predating `kind` must still deserialize");
        assert_eq!(issue.kind, IssueKind::Issue);
    }

    #[test]
    fn issue_kind_round_trips_through_serde_as_a_lowercase_string() {
        assert_eq!(
            serde_json::to_value(IssueKind::Thread).unwrap(),
            serde_json::json!("thread")
        );
        assert_eq!(
            serde_json::from_value::<IssueKind>(serde_json::json!("thread")).unwrap(),
            IssueKind::Thread
        );
    }

    /// Rest is stalled work for an issue and the normal state for a thread. The
    /// projection in `transitions::outcome` keys on exactly this.
    #[test]
    fn only_an_issue_treats_a_resting_session_as_attention() {
        assert!(IssueKind::Issue.idle_session_needs_attention());
        assert!(!IssueKind::Thread.idle_session_needs_attention());
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
            IssueAttention::Idle,
        ];
        for v in &blocking {
            assert!(
                v.blocks_status_projection(),
                "{v} should block status projection"
            );
        }
    }
}
