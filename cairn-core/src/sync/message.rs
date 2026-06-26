//! Sync message types for desktop → cloud dual-write.

use serde::Serialize;

/// A message to sync to the cloud.
///
/// Durable messages (entities) are batched and retried on failure.
/// Transcript events and streaming deltas remain local-only.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "table", content = "data")]
pub enum SyncMessage {
    // Durable entities (retry on failure)
    Project(SyncProject),
    Issue(SyncIssue),
    Job(SyncJob),
    Run(SyncRun),
    // Local-only transcript events (accepted but not forwarded)
    Event(SyncEvent),
    Artifact(SyncArtifact),
    Comment(SyncComment),

    // Local-only streaming deltas
    StreamDelta(StreamDelta),

    // Lifecycle
    Delete { table: String, id: String },
}

impl SyncMessage {
    /// Whether this message should be retried on failure.
    pub fn is_durable(&self) -> bool {
        !matches!(self, SyncMessage::Event(_) | SyncMessage::StreamDelta(_))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncProject {
    pub id: String,
    pub key: String,
    pub name: String,
    pub path: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncIssue {
    pub id: String,
    pub project_id: String,
    pub number: i32,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: i32,
    pub model: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub merged_at: Option<i64>,
    pub closed_at: Option<i64>,
    pub depends_on: Vec<String>,
    pub parent_issue_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncJob {
    pub id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub execution_id: Option<String>,
    pub node_name: Option<String>,
    pub task_description: Option<String>,
    pub status: Option<String>,
    pub model: Option<String>,
    pub branch: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncRun {
    pub id: String,
    pub job_id: Option<String>,
    pub issue_id: Option<String>,
    pub status: Option<String>,
    pub exit_reason: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i64>,
    pub exited_at: Option<i64>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncEvent {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub sequence: Option<i32>,
    pub event_type: String,
    pub data: Option<String>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub thinking_tokens: Option<i32>,
    pub created_at: Option<i64>,
    pub turn_id: Option<String>,
}

pub struct SyncTranscriptEvent {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub sequence: i32,
    pub event_type: String,
    pub data: String,
    pub created_at: i64,
    pub turn_id: Option<String>,
}

impl SyncEvent {
    pub fn transcript(event: SyncTranscriptEvent) -> Self {
        Self {
            id: event.id,
            run_id: event.run_id,
            session_id: event.session_id,
            sequence: Some(event.sequence),
            event_type: event.event_type,
            data: Some(event.data),
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            thinking_tokens: None,
            created_at: Some(event.created_at),
            turn_id: event.turn_id,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncArtifact {
    pub id: String,
    pub job_id: Option<String>,
    pub data: Option<String>,
    pub version: Option<i32>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncComment {
    pub id: String,
    pub issue_id: String,
    pub content: String,
    pub source: Option<String>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamDelta {
    pub run_id: String,
    pub event_id: String,
    pub tokens: String,
}

// === From conversions for domain models ===

impl From<&crate::models::Project> for SyncProject {
    fn from(p: &crate::models::Project) -> Self {
        SyncProject {
            id: p.id.clone(),
            key: p.key.clone(),
            name: p.name.clone(),
            path: Some(p.repo_path.clone()),
            created_at: Some(p.created_at),
            updated_at: Some(p.updated_at),
        }
    }
}

impl From<&crate::models::Issue> for SyncIssue {
    fn from(i: &crate::models::Issue) -> Self {
        SyncIssue {
            id: i.id.clone(),
            project_id: i.project_id.clone(),
            number: i.number,
            title: i.title.clone(),
            description: Some(i.description.clone()),
            status: i.status.to_string(),
            priority: i.priority,
            model: i.backend_override.clone(),
            created_at: Some(i.created_at),
            updated_at: Some(i.updated_at),
            completed_at: i.completed_at,
            merged_at: i.merged_at,
            closed_at: i.closed_at,
            depends_on: i.depends_on.clone(),
            parent_issue_id: i.parent_issue_id.clone(),
        }
    }
}

impl From<&crate::models::Job> for SyncJob {
    fn from(j: &crate::models::Job) -> Self {
        SyncJob {
            id: j.id.clone(),
            issue_id: j.issue_id.clone(),
            project_id: Some(j.project_id.clone()),
            execution_id: j.execution_id.clone(),
            node_name: j.node_name.clone(),
            task_description: j.task_description.clone(),
            status: Some(j.status.to_string()),
            model: j.model.as_ref().map(|m| m.to_string()),
            branch: j.branch.clone(),
            created_at: Some(j.created_at),
            updated_at: Some(j.updated_at),
            started_at: j.started_at,
            completed_at: j.completed_at,
        }
    }
}

impl From<&crate::models::Run> for SyncRun {
    fn from(r: &crate::models::Run) -> Self {
        SyncRun {
            id: r.id.clone(),
            job_id: r.job_id.clone(),
            issue_id: r.issue_id.clone(),
            status: Some(r.status.to_string()),
            exit_reason: r.exit_reason.clone(),
            error_message: r.error_message.clone(),
            started_at: r.started_at,
            exited_at: r.exited_at,
            created_at: Some(r.created_at),
        }
    }
}

impl From<&crate::models::Event> for SyncEvent {
    fn from(e: &crate::models::Event) -> Self {
        SyncEvent {
            id: e.id.clone(),
            run_id: e.run_id.clone(),
            session_id: e.session_id.clone(),
            sequence: Some(e.sequence),
            event_type: e.event_type.clone(),
            data: Some(e.data.clone()),
            input_tokens: e.input_tokens,
            output_tokens: e.output_tokens,
            cache_read_tokens: e.cache_read_tokens,
            cache_create_tokens: e.cache_create_tokens,
            thinking_tokens: e.thinking_tokens,
            created_at: Some(e.created_at),
            turn_id: e.turn_id.clone(),
        }
    }
}

impl From<&crate::models::Artifact> for SyncArtifact {
    fn from(a: &crate::models::Artifact) -> Self {
        SyncArtifact {
            id: a.id.clone(),
            job_id: a.job_id.clone(),
            data: serde_json::to_string(&a.data).ok(),
            version: Some(a.version),
            updated_at: Some(a.updated_at),
        }
    }
}

impl From<&crate::models::Comment> for SyncComment {
    fn from(c: &crate::models::Comment) -> Self {
        SyncComment {
            id: c.id.clone(),
            issue_id: c.issue_id.clone(),
            content: c.content.clone(),
            source: Some(c.source.to_string()),
            created_at: Some(c.created_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_durable_for_entity_messages() {
        let project = SyncMessage::Project(SyncProject {
            id: "p1".into(),
            key: "P".into(),
            name: "Test".into(),
            path: None,
            created_at: None,
            updated_at: None,
        });
        assert!(project.is_durable());

        let issue = SyncMessage::Issue(SyncIssue {
            id: "i1".into(),
            project_id: "p1".into(),
            number: 1,
            title: "T".into(),
            description: None,
            status: "backlog".into(),
            priority: 0,
            model: None,
            created_at: None,
            updated_at: None,
            completed_at: None,
            merged_at: None,
            closed_at: None,
            depends_on: Vec::new(),
            parent_issue_id: None,
        });
        assert!(issue.is_durable());

        let delete = SyncMessage::Delete {
            table: "issues".into(),
            id: "i1".into(),
        };
        assert!(delete.is_durable());
    }

    #[test]
    fn is_durable_false_for_event_and_stream_delta() {
        let event = SyncMessage::Event(SyncEvent {
            id: "e1".into(),
            run_id: "r1".into(),
            session_id: None,
            sequence: Some(1),
            event_type: "assistant".into(),
            data: Some("{}".into()),
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            thinking_tokens: None,
            created_at: Some(1),
            turn_id: None,
        });
        assert!(!event.is_durable());

        let delta = SyncMessage::StreamDelta(StreamDelta {
            run_id: "r1".into(),
            event_id: "e1".into(),
            tokens: "hello".into(),
        });
        assert!(!delta.is_durable());
    }

    #[test]
    fn from_project_maps_fields() {
        let project = crate::models::Project {
            id: "p1".into(),
            workspace_id: "ws".into(),
            name: "My Project".into(),
            key: "MP".into(),
            repo_path: "/path/to/repo".into(),
            context: "".into(),
            docs_enabled: true,
            default_branch: "main".into(),
            next_issue_number: 5,
            setup_commands: None,
            terminal_commands: None,
            worktree_populate: None,
            created_at: 1000,
            updated_at: 2000,
            remote_url: None,
            server_id: None,
            hidden: false,
            is_workspace: false,
        };

        let sync = SyncProject::from(&project);
        assert_eq!(sync.id, "p1");
        assert_eq!(sync.key, "MP");
        assert_eq!(sync.name, "My Project");
        assert_eq!(sync.path, Some("/path/to/repo".into()));
        assert_eq!(sync.created_at, Some(1000));
        assert_eq!(sync.updated_at, Some(2000));
    }

    #[test]
    fn from_issue_handles_empty_and_nonempty_skills() {
        use crate::models::{IssueAttention, IssueProgress, IssueStatus};

        let issue = crate::models::Issue {
            id: "i1".into(),
            project_id: "p1".into(),
            number: 42,
            title: "Test Issue".into(),
            description: "Desc".into(),
            status: IssueStatus::Active,
            progress: IssueProgress::Active,
            attention: IssueAttention::None,
            priority: 2,
            completed_at: None,
            dismissed_at: None,
            created_at: 1000,
            updated_at: 2000,
            backend_override: None,
            merged_at: None,
            closed_at: None,
            parent_issue_id: None,
            unmet_dependency_count: 0,
            depends_on: Vec::new(),
            unmet_depends_on: Vec::new(),
            labels: Vec::new(),
        };

        let sync = SyncIssue::from(&issue);
        assert_eq!(sync.id, "i1");
        assert_eq!(sync.number, 42);
        assert_eq!(sync.status, "active");
    }

    #[test]
    fn from_comment_maps_source() {
        use crate::models::{Comment, CommentSource};

        let comment = Comment {
            id: "c1".into(),
            issue_id: "i1".into(),
            content: "Hello".into(),
            source: CommentSource::Agent,
            created_at: 5000,
            seq: 1,
        };

        let sync = SyncComment::from(&comment);
        assert_eq!(sync.id, "c1");
        assert_eq!(sync.content, "Hello");
        assert_eq!(sync.source, Some("agent".into()));
    }

    #[test]
    fn sync_message_serialization_uses_tag() {
        let msg = SyncMessage::Project(SyncProject {
            id: "p1".into(),
            key: "P".into(),
            name: "Test".into(),
            path: None,
            created_at: None,
            updated_at: None,
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"table\":\"Project\""));

        let delta = SyncMessage::StreamDelta(StreamDelta {
            run_id: "r1".into(),
            event_id: "e1".into(),
            tokens: "hi".into(),
        });
        let json = serde_json::to_string(&delta).unwrap();
        assert!(json.contains("\"table\":\"StreamDelta\""));
    }
}
