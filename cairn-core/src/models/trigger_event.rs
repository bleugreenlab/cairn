//! Event payload types for event-driven recipe triggers.
//!
//! These types represent the data passed to triggered recipes when events fire.
//! They are serialized as JSON and stored in `TriggerContext.event_payload`.

use serde::{Deserialize, Serialize};

/// Event sent through the trigger dispatch channel.
///
/// Carries minimal data from the emission site; the dispatcher enriches
/// into full typed events (`JobEndedEvent` / `SkillCalledEvent`) before dispatch.
#[derive(Debug, Clone)]
pub enum TriggerEvent {
    JobEnded {
        job_id: String,
        status: String, // "complete" or "failed"
        execution_id: Option<String>,
        issue_id: Option<String>,
        project_id: String,
    },
    SkillCalled {
        skill_id: String,
        skill_name: String,
        run_id: String,
        job_id: String,
        execution_id: Option<String>,
        issue_id: Option<String>,
        project_id: String,
        // Fields from RunContext, used to build the full SkillCalledEvent
        project_key: String,
        issue_number: Option<i32>,
        exec_seq: Option<i32>,
        node_name: Option<String>,
    },
}

/// Payload for a JobEnded event.
///
/// Emitted when a top-level job completes or fails, allowing event-triggered
/// recipes to react to execution outcomes.
///
/// Uses human-readable identifiers (project key, issue number, execution seq)
/// instead of UUIDs so downstream agents can reference them directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobEndedEvent {
    /// Stable event identity for deduplication (= job_id)
    pub event_id: String,
    /// The job that completed — used for trigger source tracking
    pub source_job_id: String,
    /// Project key (e.g., "CAIRN")
    pub project_key: String,
    /// Issue number (e.g., 123) — None for project-level jobs
    pub issue_number: Option<i32>,
    /// Execution sequence within the issue (e.g., 1, 2, 3)
    pub execution_seq: Option<i32>,
    /// Agent config ID (e.g., "build", "planner")
    pub agent_config_id: Option<String>,
    /// Node name from the recipe (e.g., "Builder", "Planner")
    pub node_name: Option<String>,
    /// "complete" or "failed"
    pub status: String,
    pub completed_at: i64,
    /// cairn://PROJECT/ISSUE/EXEC/NODE/chat — for fetching transcript
    pub transcript_uri: Option<String>,
}

/// Payload for a SkillCalled event.
///
/// Emitted when an agent retrieves a skill, allowing event-triggered
/// recipes to react to skill usage.
///
/// Uses human-readable identifiers instead of UUIDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillCalledEvent {
    /// Stable event identity for deduplication (= "{run_id}:{skill_id}")
    pub event_id: String,
    /// The job that called the skill — used for trigger source tracking
    pub source_job_id: String,
    pub skill_id: String,
    pub skill_name: String,
    /// Project key (e.g., "CAIRN")
    pub project_key: String,
    /// Issue number — None for project-level runs
    pub issue_number: Option<i32>,
    /// Execution sequence within the issue
    pub execution_seq: Option<i32>,
    /// Node name (e.g., "Builder")
    pub node_name: Option<String>,
    /// cairn://PROJECT/ISSUE/EXEC/NODE/chat — for fetching transcript
    pub transcript_uri: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_ended_event_roundtrip() {
        let event = JobEndedEvent {
            event_id: "job-abc-123".to_string(),
            source_job_id: "job-abc-123".to_string(),
            project_key: "CAIRN".to_string(),
            issue_number: Some(42),
            execution_seq: Some(1),
            agent_config_id: Some("build".to_string()),
            node_name: Some("Builder".to_string()),
            status: "complete".to_string(),
            completed_at: 1700000000,
            transcript_uri: Some("cairn://CAIRN/42/1/Builder/chat".to_string()),
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: JobEndedEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.event_id, "job-abc-123");
        assert_eq!(parsed.project_key, "CAIRN");
        assert_eq!(parsed.issue_number, Some(42));
        assert_eq!(parsed.status, "complete");
        assert_eq!(
            parsed.transcript_uri,
            Some("cairn://CAIRN/42/1/Builder/chat".to_string())
        );
    }

    #[test]
    fn skill_called_event_roundtrip() {
        let event = SkillCalledEvent {
            event_id: "run-abc:code-review".to_string(),
            source_job_id: "job-xyz".to_string(),
            skill_id: "code-review".to_string(),
            skill_name: "Code Review".to_string(),
            project_key: "CAIRN".to_string(),
            issue_number: Some(42),
            execution_seq: Some(1),
            node_name: Some("Builder".to_string()),
            transcript_uri: Some("cairn://CAIRN/42/1/Builder/chat".to_string()),
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: SkillCalledEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.event_id, "run-abc:code-review");
        assert_eq!(parsed.skill_id, "code-review");
        assert_eq!(parsed.project_key, "CAIRN");
    }

    #[test]
    fn job_ended_event_project_level() {
        let event = JobEndedEvent {
            event_id: "job-xyz-789".to_string(),
            source_job_id: "job-xyz-789".to_string(),
            project_key: "CAIRN".to_string(),
            issue_number: None,
            execution_seq: None,
            agent_config_id: None,
            node_name: None,
            status: "failed".to_string(),
            completed_at: 1700000000,
            transcript_uri: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        let parsed: JobEndedEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.status, "failed");
        assert!(parsed.issue_number.is_none());
    }
}
