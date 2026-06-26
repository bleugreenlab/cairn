//! Run, event, and prompt types.

use serde::{Deserialize, Serialize};

/// A run — one process attachment lifetime.
///
/// Created when a process spawns, finalized when it exits.
/// A warm process that serves multiple turns is one Run with multiple Turns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub job_id: Option<String>,
    pub chat_id: Option<String>,
    pub status: RunStatus,
    pub session_id: Option<String>,
    pub exit_reason: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i64>,
    pub exited_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub start_mode: Option<RunStartMode>,
}

/// How the run was started — fresh session or resuming existing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RunStartMode {
    /// Started with --session-id (new conversation)
    Fresh,
    /// Started with --resume (continuing existing conversation)
    Resume,
    /// Started by forking an existing session into a new child session
    Fork,
}

impl std::fmt::Display for RunStartMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunStartMode::Fresh => write!(f, "fresh"),
            RunStartMode::Resume => write!(f, "resume"),
            RunStartMode::Fork => write!(f, "fork"),
        }
    }
}

impl std::str::FromStr for RunStartMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fresh" => Ok(RunStartMode::Fresh),
            "resume" => Ok(RunStartMode::Resume),
            "fork" => Ok(RunStartMode::Fork),
            other => Err(format!("unknown start mode: {}", other)),
        }
    }
}

/// Run lifecycle status (stored).
///
/// Tracks the process lifecycle only — semantic work outcome is on Turn.
/// Transitions are validated by `transitions::transition_run`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// Process spawned, not yet producing events.
    Starting,
    /// Process connected and responsive.
    Live,
    /// Process exited cleanly.
    Exited,
    /// Process died unexpectedly.
    Crashed,
}

impl RunStatus {
    /// Whether the run is in a terminal state (process no longer alive).
    pub fn is_terminal(&self) -> bool {
        matches!(self, RunStatus::Exited | RunStatus::Crashed)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunStatus::Starting => write!(f, "starting"),
            RunStatus::Live => write!(f, "live"),
            RunStatus::Exited => write!(f, "exited"),
            RunStatus::Crashed => write!(f, "crashed"),
        }
    }
}

impl std::str::FromStr for RunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "starting" => Ok(RunStatus::Starting),
            "live" => Ok(RunStatus::Live),
            "exited" => Ok(RunStatus::Exited),
            "crashed" => Ok(RunStatus::Crashed),
            // Backwards compat for pre-migration data
            "pending" => Ok(RunStatus::Starting),
            "running" | "idle" => Ok(RunStatus::Live),
            "complete" | "completed" => Ok(RunStatus::Exited),
            "failed" => Ok(RunStatus::Crashed),
            "paused" => Ok(RunStatus::Live),
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

/// An event in a run's transcript
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub sequence: i32,
    pub timestamp: i64,
    pub event_type: String,
    pub data: String,
    pub parent_tool_use_id: Option<String>,
    pub created_at: i64,
    pub input_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub turn_id: Option<String>,
    pub thinking_tokens: Option<i32>,
    /// Metered per-event cost in USD. Written only on a metered backend's
    /// `result:success` event (OpenRouter); `None` for subscription backends
    /// (Claude/Codex) and for streamed assistant messages, where cost lands on
    /// the result event. Read-only here — surfaced in the event detail; the
    /// write path lives in the backend runtime.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub cost_usd: Option<f64>,
    /// Archival storage mode (CAIRN-1538): `full` (or absent) for a live inline
    /// event, `gitcoord`/`zstd` for one rewritten at worktree teardown. These
    /// coordinate fields drive `archival::reconstruct_events` and are skipped
    /// from serialization — consumers only ever see reconstructed `data`.
    #[serde(skip, default)]
    pub storage_mode: Option<String>,
    #[serde(skip, default)]
    pub content_commit: Option<String>,
    /// The stable jj change-id of the commit `content_commit` was forward-mapped
    /// to at teardown (CAIRN-1964). Durable provenance for the git coordinate;
    /// never consumed by reconstruction. `None` for plain-git worktrees and for
    /// non-git-addressed shapes.
    #[serde(skip, default)]
    pub content_change_id: Option<String>,
    #[serde(skip, default)]
    pub content_render_sha: Option<String>,
    #[serde(skip, default)]
    pub data_blob: Option<Vec<u8>>,
    #[serde(skip, default)]
    pub codec: Option<String>,
    /// Per-target read token counts (CAIRN-1593). Populated only by the lean
    /// read projection on frontend-facing event loads, where the read body is
    /// stripped from `data` and replaced by these counts. `None` everywhere
    /// else (skyline path, plain row loads).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub read_segments: Option<Vec<crate::runs::read_tokens::ReadSegmentTokens>>,
}

/// A prompt awaiting user response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Prompt {
    pub id: String,
    pub run_id: String,
    pub questions: String,
    pub response: Option<String>,
    pub created_at: i64,
    pub answered_at: Option<i64>,
}

/// A permission request awaiting user approval
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequest {
    pub id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub tool_input: serde_json::Value,
    pub status: PermissionStatus,
    pub response: Option<serde_json::Value>,
    pub created_at: i64,
    pub responded_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PermissionStatus {
    Pending,
    Allowed,
    Denied,
}

impl std::fmt::Display for PermissionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PermissionStatus::Pending => write!(f, "pending"),
            PermissionStatus::Allowed => write!(f, "allowed"),
            PermissionStatus::Denied => write!(f, "denied"),
        }
    }
}

impl std::str::FromStr for PermissionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(PermissionStatus::Pending),
            "allowed" => Ok(PermissionStatus::Allowed),
            "denied" => Ok(PermissionStatus::Denied),
            _ => Err(format!("Unknown permission status: {}", s)),
        }
    }
}

impl From<crate::db_records::DbPermissionRequest> for PermissionRequest {
    fn from(db: crate::db_records::DbPermissionRequest) -> Self {
        PermissionRequest {
            id: db.id,
            run_id: db.run_id,
            tool_use_id: db.tool_use_id,
            tool_name: db.tool_name,
            tool_input: serde_json::from_str(&db.tool_input).unwrap_or(serde_json::Value::Null),
            status: db.status.parse().unwrap_or(PermissionStatus::Pending),
            response: db.response.and_then(|r| serde_json::from_str(&r).ok()),
            created_at: db.created_at as i64,
            responded_at: db.responded_at.map(|t| t as i64),
        }
    }
}

/// CI run status for local CI checks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum CiStatus {
    #[default]
    None,
    Pending,
    Passed,
    Failed,
}

impl std::fmt::Display for CiStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CiStatus::None => write!(f, "none"),
            CiStatus::Pending => write!(f, "pending"),
            CiStatus::Passed => write!(f, "passed"),
            CiStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for CiStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(CiStatus::None),
            "pending" => Ok(CiStatus::Pending),
            "passed" => Ok(CiStatus::Passed),
            "failed" => Ok(CiStatus::Failed),
            _ => Err(format!("Unknown CI status: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_terminal() {
        assert!(!RunStatus::Starting.is_terminal());
        assert!(!RunStatus::Live.is_terminal());
        assert!(RunStatus::Exited.is_terminal());
        assert!(RunStatus::Crashed.is_terminal());
    }

    #[test]
    fn test_display() {
        assert_eq!(RunStatus::Starting.to_string(), "starting");
        assert_eq!(RunStatus::Live.to_string(), "live");
        assert_eq!(RunStatus::Exited.to_string(), "exited");
        assert_eq!(RunStatus::Crashed.to_string(), "crashed");
    }

    #[test]
    fn test_from_str_canonical() {
        assert_eq!(
            "starting".parse::<RunStatus>().unwrap(),
            RunStatus::Starting
        );
        assert_eq!("live".parse::<RunStatus>().unwrap(), RunStatus::Live);
        assert_eq!("exited".parse::<RunStatus>().unwrap(), RunStatus::Exited);
        assert_eq!("crashed".parse::<RunStatus>().unwrap(), RunStatus::Crashed);
    }

    #[test]
    fn test_from_str_case_insensitive() {
        assert_eq!(
            "STARTING".parse::<RunStatus>().unwrap(),
            RunStatus::Starting
        );
        assert_eq!("Live".parse::<RunStatus>().unwrap(), RunStatus::Live);
    }

    #[test]
    fn test_from_str_backwards_compat() {
        // pending → Starting
        assert_eq!("pending".parse::<RunStatus>().unwrap(), RunStatus::Starting);
        // running, idle → Live
        assert_eq!("running".parse::<RunStatus>().unwrap(), RunStatus::Live);
        assert_eq!("idle".parse::<RunStatus>().unwrap(), RunStatus::Live);
        // complete, completed → Exited
        assert_eq!("complete".parse::<RunStatus>().unwrap(), RunStatus::Exited);
        assert_eq!("completed".parse::<RunStatus>().unwrap(), RunStatus::Exited);
        // failed → Crashed
        assert_eq!("failed".parse::<RunStatus>().unwrap(), RunStatus::Crashed);
        // paused → Live
        assert_eq!("paused".parse::<RunStatus>().unwrap(), RunStatus::Live);
    }

    #[test]
    fn test_from_str_unknown_returns_error() {
        let result = "bogus".parse::<RunStatus>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown status"));
    }

    #[test]
    fn test_display_roundtrip() {
        for status in [
            RunStatus::Starting,
            RunStatus::Live,
            RunStatus::Exited,
            RunStatus::Crashed,
        ] {
            let s = status.to_string();
            let parsed: RunStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_run_start_mode_display() {
        assert_eq!(RunStartMode::Fresh.to_string(), "fresh");
        assert_eq!(RunStartMode::Resume.to_string(), "resume");
        assert_eq!(RunStartMode::Fork.to_string(), "fork");
    }

    #[test]
    fn test_run_start_mode_from_str() {
        assert_eq!(
            "fresh".parse::<RunStartMode>().unwrap(),
            RunStartMode::Fresh
        );
        assert_eq!(
            "resume".parse::<RunStartMode>().unwrap(),
            RunStartMode::Resume
        );
        assert_eq!("fork".parse::<RunStartMode>().unwrap(), RunStartMode::Fork);
    }

    #[test]
    fn test_run_start_mode_from_str_unknown() {
        let result = "warm".parse::<RunStartMode>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown start mode"));
    }

    #[test]
    fn test_run_start_mode_roundtrip() {
        for mode in [
            RunStartMode::Fresh,
            RunStartMode::Resume,
            RunStartMode::Fork,
        ] {
            let s = mode.to_string();
            let parsed: RunStartMode = s.parse().unwrap();
            assert_eq!(parsed, mode);
        }
    }
}

/// Todo item stored in the todos table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub id: String,
    pub content: String,
    pub status: String, // "pending" | "in_progress" | "completed"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>, // "high" | "medium" | "low"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_form: Option<String>,
}

impl From<crate::db_records::DbTodo> for TodoItem {
    fn from(db: crate::db_records::DbTodo) -> Self {
        TodoItem {
            id: db.todo_id,
            content: db.content,
            status: db.status,
            priority: db.priority,
            active_form: db.active_form,
        }
    }
}

/// Todos grouped by run, for showing accumulated todos across multiple runs
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunTodos {
    pub run_id: String,
    pub run_number: u32,
    pub todos: Vec<TodoItem>,
}
