//! Run, event, and prompt types.

use serde::{Deserialize, Serialize};

/// A run - a single execution attempt within a job or chat
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub job_id: Option<String>,
    pub chat_id: Option<String>,
    pub status: RunStatus,
    pub claude_session_id: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub todos: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunStatus::Pending => write!(f, "pending"),
            RunStatus::Running => write!(f, "running"),
            RunStatus::Paused => write!(f, "paused"),
            RunStatus::Completed => write!(f, "completed"),
            RunStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for RunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(RunStatus::Pending),
            "running" => Ok(RunStatus::Running),
            "paused" => Ok(RunStatus::Paused),
            "completed" => Ok(RunStatus::Completed),
            "failed" => Ok(RunStatus::Failed),
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

impl From<crate::diesel_models::DbPermissionRequest> for PermissionRequest {
    fn from(db: crate::diesel_models::DbPermissionRequest) -> Self {
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

/// Todo item extracted from TodoWrite tool calls
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub content: String,
    pub status: String, // "pending" | "in_progress" | "completed"
    pub active_form: Option<String>,
}

/// Todos grouped by run, for showing accumulated todos across multiple runs
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunTodos {
    pub run_id: String,
    pub run_number: u32,
    pub todos: Vec<TodoItem>,
}
