//! Shared payload types for MCP callback requests.
//!
//! These types are deserialized from the JSON payloads sent by the MCP server
//! binary to the callback HTTP server.

use serde::{Deserialize, Serialize};

// ============================================================================
// Request/Response Types (re-exported from cairn-common)
// ============================================================================

pub type McpCallbackRequest = cairn_common::protocol::CallbackRequest;
pub type McpCallbackResponse = cairn_common::protocol::CallbackResponse;

// ============================================================================
// Payload Types
// ============================================================================

/// A single option for a question
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

/// A single question to ask the user
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Question {
    pub question: String,
    pub header: Option<String>,
    pub options: Vec<QuestionOption>,
    pub multi_select: bool,
}

/// Payload for ask_user tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AskUserPayload {
    pub questions: Vec<Question>,
}

/// Payload for add_comment tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddCommentPayload {
    pub content: String,
}

/// Payload for write tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFilePayload {
    pub file_path: String,
    pub content: String,
    pub commit_msg: String,
}

/// Payload for edit tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditFilePayload {
    pub file_path: String,
    pub old_string: String,
    pub new_string: String,
    #[serde(default)]
    pub replace_all: bool,
    pub commit_msg: String,
}

/// Payload for read tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadFilePayload {
    pub path: String,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    /// Include issue history for this file path
    #[serde(default)]
    pub issue_history: Option<IssueHistoryMode>,
}

/// Mode for issue history output
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IssueHistoryMode {
    /// Brief history with issue numbers and dates
    #[default]
    Minimal,
    /// Detailed history with PR links and change stats
    Verbose,
}

/// Payload for create_issue tool
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIssuePayload {
    pub title: String,
    pub description: Option<String>,
    pub skills: Option<Vec<String>>,
    /// Project key to create the issue in (e.g., "CAIRN"). Defaults to current project.
    pub project: Option<String>,
}

/// Payload for update_issue tool
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIssuePayload {
    pub issue_number: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub skills: Option<Vec<String>>,
}

/// Payload for task tool (matches native Task schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskPayload {
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    pub model: Option<String>,
    pub run_in_background: Option<bool>,
    pub resume: Option<String>,
    /// Task index for batch_tasks ordering (0, 1, 2...)
    #[serde(default)]
    pub task_index: Option<i32>,
}

// ============================================================================
// Event Types
// ============================================================================

/// Event emitted to frontend when user prompt is needed
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct PromptUserEvent {
    pub run_id: String,
    pub session_id: Option<String>,
    pub questions: Vec<Question>,
}
