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
    /// Project key (e.g., "CAIRN"). Falls back to issue_number prefix or CWD.
    pub project: Option<String>,
}

/// A single file change in an edit batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChange {
    pub path: String,
    pub kind: String,
    #[serde(default)]
    pub content: Option<String>,
    /// Unified diff for kind=update (alternative to old_string/new_string)
    #[serde(default)]
    pub diff: Option<String>,
    /// Text to find for kind=update (use with new_string; alternative to diff)
    #[serde(default)]
    pub old_string: Option<String>,
    /// Replacement text for kind=update (use with old_string)
    #[serde(default)]
    pub new_string: Option<String>,
    /// Replace all occurrences (default: false, first match only)
    #[serde(default)]
    pub replace_all: Option<bool>,
}

/// Payload for unified edit tool (file mutations)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChangePayload {
    pub changes: Vec<FileChange>,
    pub commit_msg: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TaskSessionMode {
    #[default]
    New,
    Fork,
}

/// Payload for task tool (matches native Task schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskPayload {
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    #[serde(default, alias = "model")]
    pub tier: Option<String>,
    #[serde(default, rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
    pub run_in_background: Option<bool>,
    #[serde(default)]
    pub session: Option<TaskSessionMode>,
    /// Task index for batch_tasks ordering (0, 1, 2...)
    #[serde(default)]
    pub task_index: Option<i32>,
}

/// Payload for todo_write tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoWritePayload {
    pub todos: Vec<crate::todos::TodoWriteItem>,
}

impl TaskPayload {
    pub fn session_mode(&self) -> Result<TaskSessionMode, String> {
        Ok(self.session.unwrap_or(TaskSessionMode::New))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filechange_payload_deserializes() {
        let json = serde_json::json!({
            "changes": [
                {"path": "src/lib.rs", "kind": "add", "content": "fn main() {}"},
                {"path": "src/old.rs", "kind": "delete"},
                {"path": "src/mod.rs", "kind": "update", "diff": "@@ -1,1 +1,1 @@\n-old\n+new"}
            ],
            "commit_msg": "test commit"
        });

        let payload: FileChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.changes.len(), 3);
        assert_eq!(payload.changes[0].kind, "add");
        assert_eq!(payload.changes[0].content.as_deref(), Some("fn main() {}"));
        assert_eq!(payload.changes[1].kind, "delete");
        assert!(payload.changes[1].content.is_none());
        assert_eq!(payload.changes[2].kind, "update");
        assert!(payload.changes[2].diff.is_some());
        assert_eq!(payload.commit_msg, "test commit");
    }

    #[test]
    fn filechange_payload_missing_optional_fields() {
        let json = serde_json::json!({
            "changes": [{"path": "f.rs", "kind": "delete"}],
            "commit_msg": "^"
        });

        let payload: FileChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.changes[0].path, "f.rs");
        assert!(payload.changes[0].content.is_none());
        assert!(payload.changes[0].diff.is_none());
        assert_eq!(payload.commit_msg, "^");
    }

    #[test]
    fn filechange_update_with_old_new_string() {
        let json = serde_json::json!({
            "changes": [{
                "path": "src/lib.rs",
                "kind": "update",
                "old_string": "let x = 1;",
                "new_string": "let x = 2;"
            }],
            "commit_msg": "fix"
        });

        let payload: FileChangePayload = serde_json::from_value(json).unwrap();
        let change = &payload.changes[0];
        assert_eq!(change.old_string.as_deref(), Some("let x = 1;"));
        assert_eq!(change.new_string.as_deref(), Some("let x = 2;"));
        assert_eq!(change.replace_all, None);
        assert!(change.diff.is_none());
    }

    #[test]
    fn filechange_replace_all_true() {
        let json = serde_json::json!({
            "changes": [{
                "path": "src/lib.rs",
                "kind": "update",
                "old_string": "TODO",
                "new_string": "DONE",
                "replace_all": true
            }],
            "commit_msg": "replace all TODOs"
        });

        let payload: FileChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.changes[0].replace_all, Some(true));
    }

    #[test]
    fn filechange_replace_all_false() {
        let json = serde_json::json!({
            "changes": [{
                "path": "src/lib.rs",
                "kind": "update",
                "old_string": "TODO",
                "new_string": "DONE",
                "replace_all": false
            }],
            "commit_msg": "replace first TODO"
        });

        let payload: FileChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.changes[0].replace_all, Some(false));
    }

    #[test]
    fn task_payload_defaults_session_mode_to_new() {
        let payload: TaskPayload = serde_json::from_value(serde_json::json!({
            "description": "Explore",
            "prompt": "Inspect the code",
            "subagentType": "Explore"
        }))
        .unwrap();

        assert_eq!(payload.session_mode().unwrap(), TaskSessionMode::New);
    }

    #[test]
    fn task_payload_supports_fork_session_mode() {
        let payload: TaskPayload = serde_json::from_value(serde_json::json!({
            "description": "Explore",
            "prompt": "Inspect the code",
            "subagentType": "Explore",
            "session": "fork"
        }))
        .unwrap();

        assert_eq!(payload.session_mode().unwrap(), TaskSessionMode::Fork);
    }

    #[test]
    fn filechange_replace_all_defaults_to_none() {
        let json = serde_json::json!({
            "changes": [{
                "path": "src/lib.rs",
                "kind": "update",
                "old_string": "a",
                "new_string": "b"
            }],
            "commit_msg": "edit"
        });

        let payload: FileChangePayload = serde_json::from_value(json).unwrap();
        // When omitted, replace_all should be None (defaults to first-match-only via unwrap_or(false))
        assert_eq!(payload.changes[0].replace_all, None);
    }
}
