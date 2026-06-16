//! Shared payload types for MCP callback requests.
//!
//! These types are deserialized from the JSON payloads sent by the MCP server
//! binary to the callback HTTP server.

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub offset: Option<i64>,
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

/// Payload for update_issue tool
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIssuePayload {
    pub issue_number: String,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    /// Project key (e.g., "CAIRN"). Falls back to issue_number prefix or CWD.
    pub project: Option<String>,
}

/// Supported operations for the canonical change carrier.
///
/// Defined once in `cairn-common` so the contract table and this dispatcher
/// share a single enum; re-exported here for the existing call sites.
pub use cairn_common::contract::ChangeMode;

/// A single change item in a change batch.
///
/// Every item — file and resource targets alike — is `{target, mode, payload}`.
/// File-target keys (`content` for create/replace/append; `diff`, `patch`,
/// `old_string`/`new_string`, `replace_all` for patch variants) live inside
/// `payload`, exactly where resource-target keys already do.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeItem {
    pub target: String,
    pub mode: ChangeMode,
    /// Structured payload carrying this item's keys (file and resource targets alike).
    #[serde(default)]
    pub payload: Option<Value>,
}

/// Payload for the canonical change tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePayload {
    pub changes: Vec<ChangeItem>,
    #[serde(default)]
    pub commit_msg: Option<String>,
    #[serde(default)]
    pub preview: Option<bool>,
    #[serde(default)]
    pub atomic: Option<bool>,
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
    /// Required display title for the task: a short "what this task is" string.
    /// The tasks-collection contract lists it as required, so a missing key is
    /// rejected at deserialization (mirroring `subagentType`/`prompt`).
    pub description: String,
    pub prompt: String,
    pub subagent_type: String,
    #[serde(default, alias = "model")]
    pub tier: Option<String>,
    #[serde(default, rename = "backend", alias = "backendPreference")]
    pub backend_preference: Option<String>,
    /// Fire-and-forget when true: spawn and return the task URI without blocking.
    /// Accepts `runInBackground` (verb schema) or `background` (change-append payload).
    #[serde(default, alias = "background")]
    pub run_in_background: Option<bool>,
    #[serde(default)]
    pub session: Option<TaskSessionMode>,
    /// Task index for batch_tasks ordering (0, 1, 2...)
    #[serde(default)]
    pub task_index: Option<i32>,
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
    fn change_payload_deserializes_file_and_resource_targets() {
        let json = serde_json::json!({
            "changes": [
                {"target": "file:src/lib.rs", "mode": "create", "payload": {"content": "fn main() {}"}},
                {"target": "cairn://p/CAIRN/1", "mode": "patch", "payload": {"title": "Updated"}},
                {"target": "cairn://p/CAIRN/messages", "mode": "append", "payload": {"content": "hello"}}
            ],
            "commit_msg": "test commit"
        });

        let payload: ChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.changes.len(), 3);
        assert_eq!(payload.changes[0].target, "file:src/lib.rs");
        assert_eq!(payload.changes[0].mode, ChangeMode::Create);
        assert_eq!(
            payload.changes[0]
                .payload
                .as_ref()
                .and_then(|p| p.get("content"))
                .and_then(|v| v.as_str()),
            Some("fn main() {}")
        );
        assert_eq!(payload.changes[1].mode, ChangeMode::Patch);
        assert_eq!(payload.changes[2].mode, ChangeMode::Append);
        assert_eq!(payload.commit_msg.as_deref(), Some("test commit"));
    }

    #[test]
    fn change_payload_missing_optional_fields() {
        let json = serde_json::json!({
            "changes": [{"target": "file:f.rs", "mode": "delete"}]
        });

        let payload: ChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.changes[0].target, "file:f.rs");
        assert!(payload.changes[0].payload.is_none());
        assert_eq!(payload.commit_msg, None);
    }

    #[test]
    fn change_payload_patch_keys_live_in_payload() {
        // File-target keys now ride under `payload`, the same slot resource keys use.
        let json = serde_json::json!({
            "changes": [{
                "target": "file:src/lib.rs",
                "mode": "patch",
                "payload": {"old_string": "let x = 1;", "new_string": "let x = 2;"}
            }],
            "commit_msg": "fix"
        });

        let payload: ChangePayload = serde_json::from_value(json).unwrap();
        let change_payload = payload.changes[0].payload.as_ref().unwrap();
        assert_eq!(
            change_payload.get("old_string").and_then(|v| v.as_str()),
            Some("let x = 1;")
        );
        assert_eq!(
            change_payload.get("new_string").and_then(|v| v.as_str()),
            Some("let x = 2;")
        );
        assert!(change_payload.get("replace_all").is_none());
        assert!(change_payload.get("diff").is_none());
    }

    #[test]
    fn change_payload_replace_all_in_payload() {
        let json = serde_json::json!({
            "changes": [{
                "target": "file:src/lib.rs",
                "mode": "patch",
                "payload": {"old_string": "TODO", "new_string": "DONE", "replace_all": true}
            }],
            "commit_msg": "replace all TODOs"
        });

        let payload: ChangePayload = serde_json::from_value(json).unwrap();
        assert_eq!(
            payload.changes[0]
                .payload
                .as_ref()
                .and_then(|p| p.get("replace_all"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn task_payload_description_is_required() {
        // The tasks-collection contract lists `description` as required, so the
        // deserializer must reject a payload that omits it.
        let missing = serde_json::json!({
            "subagentType": "Explore",
            "prompt": "map parser flow"
        });
        assert!(serde_json::from_value::<TaskPayload>(missing).is_err());

        // A payload that provides it deserializes cleanly.
        let json = serde_json::json!({
            "subagentType": "Explore",
            "description": "map parser flow",
            "prompt": "trace the parser end to end"
        });
        let task: TaskPayload = serde_json::from_value(json).unwrap();
        assert_eq!(task.subagent_type, "Explore");
        assert_eq!(task.description, "map parser flow");
        assert_eq!(task.prompt, "trace the parser end to end");
    }

    #[test]
    fn change_payload_rejects_unknown_mode() {
        let json = serde_json::json!({
            "changes": [{"target": "file:src/lib.rs", "mode": "mutate"}]
        });

        let error = serde_json::from_value::<ChangePayload>(json).unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }
}
