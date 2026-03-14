//! Turn boundary detection for safe message injection
//!
//! Detects when Claude is at a "safe" turn boundary where we can stop the
//! session and inject a message without corrupting the conversation.
//!
//! Safe boundaries are:
//! - After an assistant message with no pending tool calls
//! - After all tool results have been received for pending tool calls
//! - After a result:success or result:failure event

use crate::claude::stream::TranscriptEvent;
use std::collections::HashSet;

/// Tracks conversation state to detect safe injection boundaries
#[derive(Debug, Default)]
pub struct TurnBoundaryChecker {
    /// Tool IDs from the most recent assistant message that haven't received results yet
    pending_tool_ids: HashSet<String>,
    /// Whether we just saw a definitive boundary (result event)
    at_definitive_boundary: bool,
}

impl TurnBoundaryChecker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update state based on an incoming event.
    /// Returns true if we're at a safe injection boundary after this event.
    pub fn update(&mut self, event: &TranscriptEvent) -> bool {
        // Result events are always definitive boundaries
        if event.event_type.starts_with("result:") {
            self.pending_tool_ids.clear();
            self.at_definitive_boundary = true;
            return true;
        }

        // System events don't affect boundary state
        if event.event_type.starts_with("system:") {
            return false;
        }

        self.at_definitive_boundary = false;

        match event.event_type.as_str() {
            "assistant" => {
                // Clear any previous pending tools
                self.pending_tool_ids.clear();

                // Check if this assistant message has tool uses
                if let Some(ref tool_uses) = event.tool_uses {
                    // Track all tool IDs as pending
                    for tool_use in tool_uses {
                        self.pending_tool_ids.insert(tool_use.id.clone());
                    }
                    // Not safe - tools are pending
                    false
                } else {
                    // Text-only assistant response - safe to inject after
                    true
                }
            }
            "tool_result" => {
                // Remove the completed tool from pending set
                if let Some(ref tool_use_id) = event.tool_use_id {
                    self.pending_tool_ids.remove(tool_use_id);
                }

                // Safe if all tools have received results
                self.pending_tool_ids.is_empty()
            }
            "user" => {
                // User events don't affect safety
                false
            }
            _ => false,
        }
    }

    /// Check if we're currently at a safe boundary (without updating state)
    #[cfg(test)]
    pub fn is_at_boundary(&self) -> bool {
        self.at_definitive_boundary || self.pending_tool_ids.is_empty()
    }

    /// Get the number of pending tool calls
    #[cfg(test)]
    pub fn pending_tool_count(&self) -> usize {
        self.pending_tool_ids.len()
    }

    /// Reset the checker state
    #[cfg(test)]
    pub fn reset(&mut self) {
        self.pending_tool_ids.clear();
        self.at_definitive_boundary = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claude::stream::ToolUseInfo;
    use serde_json::json;

    fn make_assistant_event(
        content: Option<&str>,
        tool_uses: Option<Vec<(&str, &str)>>,
    ) -> TranscriptEvent {
        let tool_uses = tool_uses.map(|uses| {
            uses.into_iter()
                .map(|(id, name)| ToolUseInfo {
                    id: id.to_string(),
                    name: name.to_string(),
                    input: json!({}),
                })
                .collect()
        });

        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("test-session".to_string()),
            parent_tool_use_id: None,
            content: content.map(|s| s.to_string()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            usage: None,
            raw: None,
        }
    }

    fn make_tool_result_event(tool_use_id: &str) -> TranscriptEvent {
        TranscriptEvent {
            event_type: "tool_result".to_string(),
            session_id: Some("test-session".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: Some(tool_use_id.to_string()),
            tool_result: Some("result".to_string()),
            is_error: false,
            usage: None,
            raw: None,
        }
    }

    fn make_result_event(success: bool) -> TranscriptEvent {
        TranscriptEvent {
            event_type: if success {
                "result:success"
            } else {
                "result:failure"
            }
            .to_string(),
            session_id: Some("test-session".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: !success,
            usage: None,
            raw: None,
        }
    }

    #[test]
    fn test_text_only_assistant_is_safe() {
        let mut checker = TurnBoundaryChecker::new();
        let event = make_assistant_event(Some("Hello!"), None);
        assert!(checker.update(&event));
        assert!(checker.is_at_boundary());
    }

    #[test]
    fn test_assistant_with_tool_not_safe() {
        let mut checker = TurnBoundaryChecker::new();
        let event = make_assistant_event(Some("Let me help"), Some(vec![("tool1", "read_file")]));
        assert!(!checker.update(&event));
        assert_eq!(checker.pending_tool_count(), 1);
    }

    #[test]
    fn test_tool_result_completes_boundary() {
        let mut checker = TurnBoundaryChecker::new();
        let assistant = make_assistant_event(None, Some(vec![("tool1", "read_file")]));
        assert!(!checker.update(&assistant));

        let result = make_tool_result_event("tool1");
        assert!(checker.update(&result));
    }

    #[test]
    fn test_multiple_tools_need_all_results() {
        let mut checker = TurnBoundaryChecker::new();
        let assistant = make_assistant_event(
            None,
            Some(vec![("tool1", "read_file"), ("tool2", "write_file")]),
        );
        assert!(!checker.update(&assistant));
        assert_eq!(checker.pending_tool_count(), 2);

        assert!(!checker.update(&make_tool_result_event("tool1")));
        assert!(checker.update(&make_tool_result_event("tool2")));
    }

    #[test]
    fn test_result_event_is_definitive_boundary() {
        let mut checker = TurnBoundaryChecker::new();
        let assistant = make_assistant_event(None, Some(vec![("tool1", "read_file")]));
        checker.update(&assistant);

        assert!(checker.update(&make_result_event(true)));
        assert_eq!(checker.pending_tool_count(), 0);
    }

    #[test]
    fn test_new_assistant_clears_pending() {
        let mut checker = TurnBoundaryChecker::new();
        let assistant1 = make_assistant_event(None, Some(vec![("tool1", "read_file")]));
        checker.update(&assistant1);
        assert_eq!(checker.pending_tool_count(), 1);

        let assistant2 = make_assistant_event(Some("Just text"), None);
        assert!(checker.update(&assistant2));
        assert_eq!(checker.pending_tool_count(), 0);
    }
}
