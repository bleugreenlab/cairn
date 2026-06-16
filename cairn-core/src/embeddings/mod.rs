//! Embedding storage, vibe coloring, and the async gateway worker.
//!
//! Embeddings are produced by the cloud `/embed` gateway (Bedrock Cohere v4),
//! not locally. Assistant event text is embedded at store time by an async
//! worker, used to assign a vibe color (persisted in `event_vibes`), then
//! discarded. Corpus resources (issues, skills, memories, artifacts) persist
//! their vectors in `resource_embeddings` for in-engine recall.

pub mod client;
pub mod position;
pub mod queries;
pub mod resource_text;
pub mod vector;
pub mod vibes;
mod worker;

pub use client::{EmbeddingClient, InputType, TokenProvider, COHERE_DIMS, COHERE_MODEL};
pub use position::{PositionConfig, PositionKind, PositionMeta};
pub use resource_text::artifact_embed_text;
pub use vibes::VibeState;
pub use worker::{spawn_embed_worker, EmbedJob};

/// Extract embeddable text from a TranscriptEvent JSON string.
/// Combines content and thinking fields, separated by newline.
pub fn extract_embeddable_text(data_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(data_json).ok()?;
    let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
    let thinking = value.get("thinking").and_then(|v| v.as_str()).unwrap_or("");

    match (content.is_empty(), thinking.is_empty()) {
        (false, false) => Some(format!("{}\n{}", content, thinking)),
        (false, true) => Some(content.to_string()),
        (true, false) => Some(thinking.to_string()),
        (true, true) => None,
    }
}

/// Extract the structural change signal from a TranscriptEvent JSON string:
/// the `commit_msg` plus each touched `target` (paths/URIs only — never file
/// contents) across every `write` tool-use in the event. Returns `None` when
/// the event has no `write` tool-use or yields no signal text.
pub fn extract_change_signal_text(data_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(data_json).ok()?;
    let tool_uses = value.get("toolUses").and_then(|t| t.as_array())?;

    let mut parts: Vec<String> = Vec::new();
    for tool in tool_uses {
        if tool.get("name").and_then(|n| n.as_str()) != Some("change") {
            continue;
        }
        let Some(input) = tool.get("input") else {
            continue;
        };
        // The tool input may be a JSON object or a JSON-encoded string.
        let input_obj = if input.is_string() {
            match serde_json::from_str::<serde_json::Value>(input.as_str().unwrap_or("")) {
                Ok(v) => v,
                Err(_) => continue,
            }
        } else {
            input.clone()
        };

        if let Some(msg) = input_obj.get("commit_msg").and_then(|m| m.as_str()) {
            let msg = msg.trim();
            // "^" amends the previous commit and carries no new signal.
            if !msg.is_empty() && msg != "^" {
                parts.push(msg.to_string());
            }
        }
        if let Some(changes) = input_obj.get("changes").and_then(|c| c.as_array()) {
            for change in changes {
                if let Some(target) = change.get("target").and_then(|t| t.as_str()) {
                    if !target.is_empty() {
                        parts.push(target.to_string());
                    }
                }
            }
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_change_signal_pulls_commit_msg_and_targets() {
        let json = r#"{
            "toolUses": [
                {
                    "name": "change",
                    "input": {
                        "commit_msg": "Add validation",
                        "changes": [
                            {"target": "file:src/lib.rs", "mode": "patch"},
                            {"target": "file:src/main.rs", "mode": "create"}
                        ]
                    }
                }
            ]
        }"#;
        assert_eq!(
            extract_change_signal_text(json),
            Some("Add validation\nfile:src/lib.rs\nfile:src/main.rs".to_string())
        );
    }

    #[test]
    fn extract_change_signal_handles_stringified_input() {
        let json = r#"{
            "toolUses": [
                {
                    "name": "change",
                    "input": "{\"commit_msg\":\"Fix bug\",\"changes\":[{\"target\":\"file:a.rs\"}]}"
                }
            ]
        }"#;
        assert_eq!(
            extract_change_signal_text(json),
            Some("Fix bug\nfile:a.rs".to_string())
        );
    }

    #[test]
    fn extract_change_signal_skips_amend_commit_msg() {
        let json = r#"{
            "toolUses": [
                {"name": "change", "input": {"commit_msg": "^", "changes": [{"target": "file:x.rs"}]}}
            ]
        }"#;
        assert_eq!(
            extract_change_signal_text(json),
            Some("file:x.rs".to_string())
        );
    }

    #[test]
    fn extract_change_signal_none_for_non_change_tool() {
        let json = r#"{
            "toolUses": [
                {"name": "bash", "input": {"command": "ls"}}
            ]
        }"#;
        assert_eq!(extract_change_signal_text(json), None);
    }

    #[test]
    fn extract_change_signal_none_when_no_tool_uses() {
        let json = r#"{"content": "just talking", "thinking": ""}"#;
        assert_eq!(extract_change_signal_text(json), None);
    }

    #[test]
    fn extract_change_signal_none_for_invalid_json() {
        assert_eq!(extract_change_signal_text("not json"), None);
    }

    #[test]
    fn extract_content_only() {
        let json = r#"{"content": "Hello world", "thinking": ""}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("Hello world".to_string())
        );
    }

    #[test]
    fn extract_thinking_only() {
        let json = r#"{"content": "", "thinking": "Let me consider..."}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("Let me consider...".to_string())
        );
    }

    #[test]
    fn extract_both_content_and_thinking() {
        let json = r#"{"content": "The answer is 42", "thinking": "I need to calculate"}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("The answer is 42\nI need to calculate".to_string())
        );
    }

    #[test]
    fn extract_returns_none_when_both_empty() {
        let json = r#"{"content": "", "thinking": ""}"#;
        assert_eq!(extract_embeddable_text(json), None);
    }

    #[test]
    fn extract_returns_none_when_fields_missing() {
        let json = r#"{"tool_uses": []}"#;
        assert_eq!(extract_embeddable_text(json), None);
    }

    #[test]
    fn extract_returns_none_for_invalid_json() {
        assert_eq!(extract_embeddable_text("not json"), None);
    }

    #[test]
    fn extract_handles_missing_thinking_field() {
        let json = r#"{"content": "Just content"}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("Just content".to_string())
        );
    }

    #[test]
    fn extract_handles_null_fields() {
        let json = r#"{"content": null, "thinking": null}"#;
        assert_eq!(extract_embeddable_text(json), None);
    }
}
