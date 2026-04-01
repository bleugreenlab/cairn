pub mod stream_store;

use serde_json::Value;

pub type TranscriptRow = (String, i32, String, String);

/// Format transcript rows into markdown without truncation.
///
/// Intended for reuse in places where we need a faithful text rendering of the
/// visible conversation, such as `cairn://.../chat` reads and resume fallback
/// prompt construction.
pub fn format_transcript_full(events: &[TranscriptRow]) -> String {
    let mut transcript = String::new();

    for (_run_id, _seq, event_type, data) in events {
        let event_data: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        match event_type.as_str() {
            "assistant" => {
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        transcript.push_str("**Assistant:** ");
                        transcript.push_str(content);
                        transcript.push_str("\n\n");
                    }
                }

                if let Some(tool_uses) = event_data.get("toolUses").and_then(|t| t.as_array()) {
                    for tool in tool_uses {
                        let name = tool
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("unknown");
                        let input = tool
                            .get("input")
                            .map(|i| {
                                if i.is_string() {
                                    i.as_str().unwrap_or("").to_string()
                                } else {
                                    serde_json::to_string_pretty(i).unwrap_or_default()
                                }
                            })
                            .unwrap_or_default();

                        transcript.push_str(&format!("**Tool Call ({}):**\n", name));
                        transcript.push_str(&input);
                        transcript.push_str("\n\n");
                    }
                }
            }
            "user" => {
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        transcript.push_str("**User:** ");
                        transcript.push_str(content);
                        transcript.push_str("\n\n");
                    }
                }
            }
            "result" | "tool_result" => {
                if let Some(result) = event_data.get("toolResult").and_then(|r| r.as_str()) {
                    let tool_name = event_data
                        .get("toolName")
                        .and_then(|t| t.as_str())
                        .unwrap_or("tool");
                    transcript.push_str(&format!("**Tool Result ({}):**\n", tool_name));
                    transcript.push_str(result);
                    transcript.push_str("\n\n");
                }
            }
            "system:compact_boundary" => {
                let provider = event_data
                    .get("raw")
                    .and_then(|raw| raw.get("provider"))
                    .and_then(|value| value.as_str());
                transcript.push_str("**System:** Context compacted");
                if let Some(provider) = provider {
                    transcript.push_str(" (");
                    transcript.push_str(provider);
                    transcript.push(')');
                }
                transcript.push_str("\n\n");
            }
            _ => {}
        }
    }

    if transcript.is_empty() {
        "No conversation content found.".to_string()
    } else {
        transcript
    }
}

#[cfg(test)]
mod tests {
    use super::format_transcript_full;

    #[test]
    fn format_transcript_full_renders_core_event_types() {
        let events = vec![
            (
                "run-1".to_string(),
                0,
                "user".to_string(),
                serde_json::json!({"content":"hello"}).to_string(),
            ),
            (
                "run-1".to_string(),
                1,
                "assistant".to_string(),
                serde_json::json!({"content":"hi there"}).to_string(),
            ),
            (
                "run-1".to_string(),
                2,
                "tool_result".to_string(),
                serde_json::json!({"toolResult":"done"}).to_string(),
            ),
        ];

        let rendered = format_transcript_full(&events);
        assert!(rendered.contains("**User:** hello"));
        assert!(rendered.contains("**Assistant:** hi there"));
        assert!(rendered.contains("**Tool Result (tool):**\ndone"));
    }
}
