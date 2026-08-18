pub mod file_activity;
pub mod stream_store;

use serde_json::Value;

pub(crate) type TranscriptRow = (String, i32, String, String);

/// Transcript event type for a resume prompt Cairn synthesized for itself.
///
/// A resume that carries no operator content still needs prompt text, and Cairn
/// writes it (`SYNTHETIC_CONTINUATION_PROMPT`). Namespacing the stored event
/// away from plain `user` is what makes the attribution correct by construction:
/// no projection that renders `user` as the operator can reach this row, and
/// each projection opts in deliberately with [`CONTINUATION_MARKER_LINE`]
/// (CAIRN-3175).
pub(crate) const CONTINUATION_EVENT_TYPE: &str = "user:continuation";

/// The one line every text projection renders for a [`CONTINUATION_EVENT_TYPE`]
/// event. The prompt body is deliberately not shown: it is Cairn's own wake
/// text, not conversation, and it stays addressable in the raw stream.
pub(crate) const CONTINUATION_MARKER_LINE: &str = "· [automatic resume — no operator message]";

/// Transcript event type for the prompt a job was launched with.
///
/// The third member of the namespaced user-slot family (after
/// [`CONTINUATION_EVENT_TYPE`] and `user:seed`), and for the same reason: a job's
/// opening prompt occupies the user slot because that is the only channel a
/// launch has, but nobody typed it. Cairn composes it from the issue's resolved
/// inputs, so its author is whoever filed the issue — for a delegated child, the
/// coordinator or thread that spawned it.
///
/// That authorship is what makes the plain `user` type actively wrong rather than
/// merely imprecise. A watching parent receives its child's chat as ride-along
/// catch-up context, and a launch event typed `user` renders there as
/// `**User:** …` — echoing the parent's own issue description back at it as if
/// someone had sent it (CAIRN-3408, the surface adjacent to CAIRN-3390's operator
/// digest). Namespacing fixes the attribution by construction at every
/// projection at once, rather than teaching each renderer to guess authorship
/// from content.
pub(crate) const LAUNCH_EVENT_TYPE: &str = "user:launch";

/// The speaker label full-fidelity projections give a [`LAUNCH_EVENT_TYPE`]
/// event.
///
/// Unlike a continuation or a seed, a launch prompt has a body worth reading —
/// it is the task itself, and a session reseeded from a digest needs it to know
/// what it was asked to do. So the faithful projections keep the content and
/// correct only the attribution; it is the skim projections that collapse it to
/// [`LAUNCH_MARKER_LINE`].
pub(crate) const LAUNCH_LABEL: &str = "**Launch prompt:** ";

/// The one line skim projections render for a [`LAUNCH_EVENT_TYPE`] event.
///
/// A digest exists to tell a reader what happened; the task a node was given is
/// not something that happened, and for the parent that authored it the body is
/// pure echo. It stays addressable at the issue, at `{node}/chat/turn/1`, and in
/// the raw stream.
pub(crate) const LAUNCH_MARKER_LINE: &str = "· [launch prompt — the task this node was given]";

/// Format transcript rows into markdown without truncation.
///
/// Intended for reuse in places where we need a faithful text rendering of the
/// visible conversation, such as `cairn://.../chat` reads and resume fallback
/// prompt construction.
pub(crate) fn format_transcript_full(events: &[TranscriptRow]) -> String {
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
            CONTINUATION_EVENT_TYPE => {
                transcript.push_str(CONTINUATION_MARKER_LINE);
                transcript.push_str("\n\n");
            }
            LAUNCH_EVENT_TYPE => {
                // Full fidelity: keep the body, fix the speaker. This projection
                // backs the resume-fallback prompt, so dropping the task text
                // here would strand a rebuilt session with no statement of what
                // it is doing.
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        transcript.push_str(LAUNCH_LABEL);
                        transcript.push_str(content);
                        transcript.push_str("\n\n");
                    }
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

    #[test]
    fn format_transcript_full_labels_a_launch_prompt_without_claiming_a_speaker() {
        // This is the full-fidelity projection — it backs `/chat/raw` and the
        // resume-fallback prompt — so a launch prompt keeps its body: a session
        // rebuilt from it would otherwise have no statement of its task. What it
        // must not do is attribute that body to a person (CAIRN-3408).
        let task = "Fix the panic in the CLI logger";
        let events = vec![(
            "run-1".to_string(),
            0,
            super::LAUNCH_EVENT_TYPE.to_string(),
            serde_json::json!({ "content": task }).to_string(),
        )];

        let rendered = format_transcript_full(&events);
        assert!(
            rendered.contains(&format!("{}{task}", super::LAUNCH_LABEL)),
            "the task must survive a faithful rendering: {rendered}"
        );
        assert!(
            !rendered.contains("**User:**"),
            "nobody typed a launch prompt: {rendered}"
        );
    }
}
