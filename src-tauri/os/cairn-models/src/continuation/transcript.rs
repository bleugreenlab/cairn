//! The provider-neutral record of a conversation that continuation renders.
//!
//! A host builds a [`Transcript`] however it stores history: Cairn maps its
//! persisted event rows onto one, and a host that keeps the raw
//! [`SessionEvent`] stream folds it through [`TranscriptBuilder`]. Both
//! projections, [`super::render_digest`] and [`super::render_verbatim`], read
//! only this shape.

use serde_json::Value;

use crate::session::events::SessionEvent;

/// A conversation, as an ordered list of turns.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Transcript {
    pub turns: Vec<Turn>,
}

/// One turn: what an operator (or the host) said, and everything the model did
/// in answer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Turn {
    /// The turn's addressable label. `None` renders the turn's position.
    pub number: Option<i64>,
    /// Where the turn can be read in full. Truncation trailers cite it as
    /// `[+N lines → {link}]`; without one they read `[+N lines]`.
    pub link: Option<String>,
    /// Unix seconds. `None` renders the heading without a time.
    pub started_at: Option<i64>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// The first user entry in a turn is its message; a later one, or any that
    /// carries a `<system-reminder>`, is an injection and renders as a `⚠` line.
    User(String),
    Assistant(String),
    /// Carried for completeness, rendered by neither projection: reasoning does
    /// not survive into a seed, and providers do not re-send prior turns'
    /// thinking as prefix.
    Thinking(String),
    Tool(ToolCall),
    /// A provider error that ended the turn.
    Error(String),
    Marker(Marker),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
    /// `None` while the call is unpaired — in flight or interrupted.
    pub outcome: Option<ToolOutcome>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutcome {
    pub content: String,
    pub is_error: bool,
}

/// A line the adapter has already worded, for events only it can name (a
/// launch prompt, a resume nudge, a session boundary). Each projection reads
/// its own field, and `None` renders nothing there.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Marker {
    /// The lean digest's line.
    pub skim: Option<String>,
    /// The unabridged digest's line; falls back to `skim`.
    pub full: Option<String>,
    /// What [`super::render_verbatim`] counts for it.
    pub verbatim: Option<String>,
}

/// Folds a [`SessionEvent`] stream, plus the owner's own messages, into a
/// [`Transcript`].
///
/// The rules, in the order an event meets them:
///
/// - Events carrying a `parent_tool_use_id` belong to a sub-agent and are
///   skipped entirely; the parent's own tool row stands for the sub-agent.
/// - Consecutive `ThinkingDelta`s become one [`Entry::Thinking`], flushed by
///   the next event of any other kind.
/// - `TextDelta`s accumulate. A `Text` replaces them, because they were its
///   streaming form. Deltas still pending when the turn ends — at
///   `TurnComplete`, a non-retrying `Error`, the next owner message, or
///   [`TranscriptBuilder::finish`] — are the interrupted case and become an
///   [`Entry::Assistant`] of their own.
/// - `ToolCall` pushes an unpaired [`Entry::Tool`]. `ToolResult` pairs with the
///   call of the same id anywhere earlier in the transcript; a result whose id
///   matches no call is dropped.
/// - `Error { will_retry: false }` and `TurnComplete { error: Some(..) }` become
///   an [`Entry::Error`]; the second is dropped when it repeats the first.
///   `will_retry: true` is ignored, and `TurnComplete.text` is never read,
///   because it repeats the last `Text`.
/// - `Started`, `ServerRequest` and `RateLimit` carry nothing to render.
///
/// Turns: [`TranscriptBuilder::user`] opens a turn unless the current one has
/// no entries yet. `TurnComplete` closes the turn, and the next entry opens a
/// new one. A turn's `started_at` is the `at` of its first entry.
#[derive(Debug, Default)]
pub struct TranscriptBuilder {
    turns: Vec<Turn>,
    /// The last turn ended; the next entry opens another.
    closed: bool,
    pending_text: Option<(String, Option<i64>)>,
    pending_thinking: Option<(String, Option<i64>)>,
}

impl TranscriptBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// An owner message (a triome `owner` frame, a typed reply). Opens a turn.
    pub fn user(&mut self, text: &str, at: Option<i64>) {
        self.flush_thinking();
        self.flush_text();
        let reuse = !self.closed
            && self
                .turns
                .last()
                .is_some_and(|turn| turn.entries.is_empty());
        if !reuse {
            self.closed = true;
        }
        self.push(Entry::User(text.to_string()), at);
    }

    pub fn event(&mut self, event: &SessionEvent, at: Option<i64>) {
        match event {
            SessionEvent::TextDelta {
                parent_tool_use_id: Some(_),
                ..
            }
            | SessionEvent::Text {
                parent_tool_use_id: Some(_),
                ..
            }
            | SessionEvent::ToolCall {
                parent_tool_use_id: Some(_),
                ..
            } => return,
            SessionEvent::ThinkingDelta { text } => {
                match &mut self.pending_thinking {
                    Some((pending, _)) => pending.push_str(text),
                    None => self.pending_thinking = Some((text.clone(), at)),
                }
                return;
            }
            _ => {}
        }
        self.flush_thinking();
        match event {
            SessionEvent::TextDelta { text, .. } => match &mut self.pending_text {
                Some((pending, _)) => pending.push_str(text),
                None => self.pending_text = Some((text.clone(), at)),
            },
            SessionEvent::Text { text, .. } => {
                self.pending_text = None;
                if !text.is_empty() {
                    self.push(Entry::Assistant(text.clone()), at);
                }
            }
            SessionEvent::ToolCall {
                id, name, input, ..
            } => self.push(
                Entry::Tool(ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                    outcome: None,
                }),
                at,
            ),
            SessionEvent::ToolResult {
                tool_use_id,
                content,
                is_error,
            } => {
                let call = self
                    .turns
                    .iter_mut()
                    .rev()
                    .flat_map(|turn| turn.entries.iter_mut().rev())
                    .find_map(|entry| match entry {
                        Entry::Tool(call) if call.id == *tool_use_id => Some(call),
                        _ => None,
                    });
                if let Some(call) = call {
                    call.outcome = Some(ToolOutcome {
                        content: content.clone(),
                        is_error: *is_error,
                    });
                }
            }
            SessionEvent::Error {
                message,
                will_retry: false,
            } => {
                self.flush_text();
                self.push_error(message, at);
            }
            SessionEvent::TurnComplete { error, .. } => {
                self.flush_text();
                if let Some(error) = error {
                    self.push_error(error, at);
                }
                self.closed = true;
            }
            SessionEvent::Error {
                will_retry: true, ..
            }
            | SessionEvent::Started { .. }
            | SessionEvent::ServerRequest { .. }
            | SessionEvent::RateLimit { .. }
            | SessionEvent::CredentialRejected
            | SessionEvent::CredentialAcknowledged { .. }
            | SessionEvent::ThinkingDelta { .. } => {}
        }
    }

    pub fn finish(mut self) -> Transcript {
        self.flush_thinking();
        self.flush_text();
        Transcript { turns: self.turns }
    }

    fn flush_thinking(&mut self) {
        if let Some((text, at)) = self.pending_thinking.take() {
            self.push(Entry::Thinking(text), at);
        }
    }

    fn flush_text(&mut self) {
        if let Some((text, at)) = self.pending_text.take() {
            if !text.is_empty() {
                self.push(Entry::Assistant(text), at);
            }
        }
    }

    /// A failed turn often reports its error twice — as the provider's `Error`
    /// and again on `TurnComplete` — and one line says it.
    fn push_error(&mut self, message: &str, at: Option<i64>) {
        let repeats = !self.closed
            && self
                .turns
                .last()
                .and_then(|turn| turn.entries.last())
                .is_some_and(|last| matches!(last, Entry::Error(previous) if previous == message));
        if !repeats {
            self.push(Entry::Error(message.to_string()), at);
        }
    }

    fn push(&mut self, entry: Entry, at: Option<i64>) {
        if self.closed || self.turns.is_empty() {
            self.turns.push(Turn::default());
            self.closed = false;
        }
        let turn = self.turns.last_mut().expect("a turn was just ensured");
        if turn.entries.is_empty() {
            turn.started_at = at;
        }
        turn.entries.push(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn delta(text: &str) -> SessionEvent {
        SessionEvent::TextDelta {
            text: text.to_string(),
            parent_tool_use_id: None,
        }
    }

    fn text(text: &str) -> SessionEvent {
        SessionEvent::Text {
            text: text.to_string(),
            parent_tool_use_id: None,
        }
    }

    fn complete(error: Option<&str>) -> SessionEvent {
        SessionEvent::TurnComplete {
            status: crate::session::events::TurnStatus::Completed,
            text: Some("repeated final text".to_string()),
            structured: None,
            usage: None,
            cost_usd: None,
            error: error.map(str::to_string),
        }
    }

    fn entries(transcript: &Transcript) -> Vec<Vec<Entry>> {
        transcript
            .turns
            .iter()
            .map(|turn| turn.entries.clone())
            .collect()
    }

    #[test]
    fn a_text_replaces_the_deltas_that_streamed_it() {
        let mut builder = TranscriptBuilder::new();
        builder.user("hi", Some(10));
        builder.event(&delta("Hel"), Some(11));
        builder.event(&delta("lo"), Some(12));
        builder.event(&text("Hello."), Some(13));
        builder.event(&complete(None), Some(14));
        assert_eq!(
            entries(&builder.finish()),
            vec![vec![
                Entry::User("hi".to_string()),
                Entry::Assistant("Hello.".to_string())
            ]],
            "TurnComplete.text repeats the last Text and is never read"
        );
    }

    #[test]
    fn deltas_without_a_text_survive_an_interrupted_turn() {
        let mut builder = TranscriptBuilder::new();
        builder.user("first", None);
        builder.event(&delta("half a "), None);
        builder.event(&delta("thought"), None);
        builder.user("second", None);
        builder.event(&delta("still streaming"), None);
        assert_eq!(
            entries(&builder.finish()),
            vec![
                vec![
                    Entry::User("first".to_string()),
                    Entry::Assistant("half a thought".to_string())
                ],
                vec![
                    Entry::User("second".to_string()),
                    Entry::Assistant("still streaming".to_string())
                ],
            ]
        );
    }

    #[test]
    fn consecutive_thinking_is_one_entry_flushed_by_the_next_event() {
        let mut builder = TranscriptBuilder::new();
        builder.user("q", None);
        builder.event(
            &SessionEvent::ThinkingDelta {
                text: "Let me ".to_string(),
            },
            None,
        );
        builder.event(
            &SessionEvent::ThinkingDelta {
                text: "think.".to_string(),
            },
            None,
        );
        builder.event(&text("Answer."), None);
        assert_eq!(
            entries(&builder.finish()),
            vec![vec![
                Entry::User("q".to_string()),
                Entry::Thinking("Let me think.".to_string()),
                Entry::Assistant("Answer.".to_string())
            ]]
        );
    }

    #[test]
    fn sub_agent_traffic_is_skipped_and_its_parent_row_stands_for_it() {
        let mut builder = TranscriptBuilder::new();
        builder.user("delegate", None);
        builder.event(
            &SessionEvent::ToolCall {
                id: "task-1".to_string(),
                name: "Task".to_string(),
                input: json!({ "description": "explore" }),
                parent_tool_use_id: None,
            },
            None,
        );
        builder.event(
            &SessionEvent::Text {
                text: "sub-agent prose".to_string(),
                parent_tool_use_id: Some("task-1".to_string()),
            },
            None,
        );
        builder.event(
            &SessionEvent::TextDelta {
                text: "sub-agent delta".to_string(),
                parent_tool_use_id: Some("task-1".to_string()),
            },
            None,
        );
        builder.event(
            &SessionEvent::ToolCall {
                id: "inner".to_string(),
                name: "Read".to_string(),
                input: json!({}),
                parent_tool_use_id: Some("task-1".to_string()),
            },
            None,
        );
        builder.event(
            &SessionEvent::ToolResult {
                tool_use_id: "inner".to_string(),
                content: "a sub-agent's result pairs with nothing".to_string(),
                is_error: false,
            },
            None,
        );
        builder.event(
            &SessionEvent::ToolResult {
                tool_use_id: "task-1".to_string(),
                content: "explored".to_string(),
                is_error: false,
            },
            None,
        );
        assert_eq!(
            entries(&builder.finish()),
            vec![vec![
                Entry::User("delegate".to_string()),
                Entry::Tool(ToolCall {
                    id: "task-1".to_string(),
                    name: "Task".to_string(),
                    input: json!({ "description": "explore" }),
                    outcome: Some(ToolOutcome {
                        content: "explored".to_string(),
                        is_error: false
                    }),
                }),
            ]]
        );
    }

    #[test]
    fn only_a_final_error_is_recorded_and_once() {
        let mut builder = TranscriptBuilder::new();
        builder.user("go", None);
        builder.event(
            &SessionEvent::Error {
                message: "overloaded, retrying".to_string(),
                will_retry: true,
            },
            None,
        );
        builder.event(
            &SessionEvent::Error {
                message: "rate limited".to_string(),
                will_retry: false,
            },
            None,
        );
        builder.event(&complete(Some("rate limited")), None);
        assert_eq!(
            entries(&builder.finish()),
            vec![vec![
                Entry::User("go".to_string()),
                Entry::Error("rate limited".to_string())
            ]]
        );
    }

    #[test]
    fn turns_open_on_owner_messages_and_close_on_turn_complete() {
        let mut builder = TranscriptBuilder::new();
        // Model output before any owner message still lands in a turn.
        builder.event(&text("unprompted"), Some(5));
        builder.event(&complete(None), Some(6));
        // After TurnComplete, the next entry opens a turn even without a user.
        builder.event(&text("woke up"), Some(7));
        builder.user("reply", Some(8));
        let transcript = builder.finish();
        assert_eq!(transcript.turns.len(), 3);
        assert_eq!(
            transcript
                .turns
                .iter()
                .map(|turn| turn.started_at)
                .collect::<Vec<_>>(),
            vec![Some(5), Some(7), Some(8)]
        );
        assert!(transcript.turns.iter().all(|turn| turn.number.is_none()));
    }

    #[test]
    fn an_orphan_result_is_dropped() {
        let mut builder = TranscriptBuilder::new();
        builder.user("x", None);
        builder.event(
            &SessionEvent::ToolResult {
                tool_use_id: "nobody".to_string(),
                content: "orphan".to_string(),
                is_error: false,
            },
            None,
        );
        assert_eq!(
            entries(&builder.finish()),
            vec![vec![Entry::User("x".to_string())]]
        );
    }
}
