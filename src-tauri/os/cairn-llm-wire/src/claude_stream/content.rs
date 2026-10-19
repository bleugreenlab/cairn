//! Readers for the content of `user` and `assistant` messages.

use serde_json::Value;

use super::event::{ContentBlock, MessageContentInner};
use super::tool_input::RejectedToolInput;
use crate::replay::ToolUseInfo;

/// Whether a `user` message is the user speaking or a tool's result coming
/// back. The CLI sends both as `type: user`; only the content blocks tell them
/// apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserContentKind {
    User,
    ToolResult,
}

impl UserContentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            UserContentKind::User => "user",
            UserContentKind::ToolResult => "tool_result",
        }
    }
}

/// What a `user` message carries once its content blocks are read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserContent {
    pub kind: UserContentKind,
    pub text: Option<String>,
    pub tool_use_id: Option<String>,
    pub tool_result: Option<String>,
    pub is_error: bool,
}

/// What an `assistant` message carries once its content blocks are read.
#[derive(Debug, Clone)]
pub struct AssistantContent {
    pub text: Option<String>,
    pub tool_uses: Option<Vec<ToolUseInfo>>,
    pub thinking: Option<String>,
}

/// Extract content from user messages, handling both text and tool results.
pub fn extract_user_content(content: &MessageContentInner) -> UserContent {
    match content {
        MessageContentInner::Text(s) => UserContent {
            kind: UserContentKind::User,
            text: Some(s.clone()),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
        },
        MessageContentInner::Blocks(blocks) => {
            let mut texts = Vec::new();
            let mut tool_use_id = None;
            let mut tool_result_text = None;
            let mut has_error = false;

            for block in blocks {
                match block {
                    ContentBlock::Text { text } => texts.push(text.clone()),
                    ContentBlock::ToolResult {
                        tool_use_id: id,
                        content,
                        is_error,
                    } => {
                        tool_use_id = Some(id.clone());
                        // Tool result content can be a string or array of content blocks
                        let result_text = match content {
                            Value::String(s) => s.clone(),
                            Value::Array(arr) => arr
                                .iter()
                                .filter_map(|v| v.get("text").and_then(|t| t.as_str()))
                                .collect::<Vec<_>>()
                                .join("\n"),
                            _ => content.to_string(),
                        };
                        tool_result_text = Some(result_text);
                        if is_error.unwrap_or(false) {
                            has_error = true;
                        }
                    }
                    _ => {}
                }
            }

            let text = if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            };
            // If we have a tool result, this is a tool_result event
            if tool_result_text.is_some() {
                UserContent {
                    kind: UserContentKind::ToolResult,
                    text,
                    tool_use_id,
                    tool_result: tool_result_text,
                    is_error: has_error,
                }
            } else {
                UserContent {
                    kind: UserContentKind::User,
                    text,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                }
            }
        }
    }
}

/// Extract content, tool uses, and thinking from assistant messages.
pub fn extract_assistant_content(content: &MessageContentInner) -> AssistantContent {
    match content {
        MessageContentInner::Text(s) => AssistantContent {
            text: Some(s.clone()),
            tool_uses: None,
            thinking: None,
        },
        MessageContentInner::Blocks(blocks) => {
            let mut texts = Vec::new();
            let mut tool_uses = Vec::new();
            let mut thinking_blocks = Vec::new();

            for block in blocks {
                match block {
                    ContentBlock::Text { text } => texts.push(text.clone()),
                    ContentBlock::ToolUse { id, name, input } => {
                        // A rejected input costs a full re-authoring of the
                        // payload and is otherwise visible only to whoever
                        // reads the transcript, so account for it here.
                        if let Some(rejected) = RejectedToolInput::detect(input) {
                            log::warn!(
                                "rejected-tool-input: {} never ran because the agent runtime could not \
                                 parse its input JSON (tool_use_id={}, bytes={}, inspected={}, defect={}). \
                                 The call did not reach Cairn; the agent has to re-author the payload.",
                                name,
                                id,
                                rejected.len,
                                rejected.captured,
                                rejected.defect.as_str(),
                            );
                        }
                        log::trace!(
                            "[DEBUG-TOOLUSE] Extracted tool_use from assistant content: id={} name={}",
                            id,
                            name
                        );
                        tool_uses.push(ToolUseInfo {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                        });
                    }
                    // A `thinking` block whose text is empty is the PRIVATE
                    // reasoning disposition: the model reasoned, but the prose is
                    // withheld and only its signature travels. That volume is
                    // reported by the turn's `thinking_tokens`, never by this
                    // field. Keeping it would yield `Some("")` for a private-only
                    // message -- which is not `None`, and so defeats the
                    // `is_none()` guards downstream that let real prose land --
                    // and a leading newline when it precedes a visible block.
                    ContentBlock::Thinking { thinking } if !thinking.trim().is_empty() => {
                        thinking_blocks.push(thinking.clone());
                    }
                    _ => {}
                }
            }

            AssistantContent {
                text: (!texts.is_empty()).then(|| texts.join("\n")),
                tool_uses: (!tool_uses.is_empty()).then_some(tool_uses),
                thinking: (!thinking_blocks.is_empty()).then(|| thinking_blocks.join("\n")),
            }
        }
    }
}
