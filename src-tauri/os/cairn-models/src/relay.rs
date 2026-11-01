//! The relay's body reader: the provider's bytes on their way to the client,
//! read for usage as they pass.
//!
//! Every byte the client receives is a byte the provider sent, never a
//! re-serialized line. The one exception is removal: the usage-only chunk Cairn
//! asked for on a client's behalf is dropped whole, with the blank line that
//! ends its event.

use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use cairn_llm_wire::openai_responses::wire::ResponsesUsage;
use cairn_llm_wire::TurnUsage;
use serde_json::Value;

use crate::interface::relay::{RelayDoor, RelayUsage};

/// The most of a whole (non-streamed) reply kept to read its usage. A reply
/// past it is still relayed whole; its usage just goes unread.
const WHOLE_BODY_USAGE_LIMIT: usize = 64 * 1024 * 1024;
/// How much of a whole reply one chunk carries.
const WHOLE_CHUNK: usize = 64 * 1024;

/// How the tap reads a body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TapMode {
    pub door: RelayDoor,
    /// The body is a server-sent event stream.
    pub sse: bool,
    /// Remove the usage-only chunk (chat and completions streams Cairn asked
    /// usage for).
    pub strip_usage_chunk: bool,
    /// The provider answered with success, so its body carries usage.
    pub success: bool,
}

/// The relayed body, read by whoever answers the client.
pub struct RelayBody {
    reader: BufReader<Box<dyn Read + Send>>,
    mode: TapMode,
    cancel: Arc<AtomicBool>,
    usage: RelayUsage,
    whole: Vec<u8>,
    whole_overflowed: bool,
    drop_next_blank: bool,
    finished: bool,
    cancelled: bool,
}

impl RelayBody {
    pub fn new(
        reader: Box<dyn Read + Send>,
        mode: TapMode,
        cancel: Arc<AtomicBool>,
        upstream_request_id: Option<String>,
    ) -> Self {
        Self {
            reader: BufReader::new(reader),
            mode,
            cancel,
            usage: RelayUsage {
                upstream_request_id,
                ..RelayUsage::default()
            },
            whole: Vec::new(),
            whole_overflowed: false,
            drop_next_blank: false,
            finished: false,
            cancelled: false,
        }
    }

    /// What the tap has read so far; complete once [`Self::next_chunk`] has
    /// returned `None`.
    pub fn usage(&self) -> &RelayUsage {
        &self.usage
    }

    /// Whether the read stopped because the client went away.
    pub fn cancelled(&self) -> bool {
        self.cancelled
    }

    /// The next bytes for the client. `None` once the provider's body ended or
    /// the call was cancelled; an error is a read that failed mid-body.
    pub fn next_chunk(&mut self) -> Option<Result<Vec<u8>, String>> {
        loop {
            if self.finished {
                return None;
            }
            if self.cancel.load(Ordering::Relaxed) {
                self.finished = true;
                self.cancelled = true;
                return None;
            }
            let chunk = if self.mode.sse {
                self.next_line()
            } else {
                self.next_whole_chunk()
            };
            match chunk {
                Ok(Some(bytes)) if bytes.is_empty() => continue,
                Ok(Some(bytes)) => {
                    self.usage.bytes += bytes.len() as u64;
                    return Some(Ok(bytes));
                }
                Ok(None) => {
                    self.finished = true;
                    self.finish_whole();
                    return None;
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }

    /// One SSE line, or nothing when the tap removes it.
    fn next_line(&mut self) -> Result<Option<Vec<u8>>, String> {
        let mut line = Vec::new();
        let read = self
            .reader
            .read_until(b'\n', &mut line)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Ok(None);
        }
        let text = String::from_utf8_lossy(&line);
        let trimmed = text.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            if std::mem::take(&mut self.drop_next_blank) {
                return Ok(Some(Vec::new()));
            }
            return Ok(Some(line));
        }
        self.drop_next_blank = false;
        let Some(data) = trimmed.strip_prefix("data:") else {
            return Ok(Some(line));
        };
        let data = data.trim();
        if !self.mode.success || data.is_empty() || data == "[DONE]" {
            return Ok(Some(line));
        }
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            return Ok(Some(line));
        };
        if self.read_event_usage(&event) && self.mode.strip_usage_chunk && is_usage_only(&event) {
            self.drop_next_blank = true;
            return Ok(Some(Vec::new()));
        }
        Ok(Some(line))
    }

    /// Records the usage an event carries; true when it carried some.
    fn read_event_usage(&mut self, event: &Value) -> bool {
        let usage = match self.mode.door {
            RelayDoor::Responses => event.get("response").and_then(|r| r.get("usage")),
            _ => event.get("usage"),
        };
        match usage.filter(|usage| usage.is_object()) {
            Some(usage) => {
                self.record(usage);
                true
            }
            None => false,
        }
    }

    fn next_whole_chunk(&mut self) -> Result<Option<Vec<u8>>, String> {
        let mut buffer = vec![0u8; WHOLE_CHUNK];
        let read = self
            .reader
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Ok(None);
        }
        buffer.truncate(read);
        if self.mode.success && !self.whole_overflowed {
            if self.whole.len() + read > WHOLE_BODY_USAGE_LIMIT {
                self.whole_overflowed = true;
                self.whole = Vec::new();
            } else {
                self.whole.extend_from_slice(&buffer);
            }
        }
        Ok(Some(buffer))
    }

    fn finish_whole(&mut self) {
        if self.mode.sse || !self.mode.success || self.whole_overflowed {
            return;
        }
        let whole = std::mem::take(&mut self.whole);
        if let Ok(reply) = serde_json::from_slice::<Value>(&whole) {
            if let Some(usage) = reply.get("usage").filter(|usage| usage.is_object()) {
                self.record(usage);
            }
        }
    }

    fn record(&mut self, usage: &Value) {
        let turn = turn_usage(self.mode.door, usage);
        let count = |value: Option<i32>| value.and_then(|value| u64::try_from(value).ok());
        let cached = turn
            .prompt_tokens_details
            .as_ref()
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64);
        self.usage.input_tokens = count(turn.prompt_tokens);
        self.usage.output_tokens = count(turn.completion_tokens);
        self.usage.cached_tokens = cached;
        self.usage.cost = turn.cost;
    }
}

/// The provider's usage object in the neutral shape. Responses name their
/// counts `input_tokens`/`output_tokens`; chat, completions and embeddings use
/// OpenAI's `prompt_tokens`/`completion_tokens`.
fn turn_usage(door: RelayDoor, usage: &Value) -> TurnUsage {
    let cost = usage.get("cost").and_then(Value::as_f64);
    if door == RelayDoor::Responses {
        return serde_json::from_value::<ResponsesUsage>(usage.clone())
            .unwrap_or_default()
            .into_turn_usage(cost);
    }
    serde_json::from_value::<TurnUsage>(usage.clone()).unwrap_or(TurnUsage {
        prompt_tokens: None,
        completion_tokens: None,
        total_tokens: None,
        reasoning_tokens: None,
        prompt_tokens_details: None,
        completion_tokens_details: None,
        cost,
        cost_details: None,
    })
}

/// The chunk a provider sends only because usage was asked for: no choices,
/// just usage. OpenRouter's usage riding on its last content chunk is not one.
fn is_usage_only(event: &Value) -> bool {
    event
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn body(text: &str, mode: TapMode) -> RelayBody {
        RelayBody::new(
            Box::new(Cursor::new(text.as_bytes().to_vec())),
            mode,
            Arc::new(AtomicBool::new(false)),
            None,
        )
    }

    fn drain(body: &mut RelayBody) -> String {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk() {
            out.extend(chunk.unwrap());
        }
        String::from_utf8(out).unwrap()
    }

    fn sse(door: RelayDoor, strip: bool) -> TapMode {
        TapMode {
            door,
            sse: true,
            strip_usage_chunk: strip,
            success: true,
        }
    }

    const CHAT_STREAM: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\r\n\r\n\
: keep-alive\n\n\
data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":8}}}\n\n\
data: [DONE]\n\n";

    #[test]
    fn a_stream_passes_through_byte_for_byte_when_nothing_is_stripped() {
        let mut relayed = body(CHAT_STREAM, sse(RelayDoor::ChatCompletions, false));
        assert_eq!(drain(&mut relayed), CHAT_STREAM);
        let usage = relayed.usage();
        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.output_tokens, Some(3));
        assert_eq!(usage.cached_tokens, Some(8));
        assert_eq!(usage.bytes, CHAT_STREAM.len() as u64);
    }

    #[test]
    fn the_usage_chunk_cairn_asked_for_is_counted_and_removed() {
        let mut relayed = body(CHAT_STREAM, sse(RelayDoor::ChatCompletions, true));
        let seen = drain(&mut relayed);
        assert_eq!(
            seen,
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\r\n\r\n: keep-alive\n\ndata: [DONE]\n\n"
        );
        assert_eq!(relayed.usage().input_tokens, Some(12));
    }

    #[test]
    fn openrouters_usage_on_the_last_content_chunk_passes_untouched() {
        let stream = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1,\"cost\":0.0021}}\n\ndata: [DONE]\n\n";
        let mut relayed = body(stream, sse(RelayDoor::ChatCompletions, true));
        assert_eq!(drain(&mut relayed), stream);
        assert_eq!(relayed.usage().cost, Some(0.0021));
        assert_eq!(relayed.usage().output_tokens, Some(1));
    }

    #[test]
    fn a_responses_stream_takes_usage_from_response_completed() {
        let stream = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"a\"}\n\n\
event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":40,\"output_tokens\":7,\"input_tokens_details\":{\"cached_tokens\":32}}}}\n\n";
        let mut relayed = body(stream, sse(RelayDoor::Responses, false));
        assert_eq!(drain(&mut relayed), stream);
        let usage = relayed.usage();
        assert_eq!(usage.input_tokens, Some(40));
        assert_eq!(usage.output_tokens, Some(7));
        assert_eq!(usage.cached_tokens, Some(32));
    }

    #[test]
    fn a_whole_reply_and_embeddings_read_usage_from_the_buffered_json() {
        let reply = r#"{"object":"list","data":[{"embedding":[0.1]}],"usage":{"prompt_tokens":9,"total_tokens":9}}"#;
        let mut relayed = body(
            reply,
            TapMode {
                door: RelayDoor::Embeddings,
                sse: false,
                strip_usage_chunk: false,
                success: true,
            },
        );
        assert_eq!(drain(&mut relayed), reply);
        assert_eq!(relayed.usage().input_tokens, Some(9));
        assert_eq!(relayed.usage().output_tokens, None);
    }

    #[test]
    fn a_failed_reply_passes_through_unread() {
        let reply = r#"{"error":{"message":"bad"},"usage":{"prompt_tokens":1}}"#;
        let mut relayed = body(
            reply,
            TapMode {
                door: RelayDoor::ChatCompletions,
                sse: false,
                strip_usage_chunk: false,
                success: false,
            },
        );
        assert_eq!(drain(&mut relayed), reply);
        assert_eq!(relayed.usage().input_tokens, None);
    }

    #[test]
    fn cancel_stops_the_read() {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut relayed = RelayBody::new(
            Box::new(Cursor::new(CHAT_STREAM.as_bytes().to_vec())),
            sse(RelayDoor::ChatCompletions, false),
            cancel.clone(),
            None,
        );
        assert!(relayed.next_chunk().is_some());
        cancel.store(true, Ordering::Relaxed);
        assert!(relayed.next_chunk().is_none());
        assert!(relayed.cancelled());
    }
}
