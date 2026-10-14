//! Streaming message state shared by session readers and persistence adapters.
use crate::agent_process::stream::{TranscriptEvent, TranscriptJson};
use cairn_db::db_records::DbMessageStream;
use serde::Serialize;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamChunkKind {
    Content,
    Thinking,
}

/// Record the first live delta handed to the frontend for a stream. This boundary
/// belongs with stream delivery rather than any one backend, and deliberately
/// records only aggregate sizes.
pub fn trace_first_frontend_delta_emit(
    run_id: &str,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    stream_id: &str,
    delta: &EmitDelta,
) {
    if crate::timing::mark_first(format!("frontend:{stream_id}")) {
        let mut event = crate::timing::ResumeTimingEvent::new("first_frontend_delta_emit");
        event.run_id = Some(run_id);
        event.session_id = session_id;
        event.turn_id = turn_id;
        event.stream_id = Some(stream_id);
        event.bytes = Some(
            delta.content_delta.as_ref().map_or(0, String::len)
                + delta.thinking_delta.as_ref().map_or(0, String::len),
        );
        event.emit();
    }
}

impl StreamChunkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::Thinking => "thinking",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamChunkInput {
    pub kind: StreamChunkKind,
    pub data: String,
}

impl StreamChunkInput {
    pub fn content(data: impl Into<String>) -> Self {
        Self {
            kind: StreamChunkKind::Content,
            data: data.into(),
        }
    }

    pub fn thinking(data: impl Into<String>) -> Self {
        Self {
            kind: StreamChunkKind::Thinking,
            data: data.into(),
        }
    }
}

/// Flush buffered chunk rows to the DB at most this often. Crash-recovery
/// granularity is bounded by this interval: a few hundred ms of un-flushed
/// tokens can be lost on a hard crash, which the design accepts in exchange for
/// collapsing per-token write amplification into ~4 transactions/second.
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// Emit a live `streaming-update` IPC delta at most this often (~25Hz). The
/// frontend's `useSmoothText` interpolates between emits, so a coarser cadence
/// here trades a little latency for far fewer IPC messages on long messages.
const EMIT_INTERVAL: Duration = Duration::from_millis(40);

/// The un-emitted scalar tail of a stream's content/thinking plus the current
/// absolute scalar lengths, for true-delta IPC. `*_len` always reflect current
/// totals so the frontend can detect contiguity and self-heal gaps against the
/// authoritative DB snapshot.
#[derive(Debug, Clone, Default)]
pub struct EmitDelta {
    pub content_delta: Option<String>,
    pub content_len: i32,
    pub thinking_delta: Option<String>,
    pub thinking_len: i32,
}

/// Lightweight result of [`append_chunks`]: the stream's new
/// optimistic-concurrency version. The full content is never reconstructed on
/// the hot path; callers keep the running strings in their [`StreamAccumulator`].
#[derive(Debug, Clone, Copy)]
pub struct AppendResult {
    pub version: i32,
}

/// Reader-thread-local accumulator for a single streaming message.
///
/// Holds the running content/thinking strings in memory so neither the chunk DB
/// write nor the IPC emit re-reads or re-concatenates all prior chunks. Chunks
/// are buffered and flushed to the DB on a cadence ([`FLUSH_INTERVAL`]); IPC
/// deltas carry only newly-produced scalars on a separate, faster cadence
/// ([`EMIT_INTERVAL`]).
#[derive(Debug)]
pub struct StreamAccumulator {
    content: String,
    content_len: i32,
    thinking: String,
    thinking_len: i32,
    pending: Vec<StreamChunkInput>,
    last_flush: Instant,
    last_emit: Instant,
    emitted_content_len: i32,
    emitted_thinking_len: i32,
}

impl Default for StreamAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamAccumulator {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            content: String::new(),
            content_len: 0,
            thinking: String::new(),
            thinking_len: 0,
            pending: Vec::new(),
            last_flush: now,
            last_emit: now,
            emitted_content_len: 0,
            emitted_thinking_len: 0,
        }
    }

    /// Append content tokens: grow the string, bump the scalar length, buffer a
    /// chunk for the next flush. Empty deltas are ignored.
    pub fn push_content(&mut self, text: &str) {
        let text = scrub_delta(text);
        if text.is_empty() {
            return;
        }
        self.content.push_str(&text);
        self.content_len += text.chars().count() as i32;
        self.pending.push(StreamChunkInput::content(text));
    }

    /// Append thinking tokens. See [`StreamAccumulator::push_content`].
    pub fn push_thinking(&mut self, text: &str) {
        let text = scrub_delta(text);
        if text.is_empty() {
            return;
        }
        self.thinking.push_str(&text);
        self.thinking_len += text.chars().count() as i32;
        self.pending.push(StreamChunkInput::thinking(text));
    }

    pub fn content_is_empty(&self) -> bool {
        self.content.is_empty()
    }

    pub fn pending_is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether the flush cadence has elapsed and there are buffered chunks.
    pub fn should_flush(&self, now: Instant) -> bool {
        !self.pending.is_empty() && now.duration_since(self.last_flush) >= FLUSH_INTERVAL
    }

    /// Drain buffered chunks for a DB write and reset the flush clock.
    pub fn take_pending(&mut self) -> Vec<StreamChunkInput> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.pending)
    }

    /// Whether any produced scalars have not yet been emitted over IPC.
    pub fn has_unemitted(&self) -> bool {
        self.emitted_content_len < self.content_len || self.emitted_thinking_len < self.thinking_len
    }

    /// Whether the emit cadence has elapsed and there is un-emitted content.
    pub fn should_emit(&self, now: Instant) -> bool {
        self.has_unemitted() && now.duration_since(self.last_emit) >= EMIT_INTERVAL
    }

    /// Take the un-emitted scalar tail of content/thinking, advance the emit
    /// markers, and reset the emit clock. Used both on the cadence (gated by
    /// [`StreamAccumulator::should_emit`]) and forced at boundaries (finalize,
    /// thinking-token system events) where the interval is ignored.
    pub fn take_emit_delta(&mut self) -> EmitDelta {
        let content_delta = scalar_tail(&self.content, self.emitted_content_len, self.content_len);
        if content_delta.is_some() {
            self.emitted_content_len = self.content_len;
        }
        let thinking_delta =
            scalar_tail(&self.thinking, self.emitted_thinking_len, self.thinking_len);
        if thinking_delta.is_some() {
            self.emitted_thinking_len = self.thinking_len;
        }
        self.last_emit = Instant::now();
        EmitDelta {
            content_delta,
            content_len: self.content_len,
            thinking_delta,
            thinking_len: self.thinking_len,
        }
    }
}

/// The live-event crossing for streamed model output (CAIRN-3822).
///
/// This is the one place a streamed delta enters the accumulator, and everything
/// downstream reads from that accumulation: the buffered DB chunks, the
/// reconstructed stream, and the `streaming-update` payload the frontend renders.
/// Scrubbing here therefore gives persistence and emission the same value, which
/// is the property that keeps a raw and a sanitized copy from forking.
///
/// Exact registered values only, and no cross-delta buffering. A credential
/// wholly inside one delta is removed from both the live render and the chunk
/// row; one split across two deltas is still removed from the durable transcript
/// event, which is sanitized as a whole at the transcript crossing, but can
/// briefly render in the live stream. Closing that last gap means giving the
/// accumulator a `StreamingScrubber` with an explicit end-of-stream flush, which
/// belongs with the rest of the streaming work in CAIRN-3824: without a flush on
/// every finalize path, the withheld suffix would silently truncate a message.
fn scrub_delta(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    // Lock-free fast path: this runs once per streamed token, and building a
    // sanitizer costs a lock read, an `Arc` clone, and an allocation. With
    // nothing registered there is nothing to do.
    if cairn_common::security::registry().is_empty() {
        return text.to_string();
    }
    let mut sanitizer = cairn_common::security::Sanitizer::exact();
    if sanitizer.is_noop() {
        return text.to_string();
    }
    let scrubbed = sanitizer.text(text).into_owned();
    for report in cairn_common::security::crossing::DetectionReport::from_detections(
        sanitizer.detections(),
        cairn_common::security::Crossing::LiveEvent,
        None,
        None,
    ) {
        report.log();
    }
    scrubbed
}

/// Return the scalar tail of `s` after `emitted` scalar values, or `None` if no
/// new scalars are present. Slices on a `char_indices` boundary so multibyte
/// scalars (accents, emoji) are never split.
fn scalar_tail(s: &str, emitted: i32, total: i32) -> Option<String> {
    if emitted >= total {
        return None;
    }
    let byte_idx = s
        .char_indices()
        .nth(emitted as usize)
        .map(|(idx, _)| idx)
        .unwrap_or(s.len());
    Some(s[byte_idx..].to_string())
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamingToolWrite {
    pub id: Option<String>,
    pub name: String,
    pub input_chars: i32,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_preview: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ActiveMessageStream {
    pub stream: DbMessageStream,
    pub content: String,
    pub thinking: String,
}

impl ActiveMessageStream {
    pub fn stream_id(&self) -> &str {
        &self.stream.id
    }

    pub fn version(&self) -> i32 {
        self.stream.version
    }

    pub fn to_streaming_event_json(&self) -> String {
        TranscriptEvent {
            event_type: "assistant:streaming".to_string(),
            session_id: self.stream.session_id.clone(),
            parent_tool_use_id: None,
            content: if self.content.is_empty() {
                None
            } else {
                Some(self.content.clone())
            },
            thinking: if self.thinking.is_empty() {
                None
            } else {
                Some(self.thinking.clone())
            },
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
        .observed()
        .to_event_json()
    }
}
