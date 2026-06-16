use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use crate::db_records::{DbMessageStream, DbMessageStreamChunk};
use crate::effects::outbox::{self, OutboxEntry};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use serde::{Deserialize, Serialize};
use turso::{params, Connection, Row};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamChunkKind {
    Content,
    Thinking,
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
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
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
        if text.is_empty() {
            return;
        }
        self.content.push_str(text);
        self.content_len += text.chars().count() as i32;
        self.pending
            .push(StreamChunkInput::content(text.to_string()));
    }

    /// Append thinking tokens. See [`StreamAccumulator::push_content`].
    pub fn push_thinking(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.thinking.push_str(text);
        self.thinking_len += text.chars().count() as i32;
        self.pending
            .push(StreamChunkInput::thinking(text.to_string()));
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
        serde_json::to_string(&TranscriptEvent {
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
            raw: None,
        })
        .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct FinalizedStream {
    pub stream_id: String,
    pub event_id: String,
    pub sequence: i32,
    pub created_at: i32,
    pub turn_id: Option<String>,
    pub event_type: String,
    pub data_json: String,
    pub outbox_entries: Vec<OutboxEntry>,
}

#[derive(Debug, Clone)]
pub struct EventInsert {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub sequence: i32,
    pub timestamp: i32,
    pub event_type: String,
    pub data: String,
    pub parent_tool_use_id: Option<String>,
    pub created_at: i32,
    pub input_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub thinking_tokens: Option<i32>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmbedEventPayload {
    event_id: String,
    data_json: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncEventPayload {
    id: String,
    run_id: String,
    session_id: Option<String>,
    sequence: i32,
    event_type: String,
    data: String,
    created_at: i64,
    turn_id: Option<String>,
    input_tokens: Option<i32>,
    output_tokens: Option<i32>,
    cache_read_tokens: Option<i32>,
    cache_create_tokens: Option<i32>,
    thinking_tokens: Option<i32>,
}

pub fn get_next_sequence(db: Arc<LocalDb>, run_id: &str) -> Result<i32, String> {
    let run_id = run_id.to_string();
    block_on_stream_db(async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move { get_next_sequence_conn(conn, &run_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn insert_event(db: Arc<LocalDb>, event: EventInsert) -> Result<bool, String> {
    block_on_stream_db(async move {
        db.write(|conn| {
            let event = event.clone();
            Box::pin(async move {
                let count = insert_event_conn(conn, &event).await?;
                if count > 0 {
                    record_read_tokens_if_applicable(conn, &event).await?;
                }
                Ok(count > 0)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// At ingest, compute and cache per-target read token counts the moment a read
/// tool-result lands, so live transcript rows get a count without waiting for a
/// full reload (CAIRN-1593). The result event carries only its `tool_use_id`;
/// the originating tool name + input paths live on the assistant event recorded
/// earlier in the same run, so we look up the most recent assistant events and
/// match the id. Non-read results and unmatched ids are no-ops; the full-load
/// backfill in `apply_lean_read_projection` is the correctness backstop.
async fn record_read_tokens_if_applicable(conn: &Connection, event: &EventInsert) -> DbResult<()> {
    if event.event_type != "tool_result" {
        return Ok(());
    }
    let Ok(parsed) = serde_json::from_str::<TranscriptEvent>(&event.data) else {
        return Ok(());
    };
    let (Some(tool_use_id), Some(body)) = (parsed.tool_use_id, parsed.tool_result) else {
        return Ok(());
    };

    let mut rows = conn
        .query(
            "SELECT data FROM events
             WHERE run_id = ?1 AND event_type = 'assistant'
             ORDER BY rowid DESC
             LIMIT 8",
            params![event.run_id.as_str()],
        )
        .await?;
    let mut matched: Option<(bool, Vec<String>)> = None;
    while let Some(row) = rows.next().await? {
        let data = row.text(0)?;
        let Ok(assistant) = serde_json::from_str::<TranscriptEvent>(&data) else {
            continue;
        };
        if let Some(tool) = assistant
            .tool_uses
            .as_ref()
            .and_then(|uses| uses.iter().find(|tool| tool.id == tool_use_id))
        {
            matched = Some((
                crate::runs::read_tokens::is_read_tool(&tool.name),
                crate::runs::read_tokens::extract_paths(&tool.input),
            ));
            break;
        }
    }

    if let Some((true, expected)) = matched {
        let segments = crate::runs::read_tokens::read_segment_tokens(&body, &expected);
        let total: i64 = segments.iter().map(|seg| seg.tokens).sum();
        let json = serde_json::to_string(&segments).unwrap_or_default();
        conn.execute(
            "INSERT OR REPLACE INTO event_read_tokens
                (event_id, segments_json, total_tokens, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                event.id.as_str(),
                json.as_str(),
                total,
                event.created_at as i64
            ],
        )
        .await?;
    }
    Ok(())
}

pub async fn insert_event_conn(conn: &Connection, event: &EventInsert) -> DbResult<u64> {
    let count = conn
        .execute(
            "INSERT INTO events(
                id, run_id, session_id, sequence, timestamp, event_type, data,
                parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                cache_create_tokens, output_tokens, thinking_tokens, turn_id
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                event.id.as_str(),
                event.run_id.as_str(),
                event.session_id.as_deref(),
                event.sequence,
                event.timestamp,
                event.event_type.as_str(),
                event.data.as_str(),
                event.parent_tool_use_id.as_deref(),
                event.created_at,
                event.input_tokens,
                event.cache_read_tokens,
                event.cache_create_tokens,
                event.output_tokens,
                event.thinking_tokens,
                event.turn_id.as_deref()
            ],
        )
        .await?;
    Ok(count)
}

pub fn open_stream(
    db: Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    backend: &str,
    sequence: Option<i32>,
) -> Result<ActiveMessageStream, String> {
    let run_id = run_id.to_string();
    let session_id = session_id.map(str::to_string);
    let turn_id = turn_id.map(str::to_string);
    let backend = backend.to_string();
    block_on_stream_db(async move {
        db.write(|conn| {
            let run_id = run_id.clone();
            let session_id = session_id.clone();
            let turn_id = turn_id.clone();
            let backend = backend.clone();
            Box::pin(async move {
                let sequence = match sequence {
                    Some(sequence) => sequence,
                    None => get_next_sequence_conn(conn, &run_id).await?,
                };
                let now = chrono::Utc::now().timestamp() as i32;
                let stream_id = Uuid::new_v4().to_string();
                conn.execute(
                    "INSERT INTO message_streams(
                         id, run_id, session_id, turn_id, backend, sequence, status,
                         version, content_chars, thinking_chars, chunk_count,
                         final_event_id, abort_reason, created_at, updated_at, finalized_at
                     )
                     VALUES (
                         ?1, ?2, ?3, ?4, ?5, ?6, 'open',
                         0, 0, 0, 0,
                         NULL, NULL, ?7, ?8, NULL
                     )",
                    params![
                        stream_id.as_str(),
                        run_id.as_str(),
                        session_id.as_deref(),
                        turn_id.as_deref(),
                        backend.as_str(),
                        sequence,
                        now,
                        now
                    ],
                )
                .await?;
                let stream = load_stream_conn(conn, &stream_id).await?;
                Ok(ActiveMessageStream {
                    stream,
                    content: String::new(),
                    thinking: String::new(),
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn append_chunks(
    db: Arc<LocalDb>,
    stream_id: &str,
    expected_version: i32,
    chunks: &[StreamChunkInput],
) -> Result<AppendResult, String> {
    let stream_id = stream_id.to_string();
    let chunks = chunks.to_vec();
    block_on_stream_db(async move {
        db.write(|conn| {
            let stream_id = stream_id.clone();
            let chunks = chunks.clone();
            Box::pin(async move {
                let stream = load_stream_conn(conn, &stream_id).await?;
                ensure_writable(&stream, expected_version)?;
                if chunks.is_empty() {
                    return Ok(AppendResult {
                        version: stream.version,
                    });
                }

                let mut content_index =
                    next_chunk_index_conn(conn, &stream_id, StreamChunkKind::Content).await?;
                let mut thinking_index =
                    next_chunk_index_conn(conn, &stream_id, StreamChunkKind::Thinking).await?;
                let mut content_chars = 0;
                let mut thinking_chars = 0;
                let mut inserted_count = 0;

                for chunk in &chunks {
                    if chunk.data.is_empty() {
                        continue;
                    }
                    let chunk_index = match chunk.kind {
                        StreamChunkKind::Content => {
                            let idx = content_index;
                            content_index += 1;
                            content_chars += chunk.data.chars().count() as i32;
                            idx
                        }
                        StreamChunkKind::Thinking => {
                            let idx = thinking_index;
                            thinking_index += 1;
                            thinking_chars += chunk.data.chars().count() as i32;
                            idx
                        }
                    };
                    let chunk_id = format!("msc-{}", Uuid::new_v4());
                    conn.execute(
                        "INSERT INTO message_stream_chunks(
                             id, stream_id, kind, chunk_index, data, char_count, created_at
                         )
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            chunk_id.as_str(),
                            stream_id.as_str(),
                            chunk.kind.as_str(),
                            chunk_index,
                            chunk.data.as_str(),
                            chunk.data.chars().count() as i32,
                            chrono::Utc::now().timestamp() as i32
                        ],
                    )
                    .await?;
                    inserted_count += 1;
                }

                let now = chrono::Utc::now().timestamp() as i32;
                let new_version = stream.version + 1;
                conn.execute(
                    "UPDATE message_streams
                     SET version = ?1,
                         content_chars = ?2,
                         thinking_chars = ?3,
                         chunk_count = ?4,
                         updated_at = ?5
                     WHERE id = ?6",
                    params![
                        new_version,
                        stream.content_chars + content_chars,
                        stream.thinking_chars + thinking_chars,
                        stream.chunk_count + inserted_count,
                        now,
                        stream_id.as_str()
                    ],
                )
                .await?;

                Ok(AppendResult {
                    version: new_version,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn read_active_stream(
    db: Arc<LocalDb>,
    stream_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    let stream_id = stream_id.to_string();
    block_on_stream_db(async move {
        db.read(|conn| {
            let stream_id = stream_id.clone();
            Box::pin(async move { reconstruct_stream_conn(conn, &stream_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub async fn reconstruct_stream_conn(
    conn: &Connection,
    stream_id: &str,
) -> DbResult<Option<ActiveMessageStream>> {
    let Some(stream) = load_stream_optional_conn(conn, stream_id).await? else {
        return Ok(None);
    };
    let chunks = load_chunks_for_stream_conn(conn, stream_id).await?;
    Ok(Some(materialize_stream(stream, &chunks)))
}

pub fn finalize_stream(
    db: Arc<LocalDb>,
    stream_id: &str,
    expected_version: i32,
    final_event: Option<TranscriptEvent>,
    counts: TokenCounts,
) -> Result<FinalizedStream, String> {
    let stream_id = stream_id.to_string();
    block_on_stream_db(async move {
        db.write(|conn| {
            let stream_id = stream_id.clone();
            let final_event = final_event.clone();
            Box::pin(async move {
                let mut stream = load_stream_conn(conn, &stream_id).await?;
                if stream.status == "finalized" {
                    return load_finalized_stream_conn(conn, &stream).await;
                }
                if stream.status == "aborted" {
                    return Err(DbError::internal(format!(
                        "Stream {} already aborted",
                        stream_id
                    )));
                }
                if stream.version != expected_version {
                    return Err(stale_writer_error(
                        &stream_id,
                        expected_version,
                        stream.version,
                    ));
                }

                let chunks = load_chunks_for_stream_conn(conn, &stream_id).await?;
                let active = materialize_stream(stream.clone(), &chunks);

                let mut final_event = final_event.unwrap_or_else(|| TranscriptEvent {
                    event_type: "assistant".to_string(),
                    session_id: stream.session_id.clone(),
                    parent_tool_use_id: None,
                    content: None,
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    raw: None,
                });
                if final_event.content.is_none() && !active.content.is_empty() {
                    final_event.content = Some(active.content.clone());
                }
                if final_event.thinking.is_none() && !active.thinking.is_empty() {
                    final_event.thinking = Some(active.thinking.clone());
                }
                if final_event.session_id.is_none() {
                    final_event.session_id = stream.session_id.clone();
                }

                let data_json = serde_json::to_string(&final_event)
                    .map_err(|e| DbError::internal(e.to_string()))?;
                let event_id = stream
                    .final_event_id
                    .clone()
                    .unwrap_or_else(|| stream.id.clone());
                if !event_exists_conn(conn, &event_id).await? {
                    insert_event_conn(
                        conn,
                        &EventInsert {
                            id: event_id.clone(),
                            run_id: stream.run_id.clone(),
                            session_id: stream.session_id.clone(),
                            sequence: stream.sequence,
                            timestamp: stream.created_at,
                            event_type: final_event.event_type.clone(),
                            data: data_json.clone(),
                            parent_tool_use_id: final_event.parent_tool_use_id.clone(),
                            created_at: stream.created_at,
                            input_tokens: counts.input,
                            cache_read_tokens: counts.cache_read,
                            cache_create_tokens: counts.cache_create,
                            output_tokens: counts.output,
                            thinking_tokens: counts.thinking,
                            turn_id: stream.turn_id.clone(),
                        },
                    )
                    .await?;
                }

                let embed_payload = serde_json::to_string(&EmbedEventPayload {
                    event_id: event_id.clone(),
                    data_json: data_json.clone(),
                })
                .map_err(|e| DbError::internal(e.to_string()))?;
                let sync_payload = serde_json::to_string(&SyncEventPayload {
                    id: event_id.clone(),
                    run_id: stream.run_id.clone(),
                    session_id: stream.session_id.clone(),
                    sequence: stream.sequence,
                    event_type: final_event.event_type.clone(),
                    data: data_json.clone(),
                    created_at: stream.created_at as i64,
                    turn_id: stream.turn_id.clone(),
                    input_tokens: counts.input,
                    output_tokens: counts.output,
                    cache_read_tokens: counts.cache_read,
                    cache_create_tokens: counts.cache_create,
                    thinking_tokens: counts.thinking,
                })
                .map_err(|e| DbError::internal(e.to_string()))?;

                let embed_id = outbox::insert_pending_with_payload_conn(
                    conn,
                    "embed_event",
                    &event_id,
                    &embed_payload,
                )
                .await?;
                let sync_id = outbox::insert_pending_with_payload_conn(
                    conn,
                    "sync_event",
                    &event_id,
                    &sync_payload,
                )
                .await?;

                let now = chrono::Utc::now().timestamp() as i32;
                conn.execute(
                    "UPDATE message_streams
                     SET status = 'finalized',
                         version = ?1,
                         final_event_id = ?2,
                         updated_at = ?3,
                         finalized_at = ?4,
                         abort_reason = NULL
                     WHERE id = ?5",
                    params![
                        stream.version + 1,
                        event_id.as_str(),
                        now,
                        now,
                        stream_id.as_str()
                    ],
                )
                .await?;
                stream.status = "finalized".to_string();
                stream.version += 1;
                stream.final_event_id = Some(event_id.clone());
                stream.updated_at = now;
                stream.finalized_at = Some(now);
                stream.abort_reason = None;

                Ok(FinalizedStream {
                    stream_id: stream.id,
                    event_id: event_id.clone(),
                    sequence: stream.sequence,
                    created_at: stream.created_at,
                    turn_id: stream.turn_id,
                    event_type: final_event.event_type,
                    data_json,
                    outbox_entries: vec![
                        OutboxEntry {
                            id: embed_id,
                            kind: "embed_event".to_string(),
                            dedupe_key: event_id.clone(),
                            payload_json: embed_payload,
                        },
                        OutboxEntry {
                            id: sync_id,
                            kind: "sync_event".to_string(),
                            dedupe_key: event_id,
                            payload_json: sync_payload,
                        },
                    ],
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn abort_stream(
    db: Arc<LocalDb>,
    stream_id: &str,
    expected_version: i32,
    reason: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    let stream_id = stream_id.to_string();
    let reason = reason.to_string();
    block_on_stream_db(async move {
        db.write(|conn| {
            let stream_id = stream_id.clone();
            let reason = reason.clone();
            Box::pin(async move {
                let mut stream = match load_stream_optional_conn(conn, &stream_id).await? {
                    Some(stream) => stream,
                    None => return Ok(None),
                };
                if stream.status == "finalized" {
                    return reconstruct_stream_conn(conn, &stream_id)
                        .await?
                        .map(Some)
                        .ok_or_else(|| {
                            DbError::internal(format!("Missing finalized stream {}", stream_id))
                        });
                }
                if stream.status == "aborted" {
                    return reconstruct_stream_conn(conn, &stream_id).await;
                }
                if stream.version != expected_version {
                    return Err(stale_writer_error(
                        &stream_id,
                        expected_version,
                        stream.version,
                    ));
                }
                let now = chrono::Utc::now().timestamp() as i32;
                conn.execute(
                    "UPDATE message_streams
                     SET status = 'aborted',
                         version = ?1,
                         abort_reason = ?2,
                         updated_at = ?3,
                         finalized_at = ?4
                     WHERE id = ?5",
                    params![
                        stream.version + 1,
                        reason.as_str(),
                        now,
                        now,
                        stream_id.as_str()
                    ],
                )
                .await?;
                stream.status = "aborted".to_string();
                stream.version += 1;
                stream.abort_reason = Some(reason);
                stream.updated_at = now;
                stream.finalized_at = Some(now);
                let chunks = load_chunks_for_stream_conn(conn, &stream_id).await?;
                Ok(Some(materialize_stream(stream, &chunks)))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn list_recoverable_streams(db: Arc<LocalDb>) -> Result<Vec<ActiveMessageStream>, String> {
    block_on_stream_db(async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, run_id, session_id, turn_id, backend, sequence, status,
                                version, content_chars, thinking_chars, chunk_count,
                                final_event_id, abort_reason, created_at, updated_at, finalized_at
                         FROM message_streams
                         WHERE status IN ('open', 'finalizing')
                         ORDER BY created_at ASC",
                        (),
                    )
                    .await?;
                let mut streams = Vec::new();
                while let Some(row) = rows.next().await? {
                    let stream = stream_from_row(&row)?;
                    let chunks = load_chunks_for_stream_conn(conn, &stream.id).await?;
                    streams.push(materialize_stream(stream, &chunks));
                }
                Ok(streams)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn find_active_stream_for_run(
    db: Arc<LocalDb>,
    run_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    let run_id = run_id.to_string();
    block_on_stream_db(async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, run_id, session_id, turn_id, backend, sequence, status,
                                version, content_chars, thinking_chars, chunk_count,
                                final_event_id, abort_reason, created_at, updated_at, finalized_at
                         FROM message_streams
                         WHERE run_id = ?1
                           AND status IN ('open', 'finalizing')
                         ORDER BY created_at ASC
                         LIMIT 1",
                        params![run_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let stream = stream_from_row(&row)?;
                let chunks = load_chunks_for_stream_conn(conn, &stream.id).await?;
                Ok(Some(materialize_stream(stream, &chunks)))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn find_active_stream_for_session(
    db: Arc<LocalDb>,
    session_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    let session_id = session_id.to_string();
    block_on_stream_db(async move {
        db.read(|conn| {
            let session_id = session_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, run_id, session_id, turn_id, backend, sequence, status,
                                version, content_chars, thinking_chars, chunk_count,
                                final_event_id, abort_reason, created_at, updated_at, finalized_at
                         FROM message_streams
                         WHERE session_id = ?1
                           AND status IN ('open', 'finalizing')
                         ORDER BY created_at ASC
                         LIMIT 1",
                        params![session_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                let stream = stream_from_row(&row)?;
                let chunks = load_chunks_for_stream_conn(conn, &stream.id).await?;
                Ok(Some(materialize_stream(stream, &chunks)))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub fn process_post_commit_outbox(orch: &Orchestrator, entries: &[OutboxEntry]) {
    for entry in entries {
        let result = match entry.kind.as_str() {
            "embed_event" => process_embed_event(orch, entry),
            "sync_event" => process_sync_event(orch, entry),
            _ => continue,
        };
        if let Err(error) = result {
            outbox::mark_failed(orch.db.local.clone(), &entry.id, &error);
        }
    }
}

fn process_embed_event(orch: &Orchestrator, entry: &OutboxEntry) -> Result<(), String> {
    let payload: EmbedEventPayload =
        serde_json::from_str(&entry.payload_json).map_err(|e| e.to_string())?;
    if let Some(text) = crate::embeddings::extract_embeddable_text(&payload.data_json) {
        orch.enqueue_event_embed(&payload.event_id, text);
    }
    outbox::mark_done(orch.db.local.clone(), &entry.id);
    Ok(())
}

fn process_sync_event(orch: &Orchestrator, entry: &OutboxEntry) -> Result<(), String> {
    let payload: SyncEventPayload =
        serde_json::from_str(&entry.payload_json).map_err(|e| e.to_string())?;
    orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
        id: payload.id,
        run_id: payload.run_id,
        session_id: payload.session_id,
        sequence: Some(payload.sequence),
        event_type: payload.event_type,
        data: Some(payload.data),
        input_tokens: payload.input_tokens,
        output_tokens: payload.output_tokens,
        cache_read_tokens: payload.cache_read_tokens,
        cache_create_tokens: payload.cache_create_tokens,
        thinking_tokens: payload.thinking_tokens,
        created_at: Some(payload.created_at),
        turn_id: payload.turn_id,
    }));
    outbox::mark_done(orch.db.local.clone(), &entry.id);
    Ok(())
}

async fn get_next_sequence_conn(conn: &Connection, run_id: &str) -> DbResult<i32> {
    let event_max = max_sequence_conn(
        conn,
        "SELECT MAX(sequence) FROM events WHERE run_id = ?1",
        run_id,
    )
    .await?;
    let stream_max = max_sequence_conn(
        conn,
        "SELECT MAX(sequence) FROM message_streams WHERE run_id = ?1",
        run_id,
    )
    .await?;
    Ok(std::cmp::max(event_max, stream_max) + 1)
}

async fn max_sequence_conn(conn: &Connection, sql: &'static str, run_id: &str) -> DbResult<i32> {
    let mut rows = conn.query(sql, params![run_id]).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing max sequence row".to_string()))?;
    Ok(row.opt_i64(0)?.map(|value| value as i32).unwrap_or(-1))
}

async fn load_stream_conn(conn: &Connection, stream_id: &str) -> DbResult<DbMessageStream> {
    load_stream_optional_conn(conn, stream_id)
        .await?
        .ok_or_else(|| DbError::internal(format!("Missing stream {}", stream_id)))
}

async fn load_stream_optional_conn(
    conn: &Connection,
    stream_id: &str,
) -> DbResult<Option<DbMessageStream>> {
    let mut rows = conn
        .query(
            "SELECT id, run_id, session_id, turn_id, backend, sequence, status,
                    version, content_chars, thinking_chars, chunk_count,
                    final_event_id, abort_reason, created_at, updated_at, finalized_at
             FROM message_streams
             WHERE id = ?1",
            params![stream_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| stream_from_row(&row))
        .transpose()
}

async fn load_chunks_for_stream_conn(
    conn: &Connection,
    stream_id: &str,
) -> DbResult<Vec<DbMessageStreamChunk>> {
    let mut rows = conn
        .query(
            "SELECT id, stream_id, kind, chunk_index, data, char_count, created_at
             FROM message_stream_chunks
             WHERE stream_id = ?1
             ORDER BY kind ASC, chunk_index ASC",
            params![stream_id],
        )
        .await?;
    let mut chunks = Vec::new();
    while let Some(row) = rows.next().await? {
        chunks.push(chunk_from_row(&row)?);
    }
    Ok(chunks)
}

fn ensure_writable(stream: &DbMessageStream, expected_version: i32) -> DbResult<()> {
    if stream.status != "open" {
        return Err(DbError::internal(format!(
            "stream {} is not writable with status {}",
            stream.id, stream.status
        )));
    }
    if stream.version != expected_version {
        return Err(stale_writer_error(
            &stream.id,
            expected_version,
            stream.version,
        ));
    }
    Ok(())
}

fn stale_writer_error(stream_id: &str, expected_version: i32, found_version: i32) -> DbError {
    DbError::internal(format!(
        "stale stream writer for {}: expected version {}, found {}",
        stream_id, expected_version, found_version
    ))
}

async fn next_chunk_index_conn(
    conn: &Connection,
    stream_id: &str,
    kind: StreamChunkKind,
) -> DbResult<i32> {
    let mut rows = conn
        .query(
            "SELECT MAX(chunk_index)
             FROM message_stream_chunks
             WHERE stream_id = ?1
               AND kind = ?2",
            params![stream_id, kind.as_str()],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing chunk index row".to_string()))?;
    Ok(row.opt_i64(0)?.map(|value| value as i32).unwrap_or(-1) + 1)
}

fn materialize_stream(
    stream: DbMessageStream,
    chunks: &[DbMessageStreamChunk],
) -> ActiveMessageStream {
    let mut content = String::new();
    let mut thinking = String::new();
    for chunk in chunks {
        match chunk.kind.as_str() {
            "content" => content.push_str(&chunk.data),
            "thinking" => thinking.push_str(&chunk.data),
            _ => {}
        }
    }
    ActiveMessageStream {
        stream,
        content,
        thinking,
    }
}

async fn event_exists_conn(conn: &Connection, event_id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM events WHERE id = ?1 LIMIT 1",
            params![event_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

async fn load_finalized_stream_conn(
    conn: &Connection,
    stream: &DbMessageStream,
) -> DbResult<FinalizedStream> {
    let event_id = stream
        .final_event_id
        .clone()
        .unwrap_or_else(|| stream.id.clone());
    let mut rows = conn
        .query(
            "SELECT run_id, sequence, turn_id, event_type, data,
                    input_tokens, output_tokens, cache_read_tokens
             FROM events
             WHERE id = ?1",
            params![event_id.as_str()],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal(format!("Missing finalized event {}", event_id)))?;
    Ok(FinalizedStream {
        stream_id: stream.id.clone(),
        event_id,
        sequence: row.i64(1)? as i32,
        created_at: stream.created_at,
        turn_id: row.opt_text(2)?,
        event_type: row.text(3)?,
        data_json: row.text(4)?,
        outbox_entries: Vec::new(),
    })
}

fn stream_from_row(row: &Row) -> DbResult<DbMessageStream> {
    Ok(DbMessageStream {
        id: row.text(0)?,
        run_id: row.text(1)?,
        session_id: row.opt_text(2)?,
        turn_id: row.opt_text(3)?,
        backend: row.text(4)?,
        sequence: row.i64(5)? as i32,
        status: row.text(6)?,
        version: row.i64(7)? as i32,
        content_chars: row.i64(8)? as i32,
        thinking_chars: row.i64(9)? as i32,
        chunk_count: row.i64(10)? as i32,
        final_event_id: row.opt_text(11)?,
        abort_reason: row.opt_text(12)?,
        created_at: row.i64(13)? as i32,
        updated_at: row.i64(14)? as i32,
        finalized_at: row.opt_i64(15)?.map(|value| value as i32),
    })
}

fn chunk_from_row(row: &Row) -> DbResult<DbMessageStreamChunk> {
    Ok(DbMessageStreamChunk {
        id: row.text(0)?,
        stream_id: row.text(1)?,
        kind: row.text(2)?,
        chunk_index: row.i64(3)? as i32,
        data: row.text(4)?,
        char_count: row.i64(5)? as i32,
        created_at: row.i64(6)? as i32,
    })
}

fn block_on_stream_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_stream_db_future(future))
            .join()
            .map_err(|_| "Stream database task panicked".to_string())?
    } else {
        run_stream_db_future(future)
    }
}

fn run_stream_db_future<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Failed to create stream database runtime: {error}"))?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::{tempdir, TempDir};

    async fn test_db() -> (TempDir, Arc<LocalDb>) {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("stream-store-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        (temp, Arc::new(db))
    }

    async fn insert_run(db: &LocalDb, id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        db.write(|conn| {
            let id = id.to_string();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(
                         id, status, session_id, created_at, updated_at, backend
                     )
                     VALUES (?1, 'live', 'session-1', ?2, ?3, 'claude')",
                    params![id.as_str(), now, now],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stream_round_trip_and_reconstruct() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let opened = open_stream(
            db.clone(),
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        append_chunks(
            db.clone(),
            opened.stream_id(),
            opened.version(),
            &[
                StreamChunkInput::thinking("think "),
                StreamChunkInput::thinking("more"),
                StreamChunkInput::content("hello"),
            ],
        )
        .unwrap();
        // append_chunks no longer reconstructs; read the snapshot explicitly.
        let reconstructed = read_active_stream(db.clone(), opened.stream_id())
            .unwrap()
            .unwrap();
        assert_eq!(reconstructed.content, "hello");
        assert_eq!(reconstructed.thinking, "think more");
    }

    #[tokio::test]
    async fn stale_append_is_rejected() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let opened = open_stream(
            db.clone(),
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        append_chunks(
            db.clone(),
            opened.stream_id(),
            opened.version(),
            &[StreamChunkInput::content("hello")],
        )
        .unwrap();
        let err = append_chunks(
            db.clone(),
            opened.stream_id(),
            0,
            &[StreamChunkInput::content(" world")],
        )
        .unwrap_err();
        assert!(err.contains("stale stream writer"));
    }

    #[tokio::test]
    async fn finalize_is_idempotent() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let opened = open_stream(
            db.clone(),
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        let appended = append_chunks(
            db.clone(),
            opened.stream_id(),
            opened.version(),
            &[StreamChunkInput::content("hello")],
        )
        .unwrap();
        let first = finalize_stream(
            db.clone(),
            opened.stream_id(),
            appended.version,
            None,
            TokenCounts::default(),
        )
        .unwrap();
        let second = finalize_stream(
            db.clone(),
            opened.stream_id(),
            appended.version + 1,
            None,
            TokenCounts::default(),
        )
        .unwrap();
        assert_eq!(first.event_id, second.event_id);
        assert!(second.outbox_entries.is_empty());
    }

    #[tokio::test]
    async fn append_chunks_persists_buffered_without_reconstruct() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let opened = open_stream(
            db.clone(),
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        // A single flush carries several buffered chunks at once.
        let result = append_chunks(
            db.clone(),
            opened.stream_id(),
            opened.version(),
            &[
                StreamChunkInput::thinking("think "),
                StreamChunkInput::thinking("more"),
                StreamChunkInput::content("hello "),
                StreamChunkInput::content("world"),
            ],
        )
        .unwrap();
        assert_eq!(result.version, opened.version() + 1);
        let reconstructed = read_active_stream(db.clone(), opened.stream_id())
            .unwrap()
            .unwrap();
        assert_eq!(reconstructed.content, "hello world");
        assert_eq!(reconstructed.thinking, "think more");
        assert_eq!(reconstructed.stream.content_chars, 11);
        assert_eq!(reconstructed.stream.thinking_chars, 10);
        assert_eq!(reconstructed.stream.chunk_count, 4);
    }

    #[tokio::test]
    async fn finalize_after_buffered_flush_has_full_content() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let opened = open_stream(
            db.clone(),
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        let appended = append_chunks(
            db.clone(),
            opened.stream_id(),
            opened.version(),
            &[
                StreamChunkInput::content("hello "),
                StreamChunkInput::content("world"),
            ],
        )
        .unwrap();
        let finalized = finalize_stream(
            db.clone(),
            opened.stream_id(),
            appended.version,
            None,
            TokenCounts::default(),
        )
        .unwrap();
        assert!(finalized.data_json.contains("hello world"));
    }

    #[test]
    fn accumulator_push_and_emit_delta_tail() {
        let mut acc = StreamAccumulator::new();
        acc.push_content("hello ");
        acc.push_content("world");
        assert_eq!(acc.content_len, 11);
        assert!(!acc.pending_is_empty());
        assert!(acc.has_unemitted());

        let delta = acc.take_emit_delta();
        assert_eq!(delta.content_delta.as_deref(), Some("hello world"));
        assert_eq!(delta.content_len, 11);
        assert!(!acc.has_unemitted());

        // A second emit with no new content yields no delta but the same length.
        let empty = acc.take_emit_delta();
        assert_eq!(empty.content_delta, None);
        assert_eq!(empty.content_len, 11);

        // Further content emits only the new tail.
        acc.push_content("!");
        let tail = acc.take_emit_delta();
        assert_eq!(tail.content_delta.as_deref(), Some("!"));
        assert_eq!(tail.content_len, 12);
    }

    #[test]
    fn accumulator_emit_delta_is_scalar_aligned() {
        let mut acc = StreamAccumulator::new();
        acc.push_thinking("h\u{e9}llo"); // é is one scalar but multiple bytes
        acc.push_content("\u{1f642}a");
        let delta = acc.take_emit_delta();
        assert_eq!(delta.thinking_delta.as_deref(), Some("h\u{e9}llo"));
        assert_eq!(delta.thinking_len, 5);
        assert_eq!(delta.content_delta.as_deref(), Some("\u{1f642}a"));
        assert_eq!(delta.content_len, 2);

        // The tail slice after an emoji prefix stays on a scalar boundary.
        acc.push_content("\u{1f680}");
        let tail = acc.take_emit_delta();
        assert_eq!(tail.content_delta.as_deref(), Some("\u{1f680}"));
        assert_eq!(tail.content_len, 3);
    }

    #[test]
    fn accumulator_take_pending_drains_and_resets_flush() {
        let mut acc = StreamAccumulator::new();
        // No pending → never due to flush.
        assert!(!acc.should_flush(Instant::now()));
        acc.push_content("a");
        acc.push_thinking("b");
        // Interval has not elapsed yet.
        assert!(!acc.should_flush(Instant::now()));
        std::thread::sleep(FLUSH_INTERVAL + Duration::from_millis(20));
        assert!(acc.should_flush(Instant::now()));
        let pending = acc.take_pending();
        assert_eq!(pending.len(), 2);
        assert!(acc.pending_is_empty());
        // Drained → not due even though wall time has passed.
        assert!(!acc.should_flush(Instant::now()));
    }
}
