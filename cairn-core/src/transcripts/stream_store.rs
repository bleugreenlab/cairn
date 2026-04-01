use crate::agent_process::stream::TranscriptEvent;
use crate::diesel_models::{
    DbMessageStream, DbMessageStreamChunk, NewEvent, NewMessageStream, NewMessageStreamChunk,
};
use crate::effects::outbox::{self, OutboxEntry};
use crate::orchestrator::Orchestrator;
use crate::schema::{events, message_stream_chunks, message_streams};
use diesel::dsl::max;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::{Deserialize, Serialize};
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
            usage: None,
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
}

pub fn get_next_sequence(conn: &mut SqliteConnection, run_id: &str) -> Result<i32, String> {
    let event_max = events::table
        .filter(events::run_id.eq(run_id))
        .select(max(events::sequence))
        .first::<Option<i32>>(conn)
        .map_err(|e| e.to_string())?
        .unwrap_or(-1);
    let stream_max = message_streams::table
        .filter(message_streams::run_id.eq(run_id))
        .select(max(message_streams::sequence))
        .first::<Option<i32>>(conn)
        .map_err(|e| e.to_string())?
        .unwrap_or(-1);
    Ok(std::cmp::max(event_max, stream_max) + 1)
}

pub fn open_stream(
    conn: &mut SqliteConnection,
    run_id: &str,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    backend: &str,
    sequence: Option<i32>,
) -> Result<ActiveMessageStream, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    conn.transaction::<ActiveMessageStream, diesel::result::Error, _>(|conn| {
        let sequence = match sequence {
            Some(sequence) => sequence,
            None => get_next_sequence(conn, run_id).map_err(to_diesel_error)?,
        };
        let stream_id = Uuid::new_v4().to_string();
        let new_stream = NewMessageStream {
            id: &stream_id,
            run_id,
            session_id,
            turn_id,
            backend,
            sequence,
            status: "open",
            version: 0,
            content_chars: 0,
            thinking_chars: 0,
            chunk_count: 0,
            final_event_id: None,
            abort_reason: None,
            created_at: now,
            updated_at: now,
            finalized_at: None,
        };
        diesel::insert_into(message_streams::table)
            .values(&new_stream)
            .execute(conn)?;
        let stream = message_streams::table
            .find(&stream_id)
            .select(DbMessageStream::as_select())
            .first(conn)?;
        Ok(ActiveMessageStream {
            stream,
            content: String::new(),
            thinking: String::new(),
        })
    })
    .map_err(|e| e.to_string())
}

pub fn append_chunks(
    conn: &mut SqliteConnection,
    stream_id: &str,
    expected_version: i32,
    chunks: &[StreamChunkInput],
) -> Result<ActiveMessageStream, String> {
    conn.transaction(|conn| {
        let mut stream = load_stream(conn, stream_id)?;
        ensure_writable(&stream, expected_version)?;
        if chunks.is_empty() {
            return reconstruct_stream(conn, stream_id)?.ok_or_else(|| {
                to_diesel_error(format!("Missing stream {} after append", stream_id))
            });
        }

        let next_content_index = next_chunk_index(conn, stream_id, StreamChunkKind::Content)?;
        let next_thinking_index = next_chunk_index(conn, stream_id, StreamChunkKind::Thinking)?;
        let mut content_index = next_content_index;
        let mut thinking_index = next_thinking_index;
        let mut content_chars = 0;
        let mut thinking_chars = 0;

        for chunk in chunks {
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
            let new_chunk = NewMessageStreamChunk {
                id: &chunk_id,
                stream_id,
                kind: chunk.kind.as_str(),
                chunk_index,
                data: &chunk.data,
                char_count: chunk.data.chars().count() as i32,
                created_at: chrono::Utc::now().timestamp() as i32,
            };
            diesel::insert_into(message_stream_chunks::table)
                .values(&new_chunk)
                .execute(conn)?;
        }

        let now = chrono::Utc::now().timestamp() as i32;
        let new_version = stream.version + 1;
        diesel::update(message_streams::table.find(stream_id))
            .set((
                message_streams::version.eq(new_version),
                message_streams::content_chars.eq(stream.content_chars + content_chars),
                message_streams::thinking_chars.eq(stream.thinking_chars + thinking_chars),
                message_streams::chunk_count.eq(stream.chunk_count + chunks.len() as i32),
                message_streams::updated_at.eq(now),
            ))
            .execute(conn)?;

        stream.version = new_version;
        stream.content_chars += content_chars;
        stream.thinking_chars += thinking_chars;
        stream.chunk_count += chunks.len() as i32;
        stream.updated_at = now;

        reconstruct_stream(conn, stream_id)?
            .ok_or_else(|| to_diesel_error(format!("Missing stream {} after append", stream_id)))
    })
    .map_err(|e| e.to_string())
}

pub fn read_active_stream(
    conn: &mut SqliteConnection,
    stream_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    reconstruct_stream(conn, stream_id).map_err(|e| e.to_string())
}

pub fn reconstruct_stream(
    conn: &mut SqliteConnection,
    stream_id: &str,
) -> Result<Option<ActiveMessageStream>, diesel::result::Error> {
    let stream = message_streams::table
        .find(stream_id)
        .select(DbMessageStream::as_select())
        .first(conn)
        .optional()?;
    let Some(stream) = stream else {
        return Ok(None);
    };
    let chunks: Vec<DbMessageStreamChunk> = message_stream_chunks::table
        .filter(message_stream_chunks::stream_id.eq(stream_id))
        .order((
            message_stream_chunks::kind.asc(),
            message_stream_chunks::chunk_index.asc(),
        ))
        .select(DbMessageStreamChunk::as_select())
        .load(conn)?;
    Ok(Some(materialize_stream(stream, &chunks)))
}

pub fn finalize_stream(
    conn: &mut SqliteConnection,
    stream_id: &str,
    expected_version: i32,
    final_event: Option<TranscriptEvent>,
) -> Result<FinalizedStream, String> {
    conn.transaction(|conn| {
        let mut stream = load_stream(conn, stream_id)?;
        if stream.status == "finalized" {
            return load_finalized_stream(conn, &stream);
        }
        if stream.status == "aborted" {
            return Err(to_diesel_error(format!(
                "Stream {} already aborted",
                stream_id
            )));
        }
        if stream.version != expected_version {
            return Err(to_diesel_error(format!(
                "stale stream writer for {}: expected version {}, found {}",
                stream_id, expected_version, stream.version
            )));
        }

        let chunks: Vec<DbMessageStreamChunk> = message_stream_chunks::table
            .filter(message_stream_chunks::stream_id.eq(stream_id))
            .order((
                message_stream_chunks::kind.asc(),
                message_stream_chunks::chunk_index.asc(),
            ))
            .select(DbMessageStreamChunk::as_select())
            .load(conn)?;
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
            usage: None,
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

        let data_json =
            serde_json::to_string(&final_event).map_err(|e| to_diesel_error(e.to_string()))?;
        let event_id = stream
            .final_event_id
            .clone()
            .unwrap_or_else(|| stream.id.clone());
        let event_exists = events::table
            .find(&event_id)
            .select(events::id)
            .first::<String>(conn)
            .optional()?
            .is_some();
        if !event_exists {
            let (input_tokens, cache_read_tokens, cache_create_tokens, output_tokens) =
                if let Some(ref usage) = final_event.usage {
                    (
                        Some(usage.input_tokens as i32),
                        usage.cache_read_input_tokens.map(|t| t as i32),
                        usage.cache_creation_input_tokens.map(|t| t as i32),
                        Some(usage.output_tokens as i32),
                    )
                } else {
                    (None, None, None, None)
                };
            let new_event = NewEvent {
                id: &event_id,
                run_id: &stream.run_id,
                session_id: stream.session_id.as_deref(),
                sequence: stream.sequence,
                timestamp: stream.created_at,
                event_type: &final_event.event_type,
                data: &data_json,
                parent_tool_use_id: final_event.parent_tool_use_id.as_deref(),
                created_at: stream.created_at,
                input_tokens,
                cache_read_tokens,
                cache_create_tokens,
                output_tokens,
                turn_id: stream.turn_id.as_deref(),
            };
            diesel::insert_into(events::table)
                .values(&new_event)
                .execute(conn)?;
        }

        let embed_payload = serde_json::to_string(&EmbedEventPayload {
            event_id: event_id.clone(),
            data_json: data_json.clone(),
        })
        .map_err(|e| to_diesel_error(e.to_string()))?;
        let sync_payload = serde_json::to_string(&SyncEventPayload {
            id: event_id.clone(),
            run_id: stream.run_id.clone(),
            session_id: stream.session_id.clone(),
            sequence: stream.sequence,
            event_type: final_event.event_type.clone(),
            data: data_json.clone(),
            created_at: stream.created_at as i64,
            turn_id: stream.turn_id.clone(),
            input_tokens: final_event.usage.as_ref().map(|u| u.input_tokens as i32),
            output_tokens: final_event.usage.as_ref().map(|u| u.output_tokens as i32),
            cache_read_tokens: final_event
                .usage
                .as_ref()
                .and_then(|u| u.cache_read_input_tokens.map(|t| t as i32)),
        })
        .map_err(|e| to_diesel_error(e.to_string()))?;

        let embed_id =
            outbox::insert_pending_with_payload(conn, "embed_event", &event_id, &embed_payload)?;
        let sync_id =
            outbox::insert_pending_with_payload(conn, "sync_event", &event_id, &sync_payload)?;
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(message_streams::table.find(stream_id))
            .set((
                message_streams::status.eq("finalized"),
                message_streams::version.eq(stream.version + 1),
                message_streams::final_event_id.eq(Some(event_id.as_str())),
                message_streams::updated_at.eq(now),
                message_streams::finalized_at.eq(Some(now)),
                message_streams::abort_reason.eq::<Option<&str>>(None),
            ))
            .execute(conn)?;
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
    .map_err(|e| e.to_string())
}

pub fn abort_stream(
    conn: &mut SqliteConnection,
    stream_id: &str,
    expected_version: i32,
    reason: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    conn.transaction(|conn| {
        let mut stream = match load_stream_optional(conn, stream_id)? {
            Some(stream) => stream,
            None => return Ok(None),
        };
        if stream.status == "finalized" {
            return Ok(Some(reconstruct_stream(conn, stream_id)?.ok_or_else(
                || to_diesel_error(format!("Missing finalized stream {}", stream_id)),
            )?));
        }
        if stream.status == "aborted" {
            return Ok(reconstruct_stream(conn, stream_id)?);
        }
        if stream.version != expected_version {
            return Err(to_diesel_error(format!(
                "stale stream writer for {}: expected version {}, found {}",
                stream_id, expected_version, stream.version
            )));
        }
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(message_streams::table.find(stream_id))
            .set((
                message_streams::status.eq("aborted"),
                message_streams::version.eq(stream.version + 1),
                message_streams::abort_reason.eq(Some(reason)),
                message_streams::updated_at.eq(now),
                message_streams::finalized_at.eq(Some(now)),
            ))
            .execute(conn)?;
        stream.status = "aborted".to_string();
        stream.version += 1;
        stream.abort_reason = Some(reason.to_string());
        stream.updated_at = now;
        stream.finalized_at = Some(now);
        let chunks: Vec<DbMessageStreamChunk> = message_stream_chunks::table
            .filter(message_stream_chunks::stream_id.eq(stream_id))
            .order((
                message_stream_chunks::kind.asc(),
                message_stream_chunks::chunk_index.asc(),
            ))
            .select(DbMessageStreamChunk::as_select())
            .load(conn)?;
        Ok(Some(materialize_stream(stream, &chunks)))
    })
    .map_err(|e| e.to_string())
}

pub fn list_recoverable_streams(
    conn: &mut SqliteConnection,
) -> Result<Vec<ActiveMessageStream>, String> {
    let streams: Vec<DbMessageStream> = message_streams::table
        .filter(message_streams::status.eq_any(["open", "finalizing"]))
        .order(message_streams::created_at.asc())
        .select(DbMessageStream::as_select())
        .load(conn)
        .map_err(|e| e.to_string())?;
    let mut result = Vec::with_capacity(streams.len());
    for stream in streams {
        let chunks: Vec<DbMessageStreamChunk> = message_stream_chunks::table
            .filter(message_stream_chunks::stream_id.eq(&stream.id))
            .order((
                message_stream_chunks::kind.asc(),
                message_stream_chunks::chunk_index.asc(),
            ))
            .select(DbMessageStreamChunk::as_select())
            .load(conn)
            .map_err(|e| e.to_string())?;
        result.push(materialize_stream(stream, &chunks));
    }
    Ok(result)
}

pub fn find_active_stream_for_run(
    conn: &mut SqliteConnection,
    run_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    let stream = message_streams::table
        .filter(message_streams::run_id.eq(run_id))
        .filter(message_streams::status.eq_any(["open", "finalizing"]))
        .order(message_streams::created_at.asc())
        .select(DbMessageStream::as_select())
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;
    match stream {
        Some(stream) => {
            let chunks: Vec<DbMessageStreamChunk> = message_stream_chunks::table
                .filter(message_stream_chunks::stream_id.eq(&stream.id))
                .order((
                    message_stream_chunks::kind.asc(),
                    message_stream_chunks::chunk_index.asc(),
                ))
                .select(DbMessageStreamChunk::as_select())
                .load(conn)
                .map_err(|e| e.to_string())?;
            Ok(Some(materialize_stream(stream, &chunks)))
        }
        None => Ok(None),
    }
}

pub fn find_active_stream_for_session(
    conn: &mut SqliteConnection,
    session_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    let stream = message_streams::table
        .filter(message_streams::session_id.eq(session_id))
        .filter(message_streams::status.eq_any(["open", "finalizing"]))
        .order(message_streams::created_at.asc())
        .select(DbMessageStream::as_select())
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;
    match stream {
        Some(stream) => {
            let chunks: Vec<DbMessageStreamChunk> = message_stream_chunks::table
                .filter(message_stream_chunks::stream_id.eq(&stream.id))
                .order((
                    message_stream_chunks::kind.asc(),
                    message_stream_chunks::chunk_index.asc(),
                ))
                .select(DbMessageStreamChunk::as_select())
                .load(conn)
                .map_err(|e| e.to_string())?;
            Ok(Some(materialize_stream(stream, &chunks)))
        }
        None => Ok(None),
    }
}

pub fn process_post_commit_outbox(orch: &Orchestrator, entries: &[OutboxEntry]) {
    for entry in entries {
        let result = match entry.kind.as_str() {
            "embed_event" => process_embed_event(orch, entry),
            "sync_event" => process_sync_event(orch, entry),
            _ => continue,
        };
        if let Err(error) = result {
            if let Ok(mut conn) = orch.db.conn.lock() {
                outbox::mark_failed(&mut conn, &entry.id, &error);
            }
        }
    }
}

fn process_embed_event(orch: &Orchestrator, entry: &OutboxEntry) -> Result<(), String> {
    let payload: EmbedEventPayload =
        serde_json::from_str(&entry.payload_json).map_err(|e| e.to_string())?;
    if let Some(ref engine) = orch.embedding_engine {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        crate::embeddings::embed_event_inline(
            engine,
            &mut conn,
            &payload.event_id,
            &payload.data_json,
        );
        outbox::mark_done(&mut conn, &entry.id);
    } else if let Ok(mut conn) = orch.db.conn.lock() {
        outbox::mark_done(&mut conn, &entry.id);
    }
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
        created_at: Some(payload.created_at),
        turn_id: payload.turn_id,
    }));
    if let Ok(mut conn) = orch.db.conn.lock() {
        outbox::mark_done(&mut conn, &entry.id);
    }
    Ok(())
}

fn load_stream(
    conn: &mut SqliteConnection,
    stream_id: &str,
) -> Result<DbMessageStream, diesel::result::Error> {
    message_streams::table
        .find(stream_id)
        .select(DbMessageStream::as_select())
        .first(conn)
}

fn load_stream_optional(
    conn: &mut SqliteConnection,
    stream_id: &str,
) -> Result<Option<DbMessageStream>, diesel::result::Error> {
    message_streams::table
        .find(stream_id)
        .select(DbMessageStream::as_select())
        .first(conn)
        .optional()
}

fn ensure_writable(
    stream: &DbMessageStream,
    expected_version: i32,
) -> Result<(), diesel::result::Error> {
    if stream.status != "open" {
        return Err(to_diesel_error(format!(
            "stream {} is not writable with status {}",
            stream.id, stream.status
        )));
    }
    if stream.version != expected_version {
        return Err(to_diesel_error(format!(
            "stale stream writer for {}: expected version {}, found {}",
            stream.id, expected_version, stream.version
        )));
    }
    Ok(())
}

fn next_chunk_index(
    conn: &mut SqliteConnection,
    stream_id: &str,
    kind: StreamChunkKind,
) -> Result<i32, diesel::result::Error> {
    let current = message_stream_chunks::table
        .filter(message_stream_chunks::stream_id.eq(stream_id))
        .filter(message_stream_chunks::kind.eq(kind.as_str()))
        .select(max(message_stream_chunks::chunk_index))
        .first::<Option<i32>>(conn)?
        .unwrap_or(-1);
    Ok(current + 1)
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

fn load_finalized_stream(
    conn: &mut SqliteConnection,
    stream: &DbMessageStream,
) -> Result<FinalizedStream, diesel::result::Error> {
    let event_id = stream
        .final_event_id
        .clone()
        .unwrap_or_else(|| stream.id.clone());
    let event: (
        String,
        i32,
        Option<String>,
        String,
        String,
        Option<i32>,
        Option<i32>,
        Option<i32>,
    ) = events::table
        .find(&event_id)
        .select((
            events::run_id,
            events::sequence,
            events::turn_id,
            events::event_type,
            events::data,
            events::input_tokens,
            events::output_tokens,
            events::cache_read_tokens,
        ))
        .first(conn)?;
    Ok(FinalizedStream {
        stream_id: stream.id.clone(),
        event_id,
        sequence: event.1,
        created_at: stream.created_at,
        turn_id: event.2,
        event_type: event.3,
        data_json: event.4,
        outbox_entries: Vec::new(),
    })
}

fn to_diesel_error(message: impl Into<String>) -> diesel::result::Error {
    diesel::result::Error::QueryBuilderError(message.into().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewRun;
    use crate::schema::runs;
    use crate::test_utils::test_diesel_conn;

    fn insert_run(conn: &mut SqliteConnection, id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(runs::table)
            .values(&NewRun {
                id,
                issue_id: None,
                project_id: None,
                job_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                chat_id: None,
                backend: Some("claude"),
                exit_reason: None,
                start_mode: None,
            })
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn stream_round_trip_and_reconstruct() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1");
        let active = open_stream(
            &mut conn,
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        let active = append_chunks(
            &mut conn,
            active.stream_id(),
            active.version(),
            &[
                StreamChunkInput::thinking("think "),
                StreamChunkInput::thinking("more"),
                StreamChunkInput::content("hello"),
            ],
        )
        .unwrap();
        assert_eq!(active.content, "hello");
        assert_eq!(active.thinking, "think more");
        let reconstructed = read_active_stream(&mut conn, active.stream_id())
            .unwrap()
            .unwrap();
        assert_eq!(reconstructed.content, "hello");
        assert_eq!(reconstructed.thinking, "think more");
    }

    #[test]
    fn stale_append_is_rejected() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1");
        let active = open_stream(
            &mut conn,
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        let active = append_chunks(
            &mut conn,
            active.stream_id(),
            active.version(),
            &[StreamChunkInput::content("hello")],
        )
        .unwrap();
        let err = append_chunks(
            &mut conn,
            active.stream_id(),
            0,
            &[StreamChunkInput::content(" world")],
        )
        .unwrap_err();
        assert!(err.contains("stale stream writer"));
    }

    #[test]
    fn finalize_is_idempotent() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1");
        let active = open_stream(
            &mut conn,
            "run-1",
            Some("session-1"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        let active = append_chunks(
            &mut conn,
            active.stream_id(),
            active.version(),
            &[StreamChunkInput::content("hello")],
        )
        .unwrap();
        let first = finalize_stream(&mut conn, active.stream_id(), active.version(), None).unwrap();
        let second =
            finalize_stream(&mut conn, active.stream_id(), active.version() + 1, None).unwrap();
        assert_eq!(first.event_id, second.event_id);
        assert!(second.outbox_entries.is_empty());
    }
}
