use crate::agent_process::stream::TranscriptEvent as StoredTranscriptEvent;
use crate::error::CairnError;
use crate::models::{Event, Prompt, Run, RunStatus};
use crate::models::{RunTodos, TodoItem};
use crate::runs::read_tokens::ReadSegmentTokens;
use crate::storage::{DbResult, LocalDb, RowExt};
use crate::transcripts::stream_store::ActiveMessageStream;
use crate::transcripts::stream_store::{
    find_active_stream_for_run, find_active_stream_for_session, read_active_stream,
};
use cairn_db::turso::{params, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastEventDigest {
    #[serde(rename = "type")]
    pub kind: String,
    pub content: String,
    pub tool_name: Option<String>,
    pub is_pending: bool,
}

/// Incremental session-event delta. Durable events insert monotonically per
/// session (runs execute sequentially; once a new run starts, older runs never
/// insert), so `WHERE session_id = ? AND rowid > ?` is a sound append-only
/// delta even across run boundaries.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventsDelta {
    /// New durable events. On the initial load (no cursor) these are
    /// run-ordered; on a delta they are in insertion (rowid) order.
    pub events: Vec<Event>,
    /// Max rowid of all durable events returned; echoes the cursor when the
    /// delta is empty so the caller can keep its position. `None` only when the
    /// session has no durable events yet.
    pub last_rowid: Option<i64>,
    /// Active-stream placeholder, returned separately rather than spliced into
    /// `events` so the frontend merge stays append-only.
    pub streaming: Option<Event>,
}

fn active_stream_to_event(active: ActiveMessageStream) -> Event {
    Event {
        id: active.stream.id.clone(),
        run_id: active.stream.run_id.clone(),
        session_id: active.stream.session_id.clone(),
        sequence: active.stream.sequence,
        timestamp: active.stream.created_at as i64,
        event_type: "assistant:streaming".to_string(),
        data: active.to_streaming_event_json(),
        parent_tool_use_id: None,
        created_at: active.stream.created_at as i64,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        turn_id: active.stream.turn_id.clone(),
        thinking_tokens: None,
        cost_usd: None,
        storage_mode: None,
        content_commit: None,
        content_change_id: None,
        content_render_sha: None,
        data_blob: None,
        codec: None,
        read_segments: None,
    }
}

fn insert_active_stream_for_session(
    events: &mut Vec<Event>,
    active: ActiveMessageStream,
    run_position: &std::collections::HashMap<String, usize>,
) {
    let active_event = active_stream_to_event(active);
    let active_position = run_position
        .get(&active_event.run_id)
        .copied()
        .unwrap_or(usize::MAX);
    let insert_at = events
        .iter()
        .rposition(|event| {
            let event_position = run_position
                .get(&event.run_id)
                .copied()
                .unwrap_or(usize::MAX);
            event_position < active_position
                || (event_position == active_position
                    && event.created_at <= active_event.created_at)
        })
        .map(|idx| idx + 1)
        .unwrap_or(0);
    events.insert(insert_at, active_event);
}

fn insert_active_stream_by_created_at(events: &mut Vec<Event>, active: ActiveMessageStream) {
    let active_event = active_stream_to_event(active);
    let insert_at = events
        .iter()
        .rposition(|event| event.created_at <= active_event.created_at)
        .map(|idx| idx + 1)
        .unwrap_or(0);
    events.insert(insert_at, active_event);
}

/// Canonical column projection for the `runs` table, in the exact order
/// [`run_from_row`] reads them. This is the single source of truth for the runs
/// projection: every `SELECT ... FROM runs` that maps to a [`Run`] builds its
/// column list from here, so a `runs`-column change (such as migration 0081
/// dropping `backend`) cannot silently drift across duplicate copies.
pub(crate) const RUN_COLUMNS: &str =
    "id, issue_id, project_id, job_id, status, session_id, error_message,
    started_at, exited_at, created_at, updated_at, chat_id, exit_reason, start_mode";

// The event-column contract lives in `storage` so the read path (the storage
// search index) can reach it without an upward edge. Re-exported here so this
// module's own SQL and every existing `crate::runs::queries::EVENT_COLUMNS` /
// `event_from_row` consumer (analytics, archival, transcript) keeps compiling
// unchanged.
pub(crate) use crate::storage::events::columns::{
    event_from_row, EVENT_COLUMNS, EVENT_COLUMN_COUNT,
};

const PROMPT_COLUMNS: &str = "id, run_id, questions, response, created_at, answered_at, turn_id";

#[derive(Debug, Default)]
struct ReconcileResult {
    stale_run_count: usize,
    turn_count: usize,
}

pub fn list_runs(db: Arc<LocalDb>, issue_id: &str) -> Result<Vec<Run>, CairnError> {
    let issue_id = issue_id.to_string();
    run_query_db(async move {
        load_runs_by_sql(
            &db,
            "SELECT id, issue_id, project_id, job_id, status, session_id, error_message,
                    started_at, exited_at, created_at, updated_at, chat_id, exit_reason,
                    start_mode
             FROM runs
             WHERE issue_id = ?1
             ORDER BY created_at DESC",
            issue_id,
        )
        .await
    })
}

pub fn list_runs_for_job(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<Run>, CairnError> {
    let job_id = job_id.to_string();
    run_query_db(async move {
        load_runs_by_sql(
            &db,
            "SELECT id, issue_id, project_id, job_id, status, session_id, error_message,
                    started_at, exited_at, created_at, updated_at, chat_id, exit_reason,
                    start_mode
             FROM runs
             WHERE job_id = ?1
             ORDER BY created_at DESC",
            job_id,
        )
        .await
    })
}

pub fn get_run(db: Arc<LocalDb>, id: &str) -> Result<Option<Run>, CairnError> {
    let id = id.to_string();
    run_query_db(async move { load_run(&db, id).await })
}

pub fn list_events(db: Arc<LocalDb>, run_id: &str) -> Result<Vec<Event>, CairnError> {
    let run_id = run_id.to_string();
    let query_db = db.clone();
    let query_run_id = run_id.clone();
    let mut events = run_query_db(async move {
        let events = load_events_for_run(&query_db, query_run_id).await?;
        let mut events = crate::storage::reconstruct_events(&query_db, events).await;
        apply_lean_read_projection(&query_db, &mut events).await?;
        Ok(events)
    })?;

    if let Some(active) = find_active_stream_for_run(db, &run_id).map_err(CairnError::Internal)? {
        events.push(active_stream_to_event(active));
        events.sort_by_key(|event| (event.sequence, event.created_at));
    }
    Ok(events)
}

pub fn list_events_limited(
    db: Arc<LocalDb>,
    run_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Event>, CairnError> {
    let run_id = run_id.to_string();
    run_query_db(async move {
        let events = load_events_limited(&db, run_id, limit, offset).await?;
        Ok(crate::storage::reconstruct_events(&db, events).await)
    })
}

pub fn list_events_for_session(
    db: Arc<LocalDb>,
    session_id: &str,
) -> Result<Vec<Event>, CairnError> {
    let session_id = session_id.to_string();
    let query_db = db.clone();
    let query_session_id = session_id.clone();
    let (mut events, run_position) = run_query_db(async move {
        let (events, run_position) = load_events_for_session(&query_db, query_session_id).await?;
        let events = crate::storage::reconstruct_events(&query_db, events).await;
        Ok((events, run_position))
    })?;

    if let Some(active) =
        find_active_stream_for_session(db, &session_id).map_err(CairnError::Internal)?
    {
        insert_active_stream_for_session(&mut events, active, &run_position);
    }

    Ok(events)
}

/// Load full-body durable session events after the caller's cached event count.
///
/// Session events append monotonically in rowid order while a session is active,
/// which is the same invariant used by `list_events_for_session_delta`. Unlike
/// that transcript-facing delta, this keeps read result bodies intact so skyline
/// bar widths can be computed without re-reading and re-parsing the whole session.
pub fn list_events_for_session_after_count(
    db: Arc<LocalDb>,
    session_id: &str,
    cached_event_count: i32,
) -> Result<Vec<Event>, CairnError> {
    let session_id = session_id.to_string();
    let offset = i64::from(cached_event_count.max(0));
    run_query_db(async move {
        let mut events = load_events_for_session_after_count(&db, session_id, offset).await?;
        events = crate::storage::reconstruct_events(&db, events).await;
        Ok(events)
    })
}

pub fn list_events_for_turn(db: Arc<LocalDb>, turn_id: &str) -> Result<Vec<Event>, CairnError> {
    let turn_id = turn_id.to_string();
    let query_db = db.clone();
    let query_turn_id = turn_id.clone();
    let mut events = run_query_db(async move {
        let events = load_events_for_turn(&query_db, query_turn_id).await?;
        Ok(crate::storage::reconstruct_events(&query_db, events).await)
    })?;
    if let Some(active) = message_streams_for_turn(db, &turn_id)? {
        insert_active_stream_by_created_at(&mut events, active);
    }
    Ok(events)
}

/// Incremental session-event loader. `after_rowid = None` returns the full
/// run-ordered history plus a cursor; `after_rowid = Some(r)` returns only
/// events inserted after the cursor, in append order. The active-stream
/// placeholder is returned in its own field rather than spliced into `events`.
pub fn list_events_for_session_delta(
    db: Arc<LocalDb>,
    session_id: &str,
    after_rowid: Option<i64>,
) -> Result<SessionEventsDelta, CairnError> {
    let session_id = session_id.to_string();
    let query_db = db.clone();
    let query_session_id = session_id.clone();
    let (events, last_rowid) = run_query_db(async move {
        let (events, last_rowid) =
            load_session_events_delta(&query_db, query_session_id, after_rowid).await?;
        let mut events = crate::storage::reconstruct_events(&query_db, events).await;
        apply_lean_read_projection(&query_db, &mut events).await?;
        Ok((events, last_rowid))
    })?;

    let streaming = find_active_stream_for_session(db, &session_id)
        .map_err(CairnError::Internal)?
        .map(active_stream_to_event);

    Ok(SessionEventsDelta {
        events,
        last_rowid,
        streaming,
    })
}

pub fn last_event_for_session(
    db: Arc<LocalDb>,
    session_id: &str,
) -> Result<Option<LastEventDigest>, CairnError> {
    let session_id = session_id.to_string();
    run_query_db(async move { load_last_event_for_session(&db, &session_id).await })
}

pub fn get_pending_prompt(db: Arc<LocalDb>, run_id: &str) -> Result<Option<Prompt>, CairnError> {
    let run_id = run_id.to_string();
    run_query_db(async move { load_pending_prompt(&db, run_id).await })
}

pub fn get_pending_prompt_for_job(
    db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Option<Prompt>, CairnError> {
    let job_id = job_id.to_string();
    run_query_db(async move { load_pending_prompt_for_job(&db, job_id).await })
}

pub fn get_job_todos(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<TodoItem>, CairnError> {
    let job_id = job_id.to_string();
    run_query_db(async move {
        crate::todos::get_todos_for_job(&db, &job_id)
            .await
            .map_err(CairnError::Internal)
    })
}

pub fn get_job_todos_by_run(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<RunTodos>, CairnError> {
    let todos = get_job_todos(db, job_id)?;
    if todos.is_empty() {
        return Ok(vec![]);
    }
    Ok(vec![RunTodos {
        run_id: String::new(),
        run_number: 1,
        todos,
    }])
}

pub fn reconcile_stale_runs(db: Arc<LocalDb>, emitter: &dyn crate::services::EventEmitter) {
    match run_query_db(reconcile_stale_runs_db(db)) {
        Ok(result) if result.stale_run_count == 0 => {}
        Ok(result) => {
            log::info!(
                "Reconciled {} stale runs on startup",
                result.stale_run_count
            );
            // Intentionally bare: this startup sweep reconciles many stale runs
            // at once and carries no ids, so the frontend broad-invalidates
            // ["runs"]. (See `crate::notify::run_db_change_ids`.)
            let _ = emitter.emit(
                "db-change",
                serde_json::json!({"table": "runs", "action": "update"}),
            );
            if result.turn_count > 0 {
                let _ = emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "turns", "action": "update"}),
                );
            }
        }
        Err(err) => {
            log::warn!("Failed to reconcile stale runs on startup: {}", err);
        }
    }
}

fn run_query_db<T, Fut>(future: Fut) -> Result<T, CairnError>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, CairnError>> + Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                CairnError::Internal(format!("Failed to start run database runtime: {e}"))
            })?
            .block_on(future)
    })
    .join()
    .map_err(|_| CairnError::Internal("Run database task panicked".to_string()))?
}

async fn load_runs_by_sql(
    db: &LocalDb,
    sql: &'static str,
    value: String,
) -> Result<Vec<Run>, CairnError> {
    db.query_all(sql, params![value.as_str()], run_from_row)
        .await
        .map_err(CairnError::from)
}

async fn load_run(db: &LocalDb, id: String) -> Result<Option<Run>, CairnError> {
    db.query_opt(
        format!("SELECT {RUN_COLUMNS} FROM runs WHERE id = ?1"),
        params![id.as_str()],
        run_from_row,
    )
    .await
    .map_err(CairnError::from)
}

async fn load_events_for_run(db: &LocalDb, run_id: String) -> Result<Vec<Event>, CairnError> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS}
             FROM events
             WHERE run_id = ?1
             ORDER BY sequence ASC"
        ),
        params![run_id.as_str()],
        event_from_row,
    )
    .await
    .map_err(CairnError::from)
}

async fn load_events_limited(
    db: &LocalDb,
    run_id: String,
    limit: i64,
    offset: i64,
) -> Result<Vec<Event>, CairnError> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS}
             FROM events
             WHERE run_id = ?1
             ORDER BY sequence ASC
             LIMIT ?2 OFFSET ?3"
        ),
        params![run_id.as_str(), limit, offset],
        event_from_row,
    )
    .await
    .map_err(CairnError::from)
}

async fn load_last_event_for_session(
    db: &LocalDb,
    session_id: &str,
) -> Result<Option<LastEventDigest>, CairnError> {
    let mut events = db
        .query_all(
            format!(
                "SELECT {EVENT_COLUMNS}
                 FROM events
                 WHERE session_id = ?1
                 ORDER BY rowid DESC
                 LIMIT 64"
            ),
            params![session_id],
            event_from_row,
        )
        .await
        .map_err(CairnError::from)?;

    if events.is_empty() {
        return Ok(None);
    }

    events.reverse();

    let parsed_events: Vec<StoredTranscriptEvent> = events
        .iter()
        .filter_map(|event| serde_json::from_str::<StoredTranscriptEvent>(&event.data).ok())
        .collect();

    if parsed_events.is_empty() {
        return Ok(None);
    }

    let completed_tool_ids: HashSet<String> = parsed_events
        .iter()
        .filter(|event| event.event_type == "tool_result")
        .filter_map(|event| event.tool_use_id.clone())
        .collect();

    for event in parsed_events.iter().rev() {
        if event.event_type == "assistant" || event.event_type == "assistant:streaming" {
            if let Some(tool_uses) = event.tool_uses.as_ref() {
                for tool in tool_uses.iter().rev() {
                    if !completed_tool_ids.contains(&tool.id) {
                        return Ok(Some(LastEventDigest {
                            kind: "tool".to_string(),
                            content: truncate_digest(clean_tool_name(&tool.name), 200),
                            tool_name: Some(tool.name.clone()),
                            is_pending: true,
                        }));
                    }
                }
            }

            if let Some(thinking) = event.thinking.as_deref().filter(|value| !value.is_empty()) {
                return Ok(Some(LastEventDigest {
                    kind: "thinking".to_string(),
                    content: truncate_digest(thinking, 200),
                    tool_name: None,
                    is_pending: true,
                }));
            }

            if let Some(content) = event.content.as_deref().filter(|value| !value.is_empty()) {
                return Ok(Some(LastEventDigest {
                    kind: "message".to_string(),
                    content: truncate_digest(content, 200),
                    tool_name: None,
                    is_pending: false,
                }));
            }
        }
    }

    Ok(Some(LastEventDigest {
        kind: "thinking".to_string(),
        content: String::new(),
        tool_name: None,
        is_pending: true,
    }))
}

fn clean_tool_name(name: &str) -> &str {
    name.rsplit_once("__").map(|(_, tail)| tail).unwrap_or(name)
}

fn truncate_digest(text: impl AsRef<str>, len: usize) -> String {
    let text = text.as_ref();
    if text.chars().count() <= len {
        return text.to_string();
    }
    text.chars().take(len).collect::<String>() + "…"
}

/// Resolve the in-place rotation lineage of a session: the session ids from the
/// lineage root to `session_id` (oldest → newest), inclusive.
///
/// A session continues its `parent_session_id` *in place* only when that parent
/// was rotated into it — `parent.replaced_by_id == child` and the same `job_id`.
/// That is the exact predicate the resume path uses (see
/// `resolve_prepare_session_start_conn`) to tell a cold-resume reseed / backend /
/// prompt rotation apart from a cross-job fork: a delegated child job also stamps
/// `parent_session_id`, but the parent keeps serving its own job and is never
/// marked `replaced_by` that child. Following only genuine rotation predecessors
/// keeps a child task's transcript from absorbing its parent agent's history,
/// while letting a rotated session's transcript span every predecessor session
/// (CAIRN-2630: a cold-resume reseed must preserve the prior events, not wipe
/// them, since the fresh session it rotates to carries none of the old runs).
async fn load_session_lineage_ids(
    db: &LocalDb,
    session_id: &str,
) -> Result<Vec<String>, CairnError> {
    let mut chain = vec![session_id.to_string()];
    let mut current = session_id.to_string();
    loop {
        let parent = db
            .query_opt(
                "SELECT parent.id
                 FROM sessions child
                 JOIN sessions parent ON parent.id = child.parent_session_id
                 WHERE child.id = ?1
                   AND parent.replaced_by_id = child.id
                   AND parent.job_id IS child.job_id",
                params![current.as_str()],
                |row| row.opt_text(0),
            )
            .await
            .map_err(CairnError::from)?
            .flatten();
        match parent {
            // Cycle guard: a corrupt parent/replaced_by loop must not spin forever.
            Some(parent_id) if !chain.contains(&parent_id) => {
                current = parent_id.clone();
                chain.push(parent_id);
            }
            _ => break,
        }
    }
    chain.reverse();
    Ok(chain)
}

async fn load_events_for_session(
    db: &LocalDb,
    session_id: String,
) -> Result<(Vec<Event>, std::collections::HashMap<String, usize>), CairnError> {
    let lineage = load_session_lineage_ids(db, &session_id).await?;

    // Runs across the whole rotation lineage, in creation order. Sessions rotate
    // strictly sequentially, so per-session `created_at` order concatenated across
    // the lineage is global chronological order — the position map the event sort
    // below keys on. For an un-rotated session the lineage is `[session_id]`, so
    // this is identical to a single-session load.
    let mut ordered_run_ids: Vec<String> = Vec::new();
    for sid in &lineage {
        let ids = db
            .query_all(
                "SELECT id
                 FROM runs
                 WHERE session_id = ?1
                 ORDER BY created_at ASC",
                params![sid.as_str()],
                |row| row.text(0),
            )
            .await
            .map_err(CairnError::from)?;
        ordered_run_ids.extend(ids);
    }

    let run_position: std::collections::HashMap<String, usize> = ordered_run_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();

    let mut db_events: Vec<Event> = Vec::new();
    for sid in &lineage {
        let events = db
            .query_all(
                format!(
                    "SELECT {EVENT_COLUMNS}
                     FROM events
                     WHERE session_id = ?1
                     ORDER BY rowid"
                ),
                params![sid.as_str()],
                event_from_row,
            )
            .await
            .map_err(CairnError::from)?;
        db_events.extend(events);
    }
    let mut indexed: Vec<(usize, Event)> = db_events.into_iter().enumerate().collect();

    indexed.sort_by(|(idx_a, a), (idx_b, b)| {
        let pos_a = run_position.get(&a.run_id).copied().unwrap_or(usize::MAX);
        let pos_b = run_position.get(&b.run_id).copied().unwrap_or(usize::MAX);
        pos_a
            .cmp(&pos_b)
            .then(a.created_at.cmp(&b.created_at))
            .then(idx_a.cmp(idx_b))
    });

    Ok((
        indexed.into_iter().map(|(_, event)| event).collect(),
        run_position,
    ))
}

/// The session ids in a session's rotation lineage (root → current). Exposed for
/// callers outside the run-db context (e.g. the skyline watermark) that scope
/// their own session-keyed reads and must cover the same continuous transcript
/// the event loaders return.
pub fn session_lineage_ids(db: Arc<LocalDb>, session_id: &str) -> Result<Vec<String>, CairnError> {
    let session_id = session_id.to_string();
    run_query_db(async move { load_session_lineage_ids(&db, &session_id).await })
}

async fn load_events_for_session_after_count(
    db: &LocalDb,
    session_id: String,
    offset: i64,
) -> Result<Vec<Event>, CairnError> {
    // Reuse the lineage-ordered stream the transcript renders so skyline bars stay
    // 1:1 with transcript rows across a cold-resume reseed rotation; `offset` skips
    // the prefix the skyline cache has already processed. New events only ever
    // append to the current (last) session in the lineage, so this prefix is
    // stable and the incremental append stays sound.
    let (events, _run_position) = load_events_for_session(db, session_id).await?;
    let skip = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
    Ok(events.into_iter().skip(skip).collect())
}

async fn load_events_for_turn(db: &LocalDb, turn_id: String) -> Result<Vec<Event>, CairnError> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS}
             FROM events
             WHERE turn_id = ?1
             ORDER BY created_at ASC, rowid"
        ),
        params![turn_id.as_str()],
        event_from_row,
    )
    .await
    .map_err(CairnError::from)
}

/// Fetch a single event with its full, unstripped `data` for on-demand detail /
/// raw rendering (CAIRN-1593). Reconstructs archival storage like the list
/// loaders, but never applies the lean read projection.
pub fn load_event_by_id(db: Arc<LocalDb>, event_id: &str) -> Result<Option<Event>, CairnError> {
    let event_id = event_id.to_string();
    let query_db = db.clone();
    run_query_db(async move {
        let event = query_db
            .query_opt(
                format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = ?1"),
                params![event_id.as_str()],
                event_from_row,
            )
            .await
            .map_err(CairnError::from)?;
        match event {
            Some(event) => {
                let mut events = crate::storage::reconstruct_events(&query_db, vec![event]).await;
                Ok(events.pop())
            }
            None => Ok(None),
        }
    })
}

/// Strip cached read tool-result bodies from `events` and attach per-segment
/// token counts (CAIRN-1593). A row in `event_read_tokens` both carries the
/// counts and signals "this event is a read result", so any event with a cache
/// row gets its `tool_result` blanked and `read_segments` set.
///
/// When the originating read tool call is present in this same batch (full
/// session and per-run loads carry the assistant event), missing cache rows are
/// backfilled first so historical reads gain counts uniformly. Deltas that do
/// not include the assistant event rely on the ingest-time cache populated when
/// the result was recorded.
///
/// Kept strictly out of the skyline event-load path, which needs real bodies to
/// compute bar line counts.
pub(crate) async fn apply_lean_read_projection(
    db: &LocalDb,
    events: &mut [Event],
) -> Result<(), CairnError> {
    if events.is_empty() {
        return Ok(());
    }

    // Correlate tool_use_id -> input paths for read tool calls visible here.
    let mut read_tool_paths: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for event in events.iter() {
        let Ok(parsed) = serde_json::from_str::<StoredTranscriptEvent>(&event.data) else {
            continue;
        };
        if let Some(tool_uses) = parsed.tool_uses {
            for tool in tool_uses {
                if crate::runs::read_tokens::is_read_tool(&tool.name) {
                    read_tool_paths.insert(
                        tool.id,
                        crate::runs::read_tokens::extract_paths(&tool.input),
                    );
                }
            }
        }
    }

    // Candidate tool_result events: every one is checked against the cache;
    // read-correlated ones can be backfilled on a miss.
    #[derive(Clone)]
    struct Candidate {
        event_id: String,
        body: Option<String>,
        expected: Vec<String>,
        is_read: bool,
    }
    let mut candidates: Vec<Candidate> = Vec::new();
    for event in events.iter() {
        if event.event_type != "tool_result" {
            continue;
        }
        let Ok(parsed) = serde_json::from_str::<StoredTranscriptEvent>(&event.data) else {
            continue;
        };
        let (is_read, expected) = match parsed.tool_use_id.as_deref() {
            Some(id) => match read_tool_paths.get(id) {
                Some(paths) => (true, paths.clone()),
                None => (false, Vec::new()),
            },
            None => (false, Vec::new()),
        };
        candidates.push(Candidate {
            event_id: event.id.clone(),
            body: parsed.tool_result,
            expected,
            is_read,
        });
    }
    if candidates.is_empty() {
        return Ok(());
    }

    // Phase 1 (hot path): read existing cache rows. The common live case — a
    // freshly-streamed read whose row ingest already wrote — stays read-only.
    let candidate_ids: Vec<String> = candidates.iter().map(|c| c.event_id.clone()).collect();
    let mut segments_by_event: std::collections::HashMap<String, Vec<ReadSegmentTokens>> = db
        .read(move |conn| {
            let candidate_ids = candidate_ids.clone();
            Box::pin(async move {
                let mut found: std::collections::HashMap<String, Vec<ReadSegmentTokens>> =
                    std::collections::HashMap::new();
                for id in candidate_ids {
                    let mut rows = conn
                        .query(
                            "SELECT segments_json FROM event_read_tokens WHERE event_id = ?1",
                            params![id.as_str()],
                        )
                        .await?;
                    if let Some(row) = rows.next().await? {
                        let json = row.text(0)?;
                        let segs: Vec<ReadSegmentTokens> =
                            serde_json::from_str(&json).unwrap_or_default();
                        found.insert(id, segs);
                    }
                }
                Ok(found)
            })
        })
        .await
        .map_err(CairnError::from)?;

    // Phase 2 (backfill): only when a correlated read result has no cache row
    // (historical events from before ingest, or a delta missing its assistant).
    // This is the one path that takes a write lock.
    let backfill: Vec<Candidate> = candidates
        .into_iter()
        .filter(|c| c.is_read && c.body.is_some() && !segments_by_event.contains_key(&c.event_id))
        .collect();
    if !backfill.is_empty() {
        let now = chrono::Utc::now().timestamp();
        let computed: std::collections::HashMap<String, Vec<ReadSegmentTokens>> = db
            .write(move |conn| {
                let backfill = backfill.clone();
                Box::pin(async move {
                    let mut computed: std::collections::HashMap<String, Vec<ReadSegmentTokens>> =
                        std::collections::HashMap::new();
                    for cand in backfill {
                        let body = cand.body.unwrap_or_default();
                        let segs =
                            crate::runs::read_tokens::read_segment_tokens(&body, &cand.expected);
                        let total: i64 = segs.iter().map(|s| s.tokens).sum();
                        let json = serde_json::to_string(&segs).unwrap_or_default();
                        conn.execute(
                            "INSERT OR REPLACE INTO event_read_tokens
                                (event_id, segments_json, total_tokens, created_at)
                             VALUES (?1, ?2, ?3, ?4)",
                            params![cand.event_id.as_str(), json.as_str(), total, now],
                        )
                        .await?;
                        computed.insert(cand.event_id, segs);
                    }
                    Ok(computed)
                })
            })
            .await
            .map_err(CairnError::from)?;
        segments_by_event.extend(computed);
    }

    if segments_by_event.is_empty() {
        return Ok(());
    }

    for event in events.iter_mut() {
        if let Some(segs) = segments_by_event.get(&event.id) {
            event.data = strip_read_body(&event.data);
            event.read_segments = Some(segs.clone());
        }
    }
    Ok(())
}

/// Blank the `tool_result` body of a serialized tool-result event, leaving every
/// other field (including `isError`) intact. On a read result the body is the
/// only large payload — `raw` already had its content stripped at ingest — so
/// nulling it removes the read text from the list payload while keeping the
/// event renderable (the detail/raw surfaces re-fetch the full body on demand).
fn strip_read_body(data: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(data) {
        Ok(mut value) => {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("toolResult".to_string(), serde_json::Value::Null);
            }
            serde_json::to_string(&value).unwrap_or_else(|_| data.to_string())
        }
        Err(_) => data.to_string(),
    }
}

async fn load_session_events_delta(
    db: &LocalDb,
    session_id: String,
    after_rowid: Option<i64>,
) -> Result<(Vec<Event>, Option<i64>), CairnError> {
    match after_rowid {
        // Initial load: reuse the run-ordered sort, and take the session-wide
        // MAX(rowid) as the cursor (all durable rows are returned here).
        None => {
            let (events, _run_position) = load_events_for_session(db, session_id.clone()).await?;
            let last_rowid = db
                .query_one(
                    "SELECT MAX(rowid) FROM events WHERE session_id = ?1",
                    params![session_id.as_str()],
                    |row| row.opt_i64(0),
                )
                .await
                .map_err(CairnError::from)?;
            Ok((events, last_rowid))
        }
        // Delta: rows insert monotonically, so rowid order is the append order
        // and no Rust re-sort is needed. Uses idx_events_session_id.
        Some(cursor) => {
            let rows = db
                .query_all(
                    format!(
                        "SELECT {EVENT_COLUMNS}, rowid
                         FROM events
                         WHERE session_id = ?1 AND rowid > ?2
                         ORDER BY rowid ASC"
                    ),
                    params![session_id.as_str(), cursor],
                    |row| {
                        let event = event_from_row(row)?;
                        // `rowid` is appended right after EVENT_COLUMNS.
                        let rowid = row.i64(EVENT_COLUMN_COUNT)?;
                        Ok((event, rowid))
                    },
                )
                .await
                .map_err(CairnError::from)?;
            // Echo the cursor when the delta is empty so the caller holds position.
            let last_rowid = rows.last().map(|(_, rowid)| *rowid).or(Some(cursor));
            let events = rows.into_iter().map(|(event, _)| event).collect();
            Ok((events, last_rowid))
        }
    }
}

fn message_streams_for_turn(
    db: Arc<LocalDb>,
    turn_id: &str,
) -> Result<Option<ActiveMessageStream>, CairnError> {
    let turn_id = turn_id.to_string();
    let query_db = db.clone();
    let stream_id =
        run_query_db(async move { active_stream_id_for_turn(&query_db, turn_id).await })?;

    match stream_id {
        Some(stream_id) => read_active_stream(db, &stream_id).map_err(CairnError::Internal),
        None => Ok(None),
    }
}

async fn active_stream_id_for_turn(
    db: &LocalDb,
    turn_id: String,
) -> Result<Option<String>, CairnError> {
    db.query_opt(
        "SELECT id
         FROM message_streams
         WHERE turn_id = ?1
           AND status IN ('open', 'finalizing')
         ORDER BY created_at ASC
         LIMIT 1",
        params![turn_id.as_str()],
        |row| row.text(0),
    )
    .await
    .map_err(CairnError::from)
}

async fn load_pending_prompt(db: &LocalDb, run_id: String) -> Result<Option<Prompt>, CairnError> {
    db.query_opt(
        format!(
            "SELECT {PROMPT_COLUMNS}
             FROM prompts
             WHERE run_id = ?1
               AND response IS NULL
             ORDER BY created_at DESC
             LIMIT 1"
        ),
        params![run_id.as_str()],
        prompt_from_row,
    )
    .await
    .map_err(CairnError::from)
}

async fn load_pending_prompt_for_job(
    db: &LocalDb,
    job_id: String,
) -> Result<Option<Prompt>, CairnError> {
    let Some(turn_id) = db
        .query_opt(
            "SELECT current_turn_id
             FROM jobs
             WHERE id = ?1",
            params![job_id.as_str()],
            |row| row.opt_text(0),
        )
        .await
        .map_err(CairnError::from)?
        .flatten()
    else {
        return Ok(None);
    };

    db.query_opt(
        format!(
            "SELECT {PROMPT_COLUMNS}
             FROM prompts
             WHERE turn_id = ?1
               AND response IS NULL
             ORDER BY created_at DESC
             LIMIT 1"
        ),
        params![turn_id.as_str()],
        prompt_from_row,
    )
    .await
    .map_err(CairnError::from)
}

async fn reconcile_stale_runs_db(db: Arc<LocalDb>) -> Result<ReconcileResult, CairnError> {
    db.write(|conn| {
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();

            let mut stale_rows = conn
                .query(
                    "SELECT id
                     FROM runs
                     WHERE status IN ('starting', 'live')",
                    (),
                )
                .await?;
            let mut stale_runs = Vec::new();
            while let Some(row) = stale_rows.next().await? {
                stale_runs.push(row.text(0)?);
            }

            let mut turn_count = 0usize;
            for run_id in &stale_runs {
                conn.execute(
                    "UPDATE runs
                     SET status = 'crashed',
                         exit_reason = 'crash',
                         exited_at = ?1,
                         updated_at = ?2
                     WHERE id = ?3",
                    params![now, now, run_id.as_str()],
                )
                .await?;

                let mut turn_rows = conn
                    .query(
                        "SELECT id, state
                         FROM turns
                         WHERE run_id = ?1
                           AND state IN ('running', 'pending')",
                        params![run_id.as_str()],
                    )
                    .await?;
                let mut orphaned_turns = Vec::new();
                while let Some(row) = turn_rows.next().await? {
                    orphaned_turns.push((row.text(0)?, row.text(1)?));
                }

                for (turn_id, turn_state) in orphaned_turns {
                    let target_state = match turn_state.as_str() {
                        "running" => "interrupted",
                        "pending" => "failed",
                        _ => continue,
                    };
                    conn.execute(
                        "UPDATE turns
                         SET state = ?1,
                             ended_at = ?2,
                             updated_at = ?3
                         WHERE id = ?4",
                        params![target_state, now, now, turn_id.as_str()],
                    )
                    .await?;
                    turn_count += 1;
                }
            }

            Ok(ReconcileResult {
                stale_run_count: stale_runs.len(),
                turn_count,
            })
        })
    })
    .await
    .map_err(CairnError::from)
}

/// Map a `runs` row projected by [`RUN_COLUMNS`] into a [`Run`]. Canonical
/// mapper shared by every runs read path; these ordinals are the only place the
/// `runs` projection's column order is interpreted.
pub(crate) fn run_from_row(row: &Row) -> DbResult<Run> {
    Ok(Run {
        id: row.text(0)?,
        issue_id: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        job_id: row.opt_text(3)?,
        status: row
            .opt_text(4)?
            .and_then(|status| status.parse().ok())
            .unwrap_or(RunStatus::Starting),
        session_id: row.opt_text(5)?,
        chat_id: row.opt_text(11)?,
        exit_reason: row.opt_text(12)?,
        error_message: row.opt_text(6)?,
        started_at: row.opt_i64(7)?,
        exited_at: row.opt_i64(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        start_mode: row.opt_text(13)?.and_then(|mode| mode.parse().ok()),
    })
}

fn prompt_from_row(row: &Row) -> DbResult<Prompt> {
    Ok(Prompt {
        id: row.text(0)?,
        run_id: row.text(1)?,
        questions: row.text(2)?,
        response: row.opt_text(3)?,
        created_at: row.i64(4)?,
        answered_at: row.opt_i64(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::stream::ToolUseInfo;
    use serde_json::json;

    async fn seed_run(db: &LocalDb, session_id: &str) {
        db.write(|conn| {
            let session_id = session_id.to_string();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(id, status, session_id, created_at, updated_at)
                     VALUES ('run-1', 'live', ?1, 1, 1)",
                    params![session_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn insert_event(
        db: &LocalDb,
        id: &str,
        session_id: &str,
        sequence: i32,
        parent_tool_use_id: Option<&str>,
        event: StoredTranscriptEvent,
    ) {
        let data = serde_json::to_string(&event).unwrap();
        let event_type = event.event_type.clone();
        db.write(|conn| {
            let id = id.to_string();
            let session_id = session_id.to_string();
            let parent_tool_use_id = parent_tool_use_id.map(ToString::to_string);
            let data = data.clone();
            let event_type = event_type.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(
                        id, run_id, session_id, sequence, timestamp, event_type, data,
                        parent_tool_use_id, created_at, thinking_tokens
                     ) VALUES (?1, 'run-1', ?2, ?3, ?3, ?4, ?5, ?6, ?3, NULL)",
                    params![
                        id.as_str(),
                        session_id.as_str(),
                        i64::from(sequence),
                        event_type.as_str(),
                        data.as_str(),
                        parent_tool_use_id.as_deref()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    fn assistant_event(
        content: Option<&str>,
        thinking: Option<&str>,
        tool_uses: Option<Vec<ToolUseInfo>>,
    ) -> StoredTranscriptEvent {
        StoredTranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("sess-1".to_string()),
            parent_tool_use_id: None,
            content: content.map(ToString::to_string),
            thinking: thinking.map(ToString::to_string),
            tool_name: None,
            tool_input: None,
            tool_uses,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: None,
        }
    }

    fn tool_result(tool_use_id: &str) -> StoredTranscriptEvent {
        StoredTranscriptEvent {
            event_type: "tool_result".to_string(),
            session_id: Some("sess-1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: Some(tool_use_id.to_string()),
            tool_result: Some("done".to_string()),
            is_error: false,
            thinking_ms: None,
            raw: None,
        }
    }

    #[tokio::test]
    async fn event_read_surfaces_cost_usd() {
        // Metered backends (OpenRouter) write `cost_usd` on the result event;
        // subscription backends leave it NULL. The transcript read path must
        // surface the value where present and `None` where absent.
        let db = crate::storage::migrated_test_db("event-cost-usd.db").await;
        seed_run(&db, "sess-1").await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(
                        id, run_id, session_id, sequence, timestamp, event_type, data,
                        created_at, cost_usd
                     ) VALUES
                        ('e-cost', 'run-1', 'sess-1', 1, 1, 'result:success', '{}', 1, 0.0023),
                        ('e-free', 'run-1', 'sess-1', 2, 2, 'assistant', '{}', 2, NULL)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let events = load_events_for_run(&db, "run-1".to_string()).await.unwrap();
        let cost = events
            .iter()
            .find(|e| e.id == "e-cost")
            .expect("cost event");
        let free = events
            .iter()
            .find(|e| e.id == "e-free")
            .expect("free event");
        assert_eq!(cost.cost_usd, Some(0.0023));
        assert_eq!(free.cost_usd, None);
    }

    #[tokio::test]
    async fn last_event_for_session_returns_pending_tool() {
        let db = crate::storage::migrated_test_db("last-event-pending-tool.db").await;
        seed_run(&db, "sess-1").await;
        insert_event(
            &db,
            "event-1",
            "sess-1",
            1,
            None,
            assistant_event(
                None,
                None,
                Some(vec![ToolUseInfo {
                    id: "tool-1".to_string(),
                    name: "mcp__cairn__read".to_string(),
                    input: json!({"paths": ["file:src/lib.rs"]}),
                }]),
            ),
        )
        .await;

        let digest = load_last_event_for_session(&db, "sess-1")
            .await
            .unwrap()
            .expect("digest");
        assert_eq!(digest.kind, "tool");
        assert_eq!(digest.content, "read");
        assert_eq!(digest.tool_name.as_deref(), Some("mcp__cairn__read"));
        assert!(digest.is_pending);
    }

    #[tokio::test]
    async fn last_event_for_session_prefers_message_after_completed_tool() {
        let db = crate::storage::migrated_test_db("last-event-message.db").await;
        seed_run(&db, "sess-1").await;
        insert_event(
            &db,
            "event-1",
            "sess-1",
            1,
            None,
            assistant_event(
                None,
                None,
                Some(vec![ToolUseInfo {
                    id: "tool-1".to_string(),
                    name: "read".to_string(),
                    input: json!({}),
                }]),
            ),
        )
        .await;
        insert_event(&db, "event-2", "sess-1", 2, None, tool_result("tool-1")).await;
        insert_event(
            &db,
            "event-3",
            "sess-1",
            3,
            None,
            assistant_event(Some("all done"), None, None),
        )
        .await;

        let digest = load_last_event_for_session(&db, "sess-1")
            .await
            .unwrap()
            .expect("digest");
        assert_eq!(digest.kind, "message");
        assert_eq!(digest.content, "all done");
        assert!(!digest.is_pending);
    }

    #[tokio::test]
    async fn last_event_for_session_handles_empty_and_parent_tagged_task_events() {
        let db = crate::storage::migrated_test_db("last-event-empty.db").await;
        seed_run(&db, "sess-1").await;
        assert!(load_last_event_for_session(&db, "sess-1")
            .await
            .unwrap()
            .is_none());

        insert_event(
            &db,
            "event-1",
            "sess-1",
            1,
            Some("parent-tool"),
            assistant_event(Some("child task work"), None, None),
        )
        .await;
        let digest = load_last_event_for_session(&db, "sess-1")
            .await
            .unwrap()
            .expect("parent-tagged task event should count within its own session");
        assert_eq!(digest.kind, "message");
        assert_eq!(digest.content, "child task work");
    }

    /// Rewrite an event to the zstd archival shape the teardown writer produces:
    /// a valid-but-empty stub in `data`, the compressed original in `data_blob`.
    async fn archive_event_zstd(db: &LocalDb, id: &str, original: &str) {
        let blob = crate::storage::compress(original.as_bytes()).unwrap();
        let id = id.to_string();
        db.write(move |conn| {
            let id = id.clone();
            let blob = blob.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE events SET storage_mode = 'zstd',
                         data = '{\"eventType\":\"assistant\",\"archived\":\"zstd\"}',
                         data_blob = ?1, codec = 'zstd_v1'
                     WHERE id = ?2",
                    params![blob, id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Initial load (no cursor): archived assistant events must come back with
    /// their original `data` reconstructed, not the empty zstd stub. The stub
    /// parses as valid JSON, so the chat UI silently renders it as an empty
    /// (invisible) assistant turn — exactly the "assistant events vanished"
    /// symptom — when reconstruction is skipped.
    #[tokio::test]
    async fn session_delta_initial_reconstructs_archived_events() {
        let db = crate::storage::migrated_test_db("session-delta-archived.db").await;
        seed_run(&db, "sess-1").await;
        let original =
            serde_json::to_string(&assistant_event(Some("hello world"), None, None)).unwrap();
        insert_event(
            &db,
            "event-1",
            "sess-1",
            1,
            None,
            assistant_event(Some("hello world"), None, None),
        )
        .await;
        archive_event_zstd(&db, "event-1", &original).await;

        let (events, _) = load_session_events_delta(&db, "sess-1".to_string(), None)
            .await
            .unwrap();
        let events = crate::storage::reconstruct_events(&db, events).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].data, original,
            "archived event must reconstruct to its original data, got the stub"
        );
        assert!(events[0].data.contains("hello world"));
    }

    /// Follow-up delta (cursor set): the loader appends `rowid` to EVENT_COLUMNS
    /// and must read it at the correct trailing index. The archival columns
    /// pushed the real index out, so a stale index read a TEXT column as i64 and
    /// failed the whole delta query — freezing the transcript at its last
    /// successful load.
    #[tokio::test]
    async fn session_delta_followup_succeeds_with_archival_columns() {
        let db = crate::storage::migrated_test_db("session-delta-followup.db").await;
        seed_run(&db, "sess-1").await;
        insert_event(
            &db,
            "event-1",
            "sess-1",
            1,
            None,
            assistant_event(Some("first"), None, None),
        )
        .await;
        let (_initial, cursor) = load_session_events_delta(&db, "sess-1".to_string(), None)
            .await
            .unwrap();
        let cursor = cursor.expect("initial load yields a cursor");

        insert_event(
            &db,
            "event-2",
            "sess-1",
            2,
            None,
            assistant_event(Some("second"), None, None),
        )
        .await;

        let (delta, next) = load_session_events_delta(&db, "sess-1".to_string(), Some(cursor))
            .await
            .expect("delta query must not error on archival columns");
        assert_eq!(delta.len(), 1, "delta returns only the new event");
        assert_eq!(delta[0].id, "event-2");
        assert!(next.unwrap() > cursor, "cursor advances past the new event");
    }

    #[tokio::test]
    async fn last_event_for_session_reads_bounded_tail() {
        let db = crate::storage::migrated_test_db("last-event-tail.db").await;
        seed_run(&db, "sess-1").await;
        for sequence in 1..=70 {
            insert_event(
                &db,
                &format!("event-{sequence}"),
                "sess-1",
                sequence,
                None,
                assistant_event(Some(&format!("message {sequence}")), None, None),
            )
            .await;
        }

        let digest = load_last_event_for_session(&db, "sess-1")
            .await
            .unwrap()
            .expect("digest");
        assert_eq!(digest.kind, "message");
        assert_eq!(digest.content, "message 70");
    }

    fn read_assistant(tool_use_id: &str, path: &str) -> StoredTranscriptEvent {
        assistant_event(
            None,
            None,
            Some(vec![ToolUseInfo {
                id: tool_use_id.to_string(),
                name: "mcp__cairn__read".to_string(),
                input: json!({ "paths": [path] }),
            }]),
        )
    }

    fn result_with_body(tool_use_id: &str, body: &str) -> StoredTranscriptEvent {
        let mut event = tool_result(tool_use_id);
        event.tool_result = Some(body.to_string());
        event
    }

    #[tokio::test]
    async fn lean_projection_strips_read_body_and_attaches_segments() {
        let db = crate::storage::migrated_test_db("lean-projection-strip.db").await;
        seed_run(&db, "sess-1").await;
        insert_event(
            &db,
            "a1",
            "sess-1",
            1,
            None,
            read_assistant("t1", "file:a.rs"),
        )
        .await;
        insert_event(
            &db,
            "r1",
            "sess-1",
            2,
            None,
            result_with_body("t1", "=== file:a.rs ===\nfn main() { println!(\"hi\"); }"),
        )
        .await;
        // A non-read result must be left untouched.
        insert_event(
            &db,
            "a2",
            "sess-1",
            3,
            None,
            assistant_event(
                None,
                None,
                Some(vec![ToolUseInfo {
                    id: "t2".to_string(),
                    name: "mcp__cairn__run".to_string(),
                    input: json!({}),
                }]),
            ),
        )
        .await;
        insert_event(
            &db,
            "r2",
            "sess-1",
            4,
            None,
            result_with_body("t2", "ran ok"),
        )
        .await;

        let (mut events, _) = load_events_for_session(&db, "sess-1".to_string())
            .await
            .unwrap();
        apply_lean_read_projection(&db, &mut events).await.unwrap();

        let read_event = events.iter().find(|e| e.id == "r1").expect("read result");
        let segments = read_event
            .read_segments
            .as_ref()
            .expect("read result carries segments");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].target, "file:a.rs");
        assert!(segments[0].tokens > 0);
        let read_data: serde_json::Value = serde_json::from_str(&read_event.data).unwrap();
        assert!(
            read_data
                .get("toolResult")
                .map(|v| v.is_null())
                .unwrap_or(true),
            "read body must be stripped from the projected data"
        );

        let run_event = events.iter().find(|e| e.id == "r2").expect("run result");
        assert!(run_event.read_segments.is_none());
        let run_data: serde_json::Value = serde_json::from_str(&run_event.data).unwrap();
        assert_eq!(
            run_data.get("toolResult").and_then(|v| v.as_str()),
            Some("ran ok"),
            "non-read result bodies must flow through untouched"
        );
    }

    #[tokio::test]
    async fn load_event_by_id_returns_full_unstripped_body() {
        let db = Arc::new(crate::storage::migrated_test_db("lean-projection-by-id.db").await);
        seed_run(&db, "sess-1").await;
        insert_event(
            &db,
            "a1",
            "sess-1",
            1,
            None,
            read_assistant("t1", "file:a.rs"),
        )
        .await;
        let body = "=== file:a.rs ===\nfn main() {}";
        insert_event(&db, "r1", "sess-1", 2, None, result_with_body("t1", body)).await;

        // The lean list load strips the body in place.
        let (mut events, _) = load_events_for_session(&db, "sess-1".to_string())
            .await
            .unwrap();
        apply_lean_read_projection(&db, &mut events).await.unwrap();
        let stripped = events.iter().find(|e| e.id == "r1").unwrap();
        let stripped_data: serde_json::Value = serde_json::from_str(&stripped.data).unwrap();
        assert!(stripped_data
            .get("toolResult")
            .map(|v| v.is_null())
            .unwrap_or(true));

        // The on-demand single-event fetch returns the full body.
        let full = load_event_by_id(db.clone(), "r1")
            .unwrap()
            .expect("event exists");
        let full_data: serde_json::Value = serde_json::from_str(&full.data).unwrap();
        assert_eq!(
            full_data.get("toolResult").and_then(|v| v.as_str()),
            Some(body)
        );
        assert!(full.read_segments.is_none());
        assert!(load_event_by_id(db, "missing").unwrap().is_none());
    }
}
