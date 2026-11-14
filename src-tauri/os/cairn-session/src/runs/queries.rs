use crate::agent_process::stream::TranscriptEvent as StoredTranscriptEvent;
use crate::runs::read_tokens::ReadSegmentTokens;
use crate::transcripts::stream_store::ActiveMessageStream;
use crate::transcripts::stream_store::{
    active_stream_for_session, find_active_stream_for_run, find_active_stream_for_session,
    read_active_stream,
};
use cairn_db::error::CairnError;
use cairn_db::storage::events::history::{
    sort_chat_order, BodyProjection, EventFilter, HistoryError, HistoryReader,
};
use cairn_db::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use cairn_db::turso::{params, Row, Value};
use cairn_domain::{Event, Prompt, Run, RunStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastEventDigest {
    #[serde(rename = "type")]
    kind: String,
    content: String,
    tool_name: Option<String>,
    is_pending: bool,
}

pub async fn list_runs_for_job_async(db: &LocalDb, job_id: &str) -> Result<Vec<Run>, CairnError> {
    load_runs_by_sql(
        db,
        format!(
            "SELECT {RUN_COLUMNS} FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC"
        ),
        job_id.to_string(),
    )
    .await
}

/// Page size for [`TranscriptWindow::Tail`] and [`TranscriptWindow::Before`]
/// when the caller does not name one.
pub const TRANSCRIPT_PAGE_LIMIT: usize = 500;

/// The largest page a caller may ask for. A page is one reconstruct-and-project
/// pass over its events, so the bound keeps a single request from becoming the
/// whole-lineage load windowing exists to avoid.
const TRANSCRIPT_PAGE_LIMIT_MAX: usize = 2_000;

/// Where an event sits in its job's transcript.
///
/// `run_seq` is the 1-based position of the event's run among the job's runs,
/// ordered by creation: the same numbering a `chat/{runSeq}/{eventSeq}` URI
/// resolves, so a row's address and its position never disagree. `sequence` is
/// the per-run ordinal, unique within a run (`idx_events_run_sequence`) and
/// reserved when a message starts streaming, so a streamed message keeps its
/// place even though its row is inserted at finalize.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptPosition {
    pub run_seq: i64,
    pub sequence: i64,
}

/// Which slice of a job's transcript a read asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum TranscriptWindow {
    /// The newest `limit` events, plus the forward cursor and a backward one.
    Tail { limit: usize },
    /// The `limit` events immediately older than `position`.
    Before {
        position: TranscriptPosition,
        limit: usize,
    },
    /// Every event inserted after the forward cursor `rowid`, in insertion order.
    After { rowid: i64 },
}

/// One window of a job's transcript.
///
/// `Tail` and `Before` pages are contiguous runs of the transcript in position
/// order, oldest first; stitched newest-to-oldest they reproduce
/// [`list_events_for_session`]'s order exactly. `After` returns what was
/// inserted since the cursor in insertion order, which can differ from position
/// order (a streamed message is inserted at finalize but positioned at its
/// start), so the caller places each delta event by its position.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTranscriptWindow {
    /// Bodies the transcript list never renders are withheld; see
    /// [`withhold_unrendered_body`].
    pub events: Vec<Event>,
    /// The run ordinal of every run present in `events` or `streaming`.
    pub run_ordinals: std::collections::HashMap<String, i64>,
    /// Forward cursor: the newest event rowid across the lineage, read before
    /// the page so anything inserted after it arrives through `After`. Echoes
    /// the cursor on an empty `After`; `None` on a `Before` page, which does not
    /// move the forward cursor.
    pub last_rowid: Option<i64>,
    /// Backward cursor: the position of the oldest event returned, to pass to
    /// the next `Before`. `None` once the head of the transcript is reached, and
    /// always `None` on `After`.
    pub older: Option<TranscriptPosition>,
    /// The active-stream placeholder, kept out of `events`. Only `Tail` and
    /// `After` read it; an older page leaves the caller's placeholder alone.
    pub streaming: Option<Event>,
}

/// Windowed transcript loader for application views, keyed by the NODE.
///
/// A displayed transcript's identity is the job, never the session instance
/// under it. Sessions are mortal: a cold-resume reseed, a prompt edit, or a
/// cross-provider model change rotates the job onto a fresh successor session
/// (`sessions::queries::rotate_session`). A caller that named a session id
/// would be holding a dead key from that moment on, and — because the delta is
/// a cursor walk that simply returns nothing for a session no longer being
/// written to — would go silently blind rather than fail (CAIRN-3262). So the
/// session is resolved here, per read, from the job itself.
///
/// Resolving from the job also makes cross-job leakage structural instead of
/// validated: the session comes from the requested job's own row, so no client
/// pointer can address another job's transcript at all.
///
/// Every step is one pooled statement on the caller's runtime, so the read
/// bound can cut any one of them and a stall names the statement it stalled on.
pub async fn list_job_transcript_window(
    db: &LocalDb,
    job_id: &str,
    window: TranscriptWindow,
) -> Result<JobTranscriptWindow, CairnError> {
    let echoed_cursor = match window {
        TranscriptWindow::After { rowid } => Some(rowid),
        _ => None,
    };
    let Some(session_id) = resolve_job_transcript_session(db, job_id.to_string()).await? else {
        // A node that has never opened a session has an empty transcript, not an
        // error; echo the cursor so the caller holds position.
        return Ok(JobTranscriptWindow {
            events: Vec::new(),
            run_ordinals: std::collections::HashMap::new(),
            last_rowid: echoed_cursor,
            older: None,
            streaming: None,
        });
    };

    let job_runs = load_job_run_ordinals(db, job_id).await?;
    let mut reader = HistoryReader::new(db);
    let (events, last_rowid, older, read_stream) = match window {
        TranscriptWindow::Tail { limit } => {
            let lineage = load_session_lineage_ids(db, &session_id).await?;
            let runs = lineage_runs(&job_runs, &lineage);
            prime_transcript_sources(&mut reader, &lineage, &runs).await?;
            let last_rowid = max_event_rowid_for_lineage(&mut reader, &lineage).await?;
            let (events, older) = load_transcript_page(&mut reader, &runs, None, limit).await?;
            (events, last_rowid, older, true)
        }
        TranscriptWindow::Before { position, limit } => {
            let lineage = load_session_lineage_ids(db, &session_id).await?;
            let runs = lineage_runs(&job_runs, &lineage);
            prime_transcript_sources(&mut reader, &lineage, &runs).await?;
            let (events, older) =
                load_transcript_page(&mut reader, &runs, Some(position), limit).await?;
            (events, None, older, false)
        }
        TranscriptWindow::After { rowid } => {
            let sessions = [session_id.clone()];
            let runs = lineage_runs(&job_runs, &sessions);
            prime_transcript_sources(&mut reader, &sessions, &runs).await?;
            let (events, last_rowid) =
                load_session_events_after(&mut reader, &session_id, rowid).await?;
            (events, last_rowid, None, true)
        }
    };

    let mut events = events;
    apply_lean_read_projection(db, &mut reader, &mut events).await?;
    let streaming = if read_stream {
        active_stream_event_for_session(db, &session_id).await?
    } else {
        None
    };

    let ordinal_of: std::collections::HashMap<&str, i64> = job_runs
        .iter()
        .map(|run| (run.id.as_str(), run.ordinal))
        .collect();
    let run_ordinals = events
        .iter()
        .chain(streaming.iter())
        .filter_map(|event| {
            ordinal_of
                .get(event.run_id.as_str())
                .map(|ordinal| (event.run_id.clone(), *ordinal))
        })
        .collect();

    Ok(JobTranscriptWindow {
        events,
        run_ordinals,
        last_rowid,
        older,
        streaming,
    })
}

/// Where `event_id` sits in this job's transcript, in the coordinates a
/// [`TranscriptWindow::Before`] takes: `None` when the job does not hold it, so
/// no amount of paging can reach it. For a caller holding an event id with no
/// event, such as a link to one.
pub async fn locate_job_event(
    db: &LocalDb,
    job_id: &str,
    event_id: &str,
) -> Result<Option<TranscriptPosition>, CairnError> {
    let job_runs = load_job_run_ordinals(db, job_id).await?;
    let run_ids: Vec<String> = job_runs.iter().map(|run| run.id.clone()).collect();
    let located = HistoryReader::new(db)
        .locate_event(&run_ids, event_id)
        .await
        .map_err(history_error)?;
    Ok(located.and_then(|location| {
        job_runs
            .iter()
            .find(|run| run.id == location.run_id)
            .map(|run| TranscriptPosition {
                run_seq: run.ordinal,
                sequence: location.sequence,
            })
    }))
}

fn history_error(error: HistoryError) -> CairnError {
    CairnError::from(DbError::from(error))
}

/// The session's in-flight message as a transcript placeholder event.
pub async fn active_stream_event_for_session(
    db: &LocalDb,
    session_id: &str,
) -> Result<Option<Event>, CairnError> {
    Ok(active_stream_for_session(db, session_id)
        .await
        .map_err(CairnError::from)?
        .map(active_stream_to_event))
}

/// One of a job's runs, numbered by its position among them.
struct JobRun {
    id: String,
    session_id: Option<String>,
    ordinal: i64,
}

/// Every run of the job, numbered 1.. in creation order — the numbering
/// `transcript::get_nth_run_id` resolves `chat/{runSeq}/...` URIs by.
const JOB_RUN_ORDINALS_SQL: &str = "SELECT id, session_id
     FROM runs
     WHERE job_id = ?1
     ORDER BY created_at ASC, rowid ASC";

async fn load_job_run_ordinals(db: &LocalDb, job_id: &str) -> Result<Vec<JobRun>, CairnError> {
    let rows = db
        .query_all(JOB_RUN_ORDINALS_SQL, params![job_id], |row| {
            Ok((row.text(0)?, row.opt_text(1)?))
        })
        .await
        .map_err(CairnError::from)?;
    Ok(rows
        .into_iter()
        .zip(1..)
        .map(|((id, session_id), ordinal)| JobRun {
            id,
            session_id,
            ordinal,
        })
        .collect())
}

/// The job's runs that belong to the transcript's rotation lineage, oldest
/// first, as `(run_id, ordinal)`. A run of a session outside the lineage (a
/// session the job started fresh rather than rotated into) keeps its ordinal —
/// URIs count it — but contributes no events.
fn lineage_runs<'a>(job_runs: &'a [JobRun], lineage: &[String]) -> Vec<(&'a str, i64)> {
    let lineage: HashSet<&str> = lineage.iter().map(String::as_str).collect();
    job_runs
        .iter()
        .filter(|run| {
            run.session_id
                .as_deref()
                .is_some_and(|session| lineage.contains(session))
        })
        .map(|run| (run.id.as_str(), run.ordinal))
        .collect()
}

async fn prime_transcript_sources(
    reader: &mut HistoryReader<'_>,
    sessions: &[String],
    runs: &[(&str, i64)],
) -> Result<(), CairnError> {
    let run_ids: Vec<String> = runs.iter().map(|(id, _)| (*id).to_string()).collect();
    reader
        .prime_sessions_and_runs(sessions, &run_ids)
        .await
        .map_err(history_error)
}

/// One backward page of the transcript, oldest first, with the backward cursor
/// for the page after it.
///
/// Walks the lineage's runs newest to oldest and pages each one on
/// `idx_events_run_sequence` (`run_id = ? AND sequence < ? ORDER BY sequence
/// DESC LIMIT ?`), so a page costs what it returns rather than what precedes
/// it. Runs are long (hundreds of events), so a page usually cuts inside one.
///
/// The page then snaps back to the start of the turn its oldest event belongs
/// to, up to twice `limit`, so a page boundary does not split a tool call from
/// its result or break up a lifecycle group.
async fn load_transcript_page(
    reader: &mut HistoryReader<'_>,
    runs: &[(&str, i64)],
    before: Option<TranscriptPosition>,
    limit: usize,
) -> Result<(Vec<Event>, Option<TranscriptPosition>), CairnError> {
    let limit = limit.clamp(1, TRANSCRIPT_PAGE_LIMIT_MAX);
    // Newest first while walking: (run ordinal, event).
    let mut page: Vec<(i64, Event)> = Vec::new();
    let mut reached_head = true;
    for (run_id, ordinal) in runs.iter().rev() {
        let below = match before {
            Some(position) if *ordinal > position.run_seq => continue,
            Some(position) if *ordinal == position.run_seq => position.sequence,
            _ => i64::MAX,
        };
        let remaining = limit - page.len();
        let rows = load_run_events_before(reader, run_id, below, None, remaining).await?;
        page.extend(rows.into_iter().map(|event| (*ordinal, event)));
        if page.len() >= limit {
            reached_head = false;
            break;
        }
    }

    if !reached_head {
        snap_page_to_turn_start(reader, runs, &mut page, limit * 2).await?;
    }

    let older = if reached_head {
        None
    } else {
        page.last().map(|(ordinal, event)| TranscriptPosition {
            run_seq: *ordinal,
            sequence: i64::from(event.sequence),
        })
    };
    page.reverse();
    Ok((page.into_iter().map(|(_, event)| event).collect(), older))
}

/// A run's events with `floor <= sequence < below`, newest first, reconstructed
/// with their unrendered bodies withheld.
async fn load_run_events_before(
    reader: &mut HistoryReader<'_>,
    run_id: &str,
    below: i64,
    floor: Option<i64>,
    limit: usize,
) -> Result<Vec<Event>, CairnError> {
    let page = reader
        .run_events_window(
            run_id,
            below,
            floor.unwrap_or(i64::MIN),
            limit,
            EventFilter::projecting(list_bodies()),
        )
        .await
        .map_err(history_error)?;
    Ok(page.into_events())
}

/// Extend a newest-first `page` back to the first event of the turn its oldest
/// event belongs to, without growing it past `cap` events.
async fn snap_page_to_turn_start(
    reader: &mut HistoryReader<'_>,
    runs: &[(&str, i64)],
    page: &mut Vec<(i64, Event)>,
    cap: usize,
) -> Result<(), CairnError> {
    let Some((ordinal, oldest)) = page.last() else {
        return Ok(());
    };
    let Some(turn_id) = oldest.turn_id.clone() else {
        return Ok(());
    };
    let room = cap.saturating_sub(page.len());
    if room == 0 {
        return Ok(());
    }
    let ordinal = *ordinal;
    let oldest_sequence = i64::from(oldest.sequence);
    let Some(run_id) = runs
        .iter()
        .find(|(_, run_ordinal)| *run_ordinal == ordinal)
        .map(|(run_id, _)| *run_id)
    else {
        return Ok(());
    };
    let turn_start = reader
        .turn_start_sequence(&turn_id, run_id)
        .await
        .map_err(history_error)?;
    let Some(turn_start) = turn_start.filter(|start| *start < oldest_sequence) else {
        return Ok(());
    };
    let rows =
        load_run_events_before(reader, run_id, oldest_sequence, Some(turn_start), room).await?;
    page.extend(rows.into_iter().map(|event| (ordinal, event)));
    Ok(())
}

/// The session whose rotation lineage is this job's transcript.
///
/// `jobs.current_session_id` is the live head. Before a job's first turn stamps
/// it, the newest run already carries the session its events are written under,
/// so that is the same transcript by another name — one resolution covers both
/// windows and there is no second client-side fallback path to keep in step.
async fn resolve_job_transcript_session(
    db: &LocalDb,
    job_id: String,
) -> Result<Option<String>, CairnError> {
    let current = db
        .query_opt(
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            params![job_id.as_str()],
            |row| row.opt_text(0),
        )
        .await
        .map_err(CairnError::from)?
        .flatten();
    if current.is_some() {
        return Ok(current);
    }

    db.query_opt(
        "SELECT session_id
         FROM runs
         WHERE job_id = ?1 AND session_id IS NOT NULL
         ORDER BY created_at DESC, rowid DESC
         LIMIT 1",
        params![job_id.as_str()],
        |row| row.opt_text(0),
    )
    .await
    .map_err(CairnError::from)
    .map(Option::flatten)
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
pub const RUN_COLUMNS: &str = "id, issue_id, project_id, job_id, status, session_id, error_message,
    started_at, exited_at, created_at, updated_at, chat_id, exit_reason, start_mode";

// The event-column contract lives in `storage` so the read path (the storage
// search index) can reach it without an upward edge. Re-exported here so this
// module's own SQL and every existing `crate::runs::queries::EVENT_COLUMNS` /
// `event_from_row` consumer (analytics, archival, transcript) keeps compiling
// unchanged.
pub use cairn_db::storage::events::columns::{event_from_row, EVENT_COLUMNS, EVENT_COLUMN_COUNT};

const PROMPT_COLUMNS: &str = "id, run_id, questions, response, created_at, answered_at, turn_id, resolution_id, resolution_surface, resolution_provider, resolution_conversation, resolution_actor, tool_use_id";

/// Exit reason for a stale run whose surviving process this sweep stopped. Kept
/// distinct from the blanket `crash` so a row says whether the death was caused
/// or assumed.
pub const ORPHAN_REAPED_EXIT_REASON: &str = "orphan_reaped";

/// Which question [`settle_stale_run`] is answering, which decides what it
/// writes on the turns the run stranded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleRunSettlement {
    /// The startup sweep: the run belonged to a host that is gone. Its turns
    /// are marked `host_restart` so the job is resumed once after boot.
    HostRestart,
    /// The in-session reaper: a run with no process behind it that no launch
    /// claimed. Its turns keep the ordinary crash settlement.
    Reaped,
}

/// What a stale-run settlement or the boot sweep did to one turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettledTurnOutcome {
    /// A running turn, `interrupted`. `host_restart` when the boot sweep did it.
    Interrupted,
    /// A pending turn of a stale run the boot sweep settled, `cancelled`
    /// `host_restart`.
    Cancelled,
    /// A pending turn of a stale run the in-session reaper settled, `failed`.
    Failed,
    /// A running turn that held a `run` call open, `yielded` for that call's
    /// wait, which resumes it.
    YieldedForHeldCall,
    /// A pending turn no run ever claimed, `cancelled` `orphaned_pending`.
    OrphanedPending,
}

/// One turn a settlement ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettledTurn {
    pub job_id: Option<String>,
    /// `None` for a turn no run ever claimed.
    pub run_id: Option<String>,
    pub turn_id: String,
    pub outcome: SettledTurnOutcome,
}

#[derive(Default)]
pub struct ReconcileResult {
    stale_run_count: usize,
    turn_count: usize,
    orphaned_pending_count: usize,
    /// Jobs whose cached status may no longer match their facts, to re-derive
    /// once the sweep's writes have landed. Re-derivation is idempotent, so a
    /// job that is genuinely live is simply written Running again.
    pub reproject_job_ids: Vec<String>,
    /// Thread tasks a release that projected every thread-owned job by liveness
    /// alone left cached `idle` after their work ended. The caller settles them
    /// rather than re-deriving them; see the query that selects them.
    pub stranded_thread_task_ids: Vec<String>,
    /// Every turn this sweep ended, for the work owed per turn once its writes
    /// have landed.
    pub settled_turns: Vec<SettledTurn>,
    /// Stale runs whose surviving process this sweep actually stopped, as
    /// opposed to ones merely assumed dead.
    reaped_count: usize,
}

pub fn list_runs(db: Arc<LocalDb>, issue_id: &str) -> Result<Vec<Run>, CairnError> {
    let issue_id = issue_id.to_string();
    run_query_db(async move {
        load_runs_by_sql(
            &db,
            format!("SELECT {RUN_COLUMNS} FROM runs WHERE issue_id = ?1 ORDER BY created_at DESC"),
            issue_id,
        )
        .await
    })
}

/// The job's runs, newest first. `created_at` has one-second resolution, so
/// `rowid` breaks its ties — the same rule `list_job_transcript_window` and
/// `chat/{runSeq}/...` URIs number runs by, reversed.
pub fn list_runs_for_job(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<Run>, CairnError> {
    let job_id = job_id.to_string();
    run_query_db(async move {
        load_runs_by_sql(
            &db,
            format!(
                "SELECT {RUN_COLUMNS} FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC"
            ),
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
        let mut reader = HistoryReader::new(&query_db);
        let mut events = load_events_for_run_with_reader(&mut reader, &query_run_id).await?;
        apply_lean_read_projection(&query_db, &mut reader, &mut events).await?;
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
    run_query_db(async move { load_events_limited(&db, run_id, limit, offset).await })
}

pub fn list_events_for_session(
    db: Arc<LocalDb>,
    session_id: &str,
) -> Result<Vec<Event>, CairnError> {
    let session_id = session_id.to_string();
    let query_db = db.clone();
    let query_session_id = session_id.clone();
    let (mut events, run_position) =
        run_query_db(async move { load_events_for_session(&query_db, query_session_id).await })?;

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
/// which is the same invariant the transcript window's `After` delta relies on.
/// Unlike that transcript-facing read, this keeps read result bodies intact so skyline
/// bar widths can be computed without re-reading and re-parsing the whole session.
pub fn list_events_for_session_after_count(
    db: Arc<LocalDb>,
    session_id: &str,
    cached_event_count: i32,
) -> Result<Vec<Event>, CairnError> {
    let session_id = session_id.to_string();
    let offset = i64::from(cached_event_count.max(0));
    run_query_db(async move { load_events_for_session_after_count(&db, session_id, offset).await })
}

pub fn list_events_for_turn(db: Arc<LocalDb>, turn_id: &str) -> Result<Vec<Event>, CairnError> {
    let turn_id = turn_id.to_string();
    let query_db = db.clone();
    let query_turn_id = turn_id.clone();
    let mut events =
        run_query_db(async move { load_events_for_turn(&query_db, query_turn_id).await })?;
    if let Some(active) = message_streams_for_turn(db, &turn_id)? {
        insert_active_stream_by_created_at(&mut events, active);
    }
    Ok(events)
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

/// Reconcile against an injected process table, so a test can
/// assert which pids were signalled without spawning anything.
pub fn reconcile_stale_runs_with(
    db: Arc<LocalDb>,
    emitter: &dyn cairn_platform::services::emitter::EventEmitter,
    boot_at: i64,
    processes: &dyn crate::agent_process::orphan::ProcessTable,
) {
    let _ = reconcile_stale_runs_inner(
        db,
        emitter,
        boot_at,
        processes,
        &Default::default(),
        &Default::default(),
    );
}

/// Testable startup reconcile with an explicit snapshot of in-flight job launch
/// claims. Production obtains the same snapshot from the orchestrator.
pub fn reconcile_stale_runs_with_claims(
    db: Arc<LocalDb>,
    emitter: &dyn cairn_platform::services::emitter::EventEmitter,
    boot_at: i64,
    processes: &dyn crate::agent_process::orphan::ProcessTable,
    claimed_job_ids: &std::collections::HashSet<String>,
) {
    let _ = reconcile_stale_runs_inner(
        db,
        emitter,
        boot_at,
        processes,
        claimed_job_ids,
        &Default::default(),
    );
}

/// `admitted_run_ids` are runs whose launch is durably queued, leased or
/// starting in ordinary admission. They predate boot but are not stale: startup
/// recovery requeues and relaunches them, so settling them here would crash
/// exactly the launches recovery exists to revive.
pub fn reconcile_stale_runs_inner(
    db: Arc<LocalDb>,
    emitter: &dyn cairn_platform::services::emitter::EventEmitter,
    boot_at: i64,
    processes: &dyn crate::agent_process::orphan::ProcessTable,
    claimed_job_ids: &std::collections::HashSet<String>,
    admitted_run_ids: &std::collections::HashSet<String>,
) -> Result<ReconcileResult, CairnError> {
    let claims = claimed_job_ids.clone();
    let admitted = admitted_run_ids.clone();
    let outcome = run_query_db_scoped(|| {
        reconcile_stale_runs_db(
            db.clone(),
            processes,
            boot_at,
            claims.clone(),
            admitted.clone(),
        )
    });
    match &outcome {
        Ok(result) if result.stale_run_count == 0 && result.orphaned_pending_count == 0 => {}
        Ok(result) => {
            log::info!(
                "Reconciled {} stale runs and {} orphaned pending turns on startup; orphan processes stopped: {}",
                result.stale_run_count,
                result.orphaned_pending_count,
                result.reaped_count
            );
            // Intentionally bare: this startup sweep reconciles many stale runs
            // at once and carries no ids, so the frontend broad-invalidates
            // ["runs"]. (See `crate::notify::run_db_change_ids`.)
            let _ = emitter.emit(
                "db-change",
                serde_json::json!({"table": "runs", "action": "update"}),
            );
            if result.turn_count > 0 || result.orphaned_pending_count > 0 {
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
    outcome
}

/// Drive a run query from synchronous code through the one sync-to-async
/// bridge. The query's own `CairnError` rides through the bridge untouched;
/// only the bridge's failures (no runtime, a panic) become `Internal`.
pub fn run_query_db<T, Fut>(future: Fut) -> Result<T, CairnError>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, CairnError>> + Send + 'static,
{
    run_query_db_scoped(move || future)
}

/// [`run_query_db`] for work that borrows from its caller: the bridge's scoped
/// thread joins before the borrow can end.
fn run_query_db_scoped<T, F, Fut>(build: F) -> Result<T, CairnError>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, CairnError>>,
{
    cairn_db::storage::run_db_blocking(move || async move { Ok(build().await) })
        .map_err(CairnError::Internal)?
}

/// `sql` is built from [`RUN_COLUMNS`] by every caller — it is a `String`, not a
/// `&'static str`, precisely so it can be, which is what keeps the projection
/// single-sourced instead of hand-copied at each list site.
async fn load_runs_by_sql(
    db: &LocalDb,
    sql: String,
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

/// A run's events in sequence order, as a transcript list reads them: every
/// body the list never renders withheld, the rest reconstructed.
async fn load_events_for_run_with_reader(
    reader: &mut HistoryReader<'_>,
    run_id: &str,
) -> Result<Vec<Event>, CairnError> {
    let mut rows = reader
        .run_events(run_id, EventFilter::projecting(list_bodies()))
        .await
        .map_err(history_error)?
        .rows;
    rows.sort_by_key(|row| (row.event.sequence, row.rowid));
    Ok(rows.into_iter().map(|row| row.event).collect())
}

/// A slice of a run's events in sequence order, full bodies reconstructed.
async fn load_events_limited(
    db: &LocalDb,
    run_id: String,
    limit: i64,
    offset: i64,
) -> Result<Vec<Event>, CairnError> {
    Ok(HistoryReader::new(db)
        .run_events_slice(&run_id, limit, offset)
        .await
        .map_err(history_error)?
        .into_events())
}

async fn load_last_event_for_session(
    db: &LocalDb,
    session_id: &str,
) -> Result<Option<LastEventDigest>, CairnError> {
    let events = HistoryReader::new(db)
        .session_tail(session_id, 64)
        .await
        .map_err(history_error)?
        .into_events();

    if events.is_empty() {
        return Ok(None);
    }

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
    load_events_for_lineage(db, &lineage).await
}

/// The newest event rowid anywhere in a rotation lineage.
///
/// The forward cursor must span the whole lineage, not just the session named
/// in the request. A session that was just rotated to has no events of its own
/// yet while its lineage carries the entire prior transcript, so a
/// session-scoped `MAX(rowid)` would hand back a null cursor after returning
/// that history.
///
/// Sessions in a lineage are written strictly one after another, so the newest
/// rowid belongs to the newest session that has any events: the walk runs from
/// the current session backwards and stops at the first one that answers,
/// usually after one seek rather than one per session.
async fn max_event_rowid_for_lineage(
    reader: &mut HistoryReader<'_>,
    lineage: &[String],
) -> Result<Option<i64>, CairnError> {
    for session_id in lineage.iter().rev() {
        let newest = reader
            .session_max_rowid(session_id)
            .await
            .map_err(history_error)?;
        if newest.is_some() {
            return Ok(newest);
        }
    }
    Ok(None)
}

/// `?, ?, ...` and the matching text parameters, for a bound `IN` list.
fn text_in_list(values: &[String]) -> (String, Vec<Value>) {
    let placeholders = vec!["?"; values.len()].join(", ");
    let params = values
        .iter()
        .map(|value| Value::Text(value.clone()))
        .collect();
    (placeholders, params)
}

fn lineage_run_positions_sql(placeholders: &str) -> String {
    format!("SELECT id FROM runs WHERE session_id IN ({placeholders}) ORDER BY created_at ASC, rowid ASC")
}

async fn lineage_run_positions(
    db: &LocalDb,
    lineage: &[String],
) -> Result<std::collections::HashMap<String, usize>, CairnError> {
    let (placeholders, run_params) = text_in_list(lineage);
    let ordered_run_ids: Vec<String> = db
        .query_all(
            lineage_run_positions_sql(&placeholders),
            run_params,
            |row| row.text(0),
        )
        .await
        .map_err(CairnError::from)?;
    Ok(ordered_run_ids
        .into_iter()
        .enumerate()
        .map(|(i, id)| (id, i))
        .collect())
}

async fn load_events_for_lineage(
    db: &LocalDb,
    lineage: &[String],
) -> Result<(Vec<Event>, std::collections::HashMap<String, usize>), CairnError> {
    let run_position = lineage_run_positions(db, lineage).await?;
    let mut reader = HistoryReader::new(db);
    reader
        .prime_sessions_and_runs(lineage, &run_position.keys().cloned().collect::<Vec<_>>())
        .await
        .map_err(history_error)?;
    let mut db_events: Vec<Event> = Vec::new();
    for sid in lineage {
        let page = reader
            .session_events_after(sid, None, i64::MIN, EventFilter::default())
            .await
            .map_err(history_error)?;
        db_events.extend(page.into_events());
    }
    let mut indexed: Vec<(usize, Event)> = db_events.into_iter().enumerate().collect();

    // Within a run, `sequence` is the order: it is unique per run and reserved
    // when a message starts streaming, whereas the row (and its rowid) lands at
    // finalize. `created_at` has one-second resolution, so it cannot break the
    // ties that interleave makes.
    indexed.sort_by(|(idx_a, a), (idx_b, b)| {
        let pos_a = run_position.get(&a.run_id).copied().unwrap_or(usize::MAX);
        let pos_b = run_position.get(&b.run_id).copied().unwrap_or(usize::MAX);
        pos_a
            .cmp(&pos_b)
            .then(a.sequence.cmp(&b.sequence))
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
    let skip = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
    let lineage = load_session_lineage_ids(db, &session_id).await?;
    let mut reader = HistoryReader::new(db);
    if let Some(page) = reader
        .hot_sessions_after_count(&lineage, offset.max(0))
        .await
        .map_err(history_error)?
    {
        return Ok(page.into_events());
    }
    let run_position = lineage_run_positions(db, &lineage).await?;
    reader
        .prime_sessions_and_runs(&lineage, &run_position.keys().cloned().collect::<Vec<_>>())
        .await
        .map_err(history_error)?;
    let mut keys = Vec::new();
    for sid in &lineage {
        keys.extend(
            reader
                .session_event_keys(sid)
                .await
                .map_err(history_error)?,
        );
    }
    keys.sort_by_key(|key| {
        (
            run_position.get(&key.run_id).copied().unwrap_or(usize::MAX),
            key.sequence,
        )
    });
    let selected: Vec<_> = keys.into_iter().skip(skip).collect();
    let order: std::collections::HashMap<String, usize> = selected
        .iter()
        .enumerate()
        .map(|(index, key)| (key.id.clone(), index))
        .collect();
    let mut by_run = std::collections::HashMap::<String, Vec<String>>::new();
    for key in selected {
        by_run.entry(key.run_id).or_default().push(key.id);
    }
    let mut rows = Vec::new();
    for (run_id, ids) in by_run {
        rows.extend(
            reader
                .run_events_by_id(&run_id, &ids)
                .await
                .map_err(history_error)?,
        );
    }
    rows.sort_by_key(|row| order[&row.event.id]);
    Ok(rows.into_iter().map(|row| row.event).collect())
}

async fn load_events_for_turn(db: &LocalDb, turn_id: String) -> Result<Vec<Event>, CairnError> {
    let mut rows = HistoryReader::new(db)
        .turn_events(&turn_id)
        .await
        .map_err(history_error)?
        .rows;
    sort_chat_order(&mut rows);
    Ok(rows.into_iter().map(|row| row.event).collect())
}

/// Fetch a single event with its full, unstripped `data` for on-demand detail /
/// raw rendering (CAIRN-1593). Reconstructs archival storage like the list
/// loaders, but never applies the lean read projection.
pub fn load_event_by_id(
    db: Arc<LocalDb>,
    run_id: &str,
    event_id: &str,
) -> Result<Option<Event>, CairnError> {
    let run_id = run_id.to_string();
    let ids = [event_id.to_string()];
    run_query_db(async move {
        let rows = HistoryReader::new(&db)
            .run_events_by_id(&run_id, &ids)
            .await
            .map_err(history_error)?;
        Ok(rows.into_iter().next().map(|row| row.event))
    })
}

/// Whether the transcript list renders nothing from an event type's body.
///
/// Kept in step with `RENDERED_SYSTEM_EVENTS` and `isMainAgentTranscriptEvent`
/// in `src/components/chat/flattenTranscript.tsx`: `result:*`, `system:init`
/// and `system:status` are dropped there outright, and a `user:seed` renders as
/// a body-less divider. These are also the heaviest rows a transcript carries —
/// a seed holds a whole prior-context digest — so withholding them is most of
/// what keeps a page small. The flatten parity test in `flattenTranscript.test`
/// fails if a renderer starts reading one of these bodies.
fn body_is_unrendered(event_type: &str) -> bool {
    UNRENDERED_EVENT_TYPES.contains(&event_type)
        || UNRENDERED_EVENT_TYPE_PREFIXES
            .iter()
            .any(|prefix| event_type.starts_with(prefix))
}

const UNRENDERED_EVENT_TYPES: &[&str] = &["user:seed", "system:init", "system:status"];
const UNRENDERED_EVENT_TYPE_PREFIXES: &[&str] = &["result:"];

/// [`body_is_unrendered`] as a SQL predicate over `event_type`, built from the
/// same lists so the two cannot drift.
fn unrendered_sql_predicate() -> String {
    UNRENDERED_EVENT_TYPES
        .iter()
        .map(|event_type| format!("event_type = '{event_type}'"))
        .chain(
            UNRENDERED_EVENT_TYPE_PREFIXES
                .iter()
                .map(|prefix| format!("event_type LIKE '{prefix}%'")),
        )
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// What a transcript list leaves unread: an unrendered event's `data` and
/// `data_blob` are projected as empty, so the engine never reads their pages.
/// Those bodies are the heaviest rows a transcript holds (a single seed digest
/// runs to megabytes), and a page that read them only to drop them would pay
/// for the whole payload the list withholds. The row parses as an ordinary
/// event; [`withhold_unrendered_body`] then writes its envelope.
fn list_bodies() -> &'static BodyProjection {
    static BODIES: std::sync::OnceLock<BodyProjection> = std::sync::OnceLock::new();
    BODIES.get_or_init(|| {
        BodyProjection::new(
            &unrendered_sql_predicate(),
            body_is_unrendered,
            withhold_unrendered_body,
        )
    })
}

/// Replace an unrendered body with an envelope of what the list does read (type,
/// session, parent tool use) and `bodyWithheld: true`. Decided from the
/// `event_type` column, so a withheld body is never parsed, and an archived one
/// never decompressed or fetched: the row is handed to reconstruction as a
/// plain stored row.
fn withhold_unrendered_body(event: &mut Event) {
    event.data = serde_json::json!({
        "eventType": event.event_type,
        "sessionId": event.session_id,
        "parentToolUseId": event.parent_tool_use_id,
        "bodyWithheld": true,
    })
    .to_string();
    event.storage_mode = None;
    event.content_commit = None;
    event.content_change_id = None;
    event.content_render_sha = None;
    event.data_blob = None;
    event.codec = None;
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
async fn apply_lean_read_projection(
    db: &LocalDb,
    reader: &mut HistoryReader<'_>,
    events: &mut [Event],
) -> Result<(), CairnError> {
    if events.is_empty() {
        return Ok(());
    }

    // Correlate tool_use_id -> input paths for read tool calls visible here.
    let mut read_tool_paths: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for event in events.iter() {
        if body_is_unrendered(&event.event_type) {
            continue;
        }
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
        run_id: String,
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
            run_id: event.run_id.clone(),
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
    let candidate_ids: Vec<(String, String)> = candidates
        .iter()
        .map(|c| (c.run_id.clone(), c.event_id.clone()))
        .collect();
    let stored = reader
        .read_segments(&candidate_ids)
        .await
        .map_err(history_error)?;
    let mut segments_by_event: std::collections::HashMap<String, Vec<ReadSegmentTokens>> = stored
        .json
        .into_iter()
        .map(|(event_id, json)| {
            let segments = serde_json::from_str(&json).unwrap_or_default();
            (event_id, segments)
        })
        .collect();

    // Phase 2 (backfill): only when a correlated read result has no cache row
    // (historical events from before ingest, or a delta missing its assistant).
    // This is the one path that takes a write lock. A row read from a pack has
    // no hot event for a cache row to hang from, and its cache rows were frozen
    // into the pack, so a miss there stays a miss.
    let backfill: Vec<Candidate> = candidates
        .into_iter()
        .filter(|c| {
            c.is_read
                && c.body.is_some()
                && !segments_by_event.contains_key(&c.event_id)
                && !stored.pack_runs.contains(&c.run_id)
        })
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
                        let meter =
                            crate::token_meters::meter_for_run_conn(conn, &cand.run_id).await?;
                        let segs = crate::runs::read_tokens::read_segment_tokens(
                            &body,
                            &cand.expected,
                            meter,
                        );
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
/// Marked `bodyWithheld`, the one marker every list-stripped body carries.
fn strip_read_body(data: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(data) {
        Ok(mut value) => {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("toolResult".to_string(), serde_json::Value::Null);
                obj.insert("bodyWithheld".to_string(), serde_json::Value::Bool(true));
            }
            serde_json::to_string(&value).unwrap_or_else(|_| data.to_string())
        }
        Err(_) => data.to_string(),
    }
}

/// Every event of the session inserted after `cursor`, in insertion (rowid)
/// order, and the cursor past them. Only the current session is read: a
/// lineage is written strictly one session after another, so nothing lands in
/// a predecessor once it has been rotated away from. Insertion order is not
/// transcript order (a streamed message is inserted at finalize but sits at the
/// sequence it reserved when it opened), so the caller places each event by
/// its position. Uses idx_events_session_id.
async fn load_session_events_after(
    reader: &mut HistoryReader<'_>,
    session_id: &str,
    cursor: i64,
) -> Result<(Vec<Event>, Option<i64>), CairnError> {
    let page = reader
        .session_events_after(
            session_id,
            None,
            cursor,
            EventFilter::projecting(list_bodies()),
        )
        .await
        .map_err(history_error)?;
    // Echo the cursor when the delta is empty so the caller holds position.
    let last_rowid = page.last_rowid.or(Some(cursor));
    Ok((page.into_events(), last_rowid))
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
               AND withdrawn_at IS NULL
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
               AND withdrawn_at IS NULL
             ORDER BY created_at DESC
             LIMIT 1"
        ),
        params![turn_id.as_str()],
        prompt_from_row,
    )
    .await
    .map_err(CairnError::from)
}

/// Mark every run left non-terminal by a PREDECESSOR host as crashed, and stop any
/// of their processes that outlived it.
///
/// The sweep used to be blind twice over. It recorded `exit_reason = 'crash'` for a
/// death it had neither caused nor confirmed, while whatever was still running kept
/// running; and it selected every non-terminal row regardless of who spawned it.
/// The second part became dangerous the moment the sweep started sending signals:
/// the transport is already serving when startup recovery runs, so a run created in
/// that window would have appeared in this candidate set, been matched in `ps` by
/// its own session id, and had the current host SIGTERM an agent it had just
/// spawned.
///
/// So ownership is now enforced rather than assumed, through the same
/// [`crate::runs::ownership::predates_host_boot`] boundary the tool-serving fence
/// uses: only runs that positively predate this host are candidates. A run this
/// host spawned is neither signalled nor rewritten, however non-terminal it looks.
///
/// The row then distinguishes a run whose process this sweep actually stopped
/// ([`ORPHAN_REAPED_EXIT_REASON`]) from one merely presumed dead (`crash`), so the
/// forensics for the next incident say which happened.
async fn reconcile_stale_runs_db(
    db: Arc<LocalDb>,
    processes: &dyn crate::agent_process::orphan::ProcessTable,
    boot_at: i64,
    claimed_job_ids: std::collections::HashSet<String>,
    admitted_run_ids: std::collections::HashSet<String>,
) -> Result<ReconcileResult, CairnError> {
    // Filtered in Rust rather than SQL so the boundary has exactly one definition
    // and cannot drift from the fence's. The candidate set is only non-terminal
    // runs, so the rows read are few.
    let stale: Vec<(String, Option<String>, Option<String>)> = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, session_id, started_at, created_at, job_id
                         FROM runs
                         WHERE status IN ('starting', 'live')",
                        (),
                    )
                    .await?;
                let mut stale = Vec::new();
                while let Some(row) = rows.next().await? {
                    let (id, session_id) = (row.text(0)?, row.opt_text(1)?);
                    if !admitted_run_ids.contains(&id)
                        && crate::runs::ownership::predates_host_boot(
                            row.opt_i64(2)?,
                            row.i64(3)?,
                            boot_at,
                        )
                    {
                        stale.push((id, session_id, row.opt_text(4)?));
                    }
                }
                Ok(stale)
            })
        })
        .await
        .map_err(CairnError::from)?;
    // Signalling happens BEFORE the rows are written, so each row records the
    // outcome it actually had rather than being corrected afterwards.
    let session_ids: Vec<String> = stale.iter().filter_map(|(_, s, _)| s.clone()).collect();
    let reaped = crate::agent_process::orphan::reap_sessions(processes, &session_ids);

    db.write(move |conn| {
        let stale = stale.clone();
        let reaped = reaped.clone();
        let claimed_job_ids = claimed_job_ids.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();

            let mut turn_count = 0usize;
            let mut reaped_count = 0usize;
            let mut stale_run_count = 0usize;
            let mut reproject_job_ids = std::collections::HashSet::new();
            let mut swept_turns = Vec::new();
            for (run_id, session_id, job_id) in &stale {
                let was_reaped = session_id
                    .as_deref()
                    .is_some_and(|session_id| reaped.iter().any(|s| s == session_id));
                let exit_reason = if was_reaped {
                    ORPHAN_REAPED_EXIT_REASON
                } else {
                    "crash"
                };
                let Some(settled_turns) = settle_stale_run(
                    conn,
                    run_id,
                    exit_reason,
                    StaleRunSettlement::HostRestart,
                    now,
                )
                .await?
                else {
                    continue;
                };
                stale_run_count += 1;
                if was_reaped {
                    reaped_count += 1;
                }
                turn_count += settled_turns.len();
                swept_turns.extend(settled_turns);
                // Every job whose run this sweep settled is re-derived, whether
                // or not a turn was stranded on the run: its cached `running`
                // was written for a run that no longer exists, and nothing else
                // re-derives a job no process will ever finalize.
                if let Some(job_id) = job_id {
                    if !claimed_job_ids.contains(job_id) {
                        reproject_job_ids.insert(job_id.clone());
                    }
                }
            }

            // Select only after stale runs have settled. A predecessor's resume
            // run can be the sole active run hiding a runless pending head; once
            // that run is crashed, the same boot must see and cancel the turn.
            let mut rows = conn
                .query(
                    "SELECT t.id, t.job_id
                     FROM turns t
                     WHERE t.state = 'pending'
                       AND t.run_id IS NULL
                       AND t.job_id IS NOT NULL
                       AND t.created_at < ?1
                       AND NOT EXISTS (
                           SELECT 1 FROM runs r
                           WHERE r.job_id = t.job_id
                             AND r.status IN ('starting', 'live')
                       )",
                    params![boot_at],
                )
                .await?;
            let mut orphaned_pending = Vec::new();
            while let Some(row) = rows.next().await? {
                let turn_id = row.text(0)?;
                let job_id = row.text(1)?;
                if !claimed_job_ids.contains(&job_id) {
                    orphaned_pending.push((turn_id, job_id));
                }
            }
            drop(rows);

            let mut orphaned_pending_count = 0usize;
            for (turn_id, job_id) in &orphaned_pending {
                let updated = conn
                    .execute(
                        "UPDATE turns
                         SET state = 'cancelled',
                             end_reason = 'orphaned_pending',
                             ended_at = ?1,
                             updated_at = ?1
                         WHERE id = ?2
                           AND state = 'pending'
                           AND run_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM runs r
                               WHERE r.job_id = turns.job_id
                                 AND r.status IN ('starting', 'live')
                           )",
                        params![now, turn_id.as_str()],
                    )
                    .await?;
                if updated > 0 {
                    orphaned_pending_count += 1;
                    reproject_job_ids.insert(job_id.clone());
                    swept_turns.push(SettledTurn {
                        job_id: Some(job_id.clone()),
                        run_id: None,
                        turn_id: turn_id.clone(),
                        outcome: SettledTurnOutcome::OrphanedPending,
                    });
                }
            }

            // Upgrade repair: a previous release may already have terminalized
            // the orphaned turn while the unbounded sweep guard left its job
            // cached as Running. Reconsider those same orphan facts on every boot
            // until the projection has been repaired. This remains part of the
            // existing stale-run reconciliation; it creates no second sweep.
            //
            // Both repair selections below keep `jobs` as the only table in FROM
            // and reach the job's latest turn through a correlated subquery in
            // WHERE. Written as `jobs JOIN turns ON turns.id = (subquery)`, the
            // planner is free to drive the join from `turns`, and on a real
            // database it does: every turn row then re-runs the sorted subquery
            // against every running job (~84k × ~1.2k evaluations), which held
            // startup recovery — and every mutation invoke behind it — for tens
            // of minutes (CAIRN-4742 follow-up). A single-table select has no
            // join order to get wrong: one indexed `idx_turns_job_id` probe per
            // running job.
            let mut rows = conn
                .query(
                    "SELECT j.id
                     FROM jobs j
                     WHERE j.status = 'running'
                       AND EXISTS (
                           SELECT 1 FROM turns t
                           WHERE t.id = (
                               SELECT latest.id FROM turns latest
                               WHERE latest.job_id = j.id
                               ORDER BY latest.created_at DESC, latest.sequence DESC
                               LIMIT 1
                           )
                             AND t.state = 'cancelled'
                             AND t.end_reason = 'orphaned_pending'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM runs r
                           WHERE r.job_id = j.id
                             AND r.status IN ('starting', 'live')
                       )",
                    (),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                if !claimed_job_ids.contains(&job_id) {
                    reproject_job_ids.insert(job_id);
                }
            }

            // Upgrade repair: a release in which a warm provider run counted as
            // work liveness left artifact producers cached `running` after their
            // turn had already settled, so the DAG never advanced past a
            // confirmed Plan or create-PR. Those jobs receive no further turn-end
            // recompute once their session goes quiet, so reconsider the same
            // facts at boot. Like the orphan repair above, this is a
            // re-derivation and not an assertion: a job whose facts still say
            // Running is written Running.
            //
            // The ABSOLUTE latest turn is the one asked, memory review included,
            // mirroring `TurnFacts::work_live`. Keying on the latest *work* turn
            // instead would select a job whose trailing review is merely paused
            // mid-resume, on the strength of an older turn's outcome.
            let mut rows = conn
                .query(
                    "SELECT j.id
                     FROM jobs j
                     WHERE j.status = 'running'
                       AND (
                           SELECT latest.state FROM turns latest
                           WHERE latest.job_id = j.id
                           ORDER BY latest.created_at DESC, latest.sequence DESC
                           LIMIT 1
                       ) IN ('complete', 'failed')",
                    (),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                if !claimed_job_ids.contains(&job_id) {
                    reproject_job_ids.insert(job_id);
                }
            }

            // Upgrade repair: before resolution settled each job's projection,
            // a merged or closed issue could keep jobs cached running, pending,
            // idle, or blocked indefinitely — cancelled by nothing, because
            // they were not live when it resolved, or relaunched afterwards by
            // a late push. Re-deriving them on a resolved issue settles each to
            // its recorded outcome or Cancelled, quietly. Same single-table
            // shape as above: `issues` is reached by primary key inside EXISTS,
            // once per candidate job.
            let mut rows = conn
                .query(
                    "SELECT j.id
                     FROM jobs j
                     WHERE j.status IN ('running', 'pending', 'idle', 'blocked')
                       AND j.thread_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM issues i
                           WHERE i.id = j.issue_id
                             AND i.status IN ('merged', 'closed')
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM runs r
                           WHERE r.job_id = j.id
                             AND r.status IN ('starting', 'live')
                       )",
                    (),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                if !claimed_job_ids.contains(&job_id) {
                    reproject_job_ids.insert(job_id);
                }
            }

            // Upgrade repair: a release that projected every job carrying a
            // `thread_id` by liveness alone — the session's rule — left the
            // tasks a thread spawned cached `idle` once their work ended, never
            // Complete, so no parent was ever told. The forward fix limits that
            // rule to the session job; these are the residue.
            //
            // Only `idle` is selected, so the selection empties once repaired.
            // Under the fixed projection a delegated task cannot derive Idle: its
            // packet always carries an output contract, so it requires output and
            // is not long-running. A task cached `running` by a host crash is
            // deliberately not selected: the stale-run settlement above marks its
            // turn `host_restart` and re-derives it to Pending, and the boot
            // resume relaunches it once after the callback gateway is ready.
            //
            // `idle` alone does not mean finished, though. Under the defect a task
            // legitimately parked mid-work also read `idle`: one waiting on its
            // own question, permission, durable wait, or sub-task. The caller
            // SETTLES these (anything without a recorded outcome becomes
            // Cancelled, which is sticky), so settling a parked task would cancel
            // live work and tell its parent it was over. A candidate must
            // therefore have stopped, and own nothing still waiting:
            //
            // - its latest work turn (memory review excluded) is terminal —
            //   complete, failed, interrupted or cancelled, never yielded,
            //   pending or running. A yielded turn is a wait in progress,
            //   including a wait on the task's own sub-tasks;
            // - no prompt it raised is unanswered and not withdrawn;
            // - no permission it requested is pending;
            // - no `agent_waits` row of its own is pending or resolving.
            //
            // The one exception to the turn rule is a node-less task with no
            // turn and no run at all (a Thread-agent task created and never
            // launched). A node-less job has no DAG node to claim it, and every
            // node-less launch path starts a run as it creates the job, so one
            // that has neither can never start: settling it Cancelled records
            // what already happened. A node-backed task without a turn is NOT
            // excepted, since its next sweep derives Pending and the DAG may still
            // launch it.
            //
            // Single-table shape, as above: every other table is reached from
            // `j` inside a correlated EXISTS or scalar subquery.
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT j.id
                         FROM jobs j
                         WHERE j.status = 'idle'
                           AND j.thread_id IS NOT NULL
                           AND j.execution_id IS NOT NULL
                           AND NOT (CASE WHEN {} THEN 1 ELSE 0 END)
                           AND NOT EXISTS (
                               SELECT 1 FROM runs r
                               WHERE r.job_id = j.id
                                 AND r.status IN ('starting', 'live')
                           )
                           AND (
                               (
                                   SELECT latest.state FROM turns latest
                                   WHERE latest.job_id = j.id
                                     AND latest.start_reason != 'memory_review'
                                   ORDER BY latest.created_at DESC, latest.sequence DESC
                                   LIMIT 1
                               ) IN ('complete', 'failed', 'interrupted', 'cancelled')
                               OR (
                                   j.recipe_node_id IS NULL
                                   AND NOT EXISTS (SELECT 1 FROM turns t WHERE t.job_id = j.id)
                                   AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id)
                               )
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM prompts p JOIN runs r ON p.run_id = r.id
                               WHERE r.job_id = j.id
                                 AND p.response IS NULL AND p.withdrawn_at IS NULL
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM permission_requests pr JOIN runs r ON pr.run_id = r.id
                               WHERE r.job_id = j.id AND pr.status = 'pending'
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM agent_waits w
                               WHERE w.job_id = j.id AND w.state IN ('pending', 'resolving')
                           )",
                        cairn_db::lineage::SESSION_JOB_SHAPE
                    ),
                    (),
                )
                .await?;
            let mut stranded_thread_task_ids = Vec::new();
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                if !claimed_job_ids.contains(&job_id) {
                    stranded_thread_task_ids.push(job_id);
                }
            }

            Ok(ReconcileResult {
                stale_run_count,
                turn_count,
                reaped_count,
                orphaned_pending_count,
                reproject_job_ids: reproject_job_ids.into_iter().collect(),
                stranded_thread_task_ids,
                settled_turns: swept_turns,
            })
        })
    })
    .await
    .map_err(CairnError::from)
}

/// Settle one stale run: mark it `crashed` with `exit_reason`, terminalize the
/// turns it stranded, and
/// withdraw its still-queued admission request (ordinary or call) in the same
/// write, so the queue never carries a launch whose run is already over. A
/// call row past `queued` may have a process behind it, so it is not touched
/// here: the caller hands the settled run to the call-release path, which
/// stops that process as it releases the row. Returns the turns settled, or
/// `None` when the run was no longer non-terminal and nothing was written.
///
/// The status is re-checked inside the UPDATE rather than trusted from the
/// caller's read. Every caller decides staleness outside this write — the
/// startup sweep spends up to a second signalling processes first, the
/// in-session reaper reads its candidates before taking the write — and a run
/// can legitimately finalize in that window. Without the guard, a real outcome
/// would be overwritten with an invented crash.
///
/// Shared so "what settling a stale run means" has exactly one definition:
/// [`reconcile_stale_runs`] owns the question at startup, and
/// [`crate::runs::reap`] owns it inside a live session.
///
/// The stranded turns settle by `settlement`:
///
/// - [`StaleRunSettlement::HostRestart`]: `running -> interrupted` and
///   `pending -> cancelled`, both with end reason `host_restart`. A pending turn
///   never reached its provider, so it is cancelled (resumable) rather than
///   failed. An attention delivery stamped into it returns to the pending
///   queue when its run never started, which is the only case the sweep can
///   prove the prompt never reached a process.
/// - [`StaleRunSettlement::Reaped`]: `running -> interrupted` and
///   `pending -> failed`, with no end reason.
pub async fn settle_stale_run(
    conn: &TrackedConnection,
    run_id: &str,
    exit_reason: &str,
    settlement: StaleRunSettlement,
    now: i64,
) -> DbResult<Option<Vec<SettledTurn>>> {
    let updated = conn
        .execute(
            "UPDATE runs
             SET status = 'crashed',
                 exit_reason = ?1,
                 exited_at = ?2,
                 updated_at = ?3
             WHERE id = ?4
               AND status IN ('starting', 'live')",
            params![exit_reason, now, now, run_id],
        )
        .await?;
    if updated == 0 {
        return Ok(None);
    }
    if let Some(withdrawn) = cairn_db::storage::withdraw_queued_ordinary_admission(
        conn,
        run_id,
        &format!("run settled as stale ({exit_reason})"),
        now,
    )
    .await?
    {
        cairn_attention::push::return_withdrawn_delivery_conn(conn, &withdrawn).await;
    }
    cairn_db::storage::withdraw_queued_call_admission(
        conn,
        run_id,
        &format!("run settled as stale ({exit_reason})"),
        now,
    )
    .await?;

    Ok(Some(
        settle_stranded_turns(conn, run_id, settlement, now).await?,
    ))
}

/// Settle work cut off by a host loss, before signalling a provider on orderly
/// shutdown or while reconciling its stale run on boot. Held calls retain their
/// wait owner; terminal turns, including an operator Stop, are never rewritten.
pub async fn settle_stranded_turns(
    conn: &TrackedConnection,
    run_id: &str,
    settlement: StaleRunSettlement,
    now: i64,
) -> DbResult<Vec<SettledTurn>> {
    let mut turn_rows = conn
        .query(
            "SELECT id, state, job_id
             FROM turns
             WHERE run_id = ?1
               AND state IN ('running', 'pending')",
            params![run_id],
        )
        .await?;
    let mut stranded = Vec::new();
    while let Some(row) = turn_rows.next().await? {
        stranded.push((row.text(0)?, row.text(1)?, row.opt_text(2)?));
    }

    let mut settled = Vec::new();
    for (turn_id, turn_state, job_id) in stranded {
        let settled_as = |turn_id: String, outcome| SettledTurn {
            job_id: job_id.clone(),
            run_id: Some(run_id.to_string()),
            turn_id,
            outcome,
        };
        // On a host restart, a running turn whose `run` call was held open on
        // its socket owns an active durable row, and that row's reconciler
        // resumes it exactly as it resumes a park. Settle it as the park it
        // becomes (yielded for the wait), never as interrupted, or the boot
        // resume would select it too and the turn would resume twice.
        if settlement == StaleRunSettlement::HostRestart
            && turn_state == "running"
            && owns_held_call(conn, &turn_id).await?
        {
            conn.execute(
                "UPDATE turns
                 SET state = 'yielded',
                     yield_reason = 'wait',
                     ended_at = ?1,
                     updated_at = ?1
                 WHERE id = ?2 AND state = 'running'",
                params![now, turn_id.as_str()],
            )
            .await?;
            settled.push(settled_as(turn_id, SettledTurnOutcome::YieldedForHeldCall));
            continue;
        }
        if settlement == StaleRunSettlement::Reaped && turn_state == "running" {
            // The turn's process is gone and the host is not restarting, so a
            // `run` call the turn held open has nobody left to answer. Cancel
            // its row, as a stop would. Nothing else settles it: the call's
            // drop guard cannot yield a turn that is no longer running, and
            // the next boot would resume this crashed turn from the row.
            conn.execute(
                "UPDATE agent_waits
                 SET state = 'cancelled', resolved_at = ?2
                 WHERE predecessor_turn_id = ?1
                   AND state = 'pending'
                   AND hold = 'inline'",
                params![turn_id.as_str(), now],
            )
            .await?;
        }
        let (target_state, end_reason, outcome) = match (settlement, turn_state.as_str()) {
            (StaleRunSettlement::HostRestart, "running") => (
                "interrupted",
                Some("host_restart"),
                SettledTurnOutcome::Interrupted,
            ),
            (StaleRunSettlement::HostRestart, "pending") => (
                "cancelled",
                Some("host_restart"),
                SettledTurnOutcome::Cancelled,
            ),
            (StaleRunSettlement::Reaped, "running") => {
                ("interrupted", None, SettledTurnOutcome::Interrupted)
            }
            (StaleRunSettlement::Reaped, "pending") => ("failed", None, SettledTurnOutcome::Failed),
            _ => continue,
        };
        conn.execute(
            "UPDATE turns
             SET state = ?1,
                 end_reason = COALESCE(?2, end_reason),
                 ended_at = ?3,
                 updated_at = ?3
             WHERE id = ?4",
            params![target_state, end_reason, now, turn_id.as_str()],
        )
        .await?;
        // The sweep never saw the handoff, so it reverts only what it can prove
        // was undelivered: a run that never started cannot have carried the
        // prompt, while a started one may have, and a native resume of its
        // session must not receive the same wake twice.
        if settlement == StaleRunSettlement::HostRestart && turn_state == "pending" {
            match cairn_attention::push::revert_turn_deliveries_conn(
                conn,
                &turn_id,
                cairn_attention::push::TurnDelivery::Unknown,
            )
            .await
            {
                Ok(0) => {}
                Ok(restored) => log::info!(
                    "stale run {run_id}: returned {restored} push(es) from never-started turn {turn_id} to the pending queue"
                ),
                // Losing the revert costs one wake; failing the write would
                // leave the run unsettled.
                Err(error) => log::warn!(
                    "stale run {run_id}: could not return turn {turn_id}'s attention delivery ({error}); that wake will not be redelivered"
                ),
            }
        }
        settled.push(settled_as(turn_id, outcome));
    }
    Ok(settled)
}

/// Whether `turn_id` owns an active durable row for a `run` call held open on
/// its socket (`agent_waits.hold = 'inline'`).
async fn owns_held_call(conn: &TrackedConnection, turn_id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM agent_waits
             WHERE predecessor_turn_id = ?1
               AND state IN ('pending', 'resolving')
               AND hold = 'inline'
             LIMIT 1",
            params![turn_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Map a `runs` row projected by [`RUN_COLUMNS`] into a [`Run`]. Canonical
/// mapper shared by every runs read path; these ordinals are the only place the
/// `runs` projection's column order is interpreted.
pub fn run_from_row(row: &Row) -> DbResult<Run> {
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
    let created_at = row.i64(4)?;
    let answered_at = row.opt_i64(5)?;
    let resolution = match row.opt_text(8)? {
        Some(surface) => Some(cairn_db::models::ResolutionReceipt {
            id: row.opt_text(7)?,
            surface,
            provider: row.opt_text(9)?,
            conversation: row.opt_text(10)?,
            actor: row.opt_text(11)?,
            resolved_at: answered_at.unwrap_or(created_at),
        }),
        None => None,
    };
    Ok(Prompt {
        id: row.text(0)?,
        run_id: row.text(1)?,
        questions: row.text(2)?,
        response: row.opt_text(3)?,
        created_at,
        answered_at,
        resolution,
        tool_use_id: row.opt_text(12)?,
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

    /// `seed_run`, with the run, its session and the job that owns them wired
    /// together, the shape the job-keyed transcript window resolves.
    async fn seed_job_run(db: &LocalDb, session_id: &str) {
        seed_run(db, session_id).await;
        db.write(|conn| {
            let session_id = session_id.to_string();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('project-1', 'default', 'p', 'p', '/tmp/p', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, project_id, status, created_at, updated_at)
                     VALUES ('job-1', 'project-1', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute("UPDATE runs SET job_id = 'job-1' WHERE id = 'run-1'", ())
                    .await?;
                conn.execute(
                    "UPDATE jobs SET current_session_id = ?1 WHERE id = 'job-1'",
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
            queued_message_id: None,
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
            queued_message_id: None,
            raw: None,
        }
    }

    #[tokio::test]
    async fn event_read_surfaces_cost_usd() {
        // Metered backends (OpenRouter) write `cost_usd` on the result event;
        // subscription backends leave it NULL. The transcript read path must
        // surface the value where present and `None` where absent.
        let db = cairn_db::storage::migrated_test_db("event-cost-usd.db").await;
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

        let events = load_events_for_run_with_reader(&mut HistoryReader::new(&db), "run-1")
            .await
            .unwrap();
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
        let db = cairn_db::storage::migrated_test_db("last-event-pending-tool.db").await;
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
        let db = cairn_db::storage::migrated_test_db("last-event-message.db").await;
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
        let db = cairn_db::storage::migrated_test_db("last-event-empty.db").await;
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
        let blob = cairn_db::storage::compress(original.as_bytes()).unwrap();
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

    /// A tail page: archived assistant events must come back with their
    /// original `data` reconstructed, not the empty zstd stub. The stub parses
    /// as valid JSON, so the chat UI silently renders it as an empty (invisible)
    /// assistant turn — exactly the "assistant events vanished" symptom — when
    /// reconstruction is skipped.
    #[tokio::test]
    async fn transcript_tail_reconstructs_archived_events() {
        let db = cairn_db::storage::migrated_test_db("session-delta-archived.db").await;
        seed_job_run(&db, "sess-1").await;
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

        let events = list_job_transcript_window(
            &db,
            "job-1",
            TranscriptWindow::Tail {
                limit: TRANSCRIPT_PAGE_LIMIT,
            },
        )
        .await
        .unwrap()
        .events;
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
        let db = cairn_db::storage::migrated_test_db("session-delta-followup.db").await;
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
        let cursor =
            max_event_rowid_for_lineage(&mut HistoryReader::new(&db), &["sess-1".to_string()])
                .await
                .unwrap()
                .expect("the lineage has a newest event");

        insert_event(
            &db,
            "event-2",
            "sess-1",
            2,
            None,
            assistant_event(Some("second"), None, None),
        )
        .await;

        let (delta, next) =
            load_session_events_after(&mut HistoryReader::new(&db), "sess-1", cursor)
                .await
                .expect("delta query must not error on archival columns");
        assert_eq!(delta.len(), 1, "delta returns only the new event");
        assert_eq!(delta[0].id, "event-2");
        assert!(next.unwrap() > cursor, "cursor advances past the new event");
    }

    #[tokio::test]
    async fn last_event_for_session_reads_bounded_tail() {
        let db = cairn_db::storage::migrated_test_db("last-event-tail.db").await;
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
        let db = cairn_db::storage::migrated_test_db("lean-projection-strip.db").await;
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
        apply_lean_read_projection(&db, &mut HistoryReader::new(&db), &mut events)
            .await
            .unwrap();

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
        let db = Arc::new(cairn_db::storage::migrated_test_db("lean-projection-by-id.db").await);
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
        apply_lean_read_projection(&db, &mut HistoryReader::new(&db), &mut events)
            .await
            .unwrap();
        let stripped = events.iter().find(|e| e.id == "r1").unwrap();
        let stripped_data: serde_json::Value = serde_json::from_str(&stripped.data).unwrap();
        assert!(stripped_data
            .get("toolResult")
            .map(|v| v.is_null())
            .unwrap_or(true));

        // The on-demand single-event fetch returns the full body.
        let full = load_event_by_id(db.clone(), "run-1", "r1")
            .unwrap()
            .expect("event exists");
        let full_data: serde_json::Value = serde_json::from_str(&full.data).unwrap();
        assert_eq!(
            full_data.get("toolResult").and_then(|v| v.as_str()),
            Some(body)
        );
        assert!(full.read_segments.is_none());
        assert!(load_event_by_id(db, "run-1", "missing").unwrap().is_none());
    }

    async fn insert_turn(db: &LocalDb, turn_id: &str, sequence: i64) {
        db.execute(
            "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
             VALUES (?1, 'sess-1', 'run-1', 'job-1', ?2, 'complete', 1, 1)",
            params![turn_id, sequence],
        )
        .await
        .unwrap();
    }

    async fn insert_turn_event(db: &LocalDb, id: &str, sequence: i64, turn_id: &str) {
        db.execute(
            "INSERT INTO events(
                id, run_id, session_id, turn_id, sequence, timestamp, event_type, data, created_at
             ) VALUES (?1, 'run-1', 'sess-1', ?2, ?3, ?3, 'assistant', '{\"eventType\":\"assistant\"}', ?3)",
            params![id, turn_id, sequence],
        )
        .await
        .unwrap();
    }

    fn window_ids(window: &JobTranscriptWindow) -> Vec<&str> {
        window
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect()
    }

    async fn page(db: &LocalDb, window: TranscriptWindow) -> JobTranscriptWindow {
        list_job_transcript_window(db, "job-1", window)
            .await
            .unwrap()
    }

    /// A page boundary snaps back to the start of the turn it would split, but
    /// never past twice the requested size, and the backward cursor walks on
    /// from wherever the page actually ended.
    #[tokio::test]
    async fn transcript_page_snaps_to_turn_start_within_twice_the_limit() {
        let db = cairn_db::storage::migrated_test_db("transcript-turn-snap.db").await;
        seed_job_run(&db, "sess-1").await;
        insert_turn(&db, "turn-a", 1).await;
        insert_turn(&db, "turn-b", 2).await;
        for sequence in 1..=2 {
            insert_turn_event(&db, &format!("e{sequence}"), sequence, "turn-a").await;
        }
        for sequence in 3..=8 {
            insert_turn_event(&db, &format!("e{sequence}"), sequence, "turn-b").await;
        }

        // The newest two split turn-b (3..=8); the snap reaches back for its
        // start but stops at 2 x 2 events.
        let tail = page(&db, TranscriptWindow::Tail { limit: 2 }).await;
        assert_eq!(window_ids(&tail), vec!["e5", "e6", "e7", "e8"]);
        assert_eq!(
            tail.older,
            Some(TranscriptPosition {
                run_seq: 1,
                sequence: 5
            })
        );
        assert_eq!(tail.run_ordinals.get("run-1"), Some(&1));

        // The next page lands exactly on turn-b's start, so there is nothing
        // to snap.
        let older = page(
            &db,
            TranscriptWindow::Before {
                position: tail.older.unwrap(),
                limit: 2,
            },
        )
        .await;
        assert_eq!(window_ids(&older), vec!["e3", "e4"]);
        assert_eq!(
            older.last_rowid, None,
            "an older page leaves the forward cursor"
        );

        let oldest = page(
            &db,
            TranscriptWindow::Before {
                position: older.older.unwrap(),
                limit: 2,
            },
        )
        .await;
        assert_eq!(window_ids(&oldest), vec!["e1", "e2"]);

        // A full page cannot know it was the last; the one after it says so.
        let past_head = page(
            &db,
            TranscriptWindow::Before {
                position: oldest.older.unwrap(),
                limit: 2,
            },
        )
        .await;
        assert!(past_head.events.is_empty());
        assert_eq!(past_head.older, None);

        // A page whose turn starts within reach takes the whole turn.
        let whole = page(&db, TranscriptWindow::Tail { limit: 5 }).await;
        assert_eq!(window_ids(&whole), vec!["e3", "e4", "e5", "e6", "e7", "e8"]);
    }

    /// The bodies the transcript list never renders are withheld from every
    /// list page — archived or not — and come back whole through the
    /// single-event fetch.
    #[tokio::test]
    async fn transcript_window_withholds_unrendered_bodies() {
        let db = Arc::new(cairn_db::storage::migrated_test_db("transcript-withhold.db").await);
        seed_job_run(&db, "sess-1").await;
        let typed = |event_type: &str, content: &str| {
            let mut event = assistant_event(Some(content), None, None);
            event.event_type = event_type.to_string();
            event
        };
        insert_event(
            &db,
            "seed",
            "sess-1",
            1,
            None,
            typed("user:seed", "PRIOR DIGEST"),
        )
        .await;
        insert_event(
            &db,
            "init",
            "sess-1",
            2,
            None,
            typed("system:init", "init body"),
        )
        .await;
        insert_event(
            &db,
            "status",
            "sess-1",
            3,
            None,
            typed("system:status", "status body"),
        )
        .await;
        insert_event(&db, "asst", "sess-1", 4, None, typed("assistant", "hello")).await;
        insert_event(
            &db,
            "res",
            "sess-1",
            5,
            None,
            typed("result:success", "final"),
        )
        .await;
        let seed_original = serde_json::to_string(&typed("user:seed", "PRIOR DIGEST")).unwrap();
        archive_event_zstd(&db, "seed", &seed_original).await;

        let tail = page(
            &db,
            TranscriptWindow::Tail {
                limit: TRANSCRIPT_PAGE_LIMIT,
            },
        )
        .await;
        assert_eq!(
            window_ids(&tail),
            vec!["seed", "init", "status", "asst", "res"]
        );
        for event in &tail.events {
            let data: serde_json::Value = serde_json::from_str(&event.data).unwrap();
            assert_eq!(data["eventType"], event.event_type.as_str());
            if event.id == "asst" {
                assert_eq!(data["content"], "hello");
                assert!(data.get("bodyWithheld").is_none());
            } else {
                assert_eq!(data["bodyWithheld"], true, "{} keeps its body", event.id);
                assert!(data.get("content").is_none(), "{} keeps its body", event.id);
                assert_eq!(data["sessionId"], "sess-1");
                assert!(event.storage_mode.is_none() && event.data_blob.is_none());
            }
        }

        let seed = load_event_by_id(db.clone(), "run-1", "seed")
            .unwrap()
            .unwrap();
        assert_eq!(
            seed.data, seed_original,
            "the detail fetch restores the archived body"
        );
        let result = load_event_by_id(db, "run-1", "res").unwrap().unwrap();
        assert!(result.data.contains("final"));
    }

    /// Every app read of the fixture execution, rendered with all its fields: the
    /// full window walk (tail, then older pages to the head, then a delta), the
    /// lineage, run and turn lists, the digest, the detail fetch, and a position
    /// lookup.
    async fn every_app_read(db: &Arc<LocalDb>) -> Vec<String> {
        let mut reads = Vec::new();
        let tail = page(db, TranscriptWindow::Tail { limit: 2 }).await;
        reads.push(format!(
            "tail {:?} {:?} {:?}",
            tail.events, tail.last_rowid, tail.older
        ));
        let mut older = tail.older;
        while let Some(position) = older {
            let window = page(db, TranscriptWindow::Before { position, limit: 2 }).await;
            reads.push(format!("before {:?} {:?}", window.events, window.older));
            older = window.older;
        }
        let delta = page(db, TranscriptWindow::After { rowid: 0 }).await;
        reads.push(format!("after {:?} {:?}", delta.events, delta.last_rowid));

        reads.push(format!(
            "session {:?}",
            list_events_for_session(db.clone(), "sess-2").unwrap()
        ));
        reads.push(format!(
            "after count {:?}",
            list_events_for_session_after_count(db.clone(), "sess-2", 1).unwrap()
        ));
        reads.push(format!(
            "run {:?}",
            list_events(db.clone(), "run-1").unwrap()
        ));
        reads.push(format!(
            "limited {:?}",
            list_events_limited(db.clone(), "run-1", 2, 1).unwrap()
        ));
        reads.push(format!(
            "turn {:?}",
            list_events_for_turn(db.clone(), "turn-1").unwrap()
        ));
        reads.push(format!(
            "digest {:?}",
            last_event_for_session(db.clone(), "sess-1")
                .unwrap()
                .map(|digest| serde_json::to_string(&digest).unwrap())
        ));
        reads
    }

    #[tokio::test]
    async fn cached_prefix_and_limited_reads_do_not_fetch_archived_prefix_bodies() {
        use cairn_db::storage::history_pack::testing::{seed_fixture_execution, ReadCountingStore};
        let db = cairn_db::storage::migrated_test_db("app-prefix-work").await;
        let events = seed_fixture_execution(&db, "app-prefix-work").await;
        db.execute_script(
            "WITH RECURSIVE seq(n) AS (
                 SELECT 7 UNION ALL SELECT n + 1 FROM seq WHERE n < 1030
             )
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type,
                                data, created_at, storage_mode)
             SELECT 'cached-prefix-' || n, 'run-1', 'sess-1', n, 14, 'system:prompt',
                    '{\"eventType\":\"system:prompt\",\"archived\":\"system_prompt\",\"segments\":[{\"hash\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"kind\":\"base\"}]}',
                    14, 'gitcoord' FROM seq"
        ).await.unwrap();
        // Reopen the migrated file without the test helper's store, then wrap
        // that same store to observe actual reconstruction fetches.
        let path = db.path().to_path_buf();
        let store = db.content_store().unwrap().clone();
        drop(db);
        let db = LocalDb::open(path).await.unwrap();
        let store = Arc::new(ReadCountingStore::new(store));
        assert!(db.set_private_content_store(store.clone()).is_ok());

        let start = read_cost(&db);
        let limited = load_events_limited(&db, "run-1".to_string(), 1, 0)
            .await
            .unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].id, events.plain);
        assert_read_cost(&db, start, 1, 0, "limited hot slice");
        assert_eq!(
            store.reads(),
            0,
            "a one-row slice does not reconstruct later rows"
        );
        let start = read_cost(&db);
        let suffix = load_events_for_session_after_count(&db, "sess-2".to_string(), 1030)
            .await
            .unwrap();
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].id, events.second_run);
        assert_read_cost(
            &db,
            start,
            3,
            0,
            "lineage context plus one suffix statement",
        );
        assert_eq!(store.reads(), 0, "cached runs are not reconstructed");
        let start = read_cost(&db);
        assert!(
            load_events_for_session_after_count(&db, "sess-2".to_string(), 1032)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.reads(),
            0,
            "an up-to-date skyline fetches no archived bodies"
        );
        assert_read_cost(
            &db,
            start,
            3,
            0,
            "lineage context plus one empty suffix statement",
        );

        let sessions = vec!["sess-1".to_string(), "sess-2".to_string()];
        let reader = HistoryReader::new(&db);
        for (offset, expected) in [(1030, 2), (1032, 0)] {
            let start = read_cost(&db);
            let suffix = reader
                .hot_sessions_after_count(&sessions, offset)
                .await
                .unwrap();
            let suffix = suffix.expect("all sources remain hot");
            assert_eq!(suffix.rows.len(), expected);
            assert_read_cost(&db, start, 1, 0, "canonical suffix data read");
            assert_eq!(store.reads(), 0, "cached prefix is never reconstructed");
        }

        // This fixture really would fetch an archived segment if reconstructed.
        let _ = load_events_limited(&db, "run-1".to_string(), -1, 0)
            .await
            .unwrap();
        assert!(store.reads() > 0);
    }

    #[tokio::test]
    async fn cached_prefix_hot_selector_keeps_lineage_and_unknown_run_order() {
        let (db, _) = parity_db("app-prefix-order").await;
        db.execute_script(
            "INSERT INTO runs(id, session_id, status, created_at, updated_at)
               VALUES('unknown-a',NULL,'exited',1,1),('unknown-b',NULL,'exited',1,1);
             INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
               VALUES('unknown-newer-session','unknown-a','sess-2',1,1,'assistant:message','{}',1),
                     ('unknown-older-session','unknown-b','sess-1',1,1,'assistant:message','{}',1)"
        ).await.unwrap();
        let expected = load_events_for_session(&db, "sess-2".to_string())
            .await
            .unwrap()
            .0;
        let expected: Vec<String> = expected.into_iter().map(|event| event.id).collect();
        assert_eq!(
            &expected[expected.len() - 2..],
            &[
                "unknown-older-session".to_string(),
                "unknown-newer-session".to_string(),
            ]
        );
        for offset in [-2, 0, 1, 7, 9, 100] {
            let actual = load_events_for_session_after_count(&db, "sess-2".to_string(), offset)
                .await
                .unwrap();
            let actual: Vec<String> = actual.into_iter().map(|event| event.id).collect();
            assert_eq!(
                actual,
                expected
                    .iter()
                    .skip(offset.max(0) as usize)
                    .cloned()
                    .collect::<Vec<_>>(),
                "offset {offset} preserves the original stable run/sequence/session-rowid order"
            );
        }
    }

    #[tokio::test]
    async fn limited_reads_keep_sql_limit_and_offset_semantics_for_both_sources() {
        use cairn_db::storage::history_pack::testing::{publish_for_test, read_both};
        let (db, events) = parity_db("app-limit-semantics").await;
        publish_for_test(&db, "exec-1").await;
        for (limit, offset, expected) in
            [(0, 0, 0), (-1, -2, 6), (-5, 2, 4), (2, -1, 2), (3, 99, 0)]
        {
            let (hot, pack) = read_both(&db, || async {
                load_events_limited(&db, "run-1".to_string(), limit, offset)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|event| format!("{event:?}"))
                    .collect::<Vec<_>>()
            })
            .await;
            assert_eq!(hot, pack, "limit {limit}, offset {offset}");
            assert_eq!(hot.len(), expected, "limit {limit}, offset {offset}");
            if offset <= 0 && limit != 0 {
                assert!(hot[0].contains(&events.plain));
            }
        }
    }

    async fn parity_db(
        name: &str,
    ) -> (
        Arc<LocalDb>,
        cairn_db::storage::history_pack::testing::FixtureEvents,
    ) {
        use cairn_db::storage::history_pack::testing::seed_fixture_execution;
        let db = Arc::new(cairn_db::storage::migrated_test_db(name).await);
        let events = seed_fixture_execution(&db, name).await;
        (db, events)
    }

    #[tokio::test]
    async fn every_app_read_is_the_same_through_a_pack() {
        use cairn_db::storage::history_pack::testing::{evict_all, publish_for_test, read_both};
        let (db, events) = parity_db("app-parity").await;
        publish_for_test(&db, "exec-1").await;

        let (hot, forced) = read_both(&db, || every_app_read(&db)).await;
        assert_eq!(hot, forced);
        let joined = hot.join("\n");
        assert!(joined.contains("the restored body"));
        assert!(
            joined.contains("bodyWithheld"),
            "the list withholds what it never renders"
        );
        assert!(
            joined.contains("read_segments: Some"),
            "the lean projection attaches segments: {joined}"
        );

        let (hot_detail, forced_detail) = read_both(&db, || async {
            (
                format!(
                    "{:?}",
                    load_event_by_id(db.clone(), "run-1", &events.read_result).unwrap()
                ),
                locate_job_event(&db, "job-1", &events.second_run)
                    .await
                    .unwrap(),
                locate_job_event(&db, "job-1", "absent").await.unwrap(),
            )
        })
        .await;
        assert_eq!(hot_detail, forced_detail);
        assert!(hot_detail.0.contains("fn main()"), "{}", hot_detail.0);
        assert_eq!(
            hot_detail.1,
            Some(TranscriptPosition {
                run_seq: 2,
                sequence: 1
            })
        );
        assert_eq!(hot_detail.2, None);

        // With the hot rows gone, only the pack can answer.
        evict_all(&db).await;
        assert_eq!(hot, every_app_read(&db).await);
        assert!(load_event_by_id(db.clone(), "run-1", &events.read_result)
            .unwrap()
            .is_some());
        assert_eq!(
            locate_job_event(&db, "job-1", &events.second_run)
                .await
                .unwrap(),
            hot_detail.1
        );
    }

    /// The list's lean projection backfills a missing cache row for a hot event.
    /// A pack's rows are frozen: the same miss stays a miss and writes nothing.
    #[tokio::test]
    async fn the_read_token_backfill_writes_nothing_for_a_pack_row() {
        use cairn_db::storage::events::history::HistorySource;
        use cairn_db::storage::history_pack::testing::publish_for_test;
        let (db, _) = parity_db("app-backfill").await;
        db.execute("DELETE FROM event_read_tokens", ())
            .await
            .unwrap();
        publish_for_test(&db, "exec-1").await;
        let cached = "SELECT COUNT(*) FROM event_read_tokens";

        db.set_history_source_override(Some(HistorySource::Pack));
        let _ = list_events(db.clone(), "run-1").unwrap();
        assert_eq!(db.query_opt_i64(cached, ()).await.unwrap(), Some(0));

        db.set_history_source_override(None);
        let _ = list_events(db.clone(), "run-1").unwrap();
        assert_eq!(
            db.query_opt_i64(cached, ()).await.unwrap(),
            Some(1),
            "the hot path still backfills"
        );
    }

    fn read_cost(db: &LocalDb) -> (usize, usize, usize, usize) {
        (
            db.read_checkout_count(),
            db.read_statement_count(),
            db.statement_count(),
            db.read_transaction_count(),
        )
    }

    fn assert_read_cost(
        db: &LocalDb,
        start: (usize, usize, usize, usize),
        expected: usize,
        transactions: usize,
        label: &str,
    ) {
        let end = read_cost(db);
        assert_eq!(
            (
                end.0 - start.0,
                end.1 - start.1,
                end.2 - start.2,
                end.3 - start.3
            ),
            (expected, expected, expected, transactions),
            "{label}: checkouts, read statements, all statements, explicit read transactions"
        );
    }

    async fn explain(db: &LocalDb, sql: &str, params: Vec<Value>) -> Vec<String> {
        db.query_all(format!("EXPLAIN QUERY PLAN {sql}"), params, |row| {
            row.text(3)
        })
        .await
        .unwrap()
    }

    /// Each step of a transcript page is an index seek, so a page costs what it
    /// returns rather than what precedes it. Turso keeps no statistics, so the
    /// plan is the only evidence an index is load-bearing.
    #[tokio::test]
    async fn transcript_window_statements_stay_on_their_indexes() {
        use cairn_db::storage::events::history::statements;

        let db = cairn_db::storage::migrated_test_db("transcript-window-plans.db").await;
        let text = |value: &str| Value::Text(value.to_string());

        let page_plan = explain(
            &db,
            &statements::run_window(&EventFilter::projecting(list_bodies())),
            vec![
                text("run-1"),
                Value::Integer(i64::MAX),
                Value::Integer(i64::MIN),
                Value::Integer(500),
            ],
        )
        .await;
        assert!(
            page_plan
                .iter()
                .any(|step| step.contains("USING INDEX idx_events_run_sequence")),
            "the page step must seek idx_events_run_sequence, got {page_plan:?}"
        );
        assert!(
            !page_plan.iter().any(|step| step.contains("SCAN")
                || step.contains("SORT")
                || step.contains("B-TREE")),
            "the page step must neither scan nor sort, got {page_plan:?}"
        );

        for count in [256, 512] {
            let sessions: Vec<String> = (0..count).map(|n| format!("session-{n}")).collect();
            let (placeholders, binds) = text_in_list(&sessions);
            let lineage_plan = explain(&db, &lineage_run_positions_sql(&placeholders), binds).await;
            assert!(
                lineage_plan
                    .iter()
                    .any(|step| step.contains("idx_runs_session_id"))
                    && !lineage_plan.iter().any(|step| step.contains("SCAN runs")),
                "lineage must seek session membership at {count} binds: {lineage_plan:?}"
            );
            // Multiple sessions require a global creation-order sort. This
            // statement predates the reader seam; it is not a page keyset.
        }
        let locate_plan = explain(&db, statements::LOCATE_EVENT, vec![text("event-1")]).await;
        assert!(
            locate_plan
                .iter()
                .any(|step| step.contains("sqlite_autoindex_events_1"))
                && !locate_plan.iter().any(|step| step.contains("SCAN")),
            "locating one event must use its primary key: {locate_plan:?}"
        );
        let delta_plan = explain(
            &db,
            &statements::session_page(&EventFilter::projecting(list_bodies()), false),
            vec![text("session-1"), Value::Integer(1000), Value::Integer(-1)],
        )
        .await;
        assert!(
            delta_plan
                .iter()
                .any(|step| step.contains("idx_events_session_id"))
                && !delta_plan.iter().any(|step| step.contains("SCAN")
                    || step.contains("SORT")
                    || step.contains("B-TREE")),
            "delta must seek its session and rowid cursor: {delta_plan:?}"
        );

        let runs_plan = explain(&db, JOB_RUN_ORDINALS_SQL, vec![text("job-1")]).await;
        assert!(
            runs_plan
                .iter()
                .any(|step| step.contains("USING INDEX idx_runs_job_id")),
            "the run list must seek idx_runs_job_id, got {runs_plan:?}"
        );

        // Turso may intersect the turn index with the run index
        // (`MULTI-INDEX AND events (idx_events_turn_id, ...)`); either way the
        // turn index drives it and nothing is scanned.
        let turn_plan = explain(
            &db,
            statements::TURN_START,
            vec![text("turn-1"), text("run-1")],
        )
        .await;
        assert!(
            turn_plan
                .iter()
                .any(|step| step.contains("idx_events_turn_id"))
                && !turn_plan.iter().any(|step| step.contains("SCAN")),
            "the turn snap must seek idx_events_turn_id, got {turn_plan:?}"
        );
    }
}
