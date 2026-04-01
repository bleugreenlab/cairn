use crate::diesel_models::{DbEvent, DbPrompt, DbRun};
use crate::models::{Event, Prompt, Run, RunStatus};
use crate::models::{RunTodos, TodoItem};
use crate::schema::{events, prompts, runs};
use crate::transcripts::stream_store::{
    find_active_stream_for_run, find_active_stream_for_session, ActiveMessageStream,
};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::Serialize;

/// Paginated response for events
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedEvents {
    /// Events in chronological order (oldest first within this page)
    pub events: Vec<Event>,
    /// Whether there are more (older) events to load
    pub has_more: bool,
    /// Cursor for loading older events (created_at of oldest event in full result set)
    pub next_cursor: Option<i64>,
}

/// Token usage summary for a session
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTokenUsage {
    /// Current context size: input + cache_read + cache_create from latest event
    pub current: i64,
    /// Total output tokens generated across all events in session
    pub total_output: i64,
    /// Number of times the session has been compacted
    pub compaction_count: i64,
}

/// Convert DbRun to Run model
pub fn db_run_to_run(db: DbRun) -> Run {
    Run {
        id: db.id,
        issue_id: db.issue_id,
        project_id: db.project_id,
        job_id: db.job_id,
        chat_id: db.chat_id,
        status: db
            .status
            .and_then(|s| s.parse().ok())
            .unwrap_or(RunStatus::Starting),
        session_id: db.session_id,
        backend: db.backend,
        exit_reason: db.exit_reason,
        error_message: db.error_message,
        started_at: db.started_at.map(|t| t as i64),
        exited_at: db.exited_at.map(|t| t as i64),
        created_at: db.created_at as i64,
        updated_at: db.updated_at as i64,
        start_mode: db.start_mode.and_then(|s| s.parse().ok()),
    }
}

/// Convert DbEvent to Event model
pub fn db_event_to_event(db: DbEvent) -> Event {
    Event {
        id: db.id,
        run_id: db.run_id,
        session_id: db.session_id,
        sequence: db.sequence,
        timestamp: db.timestamp as i64,
        event_type: db.event_type,
        data: db.data,
        parent_tool_use_id: db.parent_tool_use_id,
        created_at: db.created_at as i64,
        input_tokens: db.input_tokens,
        cache_read_tokens: db.cache_read_tokens,
        cache_create_tokens: db.cache_create_tokens,
        output_tokens: db.output_tokens,
        turn_id: db.turn_id,
    }
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

/// Convert DbPrompt to Prompt model
pub fn db_prompt_to_prompt(db: DbPrompt) -> Prompt {
    Prompt {
        id: db.id,
        run_id: db.run_id,
        questions: db.questions,
        response: db.response,
        created_at: db.created_at as i64,
        answered_at: db.answered_at.map(|t| t as i64),
    }
}

/// List runs for an issue.
pub fn list_runs(conn: &mut SqliteConnection, issue_id: &str) -> Result<Vec<Run>, String> {
    let db_runs: Vec<DbRun> = runs::table
        .filter(runs::issue_id.eq(issue_id))
        .order(runs::created_at.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_runs.into_iter().map(db_run_to_run).collect())
}

/// List runs for a job.
pub fn list_runs_for_job(conn: &mut SqliteConnection, job_id: &str) -> Result<Vec<Run>, String> {
    let db_runs: Vec<DbRun> = runs::table
        .filter(runs::job_id.eq(job_id))
        .order(runs::created_at.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_runs.into_iter().map(db_run_to_run).collect())
}

/// List runs for a chat.
pub fn list_runs_for_chat(conn: &mut SqliteConnection, chat_id: &str) -> Result<Vec<Run>, String> {
    let db_runs: Vec<DbRun> = runs::table
        .filter(runs::chat_id.eq(chat_id))
        .order(runs::created_at.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_runs.into_iter().map(db_run_to_run).collect())
}

/// Get a single run by ID.
pub fn get_run(conn: &mut SqliteConnection, id: &str) -> Result<Option<Run>, String> {
    let db_run: Option<DbRun> = runs::table
        .find(id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_run.map(db_run_to_run))
}

/// List events for a run.
pub fn list_events(conn: &mut SqliteConnection, run_id: &str) -> Result<Vec<Event>, String> {
    let db_events: Vec<DbEvent> = events::table
        .filter(events::run_id.eq(run_id))
        .order(events::sequence.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    let mut events: Vec<Event> = db_events.into_iter().map(db_event_to_event).collect();
    if let Some(active) = find_active_stream_for_run(conn, run_id)? {
        events.push(active_stream_to_event(active));
        events.sort_by_key(|event| (event.sequence, event.created_at));
    }
    Ok(events)
}

/// List events for a run with limit/offset.
pub fn list_events_limited(
    conn: &mut SqliteConnection,
    run_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Event>, String> {
    let db_events: Vec<DbEvent> = events::table
        .filter(events::run_id.eq(run_id))
        .order(events::sequence.asc())
        .limit(limit)
        .offset(offset)
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_events.into_iter().map(db_event_to_event).collect())
}

/// List events for a session (spanning multiple runs).
///
/// Uses two-query strategy: load run ordering from `runs.created_at`,
/// then sort events by `(run_position, created_at, rowid)` for stable
/// ordering at turn boundaries.
///
/// - `run_position` fixes cross-run ordering when `created_at` ties (same second)
/// - `created_at` separates events across seconds
/// - `rowid` (via insertion-order index) is the stable intra-second tiebreaker,
///   because `sequence` is unreliable: on cold resume the user event gets
///   `max_session_seq + 1` while the backend process resets to 0
pub fn list_events_for_session(
    conn: &mut SqliteConnection,
    session_id: &str,
) -> Result<Vec<Event>, String> {
    // 1. Load run_ids ordered by creation time
    let ordered_run_ids: Vec<String> = runs::table
        .filter(runs::session_id.eq(session_id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .map_err(|e| e.to_string())?;

    // 2. Build run_id -> position map for O(1) lookup
    let run_position: std::collections::HashMap<String, usize> = ordered_run_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();

    // 3. Load all session events in rowid order (insertion order)
    let db_events: Vec<DbEvent> = events::table
        .filter(events::session_id.eq(session_id))
        .order(diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid"))
        .load(conn)
        .map_err(|e| e.to_string())?;

    // 4. Build insertion-order index from rowid-ordered load
    let mut indexed: Vec<(usize, DbEvent)> = db_events.into_iter().enumerate().collect();

    // 5. Sort by (run_position, created_at, insertion_index)
    indexed.sort_by(|(idx_a, a), (idx_b, b)| {
        let pos_a = run_position.get(&a.run_id).copied().unwrap_or(usize::MAX);
        let pos_b = run_position.get(&b.run_id).copied().unwrap_or(usize::MAX);
        pos_a
            .cmp(&pos_b)
            .then(a.created_at.cmp(&b.created_at))
            .then(idx_a.cmp(idx_b))
    });

    let mut events: Vec<Event> = indexed
        .into_iter()
        .map(|(_, e)| db_event_to_event(e))
        .collect();

    if let Some(active) = find_active_stream_for_session(conn, session_id)? {
        insert_active_stream_for_session(&mut events, active, &run_position);
    }

    Ok(events)
}

/// List events for a specific turn.
///
/// Orders by `(created_at, rowid)`. On cold resume, the user event
/// (global session sequence) and backend events (process-local seq 0)
/// can share the same `created_at` second; `rowid` preserves insertion
/// order as a stable tiebreaker.
pub fn list_events_for_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
) -> Result<Vec<Event>, String> {
    let db_events: Vec<DbEvent> = events::table
        .filter(events::turn_id.eq(turn_id))
        .order((
            events::created_at.asc(),
            diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid"),
        ))
        .load(conn)
        .map_err(|e| e.to_string())?;

    let mut events: Vec<Event> = db_events.into_iter().map(db_event_to_event).collect();
    if let Some(active) = message_streams_for_turn(conn, turn_id)? {
        insert_active_stream_by_created_at(&mut events, active);
    }
    Ok(events)
}

/// List events for a session with pagination.
/// Returns events in chronological order (oldest first), loading from newest.
/// - limit: max events to return
/// - before: optional cursor (created_at timestamp) - load events older than this
pub fn list_events_paginated(
    conn: &mut SqliteConnection,
    session_id: &str,
    limit: i64,
    before: Option<i64>,
) -> Result<PaginatedEvents, String> {
    let mut query = events::table
        .filter(events::session_id.eq(session_id))
        .into_boxed();

    if let Some(cursor) = before {
        query = query.filter(events::created_at.lt(cursor as i32));
    }

    let db_events: Vec<DbEvent> = query
        .order((events::created_at.desc(), events::sequence.desc()))
        .limit(limit + 1)
        .load(conn)
        .map_err(|e| e.to_string())?;

    let has_more = db_events.len() > limit as usize;
    let events_to_return: Vec<DbEvent> = db_events.into_iter().take(limit as usize).collect();

    let next_cursor = events_to_return.last().map(|e| e.created_at as i64);

    let mut events: Vec<Event> = events_to_return
        .into_iter()
        .rev()
        .map(db_event_to_event)
        .collect();

    if before.is_none() {
        if let Some(active) = find_active_stream_for_session(conn, session_id)? {
            insert_active_stream_by_created_at(&mut events, active);
        }
    }

    Ok(PaginatedEvents {
        events,
        has_more,
        next_cursor,
    })
}

/// List events for a run with pagination.
/// Used for initial events before session_id is known.
pub fn list_events_for_run_paginated(
    conn: &mut SqliteConnection,
    run_id: &str,
    limit: i64,
    before: Option<i64>,
) -> Result<PaginatedEvents, String> {
    let mut query = events::table.filter(events::run_id.eq(run_id)).into_boxed();

    if let Some(cursor) = before {
        query = query.filter(events::created_at.lt(cursor as i32));
    }

    let db_events: Vec<DbEvent> = query
        .order((events::created_at.desc(), events::sequence.desc()))
        .limit(limit + 1)
        .load(conn)
        .map_err(|e| e.to_string())?;

    let has_more = db_events.len() > limit as usize;
    let events_to_return: Vec<DbEvent> = db_events.into_iter().take(limit as usize).collect();

    let next_cursor = events_to_return.last().map(|e| e.created_at as i64);

    let mut events: Vec<Event> = events_to_return
        .into_iter()
        .rev()
        .map(db_event_to_event)
        .collect();

    if before.is_none() {
        if let Some(active) = find_active_stream_for_run(conn, run_id)? {
            insert_active_stream_by_created_at(&mut events, active);
        }
    }

    Ok(PaginatedEvents {
        events,
        has_more,
        next_cursor,
    })
}

fn message_streams_for_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
) -> Result<Option<ActiveMessageStream>, String> {
    use crate::schema::message_streams;

    let stream_id = message_streams::table
        .filter(message_streams::turn_id.eq(turn_id))
        .filter(message_streams::status.eq_any(["open", "finalizing"]))
        .order(message_streams::created_at.asc())
        .select(message_streams::id)
        .first::<String>(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    match stream_id {
        Some(stream_id) => crate::transcripts::stream_store::read_active_stream(conn, &stream_id),
        None => Ok(None),
    }
}

/// Get the pending prompt for a run (if any).
pub fn get_pending_prompt(
    conn: &mut SqliteConnection,
    run_id: &str,
) -> Result<Option<Prompt>, String> {
    let db_prompt: Option<DbPrompt> = prompts::table
        .filter(prompts::run_id.eq(run_id))
        .filter(prompts::response.is_null())
        .order(prompts::created_at.desc())
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_prompt.map(db_prompt_to_prompt))
}

/// Get the pending prompt for a job (finds via yielded turn).
/// This is the primary lookup path — prompts anchor to turns, jobs own turns.
pub fn get_pending_prompt_for_job(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Option<Prompt>, String> {
    use crate::schema::jobs;

    // Get current_turn_id from the job
    let current_turn_id: Option<String> = jobs::table
        .find(job_id)
        .select(jobs::current_turn_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    let Some(turn_id) = current_turn_id else {
        return Ok(None);
    };

    // Look for pending prompt on the current turn
    let db_prompt: Option<DbPrompt> = prompts::table
        .filter(prompts::turn_id.eq(&turn_id))
        .filter(prompts::response.is_null())
        .order(prompts::created_at.desc())
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_prompt.map(db_prompt_to_prompt))
}

/// Get token usage statistics for a session
pub fn get_session_token_usage(
    conn: &mut SqliteConnection,
    session_id: &str,
) -> Result<SessionTokenUsage, String> {
    // Get the latest assistant event with token data for current context size
    // Filter to main agent only (exclude sub-agent events)
    let latest_tokens: Option<(Option<i32>, Option<i32>, Option<i32>)> = events::table
        .filter(events::session_id.eq(session_id))
        .filter(events::event_type.eq("assistant"))
        .filter(events::parent_tool_use_id.is_null())
        .filter(events::input_tokens.is_not_null())
        .order((events::created_at.desc(), events::sequence.desc()))
        .select((
            events::input_tokens,
            events::cache_read_tokens,
            events::cache_create_tokens,
        ))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    let current = if let Some((input, cache_read, cache_create)) = latest_tokens {
        let input_val = input.unwrap_or(0) as i64;
        let cache_read_val = cache_read.unwrap_or(0) as i64;
        let cache_create_val = cache_create.unwrap_or(0) as i64;
        input_val + cache_read_val + cache_create_val
    } else {
        0
    };

    // Sum all output tokens for the session (main agent only)
    let total_output: i64 = events::table
        .filter(events::session_id.eq(session_id))
        .filter(events::parent_tool_use_id.is_null())
        .filter(events::output_tokens.is_not_null())
        .select(diesel::dsl::sum(events::output_tokens))
        .first::<Option<i64>>(conn)
        .map_err(|e| e.to_string())?
        .unwrap_or(0);

    // Count compaction events (main agent only)
    let compaction_count: i64 = events::table
        .filter(events::session_id.eq(session_id))
        .filter(events::parent_tool_use_id.is_null())
        .filter(events::event_type.eq("system:compact_boundary"))
        .count()
        .get_result(conn)
        .map_err(|e| e.to_string())?;

    Ok(SessionTokenUsage {
        current,
        total_output,
        compaction_count,
    })
}

/// Get the current todo list for a job.
/// Queries the todos table directly.
pub fn get_job_todos(conn: &mut SqliteConnection, job_id: &str) -> Result<Vec<TodoItem>, String> {
    crate::todos::get_todos_for_job(conn, job_id)
}

/// Get todos for a job (single list — no longer grouped by run).
/// Kept for backward compatibility; returns the current todos as a single RunTodos entry.
pub fn get_job_todos_by_run(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Vec<RunTodos>, String> {
    let todos = crate::todos::get_todos_for_job(conn, job_id)?;
    if todos.is_empty() {
        return Ok(vec![]);
    }
    Ok(vec![RunTodos {
        run_id: String::new(),
        run_number: 1,
        todos,
    }])
}

/// Reconcile stale runs on startup.
///
/// Any run marked starting/live in the DB has no live process after restart.
/// Mark them as crashed with exit_reason "crash" and interrupt orphaned turns.
pub fn reconcile_stale_runs(
    conn: &mut SqliteConnection,
    emitter: &dyn crate::services::EventEmitter,
) {
    use crate::schema::turns;

    let now = chrono::Utc::now().timestamp() as i32;

    let stale_runs: Vec<String> = runs::table
        .filter(runs::status.eq_any(&["starting", "live"]))
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    if stale_runs.is_empty() {
        return;
    }

    for run_id in &stale_runs {
        // Mark as crashed (unclean shutdown)
        let _ = diesel::update(runs::table.find(run_id))
            .set((
                runs::status.eq("crashed"),
                runs::exit_reason.eq(Some("crash")),
                runs::exited_at.eq(Some(now)),
                runs::updated_at.eq(now),
            ))
            .execute(conn);

        // Interrupt any running/pending turns attached to this run
        let orphaned_turns: Vec<(String, String)> = turns::table
            .filter(turns::run_id.eq(run_id))
            .filter(turns::state.eq_any(&["running", "pending"]))
            .select((turns::id, turns::state))
            .load(conn)
            .unwrap_or_default();

        for (turn_id, turn_state) in &orphaned_turns {
            let result = match turn_state.as_str() {
                "running" => crate::transitions::interrupt_turn(conn, turn_id, emitter),
                "pending" => crate::transitions::apply_turn_outcome(
                    conn,
                    turn_id,
                    crate::models::TurnState::Failed,
                    emitter,
                ),
                _ => continue,
            };
            if let Err(err) = result {
                log::warn!(
                    "Failed to reconcile orphaned turn {} for run {}: {}",
                    turn_id,
                    run_id,
                    err.reason
                );
            }
        }
    }

    log::info!("Reconciled {} stale runs on startup", stale_runs.len());

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "update"}),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewMessageStream, NewMessageStreamChunk, NewRun, NewTurn};
    use crate::schema::{message_stream_chunks, message_streams, turns};
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::test_diesel_conn;

    fn insert_run(conn: &mut SqliteConnection, id: &str, status: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id,
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some(status),
            session_id: None,
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .unwrap();
    }

    fn insert_run_with_fields(
        conn: &mut SqliteConnection,
        id: &str,
        status: &str,
        backend: Option<&str>,
        exit_reason: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id,
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some(status),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: if status == "exited" || status == "crashed" {
                Some(now)
            } else {
                None
            },
            created_at: now,
            updated_at: now,
            backend,
            exit_reason,
            start_mode: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .unwrap();
    }

    // ========================================================================
    // db_run_to_run tests
    // ========================================================================

    #[test]
    fn test_db_run_to_run_maps_new_fields() {
        let mut conn = test_diesel_conn();
        insert_run_with_fields(
            &mut conn,
            "run-1",
            "exited",
            Some("codex"),
            Some("user_stop"),
        );

        let db_run: crate::diesel_models::DbRun =
            runs::table.find("run-1").first(&mut conn).unwrap();
        let run = db_run_to_run(db_run);

        assert_eq!(run.status, RunStatus::Exited);
        assert_eq!(run.backend.as_deref(), Some("codex"));
        assert_eq!(run.exit_reason.as_deref(), Some("user_stop"));
        assert!(run.exited_at.is_some());
    }

    #[test]
    fn test_db_run_to_run_defaults_to_starting_when_status_none() {
        let mut conn = test_diesel_conn();
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id: "run-null",
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: None,
            session_id: None,
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(&mut conn)
            .unwrap();

        let db_run: crate::diesel_models::DbRun =
            runs::table.find("run-null").first(&mut conn).unwrap();
        let run = db_run_to_run(db_run);

        assert_eq!(run.status, RunStatus::Starting);
    }

    #[test]
    fn test_db_run_to_run_parses_legacy_status() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-legacy", "running");

        let db_run: crate::diesel_models::DbRun =
            runs::table.find("run-legacy").first(&mut conn).unwrap();
        let run = db_run_to_run(db_run);

        assert_eq!(run.status, RunStatus::Live);
    }

    // ========================================================================
    // reconcile_stale_runs tests
    // ========================================================================

    #[test]
    fn test_reconcile_marks_starting_as_crashed() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        let status: String = runs::table
            .find("run-1")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "crashed");

        let reason: Option<String> = runs::table
            .find("run-1")
            .select(runs::exit_reason)
            .first(&mut conn)
            .unwrap();
        assert_eq!(reason.as_deref(), Some("crash"));

        let exited_at: Option<i32> = runs::table
            .find("run-1")
            .select(runs::exited_at)
            .first(&mut conn)
            .unwrap();
        assert!(exited_at.is_some());
    }

    #[test]
    fn test_reconcile_marks_live_as_crashed() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        let status: String = runs::table
            .find("run-1")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(status, "crashed");
    }

    #[test]
    fn test_reconcile_leaves_terminal_runs_alone() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-exited", "exited");
        insert_run(&mut conn, "run-crashed", "crashed");

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        let exited_status: String = runs::table
            .find("run-exited")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(exited_status, "exited");

        let crashed_status: String = runs::table
            .find("run-crashed")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(crashed_status, "crashed");
    }

    #[test]
    fn test_reconcile_no_stale_runs_is_noop() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "exited");

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        // Should not emit db-change when nothing to reconcile
        let db_changes = emitter.events_named("db-change");
        assert!(db_changes.is_empty());
    }

    #[test]
    fn test_reconcile_emits_db_change() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        let db_changes = emitter.events_named("db-change");
        assert!(
            !db_changes.is_empty(),
            "should emit db-change for reconciled runs"
        );
    }

    #[test]
    fn test_reconcile_interrupts_orphaned_turns() {
        use crate::diesel_models::NewTurn;
        use crate::schema::turns;

        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        let now = chrono::Utc::now().timestamp() as i32;
        let new_turn = NewTurn {
            id: "turn-1",
            session_id: "session-1",
            run_id: Some("run-1"),
            job_id: None,
            manager_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "running",
            yield_reason: None,
            start_reason: "user",
            created_at: now,
            started_at: Some(now),
            ended_at: None,
            updated_at: now,
        };
        diesel::insert_into(turns::table)
            .values(&new_turn)
            .execute(&mut conn)
            .unwrap();

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        let turn_state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(turn_state, "interrupted");
    }

    #[test]
    fn test_reconcile_fails_pending_orphaned_turns() {
        use crate::diesel_models::NewTurn;
        use crate::schema::turns;

        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "live");

        let now = chrono::Utc::now().timestamp() as i32;
        let new_turn = NewTurn {
            id: "turn-1",
            session_id: "session-1",
            run_id: Some("run-1"),
            job_id: None,
            manager_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "pending",
            yield_reason: None,
            start_reason: "initial",
            created_at: now,
            started_at: None,
            ended_at: None,
            updated_at: now,
        };
        diesel::insert_into(turns::table)
            .values(&new_turn)
            .execute(&mut conn)
            .unwrap();

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        let turn_state: String = turns::table
            .find("turn-1")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(turn_state, "failed");
    }

    #[test]
    fn test_reconcile_multiple_stale_runs() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-1", "starting");
        insert_run(&mut conn, "run-2", "live");
        insert_run(&mut conn, "run-3", "exited"); // should not be touched

        let emitter = CapturingEmitter::new();
        reconcile_stale_runs(&mut conn, &emitter);

        for run_id in &["run-1", "run-2"] {
            let status: String = runs::table
                .find(run_id)
                .select(runs::status.assume_not_null())
                .first(&mut conn)
                .unwrap();
            assert_eq!(status, "crashed", "run {} should be crashed", run_id);
        }

        let status_3: String = runs::table
            .find("run-3")
            .select(runs::status.assume_not_null())
            .first(&mut conn)
            .unwrap();
        assert_eq!(status_3, "exited", "run-3 should remain exited");
    }

    #[test]
    fn test_db_run_to_run_maps_start_mode() {
        use crate::diesel_models::DbRun;
        use crate::models::RunStartMode;

        let now = chrono::Utc::now().timestamp() as i32;

        // start_mode = "fresh"
        let db_run = DbRun {
            id: "run-1".to_string(),
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some("live".to_string()),
            session_id: Some("sess-1".to_string()),
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: Some("fresh".to_string()),
        };
        let run = super::db_run_to_run(db_run);
        assert_eq!(run.start_mode, Some(RunStartMode::Fresh));

        // start_mode = "resume"
        let db_run_resume = DbRun {
            id: "run-2".to_string(),
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some("live".to_string()),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: Some("resume".to_string()),
        };
        let run = super::db_run_to_run(db_run_resume);
        assert_eq!(run.start_mode, Some(RunStartMode::Resume));

        let db_run_fork = DbRun {
            id: "run-5".to_string(),
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some("live".to_string()),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: Some("fork".to_string()),
        };
        let run = super::db_run_to_run(db_run_fork);
        assert_eq!(run.start_mode, Some(RunStartMode::Fork));

        // start_mode = None (legacy run)
        let db_run_none = DbRun {
            id: "run-3".to_string(),
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some("exited".to_string()),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: Some(now + 10),
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
        };
        let run = super::db_run_to_run(db_run_none);
        assert_eq!(run.start_mode, None);

        // start_mode = unknown string (silently ignored)
        let db_run_unknown = DbRun {
            id: "run-4".to_string(),
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some("live".to_string()),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: Some("warm".to_string()),
        };
        let run = super::db_run_to_run(db_run_unknown);
        assert_eq!(run.start_mode, None); // unknown parses to None via .ok()
    }

    // ========================================================================
    // list_events_for_turn tests
    // ========================================================================

    fn insert_turn(conn: &mut SqliteConnection, id: &str, session_id: &str, sequence: i32) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_turn = NewTurn {
            id,
            session_id,
            run_id: None,
            job_id: None,
            manager_id: None,
            sequence,
            predecessor_id: None,
            state: "complete",
            yield_reason: None,
            start_reason: "initial",
            created_at: now,
            started_at: Some(now),
            ended_at: Some(now),
            updated_at: now,
        };
        diesel::insert_into(turns::table)
            .values(&new_turn)
            .execute(conn)
            .unwrap();
    }

    fn insert_event(
        conn: &mut SqliteConnection,
        id: &str,
        run_id: &str,
        session_id: Option<&str>,
        sequence: i32,
        event_type: &str,
        turn_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_event = crate::diesel_models::NewEvent {
            id,
            run_id,
            session_id,
            sequence,
            timestamp: now,
            event_type,
            data: "{}",
            parent_tool_use_id: None,
            created_at: now,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            turn_id,
        };
        diesel::insert_into(events::table)
            .values(&new_event)
            .execute(conn)
            .unwrap();
    }

    fn insert_run_for_session(
        conn: &mut SqliteConnection,
        id: &str,
        session_id: &str,
        created_at: i32,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id,
            issue_id: None,
            project_id: None,
            job_id: None,
            chat_id: None,
            status: Some("exited"),
            session_id: Some(session_id),
            error_message: None,
            started_at: None,
            exited_at: None,
            created_at,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .unwrap();
    }

    fn insert_active_stream_at(
        conn: &mut SqliteConnection,
        id: &str,
        run_id: &str,
        session_id: Option<&str>,
        turn_id: Option<&str>,
        sequence: i32,
        created_at: i32,
        content: &str,
    ) {
        diesel::insert_into(message_streams::table)
            .values(&NewMessageStream {
                id,
                run_id,
                session_id,
                turn_id,
                backend: "claude",
                sequence,
                status: "open",
                version: 1,
                content_chars: content.chars().count() as i32,
                thinking_chars: 0,
                chunk_count: 1,
                final_event_id: None,
                abort_reason: None,
                created_at,
                updated_at: created_at,
                finalized_at: None,
            })
            .execute(conn)
            .unwrap();

        diesel::insert_into(message_stream_chunks::table)
            .values(&NewMessageStreamChunk {
                id: &format!("{id}-chunk"),
                stream_id: id,
                kind: "content",
                chunk_index: 0,
                data: content,
                char_count: content.chars().count() as i32,
                created_at,
            })
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_list_events_for_turn() {
        let mut conn = test_diesel_conn();

        insert_run(&mut conn, "run-1", "exited");
        insert_turn(&mut conn, "turn-a", "sess-1", 1);
        insert_turn(&mut conn, "turn-b", "sess-1", 2);
        insert_event(
            &mut conn,
            "e1",
            "run-1",
            None,
            1,
            "assistant",
            Some("turn-a"),
        );
        insert_event(&mut conn, "e2", "run-1", None, 2, "tool", Some("turn-a"));
        insert_event(
            &mut conn,
            "e3",
            "run-1",
            None,
            3,
            "assistant",
            Some("turn-b"),
        );
        insert_event(&mut conn, "e4", "run-1", None, 4, "tool", None); // no turn

        let events = list_events_for_turn(&mut conn, "turn-a").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, "e1");
        assert_eq!(events[1].id, "e2");

        let events = list_events_for_turn(&mut conn, "turn-b").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "e3");

        // Nonexistent turn returns empty
        let events = list_events_for_turn(&mut conn, "turn-nope").unwrap();
        assert!(events.is_empty());
    }

    // ========================================================================
    // list_events_for_session stable ordering tests
    // ========================================================================

    #[test]
    fn test_list_events_for_session_orders_by_run_position() {
        let mut conn = test_diesel_conn();
        let session = "session-1";

        // Create runs with same created_at (simulating same-second events)
        // but insert in alphabetical order that differs from temporal order.
        // run-b was created first, run-a second.
        let same_time = chrono::Utc::now().timestamp() as i32;
        insert_run_for_session(&mut conn, "run-b", session, same_time);
        insert_run_for_session(&mut conn, "run-a", session, same_time + 1);

        // Events in run-b (first run)
        insert_event(&mut conn, "e1", "run-b", Some(session), 1, "user", None);
        insert_event(
            &mut conn,
            "e2",
            "run-b",
            Some(session),
            2,
            "assistant",
            None,
        );

        // Events in run-a (second run)
        insert_event(&mut conn, "e3", "run-a", Some(session), 1, "user", None);
        insert_event(
            &mut conn,
            "e4",
            "run-a",
            Some(session),
            2,
            "assistant",
            None,
        );

        let events = list_events_for_session(&mut conn, session).unwrap();
        assert_eq!(events.len(), 4);

        // Should be ordered by run creation time, not alphabetical run_id
        // run-b (created first) events come before run-a events
        assert_eq!(events[0].id, "e1"); // run-b, seq 1
        assert_eq!(events[1].id, "e2"); // run-b, seq 2
        assert_eq!(events[2].id, "e3"); // run-a, seq 1
        assert_eq!(events[3].id, "e4"); // run-a, seq 2
    }

    /// Insert an event with explicit created_at for testing same-second scenarios.
    fn insert_event_at(
        conn: &mut SqliteConnection,
        id: &str,
        run_id: &str,
        session_id: Option<&str>,
        sequence: i32,
        event_type: &str,
        turn_id: Option<&str>,
        created_at: i32,
    ) {
        let new_event = crate::diesel_models::NewEvent {
            id,
            run_id,
            session_id,
            sequence,
            timestamp: created_at,
            event_type,
            data: "{}",
            parent_tool_use_id: None,
            created_at,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            turn_id,
        };
        diesel::insert_into(events::table)
            .values(&new_event)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_list_events_for_session_cold_resume_same_second() {
        let mut conn = test_diesel_conn();
        let session = "session-cr";
        let t = 1700000000;

        // Single resumed run — user event and backend events share the same second
        insert_run_for_session(&mut conn, "run-resumed", session, t);
        insert_turn(&mut conn, "turn-cr", session, 1);

        // User event inserted first with high global sequence (simulating max_session_seq + 1)
        insert_event_at(
            &mut conn,
            "user-ev",
            "run-resumed",
            Some(session),
            50,
            "user",
            Some("turn-cr"),
            t,
        );
        // Backend events inserted after with process-local seq starting at 0, same second
        insert_event_at(
            &mut conn,
            "asst-0",
            "run-resumed",
            Some(session),
            0,
            "assistant",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-1",
            "run-resumed",
            Some(session),
            1,
            "tool",
            Some("turn-cr"),
            t,
        );

        // Session query: user event must come first despite lower sequence
        let events = list_events_for_session(&mut conn, session).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0].id, "user-ev",
            "user event must precede backend events"
        );
        assert_eq!(events[1].id, "asst-0");
        assert_eq!(events[2].id, "asst-1");

        // Turn query: same correctness requirement
        let turn_events = list_events_for_turn(&mut conn, "turn-cr").unwrap();
        assert_eq!(turn_events.len(), 3);
        assert_eq!(
            turn_events[0].id, "user-ev",
            "user event must precede backend events in turn"
        );
        assert_eq!(turn_events[1].id, "asst-0");
        assert_eq!(turn_events[2].id, "asst-1");
    }

    #[test]
    fn test_list_events_for_session_active_stream_preserves_cold_resume_order() {
        let mut conn = test_diesel_conn();
        let session = "session-cr-stream";
        let t = 1700000000;

        insert_run_for_session(&mut conn, "run-resumed", session, t);
        insert_turn(&mut conn, "turn-cr", session, 1);
        insert_event_at(
            &mut conn,
            "user-ev",
            "run-resumed",
            Some(session),
            50,
            "user",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-0",
            "run-resumed",
            Some(session),
            0,
            "assistant",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-1",
            "run-resumed",
            Some(session),
            1,
            "tool",
            Some("turn-cr"),
            t,
        );
        insert_active_stream_at(
            &mut conn,
            "stream-1",
            "run-resumed",
            Some(session),
            Some("turn-cr"),
            2,
            t,
            "partial",
        );

        let events = list_events_for_session(&mut conn, session).unwrap();
        assert_eq!(
            events.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            vec!["user-ev", "asst-0", "asst-1", "stream-1"]
        );
    }

    #[test]
    fn test_list_events_for_turn_active_stream_preserves_cold_resume_order() {
        let mut conn = test_diesel_conn();
        let session = "session-turn-stream";
        let t = 1700000000;

        insert_run_for_session(&mut conn, "run-resumed", session, t);
        insert_turn(&mut conn, "turn-cr", session, 1);
        insert_event_at(
            &mut conn,
            "user-ev",
            "run-resumed",
            Some(session),
            50,
            "user",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-0",
            "run-resumed",
            Some(session),
            0,
            "assistant",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-1",
            "run-resumed",
            Some(session),
            1,
            "tool",
            Some("turn-cr"),
            t,
        );
        insert_active_stream_at(
            &mut conn,
            "stream-1",
            "run-resumed",
            Some(session),
            Some("turn-cr"),
            2,
            t,
            "partial",
        );

        let events = list_events_for_turn(&mut conn, "turn-cr").unwrap();
        assert_eq!(
            events.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            vec!["user-ev", "asst-0", "asst-1", "stream-1"]
        );
    }

    #[test]
    fn test_list_events_paginated_active_stream_preserves_existing_order() {
        let mut conn = test_diesel_conn();
        let session = "session-page-stream";
        let t = 1700000000;

        insert_run_for_session(&mut conn, "run-resumed", session, t);
        insert_turn(&mut conn, "turn-cr", session, 1);
        insert_event_at(
            &mut conn,
            "user-ev",
            "run-resumed",
            Some(session),
            50,
            "user",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-0",
            "run-resumed",
            Some(session),
            0,
            "assistant",
            Some("turn-cr"),
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-1",
            "run-resumed",
            Some(session),
            1,
            "tool",
            Some("turn-cr"),
            t,
        );

        let baseline = list_events_paginated(&mut conn, session, 10, None).unwrap();
        let baseline_ids: Vec<String> = baseline
            .events
            .iter()
            .map(|event| event.id.clone())
            .collect();

        insert_active_stream_at(
            &mut conn,
            "stream-1",
            "run-resumed",
            Some(session),
            Some("turn-cr"),
            2,
            t,
            "partial",
        );

        let paginated = list_events_paginated(&mut conn, session, 10, None).unwrap();
        let ids: Vec<String> = paginated
            .events
            .iter()
            .map(|event| event.id.clone())
            .collect();
        assert_eq!(&ids[..baseline_ids.len()], baseline_ids.as_slice());
        assert_eq!(ids.last().map(String::as_str), Some("stream-1"));
    }

    #[test]
    fn test_list_events_for_run_paginated_active_stream_preserves_existing_order() {
        let mut conn = test_diesel_conn();
        let run_id = "run-paged";
        let t = 1700000000;

        insert_run(&mut conn, run_id, "exited");
        insert_event_at(
            &mut conn,
            "user-ev",
            run_id,
            Some("session-1"),
            50,
            "user",
            None,
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-0",
            run_id,
            Some("session-1"),
            0,
            "assistant",
            None,
            t,
        );
        insert_event_at(
            &mut conn,
            "asst-1",
            run_id,
            Some("session-1"),
            1,
            "tool",
            None,
            t,
        );

        let baseline = list_events_for_run_paginated(&mut conn, run_id, 10, None).unwrap();
        let baseline_ids: Vec<String> = baseline
            .events
            .iter()
            .map(|event| event.id.clone())
            .collect();

        insert_active_stream_at(
            &mut conn,
            "stream-1",
            run_id,
            Some("session-1"),
            None,
            2,
            t,
            "partial",
        );

        let paginated = list_events_for_run_paginated(&mut conn, run_id, 10, None).unwrap();
        let ids: Vec<String> = paginated
            .events
            .iter()
            .map(|event| event.id.clone())
            .collect();
        assert_eq!(&ids[..baseline_ids.len()], baseline_ids.as_slice());
        assert_eq!(ids.last().map(String::as_str), Some("stream-1"));
    }

    #[test]
    fn test_list_events_for_turn_populates_turn_id() {
        let mut conn = test_diesel_conn();

        insert_run(&mut conn, "run-1", "exited");
        insert_turn(&mut conn, "turn-x", "sess-1", 1);
        insert_event(
            &mut conn,
            "e1",
            "run-1",
            None,
            1,
            "assistant",
            Some("turn-x"),
        );

        let events = list_events_for_turn(&mut conn, "turn-x").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].turn_id.as_deref(), Some("turn-x"));
    }

    #[test]
    fn test_list_events_for_session_orphan_events_sort_last() {
        let mut conn = test_diesel_conn();
        let session = "session-orphan";

        // Create one known run
        let now = chrono::Utc::now().timestamp() as i32;
        insert_run_for_session(&mut conn, "run-known", session, now);

        // Insert an "orphan" run that shares the session but has no runs table entry
        // (simulated by inserting events with a run_id that doesn't exist in runs)
        // We need a real run for the orphan events to reference, but we won't
        // include it in the session's runs query. Actually, the events just need
        // a run_id — there's no FK constraint enforced in SQLite by default.
        // But the function filters events by session_id, not by run_id.
        // So events with session_id=session but run_id not in the session's runs
        // should sort to the end (usize::MAX).

        // Events with known run
        insert_event(&mut conn, "e1", "run-known", Some(session), 1, "user", None);
        insert_event(
            &mut conn,
            "e2",
            "run-known",
            Some(session),
            2,
            "assistant",
            None,
        );

        // Events with unknown run_id (orphan — run exists but not for this session)
        insert_run_for_session(&mut conn, "run-orphan", "other-session", now);
        insert_event(
            &mut conn,
            "e3",
            "run-orphan",
            Some(session),
            1,
            "user",
            None,
        );

        let events = list_events_for_session(&mut conn, session).unwrap();
        assert_eq!(events.len(), 3);

        // Known run events come first, orphan sorts last
        assert_eq!(events[0].id, "e1");
        assert_eq!(events[1].id, "e2");
        assert_eq!(events[2].id, "e3");
    }
}
