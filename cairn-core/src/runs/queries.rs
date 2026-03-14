use crate::diesel_models::{DbEvent, DbPrompt, DbRun};
use crate::models::{Event, Prompt, Run, RunStatus};
use crate::models::{RunTodos, TodoItem};
use crate::schema::{events, prompts, runs};
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
    let todos = db
        .todos
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    Run {
        id: db.id,
        issue_id: db.issue_id,
        project_id: db.project_id,
        job_id: db.job_id,
        chat_id: db.chat_id,
        status: db
            .status
            .and_then(|s| s.parse().ok())
            .unwrap_or(RunStatus::Pending),
        claude_session_id: db.claude_session_id,
        error_message: db.error_message,
        started_at: db.started_at.map(|t| t as i64),
        completed_at: db.completed_at.map(|t| t as i64),
        created_at: db.created_at as i64,
        updated_at: db.updated_at as i64,
        todos,
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
    }
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

    Ok(db_events.into_iter().map(db_event_to_event).collect())
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
pub fn list_events_for_session(
    conn: &mut SqliteConnection,
    session_id: &str,
) -> Result<Vec<Event>, String> {
    let db_events: Vec<DbEvent> = events::table
        .filter(events::session_id.eq(session_id))
        .order((events::created_at.asc(), events::sequence.asc()))
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_events.into_iter().map(db_event_to_event).collect())
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

    let events: Vec<Event> = events_to_return
        .into_iter()
        .rev()
        .map(db_event_to_event)
        .collect();

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

    let events: Vec<Event> = events_to_return
        .into_iter()
        .rev()
        .map(db_event_to_event)
        .collect();

    Ok(PaginatedEvents {
        events,
        has_more,
        next_cursor,
    })
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

/// Extract todos array from TodoWrite tool input
fn extract_todos_from_input(input: &serde_json::Value) -> Option<Vec<TodoItem>> {
    let todos_value = input.get("todos")?;
    let todos_array = todos_value.as_array()?;
    let todos: Vec<TodoItem> = todos_array
        .iter()
        .filter_map(|item| {
            let content = item.get("content")?.as_str()?.to_string();
            let status = item.get("status")?.as_str()?.to_string();
            let active_form = item
                .get("activeForm")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            Some(TodoItem {
                content,
                status,
                active_form,
            })
        })
        .collect();
    if todos.is_empty() {
        None
    } else {
        Some(todos)
    }
}

/// Get the current todo list for a job.
/// Extracts todos from the most recent TodoWrite tool call in the session.
pub fn get_job_todos(conn: &mut SqliteConnection, job_id: &str) -> Result<Vec<TodoItem>, String> {
    use crate::schema::jobs;

    let session_id: Option<String> = jobs::table
        .find(job_id)
        .select(jobs::claude_session_id)
        .first::<Option<String>>(conn)
        .optional()
        .map_err(|e| format!("Job not found: {}", e))?
        .flatten();

    let session_id = match session_id {
        Some(id) => id,
        None => return Ok(vec![]),
    };

    let event_data: Vec<String> = events::table
        .filter(events::session_id.eq(&session_id))
        .order((events::created_at.desc(), events::sequence.desc()))
        .select(events::data)
        .load(conn)
        .map_err(|e| e.to_string())?;

    for data in event_data {
        // Parse the event data as JSON to check for TodoWrite tool calls
        let parsed: serde_json::Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Check direct tool_name field
        if parsed.get("tool_name").and_then(|v| v.as_str()) == Some("TodoWrite") {
            if let Some(input) = parsed.get("tool_input") {
                if let Some(todos) = extract_todos_from_input(input) {
                    return Ok(todos);
                }
            }
        }

        // Check tool_uses array
        if let Some(tool_uses) = parsed.get("tool_uses").and_then(|v| v.as_array()) {
            for tool_use in tool_uses {
                if tool_use.get("name").and_then(|v| v.as_str()) == Some("TodoWrite") {
                    if let Some(input) = tool_use.get("input") {
                        if let Some(todos) = extract_todos_from_input(input) {
                            return Ok(todos);
                        }
                    }
                }
            }
        }
    }

    Ok(vec![])
}

/// Get todos grouped by run for a job.
pub fn get_job_todos_by_run(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Vec<RunTodos>, String> {
    use crate::schema::jobs;

    let session_id: Option<String> = jobs::table
        .find(job_id)
        .select(jobs::claude_session_id)
        .first::<Option<String>>(conn)
        .optional()
        .map_err(|e| format!("Job not found: {}", e))?
        .flatten();

    let session_id = match session_id {
        Some(id) => id,
        None => return Ok(vec![]),
    };

    let run_rows: Vec<(String, Option<String>)> = runs::table
        .filter(runs::claude_session_id.eq(&session_id))
        .filter(runs::todos.is_not_null())
        .select((runs::id, runs::todos))
        .order(runs::created_at.asc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    let results: Vec<RunTodos> = run_rows
        .into_iter()
        .enumerate()
        .filter_map(|(i, (run_id, todos_json))| {
            let todos_str = todos_json?;
            let todos: Vec<TodoItem> = serde_json::from_str(&todos_str).ok()?;
            if todos.is_empty() {
                None
            } else {
                Some(RunTodos {
                    run_id,
                    run_number: (i + 1) as u32,
                    todos,
                })
            }
        })
        .collect();

    Ok(results)
}
