use crate::common;
use std::sync::Arc;

use cairn_core::internal::services::EventEmitter;
use cairn_core::internal::storage::LocalDb;
use cairn_core::models::{RunStartMode, RunStatus};
use cairn_core::runs::queries;
use cairn_core::transcripts::stream_store::{
    abort_stream, append_chunks, open_stream, StreamChunkInput,
};
use cairn_db::turso::params;
use serde_json::Value;

struct NoopEmitter;

impl EventEmitter for NoopEmitter {
    fn emit(&self, _event: &str, _payload: Value) -> Result<(), String> {
        Ok(())
    }

    fn emit_empty(&self, _event: &str) -> Result<(), String> {
        Ok(())
    }
}

async fn insert_issue(db: &LocalDb, id: &str, project_id: &str, number: i64) {
    let id = id.to_string();
    let project_id = project_id.to_string();
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'Issue', 'active', 1, 1)",
        params![id.as_str(), project_id.as_str(), number],
    )
    .await
    .unwrap();
}

async fn insert_job(db: &LocalDb, id: &str, project_id: &str, issue_id: Option<&str>) {
    let id = id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.map(str::to_string);
    db.execute(
        "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'running', 1, 1)",
        params![id.as_str(), project_id.as_str(), issue_id.as_deref()],
    )
    .await
    .unwrap();
}

// Row-insert seed helper; the parameter list mirrors the runs table columns.
#[allow(clippy::too_many_arguments)]
async fn insert_run(
    db: &LocalDb,
    id: &str,
    project_id: Option<&str>,
    issue_id: Option<&str>,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    status: &str,
    session_id: Option<&str>,
    created_at: i64,
) {
    let id = id.to_string();
    let project_id = project_id.map(str::to_string);
    let issue_id = issue_id.map(str::to_string);
    let job_id = job_id.map(str::to_string);
    let chat_id = chat_id.map(str::to_string);
    let status = status.to_string();
    let session_id = session_id.map(str::to_string);
    db.write(|conn| {
        let id = id.clone();
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let job_id = job_id.clone();
        let chat_id = chat_id.clone();
        let status = status.clone();
        let session_id = session_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO runs(
                    id, project_id, issue_id, job_id, chat_id, status, session_id,
                    exit_reason, error_message, started_at, exited_at,
                    created_at, updated_at, start_mode
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, NULL, ?9, ?10, 'resume')",
                params![
                    id.as_str(),
                    project_id.as_deref(),
                    issue_id.as_deref(),
                    job_id.as_deref(),
                    chat_id.as_deref(),
                    status.as_str(),
                    session_id.as_deref(),
                    created_at,
                    created_at,
                    created_at
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_turn(
    db: &LocalDb,
    id: &str,
    session_id: &str,
    run_id: &str,
    job_id: Option<&str>,
    sequence: i64,
    state: &str,
) {
    let id = id.to_string();
    let session_id = session_id.to_string();
    let run_id = run_id.to_string();
    let job_id = job_id.map(str::to_string);
    let state = state.to_string();
    db.execute(
        "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 1)",
        params![
            id.as_str(),
            session_id.as_str(),
            run_id.as_str(),
            job_id.as_deref(),
            sequence,
            state.as_str()
        ],
    )
    .await
    .unwrap();
}

async fn set_job_current_turn(db: &LocalDb, job_id: &str, turn_id: &str) {
    let job_id = job_id.to_string();
    let turn_id = turn_id.to_string();
    db.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![turn_id.as_str(), job_id.as_str()],
    )
    .await
    .unwrap();
}

// Row-insert seed helper; the parameter list mirrors the events table columns.
#[allow(clippy::too_many_arguments)]
async fn insert_event(
    db: &LocalDb,
    id: &str,
    run_id: &str,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    sequence: i64,
    event_type: &str,
    created_at: i64,
    parent_tool_use_id: Option<&str>,
    input_tokens: Option<i64>,
    cache_read_tokens: Option<i64>,
    cache_create_tokens: Option<i64>,
    output_tokens: Option<i64>,
) {
    let id = id.to_string();
    let run_id = run_id.to_string();
    let session_id = session_id.map(str::to_string);
    let turn_id = turn_id.map(str::to_string);
    let event_type = event_type.to_string();
    let parent_tool_use_id = parent_tool_use_id.map(str::to_string);
    db.write(|conn| {
        let id = id.clone();
        let run_id = run_id.clone();
        let session_id = session_id.clone();
        let turn_id = turn_id.clone();
        let event_type = event_type.clone();
        let parent_tool_use_id = parent_tool_use_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events(
                    id, run_id, session_id, turn_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, '{}', ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    id.as_str(),
                    run_id.as_str(),
                    session_id.as_deref(),
                    turn_id.as_deref(),
                    sequence,
                    created_at,
                    event_type.as_str(),
                    parent_tool_use_id.as_deref(),
                    created_at,
                    input_tokens,
                    cache_read_tokens,
                    cache_create_tokens,
                    output_tokens
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_prompt(
    db: &LocalDb,
    id: &str,
    run_id: &str,
    turn_id: Option<&str>,
    response: Option<&str>,
    created_at: i64,
) {
    let id = id.to_string();
    let run_id = run_id.to_string();
    let turn_id = turn_id.map(str::to_string);
    let response = response.map(str::to_string);
    db.write(|conn| {
        let id = id.clone();
        let run_id = run_id.clone();
        let turn_id = turn_id.clone();
        let response = response.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO prompts(id, run_id, turn_id, questions, response, created_at, answered_at)
                 VALUES (?1, ?2, ?3, '[{\"question\":\"Test?\"}]', ?4, ?5, ?6)",
                params![
                    id.as_str(),
                    run_id.as_str(),
                    turn_id.as_deref(),
                    response.as_deref(),
                    created_at,
                    response.as_ref().map(|_| created_at)
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn list_runs_filters_by_issue_and_job() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RUNS").await;
    insert_issue(&db, "issue-1", &project_id, 1).await;
    insert_issue(&db, "issue-2", &project_id, 2).await;
    insert_job(&db, "job-1", &project_id, Some("issue-1")).await;
    insert_job(&db, "job-2", &project_id, Some("issue-2")).await;
    insert_run(
        &db,
        "run-1",
        Some(&project_id),
        Some("issue-1"),
        Some("job-1"),
        None,
        "running",
        Some("session-1"),
        10,
    )
    .await;
    insert_run(
        &db,
        "run-2",
        Some(&project_id),
        Some("issue-1"),
        Some("job-1"),
        None,
        "complete",
        Some("session-1"),
        20,
    )
    .await;
    insert_run(
        &db,
        "run-3",
        Some(&project_id),
        Some("issue-2"),
        Some("job-2"),
        None,
        "failed",
        Some("session-2"),
        30,
    )
    .await;

    let issue_runs = queries::list_runs(db.clone(), "issue-1").unwrap();
    assert_eq!(
        issue_runs
            .iter()
            .map(|run| run.id.as_str())
            .collect::<Vec<_>>(),
        vec!["run-2", "run-1"]
    );
    assert!(issue_runs
        .iter()
        .all(|run| run.issue_id.as_deref() == Some("issue-1")));

    let job_runs = queries::list_runs_for_job(db.clone(), "job-1").unwrap();
    assert_eq!(job_runs.len(), 2);
}

#[tokio::test]
async fn get_run_maps_runtime_fields() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RUNGET").await;
    insert_run(
        &db,
        "run-1",
        Some(&project_id),
        None,
        None,
        None,
        "running",
        Some("session-1"),
        42,
    )
    .await;

    let run = queries::get_run(db.clone(), "run-1").unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Live);
    assert_eq!(run.started_at, Some(42));
    assert_eq!(run.created_at, 42);
    assert_eq!(run.updated_at, 42);
    assert_eq!(run.start_mode, Some(RunStartMode::Resume));
    assert!(queries::get_run(db.clone(), "missing").unwrap().is_none());
}

#[tokio::test]
async fn list_events_for_run_orders_by_sequence_and_supports_limit_offset() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_run(&db, "run-1", None, None, None, None, "running", None, 1).await;
    insert_event(
        &db,
        "event-2",
        "run-1",
        Some("session-1"),
        None,
        2,
        "assistant",
        12,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "event-0",
        "run-1",
        Some("session-1"),
        None,
        0,
        "system",
        10,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "event-1",
        "run-1",
        Some("session-1"),
        None,
        1,
        "user",
        11,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let events = queries::list_events(db.clone(), "run-1").unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["event-0", "event-1", "event-2"]
    );

    let limited = queries::list_events_limited(db.clone(), "run-1", 1, 1).unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].id, "event-1");
}

#[tokio::test]
async fn list_events_for_session_orders_by_run_creation_then_insertion() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_run(
        &db,
        "run-b",
        None,
        None,
        None,
        None,
        "complete",
        Some("session-1"),
        100,
    )
    .await;
    insert_run(
        &db,
        "run-a",
        None,
        None,
        None,
        None,
        "running",
        Some("session-1"),
        101,
    )
    .await;
    insert_event(
        &db,
        "b-user",
        "run-b",
        Some("session-1"),
        None,
        1,
        "user",
        200,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "b-assistant",
        "run-b",
        Some("session-1"),
        None,
        2,
        "assistant",
        200,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "a-user",
        "run-a",
        Some("session-1"),
        None,
        1,
        "user",
        200,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let events = queries::list_events_for_session(db.clone(), "session-1").unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b-user", "b-assistant", "a-user"]
    );
}

// Insert a bare session row on `job_id` (the sessions CHECK requires exactly one
// of job_id / chat_id, and chat_id's parent table is gone). Lineage links are
// wired afterward with `link_session_rotation` / `set_session_parent` so no
// insert forward-references a not-yet-existing session row.
async fn insert_session(db: &LocalDb, id: &str, job_id: &str, sequence: i64) {
    let id = id.to_string();
    let job_id = job_id.to_string();
    db.execute(
        "INSERT INTO sessions(
            id, job_id, chat_id, backend, status, parent_session_id, replaced_by_id,
            terminal_reason, sequence, created_at, closed_at, updated_at, backend_id
         )
         VALUES (?1, ?2, NULL, 'claude', 'open', NULL, NULL, NULL, ?3, 1, NULL, 1, NULL)",
        params![id.as_str(), job_id.as_str(), sequence],
    )
    .await
    .unwrap();
}

// Wire an in-place rotation: `new` continues `old` (new.parent = old, old marked
// replaced_by new) — the shape the resume path produces on a cold-resume reseed.
async fn link_session_rotation(db: &LocalDb, old: &str, new: &str) {
    let old = old.to_string();
    let new = new.to_string();
    db.execute(
        "UPDATE sessions SET parent_session_id = ?1 WHERE id = ?2",
        params![old.as_str(), new.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE sessions SET replaced_by_id = ?1 WHERE id = ?2",
        params![new.as_str(), old.as_str()],
    )
    .await
    .unwrap();
}

// Point a session at a parent WITHOUT marking the parent replaced_by it — the
// shape a delegated child job's forked session has.
async fn set_session_parent(db: &LocalDb, id: &str, parent: &str) {
    let id = id.to_string();
    let parent = parent.to_string();
    db.execute(
        "UPDATE sessions SET parent_session_id = ?1 WHERE id = ?2",
        params![parent.as_str(), id.as_str()],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn list_events_for_session_spans_reseed_rotation_lineage() {
    // A cold-resume reseed rotates a job onto a fresh session that carries none
    // of the prior runs. The transcript must still span the predecessor session
    // so prior events are preserved rather than wiped (CAIRN-2630).
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);

    let project_id = common::create_project(&db, "RESEED").await;
    insert_job(&db, "job-1", &project_id, None).await;

    // session-old (root) was rotated in place into session-new; both on job-1.
    insert_session(&db, "session-old", "job-1", 1).await;
    insert_session(&db, "session-new", "job-1", 2).await;
    link_session_rotation(&db, "session-old", "session-new").await;

    insert_run(
        &db,
        "run-old",
        None,
        None,
        None,
        None,
        "complete",
        Some("session-old"),
        100,
    )
    .await;
    insert_run(
        &db,
        "run-new",
        None,
        None,
        None,
        None,
        "running",
        Some("session-new"),
        200,
    )
    .await;

    for (id, run, session, seq, kind) in [
        ("old-user", "run-old", "session-old", 1, "user"),
        ("old-assistant", "run-old", "session-old", 2, "assistant"),
        ("new-seed", "run-new", "session-new", 1, "user:seed"),
        ("new-user", "run-new", "session-new", 2, "user"),
    ] {
        insert_event(
            &db,
            id,
            run,
            Some(session),
            None,
            seq,
            kind,
            100,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    }

    // Loading the fresh session returns the full lineage, prior events first.
    let events = queries::list_events_for_session(db.clone(), "session-new").unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["old-user", "old-assistant", "new-seed", "new-user"]
    );

    // The initial delta load carries the same full lineage.
    let delta =
        queries::list_events_for_job_session_delta(db.clone(), "job-1", "session-new", None)
            .unwrap();
    assert_eq!(
        delta
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["old-user", "old-assistant", "new-seed", "new-user"]
    );

    // Loading the predecessor directly stays scoped to its own events — the walk
    // only follows genuine in-place rotation predecessors, never successors.
    let old_only = queries::list_events_for_session(db.clone(), "session-old").unwrap();
    assert_eq!(
        old_only
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["old-user", "old-assistant"]
    );
}

#[tokio::test]
async fn list_events_for_session_excludes_cross_job_fork_parent() {
    // A delegated child job forks the parent's session: it stamps
    // parent_session_id but the parent is NOT marked replaced_by the child (it
    // keeps serving its own job). The child transcript must not absorb the
    // parent agent's history (CAIRN-2630).
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);

    let project_id = common::create_project(&db, "FORK").await;
    insert_job(&db, "job-parent", &project_id, None).await;
    insert_job(&db, "job-child", &project_id, None).await;

    // parent-session was NOT replaced by the fork (it keeps serving its own job).
    insert_session(&db, "parent-session", "job-parent", 1).await;
    insert_session(&db, "fork-session", "job-child", 2).await;
    // The fork points back at the parent, but the parent's replaced_by_id stays
    // NULL — the exact shape that must NOT chain.
    set_session_parent(&db, "fork-session", "parent-session").await;

    insert_run(
        &db,
        "run-parent",
        None,
        None,
        None,
        None,
        "complete",
        Some("parent-session"),
        100,
    )
    .await;
    insert_run(
        &db,
        "run-fork",
        None,
        None,
        None,
        None,
        "running",
        Some("fork-session"),
        200,
    )
    .await;

    insert_event(
        &db,
        "parent-ev",
        "run-parent",
        Some("parent-session"),
        None,
        1,
        "user",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "fork-ev",
        "run-fork",
        Some("fork-session"),
        None,
        1,
        "user",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let parent = queries::list_events_for_job_session_delta(
        db.clone(),
        "job-parent",
        "parent-session",
        None,
    )
    .unwrap();
    assert_eq!(
        parent
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["parent-ev"]
    );

    let child =
        queries::list_events_for_job_session_delta(db.clone(), "job-child", "fork-session", None)
            .unwrap();
    assert_eq!(
        child
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["fork-ev"]
    );

    let error = queries::list_events_for_job_session_delta(db, "job-parent", "fork-session", None)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "session fork-session belongs to job job-child, not requested job job-parent"
    );
}

#[tokio::test]
async fn list_events_for_turn_preserves_same_second_insertion_order() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_run(
        &db,
        "run-1",
        None,
        None,
        None,
        None,
        "running",
        Some("session-1"),
        1,
    )
    .await;
    insert_turn(&db, "turn-1", "session-1", "run-1", None, 1, "running").await;
    insert_event(
        &db,
        "user-ev",
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        50,
        "user",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "asst-0",
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        0,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "asst-1",
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        1,
        "tool",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let events = queries::list_events_for_turn(db.clone(), "turn-1").unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["user-ev", "asst-0", "asst-1"]
    );
    assert!(events
        .iter()
        .all(|event| event.turn_id.as_deref() == Some("turn-1")));
}

#[tokio::test]
async fn session_events_delta_initial_load_is_run_ordered_with_cursor() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    // Two runs in one session; run-1 created before run-2.
    insert_run(
        &db,
        "run-1",
        None,
        None,
        None,
        None,
        "crashed",
        Some("session-1"),
        1,
    )
    .await;
    insert_run(
        &db,
        "run-2",
        None,
        None,
        None,
        None,
        "running",
        Some("session-1"),
        2,
    )
    .await;

    // Insert order (rowids 1,2,3) deliberately interleaves runs and does not
    // match run order, to prove the initial load sorts by run creation order.
    insert_event(
        &db,
        "e-a",
        "run-1",
        Some("session-1"),
        None,
        0,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "e-c",
        "run-2",
        Some("session-1"),
        None,
        0,
        "assistant",
        200,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "e-b",
        "run-1",
        Some("session-1"),
        None,
        1,
        "assistant",
        400,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let delta = queries::list_events_for_session_delta(db.clone(), "session-1", None).unwrap();
    // run-1 events (by created_at) then run-2 events.
    assert_eq!(
        delta
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["e-a", "e-b", "e-c"]
    );
    // Cursor is the session-wide MAX(rowid) = last inserted row (e-b).
    assert_eq!(delta.last_rowid, Some(3));
    assert!(delta.streaming.is_none());
}

#[tokio::test]
async fn session_events_delta_returns_only_new_events_and_echoes_cursor() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_run(
        &db,
        "run-1",
        None,
        None,
        None,
        None,
        "crashed",
        Some("session-1"),
        1,
    )
    .await;
    insert_event(
        &db,
        "e1",
        "run-1",
        Some("session-1"),
        None,
        0,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let initial = queries::list_events_for_session_delta(db.clone(), "session-1", None).unwrap();
    assert_eq!(initial.last_rowid, Some(1));

    // Empty delta echoes the cursor so the caller holds position.
    let empty = queries::list_events_for_session_delta(db.clone(), "session-1", initial.last_rowid)
        .unwrap();
    assert!(empty.events.is_empty());
    assert_eq!(empty.last_rowid, Some(1));

    // A new run inserts after the cursor; the delta picks it up across the run
    // boundary, in append (rowid) order.
    insert_run(
        &db,
        "run-2",
        None,
        None,
        None,
        None,
        "running",
        Some("session-1"),
        2,
    )
    .await;
    insert_event(
        &db,
        "e2",
        "run-2",
        Some("session-1"),
        None,
        0,
        "assistant",
        200,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "e3",
        "run-2",
        Some("session-1"),
        None,
        1,
        "assistant",
        300,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let delta = queries::list_events_for_session_delta(db.clone(), "session-1", initial.last_rowid)
        .unwrap();
    assert_eq!(
        delta
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["e2", "e3"]
    );
    assert_eq!(delta.last_rowid, Some(3));
}

#[tokio::test]
async fn session_events_delta_returns_streaming_placeholder_separately() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_run(
        &db,
        "run-1",
        None,
        None,
        None,
        None,
        "running",
        Some("session-1"),
        1,
    )
    .await;
    insert_turn(&db, "turn-1", "session-1", "run-1", None, 1, "running").await;
    insert_event(
        &db,
        "asst-0",
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        0,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let stream = open_stream(
        db.clone(),
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        "codex",
        Some(1),
    )
    .unwrap();
    let active = append_chunks(
        db.clone(),
        stream.stream_id(),
        stream.version(),
        &[StreamChunkInput::content("partial")],
    )
    .unwrap();

    let delta = queries::list_events_for_session_delta(db.clone(), "session-1", None).unwrap();
    // The placeholder is NOT spliced into events.
    assert_eq!(
        delta
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["asst-0"]
    );
    assert_eq!(
        delta.streaming.as_ref().map(|event| event.id.as_str()),
        Some(stream.stream_id())
    );
    assert_eq!(
        delta
            .streaming
            .as_ref()
            .map(|event| event.event_type.as_str()),
        Some("assistant:streaming")
    );

    // After the stream is aborted the placeholder clears.
    abort_stream(db.clone(), stream.stream_id(), active.version, "test").unwrap();
    let cleared = queries::list_events_for_session_delta(db.clone(), "session-1", None).unwrap();
    assert!(cleared.streaming.is_none());
}

#[tokio::test]
async fn active_streams_append_after_existing_events() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_run(
        &db,
        "run-1",
        None,
        None,
        None,
        None,
        "running",
        Some("session-1"),
        1,
    )
    .await;
    insert_turn(&db, "turn-1", "session-1", "run-1", None, 1, "running").await;
    insert_event(
        &db,
        "user-ev",
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        50,
        "user",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;
    insert_event(
        &db,
        "asst-0",
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        0,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let stream = open_stream(
        db.clone(),
        "run-1",
        Some("session-1"),
        Some("turn-1"),
        "codex",
        Some(1),
    )
    .unwrap();
    append_chunks(
        db.clone(),
        stream.stream_id(),
        stream.version(),
        &[StreamChunkInput::content("partial")],
    )
    .unwrap();

    let session_events = queries::list_events_for_session(db.clone(), "session-1").unwrap();
    assert_eq!(
        session_events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["user-ev", "asst-0", stream.stream_id()]
    );

    let turn_events = queries::list_events_for_turn(db.clone(), "turn-1").unwrap();
    assert_eq!(
        turn_events.last().map(|event| event.id.as_str()),
        Some(stream.stream_id())
    );
    assert_eq!(
        turn_events.last().map(|event| event.event_type.as_str()),
        Some("assistant:streaming")
    );
}

#[tokio::test]
async fn pending_prompt_queries_use_run_and_current_turn() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "PROMPTS").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-1",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "running",
        Some("session-1"),
        1,
    )
    .await;
    insert_turn(
        &db,
        "turn-1",
        "session-1",
        "run-1",
        Some("job-1"),
        1,
        "running",
    )
    .await;
    set_job_current_turn(&db, "job-1", "turn-1").await;
    insert_prompt(&db, "answered", "run-1", Some("turn-1"), Some("yes"), 10).await;
    insert_prompt(&db, "pending-old", "run-1", Some("turn-1"), None, 20).await;
    insert_prompt(&db, "pending-new", "run-1", Some("turn-1"), None, 30).await;

    let by_run = queries::get_pending_prompt(db.clone(), "run-1")
        .unwrap()
        .unwrap();
    assert_eq!(by_run.id, "pending-new");
    assert!(by_run.response.is_none());

    let by_job = queries::get_pending_prompt_for_job(db.clone(), "job-1")
        .unwrap()
        .unwrap();
    assert_eq!(by_job.id, "pending-new");
    assert!(queries::get_pending_prompt(db.clone(), "missing")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn prompts_tool_use_id_round_trips() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "PTUID").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-1",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "running",
        Some("session-1"),
        1,
    )
    .await;

    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO prompts(id, run_id, questions, response, created_at, tool_use_id)
                 VALUES ('p-1', 'run-1', '[{\"question\":\"Test?\"}]', NULL, 10, 'toolu_abc')",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let stored =
        common::scalar_text_by_id(&db, "SELECT tool_use_id FROM prompts WHERE id = ?1", "p-1")
            .await;
    assert_eq!(stored.as_deref(), Some("toolu_abc"));
}

#[tokio::test]
async fn reconcile_stale_runs_marks_process_and_turn_terminal() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "STALE").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-live",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("session-1"),
        1,
    )
    .await;
    insert_run(
        &db,
        "run-exited",
        Some(&project_id),
        None,
        None,
        None,
        "exited",
        Some("session-2"),
        1,
    )
    .await;
    insert_turn(
        &db,
        "turn-running",
        "session-1",
        "run-live",
        Some("job-1"),
        1,
        "running",
    )
    .await;

    queries::reconcile_stale_runs(db.clone(), &NoopEmitter);

    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM runs WHERE id = ?1", "run-live")
            .await
            .as_deref(),
        Some("crashed")
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT state FROM turns WHERE id = ?1", "turn-running")
            .await
            .as_deref(),
        Some("interrupted")
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM runs WHERE id = ?1", "run-exited")
            .await
            .as_deref(),
        Some("exited")
    );
}
