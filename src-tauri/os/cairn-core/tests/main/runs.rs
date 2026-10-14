use crate::common;
use std::collections::HashSet;
use std::sync::Arc;

use cairn_core::internal::agent_process::orphan::RecordingProcessTable;
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
    let project_id = common::create_project(&db, "runs").await;
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
    let project_id = common::create_project(&db, "runget").await;
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

// Stamp a job's active session, the pointer every node-keyed transcript read
// resolves through.
async fn set_current_session(db: &LocalDb, job_id: &str, session_id: &str) {
    let job_id = job_id.to_string();
    let session_id = session_id.to_string();
    db.execute(
        "UPDATE jobs SET current_session_id = ?1 WHERE id = ?2",
        params![session_id.as_str(), job_id.as_str()],
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

    let project_id = common::create_project(&db, "reseed").await;
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

    // The initial delta load carries the same full lineage, resolved from the
    // node alone — no caller names a session.
    set_current_session(&db, "job-1", "session-new").await;
    let delta = queries::list_events_for_job_delta(db.clone(), "job-1", None).unwrap();
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

    let project_id = common::create_project(&db, "fork").await;
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

    set_current_session(&db, "job-parent", "parent-session").await;
    set_current_session(&db, "job-child", "fork-session").await;

    let parent = queries::list_events_for_job_delta(db.clone(), "job-parent", None).unwrap();
    assert_eq!(
        parent
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["parent-ev"]
    );

    // The child's forked transcript stays its own: each node resolves its own
    // session, so no request can reach across the fork in either direction.
    let child = queries::list_events_for_job_delta(db, "job-child", None).unwrap();
    assert_eq!(
        child
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["fork-ev"]
    );
}

#[tokio::test]
async fn job_transcript_delta_follows_a_session_reconstruction() {
    // Resuming a node from digest rotates it onto a NEW session under the same
    // node. A view holding a cursor from before the rotation must keep
    // receiving events — the successor's rows arrive as an ordinary delta, not
    // as a re-keyed reload (CAIRN-3262).
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);

    let project_id = common::create_project(&db, "resume").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_session(&db, "session-old", "job-1", 1).await;
    set_current_session(&db, "job-1", "session-old").await;
    insert_run(
        &db,
        "run-old",
        None,
        None,
        Some("job-1"),
        None,
        "complete",
        Some("session-old"),
        100,
    )
    .await;
    insert_event(
        &db,
        "old-assistant",
        "run-old",
        Some("session-old"),
        None,
        1,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    // The open view's position after its initial load.
    let initial = queries::list_events_for_job_delta(db.clone(), "job-1", None).unwrap();
    assert_eq!(initial.events.len(), 1);
    let cursor = initial.last_rowid.expect("initial load yields a cursor");

    // The reseed lands: a fresh session, marked as replacing the old one, and
    // stamped as the node's current session.
    insert_session(&db, "session-new", "job-1", 2).await;
    link_session_rotation(&db, "session-old", "session-new").await;
    set_current_session(&db, "job-1", "session-new").await;

    // A rotated node with no events yet on its successor still hands back a
    // usable cursor; a session-scoped MAX(rowid) would return null here and the
    // caller's append-only merge would duplicate the whole prior transcript.
    let rotated = queries::list_events_for_job_delta(db.clone(), "job-1", None).unwrap();
    assert_eq!(
        rotated
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["old-assistant"]
    );
    assert_eq!(rotated.last_rowid, Some(cursor));

    insert_run(
        &db,
        "run-new",
        None,
        None,
        Some("job-1"),
        None,
        "running",
        Some("session-new"),
        200,
    )
    .await;
    insert_event(
        &db,
        "new-assistant",
        "run-new",
        Some("session-new"),
        None,
        1,
        "assistant",
        200,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    // The pre-rotation cursor keeps working: the successor's event arrives as a
    // plain delta on the same node key.
    let delta = queries::list_events_for_job_delta(db.clone(), "job-1", Some(cursor)).unwrap();
    assert_eq!(
        delta
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["new-assistant"]
    );
    assert!(delta.last_rowid.unwrap() > cursor);
}

#[tokio::test]
async fn job_transcript_delta_resolves_the_newest_run_before_a_session_is_stamped() {
    // A new chat runs before `jobs.current_session_id` is populated. The node
    // key still resolves a transcript, so there is no second client-side
    // loading path for that window.
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);

    let project_id = common::create_project(&db, "newchat").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_session(&db, "session-1", "job-1", 1).await;
    insert_run(
        &db,
        "run-1",
        None,
        None,
        Some("job-1"),
        None,
        "running",
        Some("session-1"),
        100,
    )
    .await;
    insert_event(
        &db,
        "first",
        "run-1",
        Some("session-1"),
        None,
        1,
        "assistant",
        100,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

    let delta = queries::list_events_for_job_delta(db.clone(), "job-1", None).unwrap();
    assert_eq!(
        delta
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["first"]
    );

    // A node that has never opened a session reads as an empty transcript, not
    // an error, and holds the caller's position.
    insert_job(&db, "job-empty", &project_id, None).await;
    let empty = queries::list_events_for_job_delta(db, "job-empty", Some(7)).unwrap();
    assert!(empty.events.is_empty());
    assert_eq!(empty.last_rowid, Some(7));
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
    let project_id = common::create_project(&db, "prompts").await;
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
    let project_id = common::create_project(&db, "ptuid").await;
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
    let project_id = common::create_project(&db, "stale").await;
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

    // No process on the machine names this run's session, so nothing is stopped
    // and the row records an assumed death.
    let processes = RecordingProcessTable::new(vec![(
        4242,
        "/bin/claude --session-id some-other-session".to_string(),
    )]);
    queries::reconcile_stale_runs_with(db.clone(), &NoopEmitter, STALE_BOOT_AT, &processes);

    assert!(
        processes.stopped().is_empty(),
        "an unrelated process must never be signalled"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-live"
        )
        .await
        .as_deref(),
        Some("crash"),
        "a death this sweep did not cause stays an assumed crash"
    );
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

/// CAIRN-3287: a runner that died without cleaning up leaves its agents running.
/// The startup sweep stops the survivor identified by the session UUID in its
/// argv — and only that one — and records that it was stopped rather than
/// inventing a crash it never confirmed.
#[tokio::test]
async fn reconcile_stale_runs_reaps_the_orphan_that_names_a_stale_session() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "orphan").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-orphaned",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("934d794a-orphan"),
        1,
    )
    .await;
    insert_run(
        &db,
        "run-vanished",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("4fac7e94-vanished"),
        1,
    )
    .await;

    // A surviving agent for the first run, an unrelated tool, and this test's own
    // runner-shaped process. Only the first may be signalled.
    let processes = RecordingProcessTable::new(vec![
        (
            5001,
            "/bin/claude --session-id 934d794a-orphan --print".to_string(),
        ),
        (5002, "/usr/bin/rg --files".to_string()),
        (5003, "cairn-runner run --port 3849".to_string()),
    ]);

    queries::reconcile_stale_runs_with(db.clone(), &NoopEmitter, STALE_BOOT_AT, &processes);

    assert_eq!(
        processes.stopped(),
        vec![5001],
        "only the process naming a stale session may be signalled"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-orphaned"
        )
        .await
        .as_deref(),
        Some(queries::ORPHAN_REAPED_EXIT_REASON),
        "a run whose process WE stopped must say so"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-vanished"
        )
        .await
        .as_deref(),
        Some("crash"),
        "a run with no surviving process is still only presumed dead"
    );
}

/// A host boot far in the future of the fixtures' epoch timestamps, so a seeded run
/// reads as a predecessor's leftover. Tests needing the other side of the boundary
/// seed a run past it instead.
const STALE_BOOT_AT: i64 = 1_000_000;

#[tokio::test]
async fn reconcile_stale_runs_cancels_orphaned_pending_turn_by_turn_identity() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "orphan-turn").await;
    insert_job(&db, "job-orphan-turn", &project_id, None).await;
    db.execute(
        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('session-orphan-turn', 'job-orphan-turn', 'codex', 'open', 1, 1, 1)",
        (),
    ).await.unwrap();
    db.execute(
        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('turn-orphan', 'session-orphan-turn', 'job-orphan-turn', 1, 'pending', 'follow_up', 1, 1)",
        (),
    ).await.unwrap();
    db.execute(
        "UPDATE jobs SET current_turn_id = 'turn-orphan' WHERE id = 'job-orphan-turn'",
        (),
    )
    .await
    .unwrap();
    // The exact incident shape: a boot-time resume run exists for the job but
    // the pending head was never attached to it. One reconciliation must first
    // crash this stale run and then discover the now-unhidden runless turn.
    insert_run(
        &db,
        "run-stale-resume",
        Some(&project_id),
        None,
        Some("job-orphan-turn"),
        None,
        "live",
        None,
        1,
    )
    .await;

    queries::reconcile_stale_runs_with_claims(
        db.clone(),
        &NoopEmitter,
        STALE_BOOT_AT,
        &RecordingProcessTable::default(),
        &HashSet::new(),
    );

    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT status FROM runs WHERE id = ?1",
            "run-stale-resume"
        )
        .await
        .as_deref(),
        Some("crashed")
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT state FROM turns WHERE id = ?1", "turn-orphan")
            .await
            .as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT end_reason FROM turns WHERE id = ?1",
            "turn-orphan"
        )
        .await
        .as_deref(),
        Some("orphaned_pending")
    );
    let derived = db
        .read(|conn| {
            Box::pin(async move {
                cairn_core::internal::execution::advancement::recompute_job_status_conn(
                    conn,
                    "job-orphan-turn",
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_eq!(derived, cairn_core::models::JobStatus::Idle);
}

#[tokio::test]
async fn reconcile_stale_runs_repairs_jobs_orphaned_by_a_previous_boot() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = &orch.db.local;
    let project_id = common::create_project(db, "orphan-upgrade").await;
    let recipe_snapshot = serde_json::json!({
        "recipe": {
            "id": "recipe-upgrade", "name": "Recipe", "description": null,
            "trigger": "manual",
            "nodes": [
                {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
            ],
            "edges": []
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": null, "projectId": project_id, "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
    .to_string();
    let host_snapshot = serde_json::json!({
        "recipe": {
            "id": "host-thread", "name": "Session Host", "description": null,
            "trigger": "manual", "nodes": [], "edges": []
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": null, "projectId": project_id, "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
    .to_string();
    db.execute(
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
         VALUES ('thread-upgrade', ?1, 'upgrade', 'active', 'none', 1, 1)",
        (project_id.clone(),),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
         VALUES ('execution-recipe-upgrade', 'recipe-upgrade', ?1, 'running', 1, ?2, 'manual'),
                ('execution-thread-upgrade', 'host-thread', ?1, 'running', 1, ?3, 'manual')",
        params![project_id.as_str(), recipe_snapshot, host_snapshot],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, project_id, status, node_name, uri_segment, created_at, updated_at)
         VALUES ('job-recipe-upgrade', 'execution-recipe-upgrade', 'builder', ?1, 'running', 'Builder', 'builder', 1, 1)",
        (project_id.clone(),),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs (id, execution_id, thread_id, project_id, status, node_name, uri_segment, created_at, updated_at)
         VALUES ('job-thread-upgrade', 'execution-thread-upgrade', 'thread-upgrade', ?1, 'running', 'Thread', 'thread', 1, 1)",
        (project_id.clone(),),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
         VALUES ('session-recipe-upgrade', 'job-recipe-upgrade', 'codex', 'open', 1, 1, 1),
                ('session-thread-upgrade', 'job-thread-upgrade', 'codex', 'open', 1, 1, 1)",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, end_reason, created_at, ended_at, updated_at)
         VALUES ('turn-recipe-upgrade', 'session-recipe-upgrade', 'job-recipe-upgrade', 1, 'cancelled', 'initial', 'orphaned_pending', 1, 2, 2),
                ('turn-thread-upgrade', 'session-thread-upgrade', 'job-thread-upgrade', 1, 'cancelled', 'initial', 'orphaned_pending', 1, 2, 2)",
        (),
    )
    .await
    .unwrap();

    queries::reconcile_stale_runs(&orch, &NoopEmitter, STALE_BOOT_AT);

    assert_eq!(
        common::scalar_text_by_id(
            db,
            "SELECT status FROM jobs WHERE id = ?1",
            "job-recipe-upgrade"
        )
        .await
        .as_deref(),
        Some("pending")
    );
    assert_eq!(
        common::scalar_text_by_id(
            db,
            "SELECT status FROM jobs WHERE id = ?1",
            "job-thread-upgrade"
        )
        .await
        .as_deref(),
        Some("idle")
    );
}

/// Jobs left non-terminal on a merged issue by a release that never settled
/// resolution are re-derived at boot and settle Cancelled; the identical shape
/// on an active issue is not selected and keeps its status.
#[tokio::test]
async fn reconcile_stale_runs_settles_jobs_left_on_resolved_issues() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = &orch.db.local;
    let project_id = common::create_project(db, "resolved-repair").await;
    let snapshot = serde_json::json!({
        "recipe": {
            "id": "recipe-resolved", "name": "Recipe", "description": null,
            "trigger": "manual",
            "nodes": [
                {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
            ],
            "edges": []
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": null, "projectId": project_id, "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
    .to_string();
    db.execute(
        "INSERT INTO issues (id, project_id, number, title, status, progress, attention, merged_at, created_at, updated_at)
         VALUES ('issue-merged', ?1, 901, 'Merged', 'merged', 'merged', 'none', 1, 1, 1),
                ('issue-active', ?1, 902, 'Active', 'active', 'active', 'none', NULL, 1, 1)",
        (project_id.clone(),),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
         VALUES ('execution-merged', 'recipe-resolved', 'issue-merged', ?1, 'running', 1, 1, ?2, 'manual'),
                ('execution-active', 'recipe-resolved', 'issue-active', ?1, 'running', 1, 1, ?2, 'manual')",
        params![project_id.as_str(), snapshot],
    )
    .await
    .unwrap();
    for (job, execution, issue, session_status) in [
        ("job-merged", "execution-merged", "issue-merged", "closed"),
        ("job-active", "execution-active", "issue-active", "open"),
    ] {
        db.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, node_name, uri_segment, current_session_id, created_at, updated_at)
             VALUES (?1, ?2, 'builder', ?3, ?4, 'running', 'Builder', 'builder', 'session-' || ?1, 1, 1)",
            params![job, execution, issue, project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES ('session-' || ?1, ?1, 'codex', ?2, 1, 1, 1)",
            params![job, session_status],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
             VALUES ('turn-' || ?1, 'session-' || ?1, ?1, 1, 'interrupted', 'initial', 1, 2, 2)",
            params![job],
        )
        .await
        .unwrap();
    }

    queries::reconcile_stale_runs(&orch, &NoopEmitter, STALE_BOOT_AT);

    assert_eq!(
        common::scalar_text_by_id(db, "SELECT status FROM jobs WHERE id = ?1", "job-merged")
            .await
            .as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        common::scalar_text_by_id(db, "SELECT status FROM jobs WHERE id = ?1", "job-active")
            .await
            .as_deref(),
        Some("running"),
        "the resolved-issue repair selects only jobs on resolved issues"
    );
}

#[tokio::test]
async fn reconcile_stale_runs_repairs_a_producer_pinned_by_a_warm_run() {
    // CAIRN-4742: a release that counted a warm provider run as work liveness
    // left artifact producers cached `running` past a completed work turn, and
    // nothing recomputes such a job once its session goes quiet. The startup
    // reconciliation re-derives it — without touching the warm run, which here
    // postdates this host's boot and so is deliberately not reaped.
    let (_temp, orch) = common::test_orchestrator().await;
    let db = &orch.db.local;
    let project_id = common::create_project(db, "warm-pinned").await;
    let recipe_snapshot = serde_json::json!({
        "recipe": {
            "id": "recipe-warm", "name": "Recipe", "description": null,
            "trigger": "manual",
            "nodes": [
                {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
            ],
            "edges": []
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": null, "projectId": project_id, "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
    .to_string();
    db.execute(
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
         VALUES ('execution-warm', 'recipe-warm', ?1, 'running', 1, ?2, 'manual')",
        params![project_id.as_str(), recipe_snapshot],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, project_id, status, node_name, uri_segment, created_at, updated_at)
         VALUES ('job-warm', 'execution-warm', 'builder', ?1, 'running', 'Builder', 'builder', 1, 1)",
        (project_id.clone(),),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
         VALUES ('session-warm', 'job-warm', 'codex', 'open', 1, 1, 1)",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, end_reason, created_at, ended_at, updated_at)
         VALUES ('turn-warm', 'session-warm', 'job-warm', 1, 'complete', 'initial', 'artifact_handoff', 1, 2, 2)",
        (),
    )
    .await
    .unwrap();
    // Postdates STALE_BOOT_AT, so the stale sweep leaves this warm process alone.
    db.execute(
        "INSERT INTO runs (id, job_id, project_id, status, session_id, created_at, updated_at)
         VALUES ('run-warm', 'job-warm', ?1, 'live', 'session-warm', 2000000, 2000000)",
        (project_id.clone(),),
    )
    .await
    .unwrap();

    queries::reconcile_stale_runs(&orch, &NoopEmitter, STALE_BOOT_AT);

    assert_eq!(
        common::scalar_text_by_id(db, "SELECT status FROM jobs WHERE id = ?1", "job-warm")
            .await
            .as_deref(),
        Some("complete"),
        "a producer whose work turn completed is re-derived, not left pinned"
    );
    assert_eq!(
        common::scalar_text_by_id(db, "SELECT status FROM runs WHERE id = ?1", "run-warm")
            .await
            .as_deref(),
        Some("live"),
        "the repair re-derives status; it never tears the warm process down"
    );
}

#[tokio::test]
async fn reconcile_stale_runs_leaves_claimed_pending_turn_alone() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "claimed-turn").await;
    insert_job(&db, "job-claimed-turn", &project_id, None).await;
    db.execute(
        "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('session-claimed-turn', 'job-claimed-turn', 'codex', 'open', 1, 1, 1)",
        (),
    ).await.unwrap();
    db.execute(
        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('turn-claimed', 'session-claimed-turn', 'job-claimed-turn', 1, 'pending', 'follow_up', 1, 1)",
        (),
    ).await.unwrap();
    let claims = HashSet::from(["job-claimed-turn".to_string()]);

    queries::reconcile_stale_runs_with_claims(
        db.clone(),
        &NoopEmitter,
        STALE_BOOT_AT,
        &RecordingProcessTable::default(),
        &claims,
    );

    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT state FROM turns WHERE id = ?1", "turn-claimed")
            .await
            .as_deref(),
        Some("pending")
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT end_reason FROM turns WHERE id = ?1",
            "turn-claimed"
        )
        .await,
        None
    );
}

async fn set_run_timestamps(db: &LocalDb, run_id: &str, started_at: i64, created_at: i64) {
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE runs SET started_at = ?1, created_at = ?2 WHERE id = ?3",
                params![started_at, created_at, run_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The dangerous direction of the CAIRN-3287 sweep, and the reason it takes a boot
/// boundary at all.
///
/// Startup recovery runs AFTER the transport begins serving, so a run can be created
/// while the sweep is in flight. Such a run is non-terminal and its session id is
/// visible in `ps` — exactly what the sweep matches on — so an unscoped sweep would
/// SIGTERM an agent this very host had just spawned and then mark its run crashed.
/// It must be left entirely alone: not signalled, not rewritten.
#[tokio::test]
async fn reconcile_stale_runs_never_touches_a_run_this_host_spawned() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "owned").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-just-spawned",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("fresh-session-uuid"),
        1,
    )
    .await;
    // Spawned one second into this host's life, mid-recovery.
    set_run_timestamps(
        &db,
        "run-just-spawned",
        STALE_BOOT_AT + 1,
        STALE_BOOT_AT + 1,
    )
    .await;
    insert_turn(
        &db,
        "turn-running",
        "fresh-session-uuid",
        "run-just-spawned",
        Some("job-1"),
        1,
        "running",
    )
    .await;

    // Its agent is running and would match on session id if it were a candidate.
    let processes = RecordingProcessTable::new(vec![(
        6001,
        "/bin/claude --session-id fresh-session-uuid --print".to_string(),
    )]);

    queries::reconcile_stale_runs_with(db.clone(), &NoopEmitter, STALE_BOOT_AT, &processes);

    assert!(
        processes.stopped().is_empty(),
        "the sweep must never signal an agent this host spawned, got {:?}",
        processes.stopped()
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT status FROM runs WHERE id = ?1",
            "run-just-spawned"
        )
        .await
        .as_deref(),
        Some("live"),
        "a run this host owns stays live"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-just-spawned"
        )
        .await,
        None,
        "and gains no exit reason"
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT state FROM turns WHERE id = ?1", "turn-running")
            .await
            .as_deref(),
        Some("running"),
        "nor is its in-flight turn interrupted"
    );
}

/// The boundary is strict, matching the tool-serving fence: a run stamped exactly at
/// boot belongs to this host. Whole-second timestamps make this the common case for
/// the first run after a restart, not a curiosity.
#[tokio::test]
async fn reconcile_stale_runs_leaves_a_run_stamped_at_the_boot_second_alone() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "bound").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-boot-second",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("boot-second-session"),
        1,
    )
    .await;
    set_run_timestamps(&db, "run-boot-second", STALE_BOOT_AT, STALE_BOOT_AT).await;

    let processes = RecordingProcessTable::new(vec![(
        6002,
        "/bin/claude --session-id boot-second-session".to_string(),
    )]);

    queries::reconcile_stale_runs_with(db.clone(), &NoopEmitter, STALE_BOOT_AT, &processes);

    assert!(processes.stopped().is_empty());
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT status FROM runs WHERE id = ?1",
            "run-boot-second"
        )
        .await
        .as_deref(),
        Some("live")
    );
}

/// A run that finalizes between the sweep's read and its write keeps its real
/// outcome. Signalling a process can take up to a second, so that gap is wide
/// enough to matter, and the invented `crash` must not overwrite the truth.
#[tokio::test]
async fn reconcile_stale_runs_does_not_overwrite_a_run_that_finalized_meanwhile() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "race").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-finalizing",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("racing-session"),
        1,
    )
    .await;

    // Stand in for the concurrent finalizer by having the process table finalize
    // the run at the moment the sweep signals it: that call happens between the
    // candidate read and the terminal write, which is precisely the window.
    struct FinalizeOnStop {
        db: Arc<LocalDb>,
    }
    impl cairn_core::internal::agent_process::orphan::ProcessTable for FinalizeOnStop {
        fn list(&self) -> Vec<(u32, String)> {
            vec![(6003, "/bin/claude --session-id racing-session".to_string())]
        }

        fn stop(&self, _pid: u32) -> bool {
            let db = self.db.clone();
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        db.execute(
                            "UPDATE runs
                             SET status = 'exited', exit_reason = 'user_stop'
                             WHERE id = 'run-finalizing'",
                            (),
                        )
                        .await
                        .unwrap();
                    })
            })
            .join()
            .unwrap();
            true
        }
    }

    queries::reconcile_stale_runs_with(
        db.clone(),
        &NoopEmitter,
        STALE_BOOT_AT,
        &FinalizeOnStop { db: db.clone() },
    );

    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-finalizing"
        )
        .await
        .as_deref(),
        Some("user_stop"),
        "a real outcome recorded during the sweep must survive it"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT status FROM runs WHERE id = ?1",
            "run-finalizing"
        )
        .await
        .as_deref(),
        Some("exited")
    );
}

/// The sweep tried to stop a survivor and could not (no permission, or it outlived
/// escalation). The run is still marked crashed — its host is gone — but it must NOT
/// claim `orphan_reaped`, because nothing was confirmed stopped and the zombie may
/// still be alive.
#[tokio::test]
async fn reconcile_stale_runs_does_not_claim_a_reap_it_could_not_confirm() {
    let (_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "unstop").await;
    insert_job(&db, "job-1", &project_id, None).await;
    insert_run(
        &db,
        "run-unstoppable",
        Some(&project_id),
        None,
        Some("job-1"),
        None,
        "live",
        Some("stubborn-session"),
        1,
    )
    .await;

    let processes = RecordingProcessTable::unable_to_stop(vec![(
        6004,
        "/bin/claude --session-id stubborn-session".to_string(),
    )]);

    queries::reconcile_stale_runs_with(db.clone(), &NoopEmitter, STALE_BOOT_AT, &processes);

    assert_eq!(processes.stopped(), vec![6004], "the stop is attempted");
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-unstoppable"
        )
        .await
        .as_deref(),
        Some("crash"),
        "an unconfirmed stop stays an assumed death, never orphan_reaped"
    );
}

/// A launch queued for admission when the previous host stopped predates boot,
/// yet it is not stale: startup recovery relaunches it from the durable queue.
/// A run whose admission row was `active` had a process that died with the host,
/// so that one is settled.
#[tokio::test]
async fn reconcile_stale_runs_leaves_a_durably_queued_launch_for_recovery() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = &orch.db.local;
    let project_id = common::create_project(db, "queued-recovery").await;
    db.execute_script(&format!(
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at, seq) VALUES ('exec-q', 'r', '{project_id}', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('job-q', 'exec-q', '{project_id}', 'running', 'Builder', 'builder', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('session-q', 'job-q', 'claude', 'open', 1, 1, 1);
         INSERT INTO runs (id, project_id, job_id, session_id, status, start_mode, created_at, updated_at) VALUES
           ('run-queued', '{project_id}', 'job-q', 'session-q', 'starting', 'resume', 1, 1),
           ('run-active', '{project_id}', 'job-q', 'session-q', 'live', 'resume', 1, 1);
         INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at) VALUES
           ('req-queued','private-runtime','lane','ordinary:run-queued','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,1,'queued',NULL,0,NULL,1,1),
           ('req-active','private-runtime','lane','ordinary:run-active','{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,2,'active','ordinary-session-promoter',1,9999999999,1,1);
         INSERT INTO prepared_ordinary_launches(request_id,job_id,run_id,session_id,turn_id,execution_id,launch_event_id,prompt,reason,session_start_json,model,agent_config_json,runtime_capability_json,created_at) VALUES
           ('req-queued','job-q','run-queued','session-q','t-q','exec-q','e','p','resume','{{}}','','null','{{}}',1),
           ('req-active','job-q','run-active','session-q','t-a','exec-q','e','p','resume','{{}}','','null','{{}}',1);"
    ))
    .await
    .unwrap();

    queries::reconcile_stale_runs(&orch, &NoopEmitter, STALE_BOOT_AT);

    let status = |id: &'static str| async move {
        common::scalar_text_by_id(db, "SELECT status FROM runs WHERE id = ?1", id).await
    };
    assert_eq!(status("run-queued").await.as_deref(), Some("starting"));
    assert_eq!(status("run-active").await.as_deref(), Some("crashed"));
    assert_eq!(
        common::scalar_text_by_id(
            db,
            "SELECT state FROM runtime_admission_requests WHERE request_id = ?1",
            "req-queued"
        )
        .await
        .as_deref(),
        Some("queued"),
        "the queued launch stays for startup recovery to relaunch"
    );
}
