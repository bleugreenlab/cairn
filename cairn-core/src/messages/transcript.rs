//! Insert `system:message` transcript events for messages delivered to a run.
//!
//! Both injection paths for queued direct messages and the channel-cursor pull
//! in the Claude hook record a transcript event per delivered message so the
//! UI's transcript view shows cross-agent communication. This module exposes
//! the shared writer so the Tauri hook handler (server.rs) and the cairn-core
//! `dispatch_tool` augmentation both use the same code path — historically
//! only the hook recorded events, which left Codex tool-result augmentations
//! invisible in the transcript and split Claude's record between paths.

use crate::agent_process::stream::TranscriptEvent;
use crate::messages::queued::QueuedMessage;
use crate::messages::side_channel::SideChannelNotice;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;
use std::sync::Arc;

/// Persist a single carrying event for drained attention pushes and stamp each
/// push delivered by it, atomically (CAIRN-1881). Used by the busy-agent
/// event-boundary drain in `dispatch`: the pushes are just another source
/// feeding the same transcript-event + reminder sink as directs and side-channel
/// notices. Best-effort: an insert failure is logged and leaves the pushes
/// pending to redeliver, matching the surrounding augmentation behaviour.
pub async fn insert_attention_push_events(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    pushes: &[crate::orchestrator::attention_push::Push],
    resolved: &str,
) {
    if pushes.is_empty() {
        return;
    }
    let db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let now = chrono::Utc::now().timestamp() as i32;
    let event_id = ids::mint_child(run_id);
    // CAIRN-1891: render delivered wakes through the one wake-card formatter —
    // store the carrying event as an `attention:briefing` whose content is the
    // structured `{active, catchup}` card payload plus the `resolved` markdown the
    // agent received (for the detail modal), not a raw `system:message` line.
    let summary = crate::orchestrator::attention_push::push_event_content_json(pushes, resolved);
    let push_ids: Vec<String> = pushes.iter().map(|p| p.id.clone()).collect();
    let transcript_event = TranscriptEvent {
        event_type: "attention:briefing".to_string(),
        session_id: session_id.map(|s| s.to_string()),
        parent_tool_use_id: None,
        content: Some(summary),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        raw: Some(serde_json::json!({ "attention_push_ids": push_ids })),
    };
    let data = serde_json::to_string(&transcript_event).unwrap_or_default();

    if let Err(error) = insert_system_event(
        &db,
        &orch.services.emitter,
        &event_id,
        run_id,
        session_id,
        "system:message",
        &data,
        now,
        &push_ids,
    )
    .await
    {
        log::warn!("Failed to insert attention push event: {}", error);
    }
}

/// Insert a general quiet system message with structured metadata.
pub fn insert_system_message_sync(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    content: &str,
    raw: serde_json::Value,
) -> Result<(), String> {
    let db = crate::storage::run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|error| error.to_string())
        }
    })?;
    let event = TranscriptEvent {
        event_type: "system:message".to_string(),
        session_id: session_id.map(str::to_string),
        parent_tool_use_id: None,
        content: Some(content.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        raw: Some(raw),
    };
    let data = serde_json::to_string(&event).map_err(|error| error.to_string())?;
    insert_system_event_sync(
        &db,
        &orch.services.emitter,
        &ids::mint_child(run_id),
        run_id,
        session_id,
        "system:message",
        &data,
        chrono::Utc::now().timestamp() as i32,
        turn_id,
    )?;
    Ok(())
}

/// Insert a `user` transcript event per delivered queued user message, so the
/// follow-ups the user queued show up as normal "You" blocks once delivered.
/// Used by the tool-boundary `steer` delivery path in `dispatch`.
pub async fn insert_queued_user_events(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    messages: &[QueuedMessage],
) {
    let db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let now = chrono::Utc::now().timestamp() as i32;

    for msg in messages {
        let event_id = ids::mint_child(run_id);
        let content = msg.content.clone();
        let transcript_event = TranscriptEvent {
            event_type: "user".to_string(),
            session_id: session_id.map(|s| s.to_string()),
            parent_tool_use_id: None,
            content: Some(content),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: None,
        };
        let data = serde_json::to_string(&transcript_event).unwrap_or_default();

        if let Err(error) = insert_user_event(
            &db,
            &orch.services.emitter,
            &event_id,
            run_id,
            session_id,
            &data,
            now,
        )
        .await
        {
            log::warn!("Failed to insert queued user event: {}", error);
            continue;
        }
    }
}

/// Render side-channel notices for model prompt injection.
pub fn render_side_channel_prompt_block(notices: &[SideChannelNotice]) -> String {
    notices
        .iter()
        .map(SideChannelNotice::render)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Insert a `system:message` event per delivered child side-channel notice.
pub async fn insert_side_channel_events(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    notices: &[SideChannelNotice],
) {
    let db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let now = chrono::Utc::now().timestamp() as i32;

    for notice in notices {
        let event_id = ids::mint_child(run_id);
        let summary = notice.render();
        let transcript_event = TranscriptEvent {
            event_type: "system:message".to_string(),
            session_id: session_id.map(|s| s.to_string()),
            parent_tool_use_id: None,
            content: Some(summary.clone()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(serde_json::json!({
                "side_channel_notice_id": notice.id,
                "child_uri": notice.child_uri,
                "content": notice.content,
                "channel_type": notice.channel_type(),
            })),
        };
        let data = serde_json::to_string(&transcript_event).unwrap_or_default();

        if let Err(error) = insert_system_event(
            &db,
            &orch.services.emitter,
            &event_id,
            run_id,
            session_id,
            "system:message",
            &data,
            now,
            &[],
        )
        .await
        {
            log::warn!("Failed to insert side-channel event: {}", error);
            continue;
        }
    }
}

/// Synchronous counterpart for resume-prompt construction, where the notices
/// must be claimed and recorded before the backend receives the next turn.
pub fn insert_side_channel_events_sync(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    notices: &[SideChannelNotice],
) -> Result<(), String> {
    let db = crate::storage::run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let now = chrono::Utc::now().timestamp() as i32;

    for notice in notices {
        let event_id = ids::mint_child(run_id);
        let summary = notice.render();
        let transcript_event = TranscriptEvent {
            event_type: "system:message".to_string(),
            session_id: session_id.map(|s| s.to_string()),
            parent_tool_use_id: None,
            content: Some(summary.clone()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(serde_json::json!({
                "side_channel_notice_id": notice.id,
                "child_uri": notice.child_uri,
                "content": notice.content,
                "channel_type": notice.channel_type(),
            })),
        };
        let data = serde_json::to_string(&transcript_event).unwrap_or_default();
        insert_system_event_sync(
            &db,
            &orch.services.emitter,
            &event_id,
            run_id,
            session_id,
            "system:message",
            &data,
            now,
            turn_id,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_system_event_sync(
    db: &Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    event_id: &str,
    run_id: &str,
    session_id: Option<&str>,
    event_type: &str,
    data: &str,
    now: i32,
    turn_id: Option<&str>,
) -> Result<i32, String> {
    let db_for_emit = db.clone();
    let run_id_for_emit = run_id.to_string();
    let session_id_for_emit = session_id.map(str::to_string);

    let event_id = event_id.to_string();
    let run_id = run_id.to_string();
    let session_id = session_id.map(str::to_string);
    let event_type = event_type.to_string();
    let data = data.to_string();
    let turn_id = turn_id.map(str::to_string);

    let sequence = run_db_blocking(move || async move {
        db.write(|conn| {
            let event_id = event_id.clone();
            let run_id = run_id.clone();
            let session_id = session_id.clone();
            let event_type = event_type.clone();
            let data = data.clone();
            let turn_id = turn_id.clone();
            Box::pin(async move {
                let sequence = next_event_sequence(conn, &run_id).await?;
                conn.execute(
                    "
                    INSERT INTO events (
                        id, run_id, session_id, sequence, timestamp, event_type,
                        data, parent_tool_use_id, created_at, input_tokens,
                        cache_read_tokens, cache_create_tokens, output_tokens, turn_id
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, NULL, NULL, NULL, NULL, ?9)
                    ",
                    params![
                        event_id.as_str(),
                        run_id.as_str(),
                        session_id.as_deref(),
                        sequence,
                        now,
                        event_type.as_str(),
                        data.as_str(),
                        now,
                        turn_id.as_deref()
                    ],
                )
                .await?;
                Ok(sequence)
            })
        })
        .await
        .map_err(|error| error.to_string())
    })?;

    // Emit the scoped `events` db-change AFTER the write commits — see the async
    // twin `insert_system_event`. `event_db_change_for_run` runs its own
    // `run_db_blocking` issue-id lookup, executed sequentially here (never nested
    // inside the write transaction).
    let _ = emitter.emit(
        "db-change",
        crate::notify::event_db_change_for_run(
            db_for_emit,
            &run_id_for_emit,
            session_id_for_emit.as_deref(),
            "insert",
        ),
    );
    Ok(sequence)
}

/// Insert a `user`-typed event (used for delivered queued user messages).
async fn insert_user_event(
    db: &Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    event_id: &str,
    run_id: &str,
    session_id: Option<&str>,
    data: &str,
    now: i32,
) -> DbResult<i32> {
    insert_system_event(
        db,
        emitter,
        event_id,
        run_id,
        session_id,
        "user",
        data,
        now,
        &[],
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_system_event(
    db: &Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    event_id: &str,
    run_id: &str,
    session_id: Option<&str>,
    event_type: &str,
    data: &str,
    now: i32,
    push_ids: &[String],
) -> DbResult<i32> {
    let db_for_emit = db.clone();
    let run_id_for_emit = run_id.to_string();
    let session_id_for_emit = session_id.map(str::to_string);

    let event_id = event_id.to_string();
    let run_id = run_id.to_string();
    let session_id = session_id.map(str::to_string);
    let event_type = event_type.to_string();
    let data = data.to_string();
    let push_ids = push_ids.to_vec();

    let sequence = db
        .write(|conn| {
            let event_id = event_id.clone();
            let run_id = run_id.clone();
            let session_id = session_id.clone();
            let event_type = event_type.clone();
            let data = data.clone();
            let push_ids = push_ids.clone();
            Box::pin(async move {
                let sequence = next_event_sequence(conn, &run_id).await?;
                conn.execute(
                    "
                INSERT INTO events (
                    id, run_id, session_id, sequence, timestamp, event_type,
                    data, parent_tool_use_id, created_at, input_tokens,
                    cache_read_tokens, cache_create_tokens, output_tokens, turn_id
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, NULL, NULL, NULL, NULL, NULL)
                ",
                    params![
                        event_id.as_str(),
                        run_id.as_str(),
                        session_id.as_deref(),
                        sequence,
                        now,
                        event_type.as_str(),
                        data.as_str(),
                        now
                    ],
                )
                .await?;
                // CAIRN-1881: when this event carries attention pushes, stamp each
                // delivered by it inside the same transaction as the INSERT. Roll
                // back together → the push redelivers.
                crate::orchestrator::attention_push::stamp_delivered_conn(
                    conn, &push_ids, &event_id,
                )
                .await?;
                Ok(sequence)
            })
        })
        .await?;

    // Emit the scoped `events` db-change AFTER the write transaction commits
    // (never inside it). The live chat transcript's incremental delta cache is
    // invalidated only by scoped `events` db-changes, so a boundary-delivered
    // insert that skips the emit strands the transcript until the next turn
    // boundary (invariant in notify.rs; CAIRN-2558).
    let _ = emitter.emit(
        "db-change",
        crate::notify::event_db_change_for_run(
            db_for_emit,
            &run_id_for_emit,
            session_id_for_emit.as_deref(),
            "insert",
        ),
    );
    Ok(sequence)
}

async fn next_event_sequence(conn: &cairn_db::turso::Connection, run_id: &str) -> DbResult<i32> {
    let mut rows = conn
        .query(
            "SELECT MAX(sequence) FROM events WHERE run_id = ?1",
            params![run_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing event sequence".to_string()))?;
    Ok(row.opt_i64(0)?.unwrap_or(-1) as i32 + 1)
}

/// Look up `(session_id, run_id)` for the calling run. Useful for the dispatch
/// augmentation path, which has run_id from the request but needs the
/// session_id to attach the transcript event to the right session.
pub async fn run_session_for_event(db: &LocalDb, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT session_id FROM runs WHERE id = ?1 LIMIT 1",
                    params![run_id.as_str()],
                )
                .await?;
            crate::storage::next_opt_text(&mut rows, 0).await
        })
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::messages::queued::Delivery;
    use crate::messages::side_channel::SideChannelOrigin;
    use crate::orchestrator::attention_push::{Boundary, Push, Wake};
    use crate::services::testing::TestServicesBuilder;
    use crate::services::EventEmitter;
    use crate::storage::SearchIndex;
    use std::sync::Mutex;
    use tempfile::tempdir;

    /// Records emitted events behind a shared `Arc` handle so a test can inspect
    /// them after the emitter has been moved into `Services`.
    #[derive(Clone, Default)]
    struct RecordingEmitter {
        events: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    }

    impl EventEmitter for RecordingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push((event.to_string(), payload));
            Ok(())
        }
        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.emit(event, serde_json::Value::Null)
        }
    }

    async fn orch_with_recorder() -> (Orchestrator, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let db = crate::storage::migrated_test_db("transcript.db").await;
        let emitter = RecordingEmitter::default();
        let events = emitter.events.clone();
        let config_dir = tempdir().unwrap().keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().with_emitter(emitter).build());
        (
            Orchestrator::builder(db_state, services, config_dir).build(),
            events,
        )
    }

    /// Seed a project/issue/execution/job/run so `owning_db_for_run` resolves and
    /// `event_db_change_for_run`'s issue-id lookup returns `issue-1`.
    async fn seed_run(db: &LocalDb, run_id: &str) {
        let run_id = run_id.to_string();
        db.write(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                conn.execute("INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('proj-1', 'default', 'Project', 'PROJ', '/tmp/repo', 1, 1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-1', 'proj-1', 42, 'Issue', 'active', 1, 1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)", ()).await?;
                conn.execute("INSERT OR IGNORE INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at) VALUES ('job-1', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 1, 1)", ()).await?;
                conn.execute("INSERT INTO runs (id, project_id, issue_id, job_id, status, created_at, updated_at) VALUES (?1, 'proj-1', 'issue-1', 'job-1', 'live', 1, 1)", params![run_id.as_str()]).await?;
                Ok(())
            })
        }).await.unwrap();
    }

    /// The scoped `events` db-changes emitted so far.
    fn events_changes(
        events: &Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    ) -> Vec<serde_json::Value> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, payload)| {
                name == "db-change"
                    && payload.get("table").and_then(|t| t.as_str()) == Some("events")
            })
            .map(|(_, payload)| payload.clone())
            .collect()
    }

    fn assert_scoped_events_insert(change: &serde_json::Value) {
        assert_eq!(change["table"], "events");
        assert_eq!(change["action"], "insert");
        assert_eq!(change["runId"], "run-1");
        assert_eq!(change["sessionId"], "session-1");
        assert_eq!(change["issueId"], "issue-1");
    }

    #[tokio::test]
    async fn insert_queued_user_events_emits_scoped_events_change() {
        let (orch, events) = orch_with_recorder().await;
        seed_run(&orch.db.local, "run-1").await;

        let msg = QueuedMessage {
            id: "qm-1".to_string(),
            job_id: "job-1".to_string(),
            content: "follow up".to_string(),
            delivery: Delivery::Steer,
            created_at: 1,
            delivered_at: None,
        };
        insert_queued_user_events(&orch, "run-1", Some("session-1"), &[msg]).await;

        let changes = events_changes(&events);
        assert_eq!(changes.len(), 1);
        assert_scoped_events_insert(&changes[0]);
    }

    #[tokio::test]
    async fn insert_side_channel_events_emits_scoped_events_change() {
        let (orch, events) = orch_with_recorder().await;
        seed_run(&orch.db.local, "run-1").await;

        let notice = SideChannelNotice {
            id: "notice-1".to_string(),
            parent_job_id: "job-1".to_string(),
            child_uri: "cairn://p/PROJ/42/1/child".to_string(),
            content: "child says hi".to_string(),
            origin: SideChannelOrigin::UserChild,
            created_at: 1,
            delivered_at: None,
        };
        insert_side_channel_events(&orch, "run-1", Some("session-1"), &[notice]).await;

        let changes = events_changes(&events);
        assert_eq!(changes.len(), 1);
        assert_scoped_events_insert(&changes[0]);
    }

    #[tokio::test]
    async fn insert_attention_push_events_emits_scoped_events_change() {
        let (orch, events) = orch_with_recorder().await;
        seed_run(&orch.db.local, "run-1").await;

        let push = Push {
            id: "push-1".to_string(),
            recipient: "job-1".to_string(),
            content_ref: "cairn://p/PROJ/42".to_string(),
            wake: Wake::Passive,
            boundary: Boundary::Event,
            key: "k".to_string(),
            created_at: 1,
            delivered_event_id: None,
        };
        insert_attention_push_events(&orch, "run-1", Some("session-1"), &[push], "resolved body")
            .await;

        let changes = events_changes(&events);
        assert_eq!(changes.len(), 1);
        assert_scoped_events_insert(&changes[0]);
    }

    #[tokio::test]
    async fn insert_side_channel_events_sync_emits_scoped_events_change() {
        let (orch, events) = orch_with_recorder().await;
        seed_run(&orch.db.local, "run-1").await;

        let notice = SideChannelNotice {
            id: "notice-sync-1".to_string(),
            parent_job_id: "job-1".to_string(),
            child_uri: "cairn://p/PROJ/42/1/child".to_string(),
            content: "resume-time notice".to_string(),
            origin: SideChannelOrigin::UserChild,
            created_at: 1,
            delivered_at: None,
        };
        insert_side_channel_events_sync(&orch, "run-1", Some("session-1"), None, &[notice])
            .unwrap();

        let changes = events_changes(&events);
        assert_eq!(changes.len(), 1);
        assert_scoped_events_insert(&changes[0]);
    }
}
