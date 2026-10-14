//! Host delivery and attention policy around session-owned transcript transactions.
use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use crate::effects::outbox::{self, OutboxEntry};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use crate::transcripts::file_activity;
pub use cairn_session::transcripts::stream_store::*;
use std::sync::Arc;

/// Canonical durable-event insert: write the event and, as one atomic unit,
/// emit a fully-scoped `events` db-change so the live chat transcript's delta
/// cache always receives its invalidation. Every live/finalize insert path
/// should funnel through this rather than hand-rolling an emit after
/// [`insert_event`] — a skipped or mis-scoped emit is exactly what
/// intermittently freezes the chat transcript (CAIRN-1916). Returns whether a
/// new row landed (`false` = duplicate id); the emit fires only when a row
/// actually lands, matching the resolver's append-only delta expectations.
pub fn insert_event_emit(
    db: Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    event: EventInsert,
) -> Result<bool, String> {
    let run_id = event.run_id.clone();
    let session_id = event.session_id.clone();
    let data = event.data.clone();
    let projects_presence =
        event.event_type == "assistant" && file_activity::batch_from_event_data(&data).is_some();
    let scope = crate::notify::event_run_scope(db.clone(), &run_id);
    let inserted = insert_event(db, event)?;
    if inserted {
        let _ = emitter.emit(
            "db-change",
            crate::notify::event_db_change_scoped(&run_id, session_id.as_deref(), &scope, "insert"),
        );
        if projects_presence && scope.job_id.is_some() {
            let _ = emitter.emit("db-change", crate::notify::job_presence_db_change(&scope));
        }
    }
    Ok(inserted)
}

/// Finalize a streaming message and emit its scoped `events` db-change as one
/// unit — the finalize-path counterpart to [`insert_event_emit`]. The streamed
/// final event is inserted inside [`finalize_stream`]'s own transaction, so the
/// emit can't ride that insert directly; routing every live finalize through
/// here keeps the chat transcript's delta cache invalidated regardless of which
/// backend finalized (CAIRN-1916). Returns the [`FinalizedStream`] so the caller
/// still drains its outbox entries.
pub fn finalize_stream_emit(
    db: Arc<LocalDb>,
    private_db: Arc<LocalDb>,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    stream_id: &str,
    expected_version: i32,
    final_event: Option<TranscriptEvent>,
    counts: TokenCounts,
) -> Result<FinalizedStream, String> {
    let finalized = finalize_stream(
        db.clone(),
        private_db,
        stream_id,
        expected_version,
        final_event,
        counts,
    )?;
    let scope = crate::notify::event_run_scope(db, &finalized.run_id);
    let _ = emitter.emit(
        "db-change",
        crate::notify::event_db_change_scoped(
            &finalized.run_id,
            finalized.session_id.as_deref(),
            &scope,
            "insert",
        ),
    );
    if file_activity::batch_from_event_data(&finalized.data_json).is_some()
        && scope.job_id.is_some()
    {
        let _ = emitter.emit("db-change", crate::notify::job_presence_db_change(&scope));
    }
    Ok(finalized)
}

/// Insert an event and, in the **same transaction**, stamp each push delivered
/// by that event id (CAIRN-1881 atomic delivery seam). Event and stamps commit
/// or roll back together, so a crashed turn whose carrying event never landed
/// redelivers its pushes. Used by the resume-path push drain in
/// `continue_job_impl`; mirrors [`insert_event`] but threads the stamp.
pub fn insert_event_stamping_pushes(
    db: Arc<LocalDb>,
    event: EventInsert,
    push_ids: Vec<String>,
) -> Result<bool, String> {
    block_on_stream_db(async move {
        db.write(|conn| {
            let event = event.clone();
            let push_ids = push_ids.clone();
            Box::pin(async move {
                let count = insert_event_conn(conn, &event).await?;
                if count > 0 {
                    record_read_tokens_if_applicable(conn, &event).await?;
                    crate::orchestrator::attention_push::stamp_delivered_conn(
                        conn, &push_ids, &event.id,
                    )
                    .await?;
                    // CAIRN-1894: advance each delivered catch-up push's read
                    // cursor in the same transaction, so the cursor and the
                    // delivery stamp commit or roll back together.
                    crate::orchestrator::attention_push::advance_read_cursors_conn(conn, &push_ids)
                        .await?;
                }
                Ok(count > 0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_session::transcripts::stream_store::testing::{insert_run, test_db};
    #[tokio::test]
    async fn insert_event_emit_emits_one_scoped_change() {
        use crate::services::testing::CapturingEmitter;
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let capturing = Arc::new(CapturingEmitter::new());
        let emitter: Arc<dyn crate::services::EventEmitter> = capturing.clone();

        let event = EventInsert {
            id: "evt-1".to_string(),
            run_id: "run-1".to_string(),
            session_id: Some("session-1".to_string()),
            timestamp: 0,
            event_type: "assistant".to_string(),
            data: "{}".to_string(),
            parent_tool_use_id: None,
            created_at: 0,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            thinking_tokens: None,
            turn_id: None,
            cost_usd: None,
        };

        let inserted = insert_event_emit(db.clone(), &emitter, event.clone()).unwrap();
        assert!(inserted);

        let captured = capturing.events_named("db-change");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0]["table"], "events");
        assert_eq!(captured[0]["action"], "insert");
        assert_eq!(captured[0]["runId"], "run-1");
        assert_eq!(captured[0]["sessionId"], "session-1");
        assert_eq!(captured[0]["issueId"], "issue-1");
        assert_eq!(captured[0]["issue_id"], "issue-1");

        // Re-inserting the same id conflicts on the UNIQUE id constraint and
        // surfaces as an Err; crucially the helper emits nothing on the failure,
        // so the append-only delta cache is never poked for a non-append.
        let again = insert_event_emit(db.clone(), &emitter, event);
        assert!(again.is_err());
        assert_eq!(capturing.events_named("db-change").len(), 1);
    }

    #[tokio::test]
    async fn finalize_stream_emit_emits_scoped_change() {
        use crate::services::testing::CapturingEmitter;
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let capturing = Arc::new(CapturingEmitter::new());
        let emitter: Arc<dyn crate::services::EventEmitter> = capturing.clone();

        let opened = open_stream(db.clone(), "run-1", Some("session-1"), None, "claude").unwrap();
        let appended = append_chunks(
            db.clone(),
            opened.stream_id(),
            opened.version(),
            &[StreamChunkInput::content("hello")],
        )
        .unwrap();
        let finalized = finalize_stream_emit(
            db.clone(),
            db.clone(),
            &emitter,
            opened.stream_id(),
            appended.version,
            None,
            TokenCounts::default(),
        )
        .unwrap();

        // The finalized stream carries the run/session scope used for the emit,
        // even though the event insert happened inside finalize's transaction.
        assert_eq!(finalized.run_id, "run-1");
        assert_eq!(finalized.session_id.as_deref(), Some("session-1"));

        let captured = capturing.events_named("db-change");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0]["table"], "events");
        assert_eq!(captured[0]["runId"], "run-1");
        assert_eq!(captured[0]["sessionId"], "session-1");
    }
}
