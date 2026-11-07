//! Transcript publication shared by native session drivers.
use crate::transcripts::stream_store::{finalize_stream_emit, process_post_commit_outbox};
use crate::{orchestrator::Orchestrator, storage::LocalDb};
use cairn_session::{
    agent_process::stream::{TokenCounts, TranscriptEvent},
    transcripts::{stream_store::EventInsert, TranscriptHost},
};
use std::sync::Arc;
pub(super) struct SessionTranscriptHost {
    pub(super) orch: Orchestrator,
    pub(super) emitter: Arc<dyn crate::services::EventEmitter>,
    pub(super) run_id: String,
    pub(super) session_id: Option<String>,
    pub(super) run_db: Arc<LocalDb>,
}
impl TranscriptHost for SessionTranscriptHost {
    fn insert_event(&self, event: EventInsert) -> Result<bool, String> {
        let reports_init = event.event_type == "system:init"
            && event.parent_tool_use_id.is_none()
            && cairn_db::storage::events::columns::reported_model(&event.data).is_some();
        let inserted = crate::transcripts::stream_store::insert_event_emit(
            self.run_db.clone(),
            &self.emitter,
            event,
        )?;
        if inserted && reports_init {
            self.orch.claude_alias_report_committed();
        }
        Ok(inserted)
    }
    fn finalize_stream(
        &self,
        stream_id: &str,
        version: i32,
        event: Option<TranscriptEvent>,
        counts: TokenCounts,
    ) -> Result<cairn_session::transcripts::stream_store::FinalizedStream, String> {
        let finalized = finalize_stream_emit(
            self.run_db.clone(),
            self.orch.db.local.clone(),
            &self.emitter,
            stream_id,
            version,
            event,
            counts,
        )?;
        process_post_commit_outbox(&self.orch, &finalized.outbox_entries);
        Ok(finalized)
    }
    fn streams_changed(&self, action: &str) {
        let _ = self.emitter.emit(
            "db-change",
            crate::notify::event_db_change_for_run(
                self.orch.db.local.clone(),
                &self.run_id,
                self.session_id.as_deref(),
                action,
            ),
        );
    }
    fn event_committed(
        &self,
        event_id: &str,
        event_type: &str,
        data: &str,
        event_counts: TokenCounts,
    ) {
        let orch = &self.orch;
        let session_id = self.session_id.clone();
        {
            // Embed events for vibe coloring (agent content) and
            // session position (user / agent / change feeds).
            // Position needs a session id to key on; without one
            // we still color agent events.
            if let Some(session) = session_id.as_deref() {
                match event_type {
                    "assistant" => {
                        if let Some(text) = crate::embeddings::extract_embeddable_text(data) {
                            orch.enqueue_position_embed(
                                session,
                                event_id,
                                crate::embeddings::PositionKind::Agent,
                                text,
                                event_counts.output,
                            );
                        }
                        if let Some(signal) = crate::embeddings::extract_change_signal_text(data) {
                            orch.enqueue_position_embed(
                                session,
                                event_id,
                                crate::embeddings::PositionKind::Change,
                                signal,
                                event_counts.output,
                            );
                        }
                    }
                    "user" => {
                        if let Some(text) = crate::embeddings::extract_embeddable_text(data) {
                            orch.enqueue_position_embed(
                                session,
                                event_id,
                                crate::embeddings::PositionKind::User,
                                text,
                                event_counts.input,
                            );
                        }
                    }
                    _ => {}
                }
            } else if event_type == "assistant" {
                if let Some(text) = crate::embeddings::extract_embeddable_text(data) {
                    orch.enqueue_event_embed(event_id, text);
                }
            }
        }
    }
}

#[cfg(test)]
mod alias_tests {
    use super::*;
    use crate::orchestrator::models::tests::{bare_orchestrator, seed_alias_session};

    #[tokio::test(flavor = "multi_thread")]
    async fn committed_init_resolves_and_refreshes_a_cached_alias() {
        let orch = bare_orchestrator().await;
        seed_alias_session(&orch, "opus", "opus", "claude").await;
        let host = SessionTranscriptHost {
            run_db: orch.db.local.clone(),
            emitter: orch.services.emitter.clone(),
            run_id: "opus-run".into(),
            session_id: Some("opus-session".into()),
            orch: orch.clone(),
        };
        assert_eq!(
            orch.catalog_provider("claude", None).canonical("opus"),
            None
        );
        let now = chrono::Utc::now().timestamp() as i32;
        let mut event = EventInsert {
            id: "z-old-init".into(),
            run_id: "opus-run".into(),
            session_id: Some("opus-session".into()),
            timestamp: now,
            selected_model: Some("opus".into()),
            created_at: now,
            event_type: "system:init".into(),
            data: r#"{"raw":{"model":"claude-opus-4-7","claude_code_version":"2.1.311"}}"#.into(),
            parent_tool_use_id: None,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            thinking_tokens: None,
            turn_id: None,
            cost_usd: None,
        };
        assert!(host.insert_event(event.clone()).unwrap());
        let first = orch.catalog_provider("claude", None);
        assert_eq!(first.canonical("opus").as_deref(), Some("claude-opus-4.7"));
        assert!(first
            .identity_note("opus")
            .unwrap()
            .contains("opus-session"));
        assert_eq!(
            orch.catalog_provider("claude", None).canonical("opus"),
            first.canonical("opus")
        );

        event.id = "a-new-init".into();
        event.data =
            r#"{"raw":{"model":"claude-opus-5-5","claude_code_version":"2.1.311"}}"#.into();
        assert!(host.insert_event(event.clone()).unwrap());
        assert_eq!(
            orch.catalog_provider("claude", None)
                .canonical("opus")
                .as_deref(),
            Some("claude-opus-5.5")
        );

        orch.db
            .local
            .execute("UPDATE jobs SET model = 'sonnet' WHERE id = 'opus-job'", ())
            .await
            .unwrap();
        orch.claude_alias_report_committed();
        assert_eq!(
            orch.catalog_provider("claude", None).canonical("sonnet"),
            None
        );
        assert_eq!(
            orch.catalog_provider("claude", None)
                .canonical("opus")
                .as_deref(),
            Some("claude-opus-5.5")
        );

        event.id = "0-sonnet-init".into();
        event.selected_model = Some("sonnet".into());
        event.data =
            r#"{"raw":{"model":"claude-sonnet-4-6","claude_code_version":"2.1.311"}}"#.into();
        assert!(host.insert_event(event).unwrap());
        let after_resume = orch.catalog_provider("claude", None);
        assert_eq!(
            after_resume.canonical("sonnet").as_deref(),
            Some("claude-sonnet-4.6")
        );
        assert_eq!(
            after_resume.canonical("opus").as_deref(),
            Some("claude-opus-5.5")
        );
    }
}
