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
        crate::transcripts::stream_store::insert_event_emit(
            self.run_db.clone(),
            &self.emitter,
            event,
        )
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
