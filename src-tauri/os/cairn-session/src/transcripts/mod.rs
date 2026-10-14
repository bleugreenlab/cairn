//! Session transcript streaming and replay.
pub mod file_activity;
pub mod stream;
pub mod stream_store;

use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use stream_store::{EventInsert, FinalizedStream};

/// Publish transcript transactions and their application side effects in order.
/// Each adapter is bound to one run and its owning database.
pub trait TranscriptHost: Send + Sync {
    fn insert_event(&self, event: EventInsert) -> Result<bool, String>;
    fn finalize_stream(
        &self,
        stream_id: &str,
        version: i32,
        event: Option<TranscriptEvent>,
        counts: TokenCounts,
    ) -> Result<FinalizedStream, String>;
    fn streams_changed(&self, action: &str);
    fn event_committed(&self, event_id: &str, event_type: &str, data: &str, counts: TokenCounts);
}
