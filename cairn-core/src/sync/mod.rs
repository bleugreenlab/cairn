//! Remote sync — async channel for dual-writing SQLite changes to cloud Postgres.
//!
//! Core principle: SQLite is always source of truth. Sync failures never block local writes.
//! StreamDeltas are fire-and-forget; durable entities retry with exponential backoff.
//!
//! Architecture:
//! - Write points call `orch.sync(msg)` after local DB writes
//! - Messages flow through an mpsc channel to a background SyncTask
//! - SyncTask batches durable messages and sends via WebSocket
//! - StreamDelta messages are sent immediately (no batching, no retry)

pub mod initial;
pub mod message;
pub mod sender;
pub mod task;

pub use message::{
    StreamDelta, SyncArtifact, SyncComment, SyncEvent, SyncIssue, SyncJob, SyncMessage,
    SyncProject, SyncRun,
};
pub use sender::SyncSender;
pub use task::SyncTask;
