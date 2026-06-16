//! Remote sync — async channel for forwarding committed local changes.
//!
//! Core principle: the embedded local database is source of truth. Sync failures never block local writes.
//! StreamDeltas and transcript events stay local-only; durable non-event entities retry with exponential backoff.
//!
//! Architecture:
//! - Write points call `orch.sync(msg)` after local DB writes
//! - Messages flow through an mpsc channel to a background SyncTask
//! - SyncTask batches durable messages and sends them via HTTP
//! - Local-only messages are dropped before transport

pub mod initial;
pub mod message;
pub mod sender;
pub mod task;

pub use message::{
    StreamDelta, SyncArtifact, SyncComment, SyncEvent, SyncIssue, SyncJob, SyncMessage,
    SyncProject, SyncRun, SyncTranscriptEvent,
};
pub use sender::SyncSender;
pub use task::SyncTask;
