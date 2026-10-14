//! Cross-agent messaging layer.
//!
//! Provides DB operations for storing/querying messages and delivery routing
//! that pushes messages to per-process inboxes and (for direct messages)
//! stdin of warm processes.

pub mod db;
pub mod delivery;
pub mod pending;
pub mod queued;
pub mod render;
pub mod side_channel;
pub mod system;
pub mod transcript;
