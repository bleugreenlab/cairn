//! Session management — durable backend conversation identity.
//!
//! Sessions track the backend conversation (e.g. Claude CLI's `--session-id`).
//! Their status describes whether the conversation is resumable, not whether
//! a process is currently running.

pub mod queries;
