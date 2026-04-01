//! Diesel ORM models for database operations.
//!
//! These models are used for Diesel queries and map directly to database tables.
//! They work alongside the existing models in `models.rs` which are used for
//! API responses and business logic.

// Allow dead code for Diesel model fields - they're used for Queryable derives
// and will be used as more queries are migrated to Diesel
#![allow(dead_code)]

mod accumulator;
mod action;
mod artifact;
mod cache;
mod chat;
mod comment;
pub mod docs;
mod effect_outbox;
mod embedding;
mod execution;
mod file_change;
mod github;
mod issue;
mod job;
mod manager;
mod manager_mailbox;
mod manager_scope;
mod manager_wake_batch;
mod memory;
mod merge_request;
mod message;
mod message_stream;
mod permission;
mod project;
mod prompt;
mod recipe;
mod run;
mod session;
mod terminal;
mod todo;
mod trigger_source;
mod turn;
mod workspace;

// Re-export all public types for backward compatibility
pub use accumulator::*;
pub use action::*;
pub use artifact::*;
pub use cache::*;
pub use chat::*;
pub use comment::*;
pub use docs::*;
pub use effect_outbox::*;
pub use embedding::*;
pub use execution::*;
pub use file_change::*;
pub use github::*;
pub use issue::*;
pub use job::*;
pub use manager::*;
pub use manager_mailbox::*;
pub use manager_scope::*;
pub use manager_wake_batch::*;
pub use memory::*;
pub use merge_request::*;
pub use message::*;
pub use message_stream::*;
pub use permission::*;
pub use project::*;
pub use prompt::*;
pub use recipe::*;
pub use run::*;
pub use session::*;
pub use terminal::*;
pub use todo::*;
pub use trigger_source::*;
pub use turn::*;
pub use workspace::*;
