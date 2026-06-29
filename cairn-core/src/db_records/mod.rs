//! Database records for table-shaped data.
//!
//! These models are used for database queries and map directly to database tables.
//! They work alongside the existing models in `models.rs` which are used for
//! API responses and business logic.

// Allow dead code for record fields that are populated by narrower query paths.
#![allow(dead_code)]

mod accumulator;
mod action;
mod artifact;
mod cache;
mod comment;
pub mod docs;
mod effect_outbox;
mod embedding;
mod execution;
mod file_change;
mod github;
mod issue;
mod job;
mod memory;
mod merge_request;
mod message;
mod message_stream;
mod permission;
mod project;
mod prompt;
mod recipe;
mod session;
mod session_skyline_cache;
mod terminal;
mod todo;
mod trigger_source;
mod turn;
mod workspace;

// Re-export all table records.
pub use accumulator::*;
pub use action::*;
pub use artifact::*;
pub use cache::*;
pub use comment::*;
pub use docs::*;
pub use effect_outbox::*;
pub use embedding::*;
pub use execution::*;
pub use file_change::*;
pub use github::*;
pub use issue::*;
pub use job::*;
pub use memory::*;
pub use merge_request::*;
pub use message::*;
pub use message_stream::*;
pub use permission::*;
pub use project::*;
pub use prompt::*;
pub use recipe::*;
pub use session::*;
pub use session_skyline_cache::*;
pub use terminal::*;
pub use todo::*;
pub use trigger_source::*;
pub use turn::*;
pub use workspace::*;
