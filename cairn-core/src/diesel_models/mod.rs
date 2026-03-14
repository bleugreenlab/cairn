//! Diesel ORM models for database operations.
//!
//! These models are used for Diesel queries and map directly to database tables.
//! They work alongside the existing models in `models.rs` which are used for
//! API responses and business logic.

// Allow dead code for Diesel model fields - they're used for Queryable derives
// and will be used as more queries are migrated to Diesel
#![allow(dead_code)]

mod action;
mod artifact;
mod cache;
mod chat;
mod comment;
pub mod docs;
mod execution;
mod file_change;
mod github;
mod issue;
mod job;
mod memory;
mod message;
mod permission;
mod pr;
mod project;
mod prompt;
mod recipe;
mod run;
mod terminal;
mod workspace;

// Re-export all public types for backward compatibility
pub use action::*;
pub use artifact::*;
pub use cache::*;
pub use chat::*;
pub use comment::*;
pub use docs::*;
pub use execution::*;
pub use file_change::*;
pub use github::*;
pub use issue::*;
pub use job::*;
pub use memory::*;
pub use message::*;
pub use permission::*;
pub use pr::*;
pub use project::*;
pub use prompt::*;
pub use recipe::*;
pub use run::*;
pub use terminal::*;
pub use workspace::*;
