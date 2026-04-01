//! Cairn Core provides Cairn's publishable domain model plus a narrow set of
//! stable, data-oriented operations.
//!
//! The default public surface is intentionally small. Host/runtime wiring used by
//! the desktop app and server is available only through the `internal-api`
//! feature, via the unstable `internal` module, and is explicitly unstable.

// ── Domain operations ──────────────────────────────────
pub mod action_configs;
pub mod action_runs;
pub mod artifacts;
pub mod chats;
pub mod issues;
pub mod jobs;
pub mod managers;
pub mod memories;
pub mod merge_requests;
pub mod messages;
pub mod pr_data;
pub mod projects;
pub mod runs;
pub mod search;
pub mod sessions;
pub mod todos;
pub mod turns;

// ── Stable public operations ───────────────────────────
pub use backends::SessionStart;

pub mod condition;
pub mod snapshot;
pub mod transitions;

// ── Stable data/config surface ─────────────────────────
pub mod account;
pub mod agents;
pub mod api;
pub mod config;
pub mod docs;
pub mod github;
pub mod identity;
pub mod models;
pub mod output_schemas;
pub mod remote_servers;
pub mod resources;
pub mod skills;
pub mod system_prompt;
pub mod tools;
pub mod transcripts;

// ── Internal implementation modules ────────────────────
mod agent_process;
mod backends;
mod db;
mod diesel_models;
mod effects;
mod embeddings;
mod env;
mod execution;
mod git;
mod mcp;
mod node_segments;
mod notify;
mod orchestrator;
mod schema;
mod services;
mod sync;

/// Unstable app-facing API used by Cairn host crates.
///
/// This surface exists so the desktop app and server can share runtime wiring
/// without making those implementation details part of the default semver
/// contract for published `cairn-core` releases.
#[cfg(feature = "internal-api")]
pub mod internal;

#[cfg(test)]
pub(crate) mod test_utils;
