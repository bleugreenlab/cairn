//! Cairn Core provides Cairn's publishable domain model plus a narrow set of
//! stable, data-oriented operations.
//!
//! The default public surface is intentionally small. Host/runtime wiring used by
//! the desktop app and server is available only through the `internal-api`
//! feature, via the unstable `internal` module, and is explicitly unstable.

// ── Domain operations ──────────────────────────────────
pub mod action_configs;
pub mod action_runs;
pub mod config_disables;
pub mod analytics;
pub mod archival;
pub mod artifacts;
pub mod issues;
pub mod jobs;
pub mod labels;
pub mod memories;
pub mod symbols;
pub mod merge_requests;
pub mod messages;
pub mod pr_data;
pub mod projects;
pub mod runs;
pub mod scratch;
pub mod search;
pub mod sessions;
pub mod todos;
pub mod turns;

// ── Stable public operations ───────────────────────────
pub use backends::SessionStart;

pub mod condition;
pub mod dispatch;
pub mod transitions;

// ── Stable data/config surface ─────────────────────────
pub mod error;
pub use error::CairnError;

pub mod account;
pub mod agents;
pub mod api;
pub mod config;
pub mod diff;
pub mod docs;
pub mod github;
pub mod identity;
pub mod models;
pub mod output_schemas;
pub mod references;
pub mod remote_servers;
pub mod skills;
pub mod system_prompt;
pub mod transcripts;

// ── Internal implementation modules ────────────────────
mod agent_process;
mod backends;
mod db;
mod db_records;
mod effects;
mod embeddings;
mod env;
mod execution;
mod git;
mod mcp;
mod node_segments;
mod notify;
mod orchestrator;
mod resources;
mod services;
mod storage;
mod sync;
mod workspace;

/// Unstable app-facing API used by Cairn host crates.
///
/// This surface exists so the desktop app and server can share runtime wiring
/// without making those implementation details part of the default semver
/// contract for published `cairn-core` releases.
#[cfg(feature = "internal-api")]
pub mod internal;
