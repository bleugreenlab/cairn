//! Cairn Core provides Cairn's publishable domain model plus a narrow set of
//! stable, data-oriented operations.
//!
//! The default public surface is intentionally small. Host/runtime wiring used by
//! the desktop app and server is available only through the `internal-api`
//! feature, via the unstable `internal` module, and is explicitly unstable.

// ── Domain operations ──────────────────────────────────
pub mod action_configs;
pub mod action_runs;
pub mod archival;
pub mod artifacts;
pub mod authorization;
pub mod browser_network;
pub mod browsers;
pub mod channels;
pub mod config_disables;
pub mod images;
pub mod issues;
pub mod jobs;
pub mod labels;
pub mod memories;
pub mod merge_requests;
pub mod messages;
pub mod pr_data;
pub mod pressure;
pub mod projects;
pub mod repl_host;
pub mod runs;
pub mod runtime;
pub mod scratch;
pub mod search;
pub mod sessions;
pub use cairn_symbols::symbols;
pub mod terminal_host;
pub mod threads;
pub mod todos;
pub mod token_meters;
pub mod turns;
pub mod workflow_journal;
pub mod workflow_progress;

// ── Stable public operations ───────────────────────────
/// The canonical catalog of providers Cairn ships — the inventory the settings
/// surface installs from, and the answer to whether a backend key is supported.
pub use backends::catalog as provider_catalog;
pub use backends::SessionStart;

pub mod condition;
pub mod dispatch;
pub mod security;
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
// `models` and `db_records` descend into cairn-db (with the storage engine);
// re-exported here at their original crate paths so `crate::models` and
// `crate::db_records` consumers compile unchanged. `storage` keeps a thin core
// facade (src/storage/mod.rs) that globs cairn-db's storage and hosts the
// core-only team-sync loop.
pub use cairn_db::models;
pub mod output_schemas;
pub mod references;
pub mod responses;
pub mod routes;
pub mod skills;
pub mod system_prompt;
pub mod transcripts;

// ── Internal implementation modules ────────────────────
mod agent_process;
mod backends;
mod clock;
mod db;
mod dev_instances;
mod durable_content;
mod duration;
pub(crate) mod fleet;
pub use cairn_db::db_records;
mod effects;
mod embeddings;
mod env;
mod execution;
mod git;
mod jj;
mod markdown_frontmatter;
mod mcp;
mod node_segments;
mod notify;
mod orchestrator;
mod resources;
mod resume_timing;
mod services;
mod storage;
mod team_remote_intents;
mod voice;
mod workspace;

// Cross-engine parity tests comparing the fff worktree index (cairn-symbols)
// against this crate's canonical ripgrep walk. Only cairn-core sees both
// engines, so the comparison is anchored here rather than in cairn-symbols.

/// Unstable app-facing API used by Cairn host crates.
///
/// This surface exists so the desktop app and server can share runtime wiring
/// without making those implementation details part of the default semver
/// contract for published `cairn-core` releases.
#[cfg(feature = "internal-api")]
pub mod internal;
