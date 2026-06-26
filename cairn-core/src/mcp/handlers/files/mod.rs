//! File operation MCP handlers.
//!
//! Handles: edit (unified file mutations), read

pub(crate) mod change;

/// Host-driven user file edit, reusing the agent `write` verb's VCS seal seam.
/// Re-exported here so the Tauri `save_worktree_file` command can reach it
/// through `cairn_core::internal::mcp::handlers::files`.
pub use change::host_edit::commit_user_file_edit;
mod read;
mod target;

pub use change::handle_change;
pub use read::handle_read_file;
pub(crate) use read::produce_archived_file_segment;
pub(crate) use read::produce_file_segment;
