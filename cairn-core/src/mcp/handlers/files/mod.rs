//! Compatibility re-exports for host file commands.
//!
//! The agent-facing `read` and `write` handlers live in sibling `read` and
//! `write` modules. This module remains as the stable public path used by the
//! Tauri `save_worktree_file` command for host-driven user file edits.

/// Host-driven user file edit, reusing the agent `write` verb's VCS seal seam.
/// Re-exported here so the Tauri `save_worktree_file` command can reach it
/// through `cairn_core::internal::mcp::handlers::files`.
#[allow(unused_imports)]
pub use super::write::host_edit::commit_user_file_edit;
