//! MCP commit result with application publication state.

pub use cairn_vcs::git::{current_branch, current_commit, has_remote};

/// Result from a commit/seal operation.
#[derive(Debug, Clone)]
pub struct CommitResult {
    pub sha: String,
    pub(crate) pr_number: Option<i32>,
    /// Human-readable account of how a `^` amend landed: either the commit it
    /// rewrote in place or why it was converted to a child commit.
    pub(crate) amend_note: Option<String>,
}

impl From<cairn_vcs::SealResult> for CommitResult {
    fn from(seal: cairn_vcs::SealResult) -> Self {
        Self {
            sha: seal.sha,
            pr_number: None,
            amend_note: seal.amend_note,
        }
    }
}
