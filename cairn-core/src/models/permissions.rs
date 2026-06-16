//! Agent fence permission types.
//!
//! Agent actions are either inside the job's worktree or they escape it (outside
//! file writes, sensitive reads, shell reach beyond the worktree, privilege
//! escalation). In-worktree actions are branch-tracked or recoverable and are not
//! gated; escapes are governed by [`Fence`].
//!
//! There is no per-tool allow-listing for Cairn's three verb surface
//! (`read`/`write`/`run`). Enforcement lives in the verb handlers, which consult
//! [`Fence`] when an action reaches outside the worktree.

use serde::{Deserialize, Serialize};

/// What Cairn does when an action reaches outside the worktree fence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Fence {
    /// Reject escapes immediately.
    Deny,
    /// Suspend and ask (UI prompt or answerable `permissions` resource).
    #[default]
    Ask,
    /// Do not enforce the fence; escapes proceed without prompting.
    Allow,
}

impl Fence {
    /// Convert legacy `(sandbox, on_escape)` settings into the single fence mode.
    ///
    /// Legacy `full` disabled the boundary regardless of `onEscape`, so it maps
    /// to [`Fence::Allow`]. Missing legacy values preserve the previous default:
    /// worktree sandbox + ask on escape.
    pub fn from_legacy(sandbox: Option<LegacySandbox>, on_escape: Option<LegacyOnEscape>) -> Self {
        match sandbox.unwrap_or_default() {
            LegacySandbox::Full => Fence::Allow,
            LegacySandbox::Worktree => match on_escape.unwrap_or_default() {
                LegacyOnEscape::Deny => Fence::Deny,
                LegacyOnEscape::Ask => Fence::Ask,
                LegacyOnEscape::Allow => Fence::Allow,
            },
        }
    }

    /// Runtime stdin still speaks legacy Claude-ish mode strings.
    pub fn to_legacy_permission_mode(self) -> &'static str {
        match self {
            Fence::Allow => "acceptEdits",
            Fence::Ask | Fence::Deny => "default",
        }
    }
}

/// Deserialize-only legacy file/shell reach boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LegacySandbox {
    #[default]
    Worktree,
    Full,
}

/// Deserialize-only legacy escape behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LegacyOnEscape {
    #[default]
    Ask,
    Allow,
    Deny,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_fence() {
        let json = serde_json::to_string(&Fence::Deny).unwrap();
        assert_eq!(json, "\"deny\"");
        let back: Fence = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Fence::Deny);
    }

    #[test]
    fn fence_defaults_to_ask() {
        assert_eq!(Fence::default(), Fence::Ask);
    }

    #[test]
    fn legacy_mapping_collapses_to_fence() {
        assert_eq!(Fence::from_legacy(None, None), Fence::Ask);
        assert_eq!(
            Fence::from_legacy(Some(LegacySandbox::Worktree), Some(LegacyOnEscape::Deny)),
            Fence::Deny
        );
        assert_eq!(
            Fence::from_legacy(Some(LegacySandbox::Worktree), Some(LegacyOnEscape::Allow)),
            Fence::Allow
        );
        assert_eq!(
            Fence::from_legacy(Some(LegacySandbox::Full), Some(LegacyOnEscape::Deny)),
            Fence::Allow
        );
    }
}
