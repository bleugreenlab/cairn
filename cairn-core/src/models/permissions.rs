//! Agent permission types.
//!
//! Three concepts control what an agent can do:
//!
//! - [`ApprovalPolicy`]: default behavior for tool invocations that aren't
//!   explicitly allowed or denied by a per-tool policy.
//! - [`FilesystemScope`]: where the agent can write on disk.
//! - [`ToolPolicy`]: per-tool override — allow, deny, or inherit from
//!   the approval policy.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Default behavior for tool invocations not covered by a per-tool policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ApprovalPolicy {
    /// Prompt the user for approval.
    #[default]
    Ask,
    /// Auto-approve without prompting.
    AcceptAll,
    /// Reject (block) without prompting.
    RejectAll,
}

/// What the agent can access on disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FilesystemScope {
    /// Read-only access (no writes).
    ReadOnly,
    /// Write only within the workspace directory.
    #[default]
    CwdOnly,
    /// Full filesystem access.
    FullAccess,
}

/// Per-tool policy override.
///
/// Explicitly allowed tools always run. Explicitly denied tools never run.
/// Inherit defers to the agent's [`ApprovalPolicy`]:
/// - Ask → prompt the user
/// - AcceptAll → allow
/// - RejectAll → deny
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ToolPolicy {
    Allow,
    Deny,
    #[default]
    Inherit,
}

/// Build a tool_policies map from the legacy `tools` + `disallowed_tools` lists.
pub fn tool_policies_from_legacy_lists(
    tools: &[String],
    disallowed_tools: Option<&[String]>,
) -> HashMap<String, ToolPolicy> {
    let mut map = HashMap::new();
    for t in tools {
        map.insert(t.clone(), ToolPolicy::Allow);
    }
    if let Some(disallowed) = disallowed_tools {
        for t in disallowed {
            map.insert(t.clone(), ToolPolicy::Deny);
        }
    }
    map
}

/// Convert a tool_policies map back to legacy `tools` + `disallowed_tools` lists.
pub fn tool_policies_to_legacy_lists(
    policies: &HashMap<String, ToolPolicy>,
) -> (Vec<String>, Option<Vec<String>>) {
    let mut allowed: Vec<String> = policies
        .iter()
        .filter(|(_, p)| **p == ToolPolicy::Allow)
        .map(|(k, _)| k.clone())
        .collect();
    allowed.sort();

    let mut denied: Vec<String> = policies
        .iter()
        .filter(|(_, p)| **p == ToolPolicy::Deny)
        .map(|(k, _)| k.clone())
        .collect();
    denied.sort();

    let disallowed = if denied.is_empty() {
        None
    } else {
        Some(denied)
    };
    (allowed, disallowed)
}

/// Parse a legacy `permissionMode` string into the two canonical fields.
///
/// Used when reading old agent configs / snapshots that only have the
/// single combined field.
pub fn split_legacy_permission_mode(mode: Option<&str>) -> (ApprovalPolicy, FilesystemScope) {
    match mode {
        Some("bypassPermissions") => (ApprovalPolicy::AcceptAll, FilesystemScope::FullAccess),
        Some("acceptEdits") | Some("autoEdits") => {
            (ApprovalPolicy::AcceptAll, FilesystemScope::CwdOnly)
        }
        Some("plan") => (ApprovalPolicy::Ask, FilesystemScope::ReadOnly),
        _ => (ApprovalPolicy::Ask, FilesystemScope::CwdOnly),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // split_legacy_permission_mode
    // =========================================================================

    #[test]
    fn split_legacy_none() {
        let (a, f) = split_legacy_permission_mode(None);
        assert_eq!(a, ApprovalPolicy::Ask);
        assert_eq!(f, FilesystemScope::CwdOnly);
    }

    #[test]
    fn split_legacy_bypass() {
        let (a, f) = split_legacy_permission_mode(Some("bypassPermissions"));
        assert_eq!(a, ApprovalPolicy::AcceptAll);
        assert_eq!(f, FilesystemScope::FullAccess);
    }

    #[test]
    fn split_legacy_accept_edits() {
        let (a, f) = split_legacy_permission_mode(Some("acceptEdits"));
        assert_eq!(a, ApprovalPolicy::AcceptAll);
        assert_eq!(f, FilesystemScope::CwdOnly);
    }

    #[test]
    fn split_legacy_plan() {
        let (a, f) = split_legacy_permission_mode(Some("plan"));
        assert_eq!(a, ApprovalPolicy::Ask);
        assert_eq!(f, FilesystemScope::ReadOnly);
    }

    #[test]
    fn split_legacy_unknown_defaults() {
        let (a, f) = split_legacy_permission_mode(Some("garbage"));
        assert_eq!(a, ApprovalPolicy::Ask);
        assert_eq!(f, FilesystemScope::CwdOnly);
    }

    // =========================================================================
    // serde
    // =========================================================================

    #[test]
    fn serde_roundtrip_approval() {
        let json = serde_json::to_string(&ApprovalPolicy::AcceptAll).unwrap();
        assert_eq!(json, "\"acceptAll\"");
        let back: ApprovalPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ApprovalPolicy::AcceptAll);
    }

    #[test]
    fn serde_roundtrip_filesystem() {
        let json = serde_json::to_string(&FilesystemScope::FullAccess).unwrap();
        assert_eq!(json, "\"fullAccess\"");
        let back: FilesystemScope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, FilesystemScope::FullAccess);
    }

    #[test]
    fn serde_roundtrip_tool_policy() {
        let json = serde_json::to_string(&ToolPolicy::Inherit).unwrap();
        assert_eq!(json, "\"inherit\"");
        let back: ToolPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ToolPolicy::Inherit);
    }

    // =========================================================================
    // tool_policies conversion
    // =========================================================================

    #[test]
    fn legacy_lists_to_policies() {
        let tools = vec!["Read".into(), "Write".into()];
        let disallowed = vec!["Bash".into()];
        let map = tool_policies_from_legacy_lists(&tools, Some(&disallowed));
        assert_eq!(map.get("Read"), Some(&ToolPolicy::Allow));
        assert_eq!(map.get("Write"), Some(&ToolPolicy::Allow));
        assert_eq!(map.get("Bash"), Some(&ToolPolicy::Deny));
        assert_eq!(map.get("Unknown"), None); // not in map = Inherit
    }

    #[test]
    fn policies_to_legacy_lists() {
        let mut map = HashMap::new();
        map.insert("Read".into(), ToolPolicy::Allow);
        map.insert("Write".into(), ToolPolicy::Allow);
        map.insert("Bash".into(), ToolPolicy::Deny);
        map.insert("Task".into(), ToolPolicy::Inherit);
        let (allowed, disallowed) = tool_policies_to_legacy_lists(&map);
        assert_eq!(allowed, vec!["Read", "Write"]);
        assert_eq!(disallowed, Some(vec!["Bash".to_string()]));
    }

    #[test]
    fn policies_roundtrip() {
        let tools = vec!["Read".into(), "Write".into()];
        let disallowed = vec!["Bash".into()];
        let map = tool_policies_from_legacy_lists(&tools, Some(&disallowed));
        let (allowed, denied) = tool_policies_to_legacy_lists(&map);
        assert_eq!(allowed, vec!["Read", "Write"]);
        assert_eq!(denied, Some(vec!["Bash".to_string()]));
    }
}
