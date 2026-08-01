//! Node behavior resolution
//!
//! Resolves node configuration to determine execution behavior.

use crate::db_records::DbRecipeNode;
use crate::models::{AgentGitConfig, BranchMode};

/// Resolved behavior for executing a recipe node
#[derive(Debug, Clone)]
pub struct StepBehavior {
    /// Whether this node mints an isolated child branch.
    pub(crate) mints_branch: bool,
    /// Whether this node inherits the upstream branch coordinate.
    pub(crate) inherits_branch: bool,
    /// Whether inheritance must land on the parent branch's live head. Only
    /// meaningful alongside `inherits_branch`; see [`AgentGitConfig`].
    pub(crate) requires_parent_head: bool,
}

/// Resolve behavior for a recipe node (DAG-based execution).
pub(crate) fn resolve_node_behavior(node: &DbRecipeNode) -> StepBehavior {
    match node.node_type.as_str() {
        // Agent nodes run backend sessions; git_config selects their branch policy.
        "agent" => {
            let git_config = parse_git_config(node).unwrap_or_default();

            match git_config.branch_mode {
                BranchMode::Isolate => StepBehavior {
                    mints_branch: true,
                    inherits_branch: false,
                    requires_parent_head: false,
                },
                BranchMode::Inherit => StepBehavior {
                    mints_branch: false,
                    inherits_branch: true,
                    requires_parent_head: git_config.require_parent_head,
                },
                BranchMode::None => StepBehavior {
                    mints_branch: false,
                    inherits_branch: false,
                    requires_parent_head: false,
                },
            }
        }

        // Non-agent nodes do not own a branch coordinate.
        _ => StepBehavior {
            mints_branch: false,
            inherits_branch: false,
            requires_parent_head: false,
        },
    }
}

/// Parse the `gitConfig` block out of a recipe node's config JSON.
fn parse_git_config(node: &DbRecipeNode) -> Option<AgentGitConfig> {
    node.config
        .as_ref()
        .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok())
        .and_then(|v| v.get("gitConfig").cloned())
        .and_then(|gc| serde_json::from_value::<AgentGitConfig>(gc).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(node_type: &str, config: Option<&str>) -> DbRecipeNode {
        DbRecipeNode {
            id: "node-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            node_type: node_type.to_string(),
            name: "Test Node".to_string(),
            position_x: 0.0,
            position_y: 0.0,
            config: config.map(String::from),
            created_at: 0,
            updated_at: 0,
            parent_id: None,
        }
    }

    #[test]
    fn agent_node_default_behavior() {
        let node = make_node("agent", Some(r#"{"agentConfigId": "build"}"#));
        let behavior = resolve_node_behavior(&node);

        assert!(behavior.mints_branch);
        assert!(!behavior.inherits_branch);
    }

    #[test]
    fn agent_node_isolated_branch() {
        let node = make_node(
            "agent",
            Some(r#"{"agentConfigId": "build", "gitConfig": {"branchMode": "isolate"}}"#),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(behavior.mints_branch);
        assert!(!behavior.inherits_branch);
    }

    #[test]
    fn agent_node_inherits_branch() {
        let node = make_node(
            "agent",
            Some(r#"{"agentConfigId": "documenter", "gitConfig": {"branchMode": "inherit"}}"#),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.mints_branch);
        assert!(behavior.inherits_branch);
        assert!(
            !behavior.requires_parent_head,
            "an authored inherit node keeps the degrading ladder unless it asks not to"
        );
    }

    #[test]
    fn agent_node_requires_the_parent_head() {
        let node = make_node(
            "agent",
            Some(
                r#"{"agentConfigId": "build", "gitConfig": {"branchMode": "inherit", "requireParentHead": true}}"#,
            ),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(behavior.inherits_branch);
        assert!(behavior.requires_parent_head);
    }

    /// The requirement is meaningless without inheritance, and must not leak
    /// into a mode that never reads a parent.
    #[test]
    fn requiring_the_parent_head_is_inert_without_inheritance() {
        let node = make_node(
            "agent",
            Some(
                r#"{"agentConfigId": "build", "gitConfig": {"branchMode": "none", "requireParentHead": true}}"#,
            ),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.inherits_branch);
        assert!(!behavior.requires_parent_head);
    }

    #[test]
    fn agent_node_uses_base_coordinate() {
        let node = make_node(
            "agent",
            Some(r#"{"agentConfigId": "analyzer", "gitConfig": {"branchMode": "none"}}"#),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.mints_branch);
        assert!(!behavior.inherits_branch);
    }

    #[test]
    fn action_node_behavior() {
        let node = make_node("action", Some(r#"{"action": "create_pr"}"#));
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.mints_branch);
        assert!(!behavior.inherits_branch);
    }

    #[test]
    fn trigger_node_behavior() {
        let node = make_node("trigger", Some(r#"{"triggerType": "issue"}"#));
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.mints_branch);
        assert!(!behavior.inherits_branch);
    }
}
