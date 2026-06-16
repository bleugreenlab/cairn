//! Node behavior resolution
//!
//! Resolves node configuration to determine execution behavior.

use crate::db_records::DbRecipeNode;
use crate::models::{AgentGitConfig, WorktreeMode};

/// Resolved behavior for executing a recipe node
#[derive(Debug, Clone)]
pub struct StepBehavior {
    /// Whether this node needs its own worktree created
    pub needs_worktree: bool,
    /// Whether this node inherits worktree from upstream agent
    pub inherits_worktree: bool,
}

/// Resolve behavior for a recipe node (DAG-based execution).
pub fn resolve_node_behavior(node: &DbRecipeNode) -> StepBehavior {
    match node.node_type.as_str() {
        // Agent nodes run backend sessions - worktree behavior depends on git_config
        "agent" => {
            let worktree_mode = parse_worktree_mode(node);

            match worktree_mode {
                WorktreeMode::Own => StepBehavior {
                    needs_worktree: true,
                    inherits_worktree: false,
                },
                WorktreeMode::Inherit => StepBehavior {
                    needs_worktree: false,
                    inherits_worktree: true,
                },
                WorktreeMode::None => StepBehavior {
                    needs_worktree: false,
                    inherits_worktree: false,
                },
            }
        }

        // Action, checkpoint, trigger, artifact nodes don't need worktrees
        _ => StepBehavior {
            needs_worktree: false,
            inherits_worktree: false,
        },
    }
}

/// Parse worktree mode from node config JSON
fn parse_worktree_mode(node: &DbRecipeNode) -> WorktreeMode {
    node.config
        .as_ref()
        .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok())
        .and_then(|v| v.get("gitConfig").cloned())
        .and_then(|gc| serde_json::from_value::<AgentGitConfig>(gc).ok())
        .map(|gc| gc.worktree_mode)
        .unwrap_or_default()
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

        assert!(behavior.needs_worktree);
        assert!(!behavior.inherits_worktree);
    }

    #[test]
    fn agent_node_own_worktree() {
        let node = make_node(
            "agent",
            Some(r#"{"agentConfigId": "build", "gitConfig": {"worktreeMode": "own"}}"#),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(behavior.needs_worktree);
        assert!(!behavior.inherits_worktree);
    }

    #[test]
    fn agent_node_inherit_worktree() {
        let node = make_node(
            "agent",
            Some(r#"{"agentConfigId": "documenter", "gitConfig": {"worktreeMode": "inherit"}}"#),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.needs_worktree);
        assert!(behavior.inherits_worktree);
    }

    #[test]
    fn agent_node_no_worktree() {
        let node = make_node(
            "agent",
            Some(r#"{"agentConfigId": "analyzer", "gitConfig": {"worktreeMode": "none"}}"#),
        );
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.needs_worktree);
        assert!(!behavior.inherits_worktree);
    }

    #[test]
    fn action_node_behavior() {
        let node = make_node("action", Some(r#"{"action": "create_pr"}"#));
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.needs_worktree);
        assert!(!behavior.inherits_worktree);
    }

    #[test]
    fn trigger_node_behavior() {
        let node = make_node("trigger", Some(r#"{"triggerType": "issue"}"#));
        let behavior = resolve_node_behavior(&node);

        assert!(!behavior.needs_worktree);
        assert!(!behavior.inherits_worktree);
    }
}
