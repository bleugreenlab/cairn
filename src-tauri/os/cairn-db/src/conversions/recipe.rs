//! Database record conversion into domain values.
use cairn_models::*;

/// Convert DbRecipeNode to RecipeNode
impl TryFrom<crate::db_records::DbRecipeNode> for RecipeNode {
    type Error = String;

    fn try_from(db: crate::db_records::DbRecipeNode) -> Result<Self, Self::Error> {
        let node_type: RecipeNodeType = db
            .node_type
            .parse()
            .map_err(|e: String| format!("Invalid node_type: {}", e))?;

        // Parse config JSON
        let config: Option<NodeConfig> = db
            .config
            .as_ref()
            .map(|s| serde_json::from_str(s))
            .transpose()
            .map_err(|e| format!("Invalid config JSON: {}", e))?;

        Ok(RecipeNode::from_config(
            db.id,
            node_type,
            db.name,
            NodePosition {
                x: db.position_x,
                y: db.position_y,
            },
            db.parent_id,
            config,
        ))
    }
}

/// Convert DbRecipeEdge to RecipeEdge
impl TryFrom<crate::db_records::DbRecipeEdge> for RecipeEdge {
    type Error = String;

    fn try_from(db: crate::db_records::DbRecipeEdge) -> Result<Self, Self::Error> {
        let edge_type: RecipeEdgeType = db
            .edge_type
            .parse()
            .map_err(|e: String| format!("Invalid edge_type: {}", e))?;

        Ok(RecipeEdge {
            id: db.id,
            edge_type,
            source_node_id: db.source_node_id,
            source_handle: db.source_handle,
            target_node_id: db.target_node_id,
            target_handle: db.target_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_records::{DbRecipeEdge, DbRecipeNode};
    // =========================================================================
    // TryFrom<DbRecipeNode> tests
    // =========================================================================

    fn make_db_recipe_node(node_type: &str, config: Option<&str>) -> DbRecipeNode {
        DbRecipeNode {
            id: "node-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            node_type: node_type.to_string(),
            name: "Test Node".to_string(),
            position_x: 100.0,
            position_y: 200.0,
            config: config.map(|s| s.to_string()),
            created_at: 1000,
            updated_at: 2000,
            parent_id: None,
        }
    }

    #[test]
    fn try_from_db_recipe_node_trigger() {
        let config = r#"{"triggerType": "issue"}"#;
        let db = make_db_recipe_node("trigger", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.id, "node-1");
        assert_eq!(node.node_type, RecipeNodeType::Trigger);
        assert_eq!(node.name, "Test Node");
        assert_eq!(node.position.x, 100.0);
        assert_eq!(node.position.y, 200.0);
        assert!(node.trigger_config.is_some());
        assert_eq!(
            node.trigger_config.unwrap().trigger_type,
            RecipeTrigger::Manual
        );
    }

    #[test]
    fn try_from_db_recipe_node_agent() {
        let config = r#"{"agentConfigId": "agent-123"}"#;
        let db = make_db_recipe_node("agent", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Agent);
        assert!(node.agent_config.is_some());
        let agent_config = node.agent_config.unwrap();
        assert_eq!(agent_config.agent_config_id, Some("agent-123".to_string()));
    }

    #[test]
    fn try_from_db_recipe_node_agent_with_git_config() {
        let config = r#"{"agentConfigId": "agent-123", "gitConfig": {"branchMode": "inherit"}}"#;
        let db = make_db_recipe_node("agent", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        let agent_config = node.agent_config.unwrap();
        assert!(agent_config.git_config.is_some());
        let git_config = agent_config.git_config.unwrap();
        assert_eq!(git_config.branch_mode, BranchMode::Inherit);
    }

    #[test]
    fn try_from_db_recipe_node_action() {
        let config = r#"{"action": "create_pr", "actionParams": {"title": "PR Title"}}"#;
        let db = make_db_recipe_node("action", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Action);
        assert!(node.action_config.is_some());
        let action_config = node.action_config.unwrap();
        assert_eq!(action_config.action, "create_pr");
        assert_eq!(
            action_config.action_params.get("title").unwrap(),
            "PR Title"
        );
    }

    #[test]
    fn try_from_db_recipe_node_checkpoint() {
        let config = r#"{"command": "npm test", "prompt": "CI"}"#;
        let db = make_db_recipe_node("checkpoint", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Checkpoint);
        assert!(node.checkpoint_config.is_some());
        let checkpoint_config = node.checkpoint_config.unwrap();
        assert_eq!(checkpoint_config.command, Some("npm test".to_string()));
        assert_eq!(checkpoint_config.prompt, Some("CI".to_string()));
    }

    #[test]
    fn try_from_db_recipe_node_artifact() {
        // `artifactType` is the legacy key; it deserializes into the canonical
        // `name` via the serde alias, and `confirmPolicy` carries the gate.
        let config =
            r#"{"artifactType": "plan", "confirmPolicy": "user", "schema": {"type": "object"}}"#;
        let db = make_db_recipe_node("artifact", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Artifact);
        assert!(node.artifact_config.is_some());
        let artifact_config = node.artifact_config.unwrap();
        assert_eq!(artifact_config.name, "plan");
        assert_eq!(artifact_config.confirm_policy, ConfirmPolicy::User);
        assert!(artifact_config.schema.is_some());
    }

    #[test]
    fn try_from_db_recipe_node_condition() {
        let config =
            r#"{"conditionType": "ai", "question": "Is this ready?", "ports": ["yes", "no"]}"#;
        let db = make_db_recipe_node("condition", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Condition);
        assert!(node.condition_config.is_some());
        let condition_config = node.condition_config.unwrap();
        assert_eq!(condition_config.condition_type, ConditionType::Ai);
        assert_eq!(
            condition_config.question,
            Some("Is this ready?".to_string())
        );
        assert_eq!(condition_config.ports, vec!["yes", "no"]);
    }

    #[test]
    fn try_from_db_recipe_node_no_config() {
        let db = make_db_recipe_node("trigger", None);

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Trigger);
        assert!(node.trigger_config.is_none());
        assert!(node.agent_config.is_none());
        assert!(node.action_config.is_none());
    }

    #[test]
    fn try_from_db_recipe_node_invalid_node_type() {
        let db = make_db_recipe_node("invalid", None);

        let result = RecipeNode::try_from(db);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid node_type"));
    }

    #[test]
    fn try_from_db_recipe_node_invalid_config_json() {
        let db = make_db_recipe_node("trigger", Some("not valid json"));

        let result = RecipeNode::try_from(db);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid config JSON"));
    }

    #[test]
    fn try_from_db_recipe_node_with_parent_id() {
        let mut db = make_db_recipe_node("checkpoint", None);
        db.parent_id = Some("parent-node".to_string());

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.parent_id, Some("parent-node".to_string()));
    }

    // =========================================================================
    // TryFrom<DbRecipeEdge> tests
    // =========================================================================

    fn make_db_recipe_edge(edge_type: &str) -> DbRecipeEdge {
        DbRecipeEdge {
            id: "edge-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            edge_type: edge_type.to_string(),
            source_node_id: "node-a".to_string(),
            source_handle: "ctrl-out".to_string(),
            target_node_id: "node-b".to_string(),
            target_handle: "ctrl-in".to_string(),
            created_at: 1000,
        }
    }

    #[test]
    fn try_from_db_recipe_edge_control() {
        let db = make_db_recipe_edge("control");

        let edge = RecipeEdge::try_from(db).unwrap();

        assert_eq!(edge.id, "edge-1");
        assert_eq!(edge.edge_type, RecipeEdgeType::Control);
        assert_eq!(edge.source_node_id, "node-a");
        assert_eq!(edge.source_handle, "ctrl-out");
        assert_eq!(edge.target_node_id, "node-b");
        assert_eq!(edge.target_handle, "ctrl-in");
    }

    #[test]
    fn try_from_db_recipe_edge_context() {
        let db = make_db_recipe_edge("context");

        let edge = RecipeEdge::try_from(db).unwrap();

        assert_eq!(edge.edge_type, RecipeEdgeType::Context);
    }

    #[test]
    fn try_from_db_recipe_edge_invalid_type() {
        let db = make_db_recipe_edge("invalid");

        let result = RecipeEdge::try_from(db);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid edge_type"));
    }
}
