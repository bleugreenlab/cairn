//! DAG helper functions for recipe execution.
//!
//! Graph traversal, node reachability analysis, and schema resolution for recipe nodes.
//! All recipe data is now loaded from execution snapshots rather than database tables.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::db_records::{DbRecipeEdge, DbRecipeNode};
use crate::models::{RecipeEdge, RecipeNode};

/// Convert RecipeNode (from snapshot) to DbRecipeNode (for job creation).
pub fn recipe_node_to_db(node: &RecipeNode, recipe_id: &str) -> DbRecipeNode {
    // Serialize the config parts back to JSON
    let config = {
        let mut config_map = serde_json::Map::new();
        if let Some(ref agent_cfg) = node.agent_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(agent_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref action_cfg) = node.action_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(action_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref checkpoint_cfg) = node.checkpoint_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(checkpoint_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref condition_cfg) = node.condition_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(condition_cfg) {
                config_map.extend(map);
            }
        }
        if let Some(ref context_cfg) = node.context_config {
            if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(context_cfg) {
                config_map.extend(map);
            }
        }
        if config_map.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(config_map).to_string())
        }
    };

    DbRecipeNode {
        id: node.id.clone(),
        recipe_id: recipe_id.to_string(),
        node_type: node.node_type.to_string(),
        name: node.name.clone(),
        position_x: node.position.x,
        position_y: node.position.y,
        config,
        created_at: 0, // Not used for job creation
        updated_at: 0, // Not used for job creation
        parent_id: node.parent_id.clone(),
    }
}

/// Convert RecipeEdge (from snapshot) to DbRecipeEdge (for job creation).
pub fn recipe_edge_to_db(edge: &RecipeEdge, recipe_id: &str) -> DbRecipeEdge {
    DbRecipeEdge {
        id: edge.id.clone(),
        recipe_id: recipe_id.to_string(),
        source_node_id: edge.source_node_id.clone(),
        target_node_id: edge.target_node_id.clone(),
        source_handle: edge.source_handle.clone(),
        target_handle: edge.target_handle.clone(),
        edge_type: edge.edge_type.to_string(),
        created_at: 0, // Not used for job creation
    }
}

/// Find all nodes reachable from trigger nodes via control edges.
/// Returns nodes in BFS order (distance from trigger), ensuring parents before children.
pub fn find_reachable_nodes(nodes: &[DbRecipeNode], edges: &[DbRecipeEdge]) -> Vec<String> {
    // Find all trigger nodes
    let triggers: Vec<_> = nodes
        .iter()
        .filter(|n| n.node_type == "trigger")
        .map(|n| n.id.clone())
        .collect();

    // Build adjacency list from control edges
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges {
        if edge.edge_type == "control" {
            adjacency
                .entry(edge.source_node_id.clone())
                .or_default()
                .push(edge.target_node_id.clone());
        }
    }

    // BFS from all triggers - preserve order for deterministic parent-before-child processing
    let mut reachable_ordered = Vec::new();
    let mut visited = HashSet::new();
    let mut queue: VecDeque<String> = triggers.into();

    while let Some(node_id) = queue.pop_front() {
        if visited.insert(node_id.clone()) {
            reachable_ordered.push(node_id.clone());
            if let Some(neighbors) = adjacency.get(&node_id) {
                queue.extend(neighbors.iter().cloned());
            }
        }
    }

    reachable_ordered
}
