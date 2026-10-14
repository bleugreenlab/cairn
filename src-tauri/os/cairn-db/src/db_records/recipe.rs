//! Recipe and condition evaluation models
//!
//! Note: recipes, recipe_nodes, and recipe_edges tables have been dropped.
//! DbRecipeNode and DbRecipeEdge types are kept as in-memory DTOs for DAG execution.

// ============================================================================
// Recipe Node (in-memory DTO, no longer a database model)
// Used for DAG execution with data loaded from execution snapshots
// ============================================================================

#[derive(Debug, Clone)]
pub struct DbRecipeNode {
    pub id: String,
    pub recipe_id: String,
    pub node_type: String, // 'trigger', 'agent', 'action', 'checkpoint', 'artifact'
    pub name: String,
    pub position_x: f32,
    pub position_y: f32,
    pub config: Option<String>, // JSON for type-specific configuration
    pub created_at: i32,
    pub updated_at: i32,
    pub parent_id: Option<String>, // For slot nodes (checkpoint, artifact) - references parent node
}

// ============================================================================
// Recipe Edge (in-memory DTO, no longer a database model)
// Used for DAG execution with data loaded from execution snapshots
// ============================================================================

#[derive(Debug, Clone)]
pub struct DbRecipeEdge {
    pub id: String,
    pub recipe_id: String,
    pub edge_type: String, // 'control', 'context'
    pub source_node_id: String,
    pub source_handle: String,
    pub target_node_id: String,
    pub target_handle: String,
    pub created_at: i32,
}
