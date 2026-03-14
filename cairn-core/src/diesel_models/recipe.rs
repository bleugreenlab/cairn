//! Recipe and condition evaluation models
//!
//! Note: recipes, recipe_nodes, and recipe_edges tables have been dropped.
//! DbRecipeNode and DbRecipeEdge types are kept as in-memory DTOs for DAG execution.

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// Recipe Node (in-memory DTO, no longer a Diesel model)
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
// Recipe Edge (in-memory DTO, no longer a Diesel model)
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

// ============================================================================
// Condition Evaluation models (still backed by database table)
// ============================================================================

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = condition_evaluations)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbConditionEvaluation {
    pub id: String,
    pub execution_id: String,
    pub recipe_node_id: String,
    pub result_port: String,
    pub raw_result: Option<String>,
    pub error_message: Option<String>,
    pub evaluated_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = condition_evaluations)]
pub struct NewConditionEvaluation<'a> {
    pub id: &'a str,
    pub execution_id: &'a str,
    pub recipe_node_id: &'a str,
    pub result_port: &'a str,
    pub raw_result: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub evaluated_at: i32,
}
