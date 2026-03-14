//! Recipe execution startup logic.
//!
//! Creates jobs for executions from their snapshot, used by both issue-triggered
//! and manual recipe executions.

use diesel::prelude::*;

use crate::models::{ExecutionSnapshot, Job};
use crate::schema::executions;

use super::dag::{create_jobs_from_nodes, recipe_edge_to_db, recipe_node_to_db};

/// Create jobs for an execution using its snapshot.
///
/// Loads the execution's stored snapshot (recipe nodes/edges captured at creation time),
/// converts them to DB format, and creates pending jobs for each agent node.
/// Jobs start in "pending" status and are advanced by the DAG logic.
pub fn create_jobs_for_execution(
    conn: &mut diesel::sqlite::SqliteConnection,
    execution_id: &str,
    issue_id: &str,
    project_id: &str,
) -> Result<Vec<Job>, String> {
    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .map_err(|e| format!("Failed to load execution: {}", e))?;

    let snapshot_json = snapshot_json.ok_or_else(|| "Execution has no snapshot".to_string())?;

    let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;

    log::info!(
        "create_jobs_for_execution: snapshot has {} nodes, {} edges",
        snapshot.recipe.nodes.len(),
        snapshot.recipe.edges.len()
    );

    let recipe_id = &snapshot.recipe.id;
    let nodes: Vec<_> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| recipe_node_to_db(n, recipe_id))
        .collect();
    let edges: Vec<_> = snapshot
        .recipe
        .edges
        .iter()
        .map(|e| recipe_edge_to_db(e, recipe_id))
        .collect();

    create_jobs_from_nodes(
        conn,
        execution_id,
        &nodes,
        &edges,
        issue_id,
        project_id,
        &snapshot,
    )
}
