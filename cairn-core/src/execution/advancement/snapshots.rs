use super::*;

pub(super) async fn load_execution_snapshot_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<ExecutionSnapshot> {
    let mut rows = conn
        .query(
            "SELECT snapshot FROM executions WHERE id = ?1",
            (execution_id,),
        )
        .await?;
    let snapshot_json = rows
        .next()
        .await?
        .map(|row| row.opt_text(0))
        .transpose()?
        .flatten()
        .ok_or_else(|| db_internal(format!("Execution has no snapshot: {execution_id}")))?;

    ExecutionSnapshot::from_json(&snapshot_json)
        .map_err(|e| DbError::Row(format!("Failed to parse execution snapshot: {e}")))
}

pub(crate) fn load_execution_snapshot(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<ExecutionSnapshot, String> {
    let execution_id = execution_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move { load_execution_snapshot_conn(conn, &execution_id).await })
        })
        .await
        .map_err(|e| format!("Failed to load execution: {e}"))
    })
}

pub(crate) fn update_execution_snapshot(
    db: Arc<LocalDb>,
    execution_id: &str,
    snapshot: &ExecutionSnapshot,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let snapshot_json = serde_json::to_string(snapshot)
        .map_err(|e| format!("Failed to serialize snapshot: {e}"))?;
    run_advancement_db(async move {
        db.execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
            params![snapshot_json.as_str(), execution_id.as_str()],
        )
        .await
        .map(|_| ())
        .map_err(|e| format!("Failed to update execution snapshot: {e}"))
    })
}

pub(super) fn snapshot_nodes_to_db(snapshot: &ExecutionSnapshot) -> Vec<DbRecipeNode> {
    let recipe_id = &snapshot.recipe.id;
    snapshot
        .recipe
        .nodes
        .iter()
        .map(|node| crate::execution::dag::recipe_node_to_db(node, recipe_id))
        .collect()
}

pub(super) fn snapshot_edges_to_db(snapshot: &ExecutionSnapshot) -> Vec<DbRecipeEdge> {
    let recipe_id = &snapshot.recipe.id;
    snapshot
        .recipe
        .edges
        .iter()
        .map(|edge| crate::execution::dag::recipe_edge_to_db(edge, recipe_id))
        .collect()
}

pub fn load_nodes_from_execution(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Vec<DbRecipeNode>, String> {
    let execution_id = execution_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
                Ok(snapshot_nodes_to_db(&snapshot))
            })
        })
        .await
        .map_err(|e| format!("Failed to load execution nodes: {e}"))
    })
}
