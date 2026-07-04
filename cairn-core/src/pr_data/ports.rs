use crate::storage::{DbResult, LocalDb, RowExt};
use turso::params;

pub async fn fire_pr_node_port_conn(
    conn: &turso::Connection,
    execution_id: &str,
    recipe_node_id: &str,
    port: &str,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    let id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT OR IGNORE INTO pr_node_port_fires(id, execution_id, recipe_node_id, port, fired_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id.as_str(), execution_id, recipe_node_id, port, now],
    )
    .await?;
    Ok(())
}

/// Fire a `pr` node port for a known execution + recipe node. Used by the PR
/// action handler, which already holds both ids on its action_run.
pub async fn fire_pr_node_port(
    db: &LocalDb,
    execution_id: &str,
    recipe_node_id: &str,
    port: &str,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let recipe_node_id = recipe_node_id.to_string();
    let port = port.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        let recipe_node_id = recipe_node_id.clone();
        let port = port.clone();
        Box::pin(async move {
            fire_pr_node_port_conn(conn, &execution_id, &recipe_node_id, &port).await
        })
    })
    .await
    .map_err(|error| format!("Failed to fire PR node port: {error}"))
}

/// Fire a `pr` node port for the PR's producing job id (`merge_requests.job_id`).
/// A first-class `pr` node is represented by an `action_runs` row whose
/// `parent_job_id` is that producing job; legacy `create_pr` rows have no PR node
/// port and no-op here. The action-run-id lookup remains as a compatibility
/// fallback for unrepaired historical rows.
pub async fn fire_pr_node_port_for_owner(
    db: &LocalDb,
    owner_id: &str,
    port: &str,
) -> Result<(), String> {
    let owner_id = owner_id.to_string();
    let port = port.to_string();
    db.write(|conn| {
        let owner_id = owner_id.clone();
        let port = port.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT execution_id, recipe_node_id
                     FROM (
                         SELECT execution_id, recipe_node_id, 0 AS priority
                         FROM action_runs
                         WHERE parent_job_id = ?1
                         UNION ALL
                         SELECT execution_id, recipe_node_id, 1 AS priority
                         FROM action_runs
                         WHERE id = ?1
                     )
                     ORDER BY priority
                     LIMIT 1",
                    params![owner_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(());
            };
            match (row.opt_text(0)?, row.opt_text(1)?) {
                (Some(execution_id), Some(recipe_node_id)) => {
                    fire_pr_node_port_conn(conn, &execution_id, &recipe_node_id, &port).await
                }
                _ => Ok(()),
            }
        })
    })
    .await
    .map_err(|error| format!("Failed to fire PR node port: {error}"))
}

pub async fn pr_node_port_fired_conn(
    conn: &turso::Connection,
    execution_id: &str,
    recipe_node_id: &str,
    port: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1
             FROM pr_node_port_fires
             WHERE execution_id = ?1 AND recipe_node_id = ?2 AND port = ?3
             LIMIT 1",
            params![execution_id, recipe_node_id, port],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}
