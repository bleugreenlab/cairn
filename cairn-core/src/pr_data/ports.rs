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

/// Fire a `pr` node port for the PR's owner id — the `merge_requests.job_id`
/// value, which is either the producing action_run (a first-class `pr` node, the
/// CAIRN-1220 path) or a producing job (the legacy `create_pr` path). Resolves
/// the owner's execution + recipe node from whichever table it lives in.
///
/// No-op when the owner has no DAG execution + recipe node: firing a port on a
/// non-`pr` producing node (legacy `create_pr`) is harmless, and a detached /
/// manually-attached PR (owner job with no execution) simply has no port to fire.
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
                    "SELECT execution_id, recipe_node_id FROM jobs WHERE id = ?1
                     UNION ALL
                     SELECT execution_id, recipe_node_id FROM action_runs WHERE id = ?1
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
