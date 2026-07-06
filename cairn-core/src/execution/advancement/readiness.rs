use super::*;

async fn condition_edge_satisfied_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    edge: &DbRecipeEdge,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT result_port
             FROM condition_evaluations
             WHERE execution_id = ?1 AND recipe_node_id = ?2
             LIMIT 1",
            params![execution_id, edge.source_node_id.as_str()],
        )
        .await?;
    let result_port = crate::storage::next_text(&mut rows, 0).await?;
    Ok(result_port
        .as_deref()
        .map(|port| edge.source_handle == port)
        .unwrap_or(false))
}

pub(super) async fn is_action_node_ready_with_snapshot_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    node_id: &str,
    all_edges: &[DbRecipeEdge],
    node_map: &HashMap<&str, &DbRecipeNode>,
) -> DbResult<bool> {
    let incoming_edges: Vec<&DbRecipeEdge> = all_edges
        .iter()
        .filter(|edge| edge.target_node_id == node_id && edge.edge_type == "control")
        .collect();

    if incoming_edges.is_empty() {
        let trigger_edge_count = all_edges
            .iter()
            .filter(|edge| edge.target_node_id == node_id)
            .filter(|edge| {
                node_map
                    .get(edge.source_node_id.as_str())
                    .map(|node| node.node_type == "trigger")
                    .unwrap_or(false)
            })
            .count();
        return Ok(trigger_edge_count > 0);
    }

    for edge in incoming_edges {
        if edge.target_handle == "continue" {
            continue;
        }

        let source_node_type = node_map
            .get(edge.source_node_id.as_str())
            .map(|node| node.node_type.as_str());

        match source_node_type {
            Some("trigger") => continue,
            Some("action") => {
                let mut rows = conn
                    .query(
                        "SELECT 1
                         FROM action_runs
                         WHERE execution_id = ?1
                           AND recipe_node_id = ?2
                           AND status = 'complete'
                         LIMIT 1",
                        params![execution_id, edge.source_node_id.as_str()],
                    )
                    .await?;
                if rows.next().await?.is_none() {
                    return Ok(false);
                }
            }
            Some("condition") => {
                if !condition_edge_satisfied_conn(conn, execution_id, edge).await? {
                    return Ok(false);
                }
            }
            Some("pr") => {
                if !crate::pr_data::ports::pr_node_port_fired_conn(
                    conn,
                    execution_id,
                    &edge.source_node_id,
                    &edge.source_handle,
                )
                .await?
                {
                    return Ok(false);
                }
            }
            _ => {
                let source_job = crate::db_records::load_live_job_by_execution_node_conn(
                    conn,
                    execution_id,
                    &edge.source_node_id,
                )
                .await?;
                match source_job {
                    Some(job) if job.status == "complete" => continue,
                    _ => return Ok(false),
                }
            }
        }
    }

    Ok(true)
}

pub(crate) async fn is_job_ready_conn(
    conn: &cairn_db::turso::Connection,
    job: &DbJob,
) -> DbResult<bool> {
    let Some(node_id) = job.recipe_node_id.as_deref() else {
        return Ok(true);
    };
    let Some(execution_id) = job.execution_id.as_deref() else {
        return Ok(true);
    };

    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    let all_nodes = snapshot_nodes_to_db(&snapshot);
    let all_edges = snapshot_edges_to_db(&snapshot);
    let node_map: HashMap<&str, &DbRecipeNode> = all_nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();

    if !is_action_node_ready_with_snapshot_conn(conn, execution_id, node_id, &all_edges, &node_map)
        .await?
    {
        return Ok(false);
    }

    if let Some(issue_id) = job.issue_id.as_deref() {
        if !crate::issues::relations::dependencies_ready(conn, issue_id).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Whether an issue's executions have gone fully quiescent for review-wake
/// purposes. This is the structural "nothing is imminently happening" predicate
/// the review evaluator gates on (CAIRN-2483): a review is pushed to watchers
/// only once the reviewed child issue has actually settled, never mid-work.
///
/// Settled iff, across every job of the issue's executions:
/// - no job has a live latest turn (Running/Pending/Yielded, *including* a
///   trailing memory-review turn and a self-suspended own-work wait — a yielded
///   turn reads live), and
/// - no job is Pending **and** DAG-ready — a claimable job is imminent work; this
///   is what covers the builder-end → review-start gap once the pr node fires its
///   open port and the review job becomes ready. A Pending job whose control-deps
///   are unmet counts as settled (it may never run), and
/// - no `action_run` is in a transient state (`pending`/`running`) — this covers
///   the pr action itself over its seed → GitHub-open → open-port window.
///
/// Idle and Blocked jobs, and Blocked (open-PR gate) / terminal action_runs, all
/// count as settled: those are exactly the rest states at which watchers must be
/// woken. Terminal (complete/failed/cancelled) is settled.
pub(crate) async fn issue_settled(db: &LocalDb, issue_id: &str) -> Result<bool, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move { issue_settled_conn(conn, &issue_id).await })
    })
    .await
    .map_err(|e| e.to_string())
}

pub(crate) async fn issue_settled_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<bool> {
    // A transient action_run (the pr action opening the PR, or any other action
    // node mid-run) is imminent work. `blocked` (open-PR human gate), `complete`,
    // and `failed` are settled.
    let mut rows = conn
        .query(
            "SELECT 1 FROM action_runs ar
             JOIN executions e ON e.id = ar.execution_id
             WHERE e.issue_id = ?1 AND ar.status IN ('pending', 'running')
             LIMIT 1",
            (issue_id,),
        )
        .await?;
    if rows.next().await?.is_some() {
        return Ok(false);
    }

    // A ready-but-not-yet-dispatched action/pr node is imminent work too. This
    // closes the window at builder completion, before the pr node's action_run is
    // seeded, where the transient-action_run check above cannot yet see it: the pr
    // node is DAG-ready (builder complete) but has no action_run row. Snapshot
    // load errors (e.g. a snapshot-less fixture execution) are skipped rather than
    // propagated — a real execution always has a snapshot.
    let exec_ids = {
        let mut rows = conn
            .query("SELECT id FROM executions WHERE issue_id = ?1", (issue_id,))
            .await?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            ids.push(row.text(0)?);
        }
        ids
    };
    for exec_id in &exec_ids {
        if let Ok(ready) =
            find_ready_nodes_conn(conn, exec_id, &["action", "pr"], "action_runs").await
        {
            if !ready.is_empty() {
                return Ok(false);
            }
        }
    }

    let jobs = super::persistence::load_jobs_for_issue_conn(conn, issue_id).await?;
    for job in &jobs {
        if super::recompute::latest_turn_is_live(conn, &job.id).await? {
            return Ok(false);
        }
        if job.status == "pending" && is_job_ready_conn(conn, job).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn node_map(nodes: &[DbRecipeNode]) -> HashMap<&str, &DbRecipeNode> {
    nodes.iter().map(|node| (node.id.as_str(), node)).collect()
}

async fn find_ready_nodes_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    node_types: &[&str],
    completed_table: &str,
) -> DbResult<Vec<DbRecipeNode>> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    let all_nodes = snapshot_nodes_to_db(&snapshot);
    let all_edges = snapshot_edges_to_db(&snapshot);
    let node_map = node_map(&all_nodes);

    let completed_sql = match completed_table {
        "action_runs" => "SELECT recipe_node_id FROM action_runs WHERE execution_id = ?1",
        "condition_evaluations" => {
            "SELECT recipe_node_id FROM condition_evaluations WHERE execution_id = ?1"
        }
        other => {
            return Err(db_internal(format!(
                "Unsupported completion table: {other}"
            )));
        }
    };
    let mut completed_rows = conn.query(completed_sql, (execution_id,)).await?;
    let mut completed_node_ids = HashSet::new();
    while let Some(row) = completed_rows.next().await? {
        completed_node_ids.insert(row.text(0)?);
    }

    let mut ready_nodes = Vec::new();
    for node in all_nodes
        .iter()
        .filter(|node| node_types.contains(&node.node_type.as_str()))
    {
        if completed_node_ids.contains(&node.id) {
            continue;
        }
        if is_action_node_ready_with_snapshot_conn(
            conn,
            execution_id,
            &node.id,
            &all_edges,
            &node_map,
        )
        .await?
        {
            ready_nodes.push(node.clone());
        }
    }

    Ok(ready_nodes)
}

pub fn find_ready_action_nodes(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Vec<DbRecipeNode>, String> {
    let execution_id = execution_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                // `pr` nodes dispatch through the same action path as `action`
                // nodes (they create an action_run, not a job) — see CAIRN-1220.
                find_ready_nodes_conn(conn, &execution_id, &["action", "pr"], "action_runs").await
            })
        })
        .await
        .map_err(|e| format!("Failed to find ready action nodes: {e}"))
    })
}

pub fn find_ready_condition_nodes(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<Vec<DbRecipeNode>, String> {
    let execution_id = execution_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                find_ready_nodes_conn(conn, &execution_id, &["condition"], "condition_evaluations")
                    .await
            })
        })
        .await
        .map_err(|e| format!("Failed to find ready condition nodes: {e}"))
    })
}

/// Check if an action/condition node's dependencies are satisfied.
pub fn is_action_node_ready(
    db: Arc<LocalDb>,
    execution_id: &str,
    node_id: &str,
    _recipe_id: &str,
) -> Result<bool, String> {
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let node_id = node_id.clone();
            Box::pin(async move {
                let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
                let all_nodes = snapshot_nodes_to_db(&snapshot);
                let all_edges = snapshot_edges_to_db(&snapshot);
                let node_map = node_map(&all_nodes);
                is_action_node_ready_with_snapshot_conn(
                    conn,
                    &execution_id,
                    &node_id,
                    &all_edges,
                    &node_map,
                )
                .await
            })
        })
        .await
        .map_err(|e| format!("Failed to check action node readiness: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType,
        RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use std::collections::HashMap;
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("readiness.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn node(id: &str, node_type: RecipeNodeType) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type,
            name: id.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        }
    }

    fn snapshot(issue_id: &str) -> ExecutionSnapshot {
        ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![
                    node("trigger", RecipeNodeType::Trigger),
                    node("agent", RecipeNodeType::Agent),
                ],
                edges: vec![RecipeEdge {
                    id: "edge-1".to_string(),
                    edge_type: RecipeEdgeType::Control,
                    source_node_id: "trigger".to_string(),
                    source_handle: "continue".to_string(),
                    target_node_id: "agent".to_string(),
                    target_handle: "continue".to_string(),
                }],
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: Some(issue_id.to_string()),
                project_id: "p-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
    }

    fn job(issue_id: Option<&str>) -> DbJob {
        DbJob {
            id: "j-1".to_string(),
            execution_id: Some("e-1".to_string()),
            recipe_node_id: Some("agent".to_string()),
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            pack_anchor: None,
            current_session_id: None,
            resume_session_id: None,
            status: "pending".to_string(),
            agent_config_id: None,
            issue_id: issue_id.map(str::to_string),
            project_id: "p-1".to_string(),
            task_description: None,
            created_at: 1,
            updated_at: 1,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: None,
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
            uri_segment: None,
        }
    }

    async fn seed(conn: &cairn_db::turso::Connection, dep_status: &str) {
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)",
            (),
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','CAIRN','/tmp/p',1,1)", ()).await.unwrap();
        conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES ('dep','p-1',1,'Dep',?1,?1,'none',1,1)", params![dep_status]).await.unwrap();
        conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES ('child','p-1',2,'Child','active','active','none',1,1)", ()).await.unwrap();
        crate::issues::relations::replace_dependencies(
            conn,
            "child",
            &["cairn://p/CAIRN/1".to_string()],
            1,
        )
        .await
        .unwrap();
        let snapshot_json = snapshot("child").to_json().unwrap();
        conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, snapshot) VALUES ('e-1','recipe-1','child','p-1','running',1,?1)", params![snapshot_json]).await.unwrap();
    }

    #[tokio::test]
    async fn issue_dependencies_gate_issue_scoped_jobs() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed(conn, "active").await;
                assert!(!is_job_ready_conn(conn, &job(Some("child"))).await.unwrap());
                conn.execute("UPDATE issues SET status = 'merged' WHERE id = 'dep'", ())
                    .await
                    .unwrap();
                assert!(is_job_ready_conn(conn, &job(Some("child"))).await.unwrap());
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn issue_settled_reflects_job_and_action_states() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                // deps merged -> the child's DAG-ready gate is satisfiable.
                seed(conn, "merged").await;
                // A pending, DAG-ready job is imminent work -> not settled.
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, status, created_at, updated_at)
                     VALUES('j-1','e-1','agent','child','p-1','pending',1,1)",
                    (),
                )
                .await
                .unwrap();
                assert!(
                    !issue_settled_conn(conn, "child").await.unwrap(),
                    "a pending DAG-ready job is imminent work"
                );

                // Terminal job, no action work -> settled.
                conn.execute("UPDATE jobs SET status='complete' WHERE id='j-1'", ())
                    .await
                    .unwrap();
                assert!(issue_settled_conn(conn, "child").await.unwrap());

                // A transient (running) action_run holds the issue unsettled.
                conn.execute(
                    "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at)
                     VALUES('ar','e-1','pr','builtin:pr','child','p-1','running',1)",
                    (),
                )
                .await
                .unwrap();
                assert!(
                    !issue_settled_conn(conn, "child").await.unwrap(),
                    "a transient action_run is imminent work"
                );

                // Once it blocks (open-PR human gate) the issue settles.
                conn.execute("UPDATE action_runs SET status='blocked' WHERE id='ar'", ())
                    .await
                    .unwrap();
                assert!(issue_settled_conn(conn, "child").await.unwrap());
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn jobs_without_issues_skip_issue_dependency_gate() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed(conn, "active").await;
                assert!(is_job_ready_conn(conn, &job(None)).await.unwrap());
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
