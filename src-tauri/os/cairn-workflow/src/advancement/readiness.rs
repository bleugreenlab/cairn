use super::*;

async fn condition_edge_satisfied_conn(
    conn: &cairn_db::storage::TrackedConnection,
    execution_id: &str,
    edge: &DbRecipeEdge,
) -> DbResult<bool> {
    // Only a routed row names a port, and after a re-evaluation the latest
    // routed row decides the route.
    let mut rows = conn
        .query(
            "SELECT result_port
             FROM condition_evaluations
             WHERE execution_id = ?1 AND recipe_node_id = ?2 AND outcome = 'routed'
             ORDER BY evaluated_at DESC, rowid DESC
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

pub async fn is_action_node_ready_with_snapshot_conn(
    conn: &cairn_db::storage::TrackedConnection,
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
            Some("condition") | Some("decision") => {
                if !condition_edge_satisfied_conn(conn, execution_id, edge).await? {
                    return Ok(false);
                }
            }
            Some("pr") => {
                if !crate::ports::pr_node_port_fired_conn(
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

pub(super) struct DependencyFailureAnalysis {
    pub impossible_nodes: HashSet<String>,
    pub has_unprojected_failure: bool,
}

/// Derive dependency failure without treating inherited status projections as
/// independent failures. The caller owns snapshot validation and the transaction.
pub(super) async fn dependency_impossible_nodes_conn(
    conn: &cairn_db::storage::TrackedConnection,
    execution_id: &str,
    snapshot: &ExecutionSnapshot,
    claimed_job_ids: &HashSet<String>,
) -> DbResult<HashSet<String>> {
    Ok(
        dependency_failure_analysis_conn(conn, execution_id, snapshot, claimed_job_ids)
            .await?
            .impossible_nodes,
    )
}

/// A failure fact can commit before its job projection. That unfinished live
/// transition must never be mistaken for historical work by a repair pass.
pub(super) async fn dependency_failure_analysis_conn(
    conn: &cairn_db::storage::TrackedConnection,
    execution_id: &str,
    snapshot: &ExecutionSnapshot,
    claimed_job_ids: &HashSet<String>,
) -> DbResult<DependencyFailureAnalysis> {
    let nodes = snapshot_nodes_to_db(snapshot);
    let edges = snapshot_edges_to_db(snapshot);
    let node_map = node_map(&nodes);
    let mut protected = HashSet::new();
    let mut failed = HashSet::new();
    let mut unprojected_failures = HashSet::new();

    // Match readiness's latest non-cancelled attempt, never an arbitrary
    // historical failed job. Claimed launches protect even the pre-turn gap.
    for node in &nodes {
        let Some(job) =
            crate::db_records::load_live_job_by_execution_node_conn(conn, execution_id, &node.id)
                .await?
        else {
            continue;
        };
        let node_id = node.id.clone();
        let job_id = job.id;
        if claimed_job_ids.contains(&job_id) {
            protected.insert(node_id.clone());
        }
        let turn = super::recompute::turn_facts(conn, &job_id).await?;
        let mut runs = conn
            .query(
                "SELECT status, exit_reason FROM runs WHERE job_id = ?1
             ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (job_id.as_str(),),
            )
            .await?;
        let latest_run = runs.next().await?;
        let run_status = latest_run
            .as_ref()
            .map(|row| row.opt_text(0))
            .transpose()?
            .flatten();
        let run_stopped = latest_run
            .as_ref()
            .map(|row| row.opt_text(1))
            .transpose()?
            .flatten()
            .as_deref()
            == Some("user_stop");
        let mut stopped = conn
            .query(
                "SELECT COALESCE(t.end_reason, '') = 'user_stop'
                    OR COALESCE(r.exit_reason, '') = 'user_stop'
             FROM turns t LEFT JOIN runs r ON r.id = t.run_id
             WHERE t.job_id = ?1 AND t.start_reason != 'memory_review'
             ORDER BY t.created_at DESC, t.sequence DESC LIMIT 1",
                (job_id.as_str(),),
            )
            .await?;
        let work_stopped = stopped
            .next()
            .await?
            .map(|row| row.i64(0))
            .transpose()?
            .unwrap_or(0)
            != 0;
        let mut active = conn
            .query(
                "SELECT 1 FROM runs WHERE job_id = ?1 AND status IN ('starting', 'live') LIMIT 1",
                (job_id.as_str(),),
            )
            .await?;
        // A clean work turn can still fail its declared output contract.
        // Reuse projection's resolver rather than treating every Complete
        // turn as success or duplicating the artifact/abandonment rules.
        let completed_work_failed = if turn.work_state == Some(crate::models::TurnState::Complete) {
            super::recompute::node_failed_on_own_facts_conn(
                conn,
                &job_id,
                execution_id,
                snapshot,
                &node_id,
                &edges,
                &node_map,
            )
            .await?
        } else {
            false
        };
        let own_failure = !work_stopped
            && (completed_work_failed
                || turn.work_state == Some(crate::models::TurnState::Failed)
                || (turn.work_state.is_none()
                    && !run_stopped
                    && matches!(run_status.as_deref(), Some("failed" | "crashed"))));
        if work_stopped
            || matches!(
                turn.work_state,
                Some(crate::models::TurnState::Interrupted | crate::models::TurnState::Cancelled)
            )
            || turn.work_live(active.next().await?.is_some())
            || (!own_failure
                && (job.status == "complete"
                    || turn.work_state == Some(crate::models::TurnState::Complete)))
        {
            protected.insert(node_id);
        } else if own_failure {
            // Interrupted/cancelled turns and ordinary process exits are not
            // evidence of failure. Inherited failed jobs have neither fact.
            failed.insert(node_id.clone());
            if job.status != "failed" {
                unprojected_failures.insert(node_id);
            }
        }
    }

    let mut rows = conn
        .query(
            "SELECT recipe_node_id, status FROM action_runs
         WHERE execution_id = ?1 ORDER BY created_at DESC, rowid DESC",
            (execution_id,),
        )
        .await?;
    let mut latest_actions = HashSet::new();
    while let Some(row) = rows.next().await? {
        let node_id = row.text(0)?;
        let status = row.text(1)?;
        // Readiness accepts any completed action. A blocked PR can still fire
        // merge/close later, so upstream failure must not erase its open gate.
        let latest = latest_actions.insert(node_id.clone());
        if status == "complete"
            || (latest && matches!(status.as_str(), "pending" | "running" | "blocked"))
        {
            protected.insert(node_id.clone());
        }
        if latest && status == "failed" {
            // mark_action_run_failed commits this fact and its sweep together;
            // unlike job/turn facts, it has no unprojected live failure window.
            failed.insert(node_id);
        }
    }
    let mut rows = conn
        .query(
            "SELECT recipe_node_id FROM condition_evaluations
         WHERE execution_id = ?1 AND outcome = 'routed'",
            (execution_id,),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        protected.insert(row.text(0)?);
    }
    for node in &nodes {
        if node.node_type == "trigger" {
            protected.insert(node.id.clone());
        }
    }

    // Evaluate each required edge once with the same contract as launch
    // readiness. A fired port is a boundary, even on an otherwise failed node.
    let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
    for edge in edges
        .iter()
        .filter(|edge| edge.edge_type == "control" && edge.target_handle != "continue")
    {
        if protected.contains(&edge.target_node_id) {
            continue;
        }
        // Projection may still say Complete immediately after a continuation
        // fails. Its fresh failure fact outranks that stale job cache.
        let failed_job_source = failed.contains(&edge.source_node_id)
            && !protected.contains(&edge.source_node_id)
            && !matches!(
                node_map
                    .get(edge.source_node_id.as_str())
                    .map(|n| n.node_type.as_str()),
                Some("action" | "pr" | "condition" | "decision" | "trigger")
            );
        if failed_job_source
            || !is_action_node_ready_with_snapshot_conn(
                conn,
                execution_id,
                &edge.target_node_id,
                std::slice::from_ref(edge),
                &node_map,
            )
            .await?
        {
            outgoing
                .entry(edge.source_node_id.clone())
                .or_default()
                .push(edge.target_node_id.clone());
        }
    }
    failed.retain(|id| !protected.contains(id) && node_map.contains_key(id.as_str()));
    let mut queue: VecDeque<String> = failed.iter().cloned().collect();
    let mut visited = failed.clone();
    let mut impossible = HashSet::new();
    while let Some(source) = queue.pop_front() {
        if let Some(targets) = outgoing.get(&source) {
            for target in targets {
                if !node_map.contains_key(target.as_str()) {
                    continue;
                }
                if !failed.contains(target) {
                    impossible.insert(target.clone());
                }
                if visited.insert(target.clone()) {
                    queue.push_back(target.clone());
                }
            }
        }
    }
    Ok(DependencyFailureAnalysis {
        impossible_nodes: impossible,
        has_unprojected_failure: failed.iter().any(|id| unprojected_failures.contains(id)),
    })
}

/// Whether `job` is claimable: DAG-ready ([`is_job_dag_ready_conn`]) and not
/// held for want of supply. A held job waits for its hold to release it, and
/// claiming it would only launch it into the refusal that held it.
pub async fn is_job_ready_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job: &DbJob,
) -> DbResult<bool> {
    if job_supply_held_conn(conn, &job.id).await? {
        return Ok(false);
    }
    is_job_dag_ready_conn(conn, job).await
}

/// Whether `job_id` waits on an active supply hold.
pub async fn job_supply_held_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            concat!(
                "SELECT 1 FROM jobs j WHERE j.id = ?1 AND ",
                cairn_db::job_supply_held_sql!()
            ),
            (job_id,),
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Whether `job`'s DAG would let it run now, held or not.
pub async fn is_job_dag_ready_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job: &DbJob,
) -> DbResult<bool> {
    // A merged or closed issue takes no more work, so nothing on it is ever
    // claimable — however its DAG reads. Without this a `pending` row left on a
    // resolved issue is launched by the next advance of its execution.
    if let Some(issue_id) = job.issue_id.as_deref() {
        if super::recompute::issue_is_terminal_conn(conn, issue_id).await? {
            return Ok(false);
        }
    }
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
///   open port and the review job becomes ready. A job held for want of supply is
///   imminent too: its hold releases it on its own. A Pending job whose
///   control-deps are unmet counts as settled (it may never run), and
/// - no `action_run` is in a transient state (`pending`/`running`) — this covers
///   the pr action itself over its seed → GitHub-open → open-port window.
///
/// Idle and Blocked jobs, and Blocked (open-PR gate) / terminal action_runs, all
/// count as settled: those are exactly the rest states at which watchers must be
/// woken. Terminal (complete/failed/cancelled) is settled.
pub async fn issue_settled(db: &LocalDb, issue_id: &str) -> Result<bool, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move { issue_settled_conn(conn, &issue_id).await })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn issue_settled_conn(
    conn: &cairn_db::storage::TrackedConnection,
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
        if super::recompute::turn_facts(conn, &job.id).await?.live() {
            return Ok(false);
        }
        if job.status == "pending" && is_job_dag_ready_conn(conn, job).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn node_map(nodes: &[DbRecipeNode]) -> HashMap<&str, &DbRecipeNode> {
    nodes.iter().map(|node| (node.id.as_str(), node)).collect()
}

async fn find_ready_nodes_conn(
    conn: &cairn_db::storage::TrackedConnection,
    execution_id: &str,
    node_types: &[&str],
    completed_table: &str,
) -> DbResult<Vec<DbRecipeNode>> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    let all_nodes = snapshot_nodes_to_db(&snapshot);
    let all_edges = snapshot_edges_to_db(&snapshot);
    let node_map = node_map(&all_nodes);

    let completed_sql = match completed_table {
        // Existing rows retain their URI; the PR re-arm pass dispatches retries.
        "action_runs" => "SELECT recipe_node_id FROM action_runs WHERE execution_id = ?1",
        // Any row parks a condition, blocked ones included: advancing must not
        // re-spend a model call on every pass. Re-evaluation is explicit.
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
    let mut dispatched_node_ids = HashSet::new();
    while let Some(row) = completed_rows.next().await? {
        dispatched_node_ids.insert(row.text(0)?);
    }

    let mut ready_nodes = Vec::new();
    for node in all_nodes
        .iter()
        .filter(|node| node_types.contains(&node.node_type.as_str()))
    {
        if dispatched_node_ids.contains(&node.id) {
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
                find_ready_nodes_conn(
                    conn,
                    &execution_id,
                    &["condition", "decision"],
                    "condition_evaluations",
                )
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
    use crate::storage::LocalDb;
    use std::collections::HashMap;
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("readiness.db"))
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
            decision_config: None,
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
            branch: None,
            base_commit: None,
            pack_anchor: None,
            current_session_id: None,
            resume_session_id: None,
            status: "pending".to_string(),
            agent_config_id: None,
            issue_id: issue_id.map(str::to_string),
            thread_id: None,
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

    async fn seed(conn: &cairn_db::storage::TrackedConnection) {
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)",
            (),
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','cairn','/tmp/p',1,1)", ()).await.unwrap();
        conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES ('child','p-1',2,'Child','active','active','none',1,1)", ()).await.unwrap();
        let snapshot_json = snapshot("child").to_json().unwrap();
        conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, snapshot) VALUES ('e-1','recipe-1','child','p-1','running',1,?1)", params![snapshot_json]).await.unwrap();
    }

    fn required_edge(source: &str, target: &str) -> RecipeEdge {
        RecipeEdge {
            id: format!("{source}-{target}"),
            edge_type: RecipeEdgeType::Control,
            source_node_id: source.into(),
            source_handle: "output".into(),
            target_node_id: target.into(),
            target_handle: "input".into(),
        }
    }

    async fn dependency_job(
        conn: &cairn_db::storage::TrackedConnection,
        id: &str,
        node_id: &str,
        status: &str,
        state: Option<&str>,
        created_at: i64,
    ) {
        conn.execute(
            "INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,created_at,updated_at)
             VALUES(?1,'e-1',?2,'child','p-1',?3,?4,?4)",
            params![id, node_id, status, created_at],
        ).await.unwrap();
        if let Some(state) = state {
            conn.execute(
                "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
                 VALUES(?1,?1,?2,1,?3,'initial',?4,?4)",
                params![format!("turn-{id}"), id, state, created_at],
            ).await.unwrap();
        }
    }

    async fn dependency_result(
        conn: &cairn_db::storage::TrackedConnection,
        snapshot: &ExecutionSnapshot,
        claimed: &[&str],
    ) -> HashSet<String> {
        dependency_impossible_nodes_conn(
            conn,
            "e-1",
            snapshot,
            &claimed.iter().map(|id| id.to_string()).collect(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn dependency_impossibility_crosses_unstarted_pr_action_condition_and_decision() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed(conn).await;
                let mut s = snapshot("child");
                s.recipe.nodes = vec![
                    node("planner", RecipeNodeType::Agent),
                    node("builder", RecipeNodeType::Agent),
                    node("pr", RecipeNodeType::Pr),
                    node("condition", RecipeNodeType::Condition),
                    node("decision", RecipeNodeType::Decision),
                    node("action", RecipeNodeType::Action),
                    node("review", RecipeNodeType::Agent),
                    node("parallel", RecipeNodeType::Agent),
                ];
                let path = [
                    "planner",
                    "builder",
                    "pr",
                    "condition",
                    "decision",
                    "action",
                    "review",
                ];
                s.recipe.edges = path
                    .windows(2)
                    .map(|pair| required_edge(pair[0], pair[1]))
                    .collect();
                dependency_job(
                    conn,
                    "planner-job",
                    "planner",
                    "complete",
                    Some("complete"),
                    1,
                )
                .await;
                dependency_job(conn, "builder-job", "builder", "failed", Some("failed"), 1).await;
                dependency_job(conn, "review-job", "review", "pending", None, 1).await;
                dependency_job(
                    conn,
                    "parallel-job",
                    "parallel",
                    "running",
                    Some("running"),
                    1,
                )
                .await;
                let expected: HashSet<String> = ["pr", "condition", "decision", "action", "review"]
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                assert_eq!(dependency_result(conn, &s, &[]).await, expected);
                // Persisted inherited failures are not independent roots. Stopping
                // the real failing source must make the entire chain resumable.
                conn.execute("UPDATE jobs SET status='failed' WHERE id='review-job'", ())
                    .await?;
                conn.execute(
                    "UPDATE turns SET state='interrupted' WHERE job_id='builder-job'",
                    (),
                )
                .await?;
                assert!(dependency_result(conn, &s, &[]).await.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn dependency_impossibility_preserves_current_attempts_live_claimed_and_outcomes() {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            seed(conn).await;
            let mut s = snapshot("child");
            s.recipe.nodes = vec![node("root", RecipeNodeType::Agent)];
            s.recipe.edges.clear();
            dependency_job(conn, "root-job", "root", "failed", Some("failed"), 1).await;
            let cases = [
                ("pending", "pending", None, true),
                ("inherited", "failed", None, true),
                ("live", "running", Some("running"), false),
                ("yielded", "running", Some("yielded"), false),
                ("stopped-target", "pending", Some("interrupted"), false),
                ("cancelled-target", "pending", Some("cancelled"), false),
                ("complete", "complete", Some("complete"), false),
                ("output-gate", "blocked", Some("complete"), false),
                ("claimed", "pending", None, false),
                ("starting", "pending", None, false),
                ("retry", "failed", Some("failed"), false),
            ];
            for (id, status, state, _) in cases {
                s.recipe.nodes.push(node(id, RecipeNodeType::Agent));
                s.recipe.nodes.push(node(&format!("{id}-child"), RecipeNodeType::Agent));
                s.recipe.edges.push(required_edge("root", id));
                s.recipe.edges.push(required_edge(id, &format!("{id}-child")));
                dependency_job(conn, id, id, status, state, 1).await;
            }
            dependency_job(conn, "retry-success", "retry", "complete", Some("complete"), 2).await;
            conn.execute("INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('start-run','starting','starting',2,2)", ()).await?;
            dependency_job(conn, "archived-root", "root", "pending", None, 0).await;
            let result = dependency_result(conn, &s, &["claimed", "archived-root"]).await;
            for (id, _, _, impossible) in cases {
                assert_eq!(result.contains(id), impossible, "{id}");
                assert_eq!(result.contains(&format!("{id}-child")), impossible, "{id} child");
            }
            // Retrying the failure root successfully removes its old failure,
            // even though its earlier attempt and failed turn remain persisted.
            dependency_job(conn, "root-retry", "root", "complete", Some("complete"), 2).await;
            assert!(dependency_result(conn, &s, &[]).await.is_empty());
            Ok(())
        })).await.unwrap();
    }

    #[tokio::test]
    async fn dependency_impossibility_requires_positive_failure_not_stop_or_missing_work() {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            seed(conn).await;
            let mut s = snapshot("child");
            s.recipe.nodes.clear();
            s.recipe.edges.clear();
            let cases = [
                ("failed-turn", "failed", Some("failed"), None, true),
                ("stale-complete", "complete", Some("failed"), None, true),
                ("failed-run", "failed", None, Some("failed"), true),
                ("crashed-run", "failed", None, Some("crashed"), true),
                ("stopped", "failed", Some("interrupted"), Some("crashed"), false),
                ("cancelled-turn", "pending", Some("cancelled"), Some("failed"), false),
                ("cancelled-job", "cancelled", Some("failed"), None, false),
                ("inherited", "failed", None, None, false),
                ("parked", "pending", None, None, false),
                ("blocked", "blocked", Some("complete"), None, false),
                ("ordinary-exit", "pending", None, Some("exited"), false),
            ];
            for (id, status, state, run, _) in cases {
                s.recipe.nodes.push(node(id, RecipeNodeType::Agent));
                let child = format!("{id}-child");
                s.recipe.nodes.push(node(&child, RecipeNodeType::Agent));
                s.recipe.edges.push(required_edge(id, &child));
                dependency_job(conn, id, id, status, state, 1).await;
                if let Some(run) = run {
                    conn.execute("INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES(?1,?2,?3,1,1)",
                        params![format!("run-{id}"), id, run]).await?;
                }
            }
            let result = dependency_result(conn, &s, &[]).await;
            for (id, _, _, _, impossible) in cases {
                assert_eq!(result.contains(&format!("{id}-child")), impossible, "{id}");
            }
            conn.execute("UPDATE runs SET exit_reason='user_stop' WHERE job_id='failed-run'", ()).await?;
            conn.execute("UPDATE turns SET end_reason='user_stop' WHERE job_id='failed-turn'", ()).await?;
            let stopped = dependency_result(conn, &s, &[]).await;
            assert!(!stopped.contains("failed-run-child"));
            assert!(!stopped.contains("failed-turn-child"));
            Ok(())
        })).await.unwrap();
    }

    #[tokio::test]
    async fn dependency_impossibility_respects_readiness_edges_ports_and_action_attempts() {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            seed(conn).await;
            let mut s = snapshot("child");
            s.recipe.nodes = vec![node("root", RecipeNodeType::Agent)];
            s.recipe.edges.clear();
            dependency_job(conn, "root", "root", "failed", Some("failed"), 1).await;
            for (id, kind) in [
                ("pr", RecipeNodeType::Pr), ("action", RecipeNodeType::Action),
                ("condition", RecipeNodeType::Condition), ("decision", RecipeNodeType::Decision),
                ("trigger", RecipeNodeType::Trigger), ("open-pr", RecipeNodeType::Pr),
                ("failed-action", RecipeNodeType::Action),
            ] {
                s.recipe.nodes.push(node(id, kind));
                s.recipe.edges.push(required_edge("root", id));
                let child = format!("{id}-child");
                s.recipe.nodes.push(node(&child, RecipeNodeType::Agent));
                let mut edge = required_edge(id, &child);
                edge.source_handle = "open".into();
                s.recipe.edges.push(edge);
            }
            for id in ["continue", "data"] {
                s.recipe.nodes.push(node(id, RecipeNodeType::Agent));
                let mut edge = required_edge("root", id);
                if id == "continue" { edge.target_handle = "continue".into(); }
                else { edge.edge_type = RecipeEdgeType::Context; }
                s.recipe.edges.push(edge);
            }
            conn.execute("INSERT INTO pr_node_port_fires(id,execution_id,recipe_node_id,port,fired_at) VALUES('fire','e-1','pr','open',1)", ()).await?;
            for (id, status, time) in [("action", "complete", 1), ("open-pr", "blocked", 1), ("failed-action", "running", 1), ("failed-action", "failed", 2)] {
                conn.execute("INSERT INTO action_runs(id,execution_id,recipe_node_id,action_config_id,project_id,status,created_at) VALUES(?1,'e-1',?2,'builtin:pr','p-1',?3,?4)",
                    params![format!("{id}-{time}"), id, status, time]).await?;
            }
            for id in ["condition", "decision"] {
                conn.execute("INSERT INTO condition_evaluations(id,execution_id,recipe_node_id,result_port,outcome,evaluated_at) VALUES(?1,'e-1',?1,'open','routed',1)", params![id]).await?;
                // The latest routed row is authoritative, but a later blocked
                // evaluation must not erase the already-fired route.
                conn.execute("INSERT INTO condition_evaluations(id,execution_id,recipe_node_id,result_port,outcome,evaluated_at) VALUES(?1,'e-1',?2,'other','routed',2)",
                    params![format!("{id}-new"), id]).await?;
                conn.execute("INSERT INTO condition_evaluations(id,execution_id,recipe_node_id,result_port,outcome,evaluated_at) VALUES(?1,'e-1',?2,'','blocked',3)",
                    params![format!("{id}-blocked"), id]).await?;
            }
            let result = dependency_result(conn, &s, &[]).await;
            assert_eq!(result, ["pr", "failed-action-child"].into_iter().map(str::to_string).collect());
            // A different, unsatisfied PR port remains downstream of failure.
            s.recipe.edges.iter_mut().find(|e| e.source_node_id == "pr").unwrap().source_handle = "merge".into();
            assert!(dependency_result(conn, &s, &[]).await.contains("pr-child"));
            Ok(())
        })).await.unwrap();
    }

    #[tokio::test]
    async fn dependency_impossibility_distinguishes_abandoned_output_from_blocked_and_late_output()
    {
        let db = test_db().await;
        db.write(|conn| Box::pin(async move {
            seed(conn).await;
            let mut s = snapshot("child");
            let mut output = node("output", RecipeNodeType::Artifact);
            output.artifact_config = Some(serde_json::from_value(serde_json::json!({
                "name": "result", "schema": {"type": "object"}
            })).unwrap());
            s.recipe.nodes = vec![
                node("builder", RecipeNodeType::Agent),
                node("pr", RecipeNodeType::Pr),
                node("review", RecipeNodeType::Agent),
                output,
            ];
            let mut context = required_edge("builder", "output");
            context.edge_type = RecipeEdgeType::Context;
            context.source_handle = "context-out".into();
            context.target_handle = "context-in".into();
            s.recipe.edges = vec![
                required_edge("builder", "pr"),
                required_edge("pr", "review"),
                context,
            ];
            dependency_job(conn, "builder", "builder", "complete", Some("complete"), 1).await;
            dependency_job(conn, "review", "review", "pending", None, 1).await;
            // Missing required output is a resumable Blocked gate, not failure.
            assert!(dependency_result(conn, &s, &[]).await.is_empty());
            conn.execute("UPDATE jobs SET return_abandoned_at=2 WHERE id='builder'", ()).await?;
            let expected = ["pr", "review"].into_iter().map(str::to_string).collect();
            assert_eq!(dependency_result(conn, &s, &[]).await, expected);
            // An unrelated living document cannot satisfy the terminal contract.
            conn.execute("INSERT INTO artifacts(id,job_id,artifact_type,output_name,data,confirmed,created_at,updated_at)
                VALUES('living','builder','custom','notes','{}',1,3,3)", ()).await?;
            assert!(dependency_result(conn, &s, &[]).await.contains("review"));
            // The real output arriving late completes the work, even while the
            // job cache still holds the earlier abandonment's failed projection.
            conn.execute("UPDATE jobs SET status='failed' WHERE id='builder'", ()).await?;
            conn.execute("INSERT INTO artifacts(id,job_id,artifact_type,output_name,data,confirmed,created_at,updated_at)
                VALUES('result','builder','custom','result','{}',1,4,4)", ()).await?;
            assert!(dependency_result(conn, &s, &[]).await.is_empty());
            Ok(())
        })).await.unwrap();
    }

    #[tokio::test]
    async fn dependency_impossibility_walks_large_shared_dag_without_depth_limit() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed(conn).await;
                let mut s = snapshot("child");
                s.recipe.nodes = vec![node("root", RecipeNodeType::Agent)];
                s.recipe.edges.clear();
                dependency_job(conn, "root", "root", "failed", Some("failed"), 1).await;
                // More than a typical recipe's size and deeper than a recursive SQL
                // cutoff: every intermediate deliberately has no runtime row.
                for index in 0..512 {
                    let id = format!("node-{index}");
                    s.recipe.nodes.push(node(&id, RecipeNodeType::Action));
                    let previous = if index == 0 {
                        "root".into()
                    } else {
                        format!("node-{}", index - 1)
                    };
                    s.recipe.edges.push(required_edge(&previous, &id));
                    if index > 1 {
                        s.recipe.edges.push(required_edge("node-0", &id));
                    }
                }
                assert_eq!(dependency_result(conn, &s, &[]).await.len(), 512);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Readiness is the DAG's alone. An issue-scoped job is not held by any
    /// other issue's state: an execution that must wait is filed held through
    /// its `after:` gates, before it has any job to hold.
    #[tokio::test]
    async fn an_issue_scoped_job_is_ready_on_dag_grounds_alone() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed(conn).await;
                assert!(is_job_ready_conn(conn, &job(Some("child"))).await.unwrap());
                assert!(is_job_ready_conn(conn, &job(None)).await.unwrap());
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
                seed(conn).await;
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
    async fn jobs_on_a_resolved_issue_are_never_ready() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed(conn).await;
                assert!(is_job_ready_conn(conn, &job(Some("child"))).await.unwrap());
                for status in ["merged", "closed"] {
                    conn.execute(
                        "UPDATE issues SET status = ?1 WHERE id = 'child'",
                        params![status],
                    )
                    .await
                    .unwrap();
                    assert!(
                        !is_job_ready_conn(conn, &job(Some("child"))).await.unwrap(),
                        "a pending job on a {status} issue must never be claimed"
                    );
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
