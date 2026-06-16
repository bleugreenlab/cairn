//! Reconcile an execution's jobs with an edited snapshot's node set.
//!
//! Adding job materialization for newly added nodes already lives in
//! [`super::job_creation`]. This module owns the inverse: when a node is removed
//! from a live execution's snapshot, its job is **archived, not erased** — but
//! only when it is in a state the editor is allowed to discard. Running and
//! complete nodes are immutable; an attempt to remove one is rejected here,
//! server-side, so the rule holds regardless of what the editor UI permits.
//!
//! Archival means flipping the job to [`JobStatus::Cancelled`] (a sticky
//! override the status projection never re-derives) and preserving the entire
//! transcript — runs, events, turns, sessions, artifacts all stay readable from
//! the issue's execution history. The job is excluded from DAG
//! readiness/advancement and counts as terminal for execution status, so an
//! archived node never blocks, triggers, or wedges the execution.
//!
//! The DB half (validate + cancel) is a pure, testable connection-level routine.
//! Filesystem teardown of any worktree a cancelled job exclusively owned is
//! handed back to the caller as a set of [`TeardownTarget`]s and performed
//! through [`crate::execution::teardown::teardown_removed_node_worktrees`],
//! since the worktree removal needs host git/fs services and must run outside
//! the DB transaction. Branches are preserved (worktrees are disposable; commit
//! history is not).

use super::*;
use crate::execution::teardown::TeardownTarget;
use std::collections::BTreeMap;

/// Outcome of reconciling removed nodes: the job rows archived (cancelled) and
/// the worktree paths now owned by nobody (eligible for filesystem teardown).
#[derive(Debug, Default, Clone)]
pub struct RemovedNodesReconcile {
    /// Ids of the jobs flipped to `cancelled` for removed nodes. Their rows and
    /// transcript history are preserved.
    pub cancelled_job_ids: Vec<String>,
    /// Worktree paths exclusively owned by the cancelled jobs. A path still
    /// referenced by a surviving job is never included (inheritance fan-out /
    /// shared worktrees stay intact).
    pub worktree_targets: Vec<TeardownTarget>,
}

impl RemovedNodesReconcile {
    pub fn is_empty(&self) -> bool {
        self.cancelled_job_ids.is_empty()
    }
}

struct ExecutionJobRow {
    id: String,
    recipe_node_id: Option<String>,
    status: String,
    worktree_path: Option<String>,
    branch: Option<String>,
    project_id: String,
}

/// Validate and archive jobs whose recipe nodes were removed from the snapshot.
///
/// Rejects (returns `Err`, before any mutation) when a removed node's job is
/// `running` or `complete`. Pending, failed, and blocked jobs are archivable:
/// each is flipped to `cancelled` with its transcript preserved, and any
/// worktree it exclusively owned is returned for filesystem teardown.
pub(crate) async fn reconcile_removed_nodes_conn(
    conn: &turso::Connection,
    execution_id: &str,
    removed_node_ids: &HashSet<String>,
) -> DbResult<RemovedNodesReconcile> {
    if removed_node_ids.is_empty() {
        return Ok(RemovedNodesReconcile::default());
    }

    let mut rows = conn
        .query(
            "SELECT id, recipe_node_id, status, worktree_path, branch, project_id
             FROM jobs
             WHERE execution_id = ?1",
            (execution_id,),
        )
        .await?;
    let mut all_jobs = Vec::new();
    while let Some(row) = rows.next().await? {
        all_jobs.push(ExecutionJobRow {
            id: row.text(0)?,
            recipe_node_id: row.opt_text(1)?,
            status: row.text(2)?,
            worktree_path: row.opt_text(3)?,
            branch: row.opt_text(4)?,
            project_id: row.text(5)?,
        });
    }

    // Partition into the jobs being removed and the worktree paths still held by
    // surviving jobs (so a shared/inherited path is never torn down).
    let mut removed: Vec<&ExecutionJobRow> = Vec::new();
    let mut surviving_paths: HashSet<String> = HashSet::new();
    for job in &all_jobs {
        let is_removed = job
            .recipe_node_id
            .as_deref()
            .map(|node_id| removed_node_ids.contains(node_id))
            .unwrap_or(false);
        if is_removed {
            removed.push(job);
        } else if let Some(path) = &job.worktree_path {
            surviving_paths.insert(path.clone());
        }
    }

    // Server-side immutability: running and complete nodes can never be dropped,
    // whatever the editor allowed. Reject before mutating anything.
    for job in &removed {
        if matches!(job.status.as_str(), "running" | "complete") {
            return Err(db_internal(format!(
                "Cannot remove node {}: its job is {} (running and complete nodes are immutable)",
                job.recipe_node_id.as_deref().unwrap_or("?"),
                job.status
            )));
        }
    }

    // Group orphaned worktree paths into teardown targets, then archive the jobs.
    let mut path_groups: BTreeMap<String, (Option<String>, String, Vec<String>)> = BTreeMap::new();
    let mut cancelled_job_ids = Vec::with_capacity(removed.len());
    for job in &removed {
        cancelled_job_ids.push(job.id.clone());
        if let Some(path) = &job.worktree_path {
            if !surviving_paths.contains(path) {
                let repo_path = project_repo_path_conn(conn, &job.project_id).await?;
                let entry = path_groups
                    .entry(path.clone())
                    .or_insert_with(|| (job.branch.clone(), repo_path, Vec::new()));
                if entry.0.is_none() {
                    entry.0 = job.branch.clone();
                }
                entry.2.push(job.id.clone());
            }
        }
    }

    let now = chrono::Utc::now().timestamp();
    for job in &removed {
        cancel_job_conn(conn, &job.id, now).await?;
    }

    let worktree_targets = path_groups
        .into_iter()
        .map(
            |(worktree_path, (branch, repo_path, job_ids))| TeardownTarget {
                worktree_path,
                branch,
                repo_path,
                job_ids,
            },
        )
        .collect();

    Ok(RemovedNodesReconcile {
        cancelled_job_ids,
        worktree_targets,
    })
}

/// Blocking wrapper over [`reconcile_removed_nodes_conn`] for host command paths.
pub fn reconcile_removed_nodes(
    db: Arc<LocalDb>,
    execution_id: &str,
    removed_node_ids: &HashSet<String>,
) -> Result<RemovedNodesReconcile, String> {
    let execution_id = execution_id.to_string();
    let removed_node_ids = removed_node_ids.clone();
    run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id.clone();
            let removed_node_ids = removed_node_ids.clone();
            Box::pin(async move {
                reconcile_removed_nodes_conn(conn, &execution_id, &removed_node_ids).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

async fn project_repo_path_conn(conn: &turso::Connection, project_id: &str) -> DbResult<String> {
    let mut rows = conn
        .query(
            "SELECT repo_path FROM projects WHERE id = ?1",
            (project_id,),
        )
        .await?;
    Ok(crate::storage::next_text(&mut rows, 0)
        .await?
        .unwrap_or_default())
}

/// Archive a job: flip it to `cancelled` and close any open session, leaving the
/// transcript (runs, events, turns, sessions, artifacts) intact for history.
///
/// `cancelled` is terminal and sticky — the status projection never re-derives
/// it (see `recompute`), so this single write is durable. Closing the open
/// session (without deleting it) keeps the resume/warm-process machinery from
/// treating an archived job as live work while preserving its record.
async fn cancel_job_conn(conn: &turso::Connection, job_id: &str, now: i64) -> DbResult<()> {
    conn.execute(
        "UPDATE jobs SET status = 'cancelled', completed_at = ?1, updated_at = ?1 WHERE id = ?2",
        params![now, job_id],
    )
    .await?;
    conn.execute(
        "UPDATE sessions
         SET status = 'closed', terminal_reason = 'node_removed', closed_at = ?1, updated_at = ?1
         WHERE job_id = ?2 AND status = 'open'",
        params![now, job_id],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType,
        RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };
    use std::collections::HashMap;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("snapshot-edit-test.db").await
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

    fn control_edge(id: &str, from: &str, to: &str) -> RecipeEdge {
        RecipeEdge {
            id: id.to_string(),
            edge_type: RecipeEdgeType::Control,
            source_node_id: from.to_string(),
            source_handle: "control-out".to_string(),
            target_node_id: to.to_string(),
            target_handle: "control-in".to_string(),
        }
    }

    fn snapshot(nodes: Vec<RecipeNode>, edges: Vec<RecipeEdge>) -> ExecutionSnapshot {
        ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes,
                edges,
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
    }

    async fn seed_project_issue_execution(conn: &turso::Connection, snapshot: &ExecutionSnapshot) {
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj-1','w-1','P','CAIRN','/tmp/repo','main',1,1)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES ('issue-1','proj-1',1,'Issue','active','active','none',1,1)",
            (),
        )
        .await
        .unwrap();
        let snapshot_json = snapshot.to_json().unwrap();
        conn.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, snapshot)
             VALUES ('exec-1','recipe-1','issue-1','proj-1','running',1,?1)",
            params![snapshot_json.as_str()],
        )
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_job(
        conn: &turso::Connection,
        id: &str,
        node_id: &str,
        status: &str,
        worktree_path: Option<&str>,
        with_session: bool,
    ) {
        conn.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, status, project_id, issue_id,
                 worktree_path, created_at, updated_at)
             VALUES (?1,'exec-1',?2,?3,'proj-1','issue-1',?4,1,1)",
            params![id, node_id, status, worktree_path],
        )
        .await
        .unwrap();
        if with_session {
            conn.execute(
                "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES (?1, ?2, 'claude', 'open', 1, 1, 1)",
                params![format!("sess-{id}"), id],
            )
            .await
            .unwrap();
        }
    }

    async fn job_exists(conn: &turso::Connection, id: &str) -> bool {
        let mut rows = conn
            .query("SELECT 1 FROM jobs WHERE id = ?1", params![id])
            .await
            .unwrap();
        rows.next().await.unwrap().is_some()
    }

    async fn session_exists(conn: &turso::Connection, job_id: &str) -> bool {
        let mut rows = conn
            .query("SELECT 1 FROM sessions WHERE job_id = ?1", params![job_id])
            .await
            .unwrap();
        rows.next().await.unwrap().is_some()
    }

    async fn job_status(conn: &turso::Connection, id: &str) -> Option<String> {
        let mut rows = conn
            .query("SELECT status FROM jobs WHERE id = ?1", params![id])
            .await
            .unwrap();
        rows.next().await.unwrap().map(|row| row.text(0).unwrap())
    }

    async fn run_exists(conn: &turso::Connection, run_id: &str) -> bool {
        let mut rows = conn
            .query("SELECT 1 FROM runs WHERE id = ?1", params![run_id])
            .await
            .unwrap();
        rows.next().await.unwrap().is_some()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancels_pending_node_job_preserving_transcript() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("keep", RecipeNodeType::Agent),
                        node("drop", RecipeNodeType::Agent),
                    ],
                    vec![
                        control_edge("e1", "trigger", "keep"),
                        control_edge("e2", "keep", "drop"),
                    ],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(
                    conn,
                    "job-keep",
                    "keep",
                    "running",
                    Some("/tmp/wt-keep"),
                    true,
                )
                .await;
                seed_job(conn, "job-drop", "drop", "pending", None, true).await;
                // A transcript row that must survive archival.
                conn.execute(
                    "INSERT INTO runs (id, job_id, status, created_at, updated_at)
                     VALUES ('run-drop', 'job-drop', 'exited', 1, 1)",
                    (),
                )
                .await?;

                let removed: HashSet<String> = ["drop".to_string()].into_iter().collect();
                let outcome = reconcile_removed_nodes_conn(conn, "exec-1", &removed)
                    .await
                    .unwrap();

                assert_eq!(outcome.cancelled_job_ids, vec!["job-drop".to_string()]);
                assert!(
                    outcome.worktree_targets.is_empty(),
                    "pending job has no worktree"
                );
                // Archived, not erased: the job and its whole transcript remain.
                assert_eq!(
                    job_status(conn, "job-drop").await.as_deref(),
                    Some("cancelled"),
                    "removed node's job is cancelled, not deleted"
                );
                assert!(session_exists(conn, "job-drop").await, "session preserved");
                assert!(
                    run_exists(conn, "run-drop").await,
                    "transcript run preserved"
                );
                // The session is closed so resume/warm-process never treats it as live.
                let mut srows = conn
                    .query("SELECT status FROM sessions WHERE job_id = 'job-drop'", ())
                    .await?;
                let sess_status = srows.next().await?.unwrap().text(0)?;
                assert_eq!(sess_status, "closed", "archived job's session is closed");
                assert_eq!(
                    job_status(conn, "job-keep").await.as_deref(),
                    Some("running"),
                    "surviving job untouched"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_removing_a_running_node() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("busy", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "busy")],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(
                    conn,
                    "job-busy",
                    "busy",
                    "running",
                    Some("/tmp/wt-busy"),
                    true,
                )
                .await;

                let removed: HashSet<String> = ["busy".to_string()].into_iter().collect();
                let err = reconcile_removed_nodes_conn(conn, "exec-1", &removed)
                    .await
                    .unwrap_err();
                assert!(
                    err.to_string()
                        .contains("running and complete nodes are immutable"),
                    "unexpected error: {err}"
                );
                assert!(
                    job_exists(conn, "job-busy").await,
                    "running job not deleted"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_removing_a_complete_node() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("done", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "done")],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(
                    conn,
                    "job-done",
                    "done",
                    "complete",
                    Some("/tmp/wt-done"),
                    false,
                )
                .await;

                let removed: HashSet<String> = ["done".to_string()].into_iter().collect();
                let err = reconcile_removed_nodes_conn(conn, "exec-1", &removed)
                    .await
                    .unwrap_err();
                assert!(
                    err.to_string().contains("immutable"),
                    "unexpected error: {err}"
                );
                assert!(
                    job_exists(conn, "job-done").await,
                    "complete job not deleted"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn added_node_is_ready_once_its_control_dep_completes() {
        // A node added downstream of an existing node: its freshly-created
        // pending job gates on the upstream job and becomes ready the moment the
        // upstream completes — readiness is recomputed from existing facts, so an
        // add downstream of an already-complete node starts immediately.
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("up", RecipeNodeType::Agent),
                        node("added", RecipeNodeType::Agent),
                    ],
                    vec![
                        control_edge("e1", "trigger", "up"),
                        control_edge("e2", "up", "added"),
                    ],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(conn, "job-up", "up", "running", Some("/tmp/wt-up"), true).await;
                seed_job(conn, "job-added", "added", "pending", None, true).await;

                let added = load_job_by_id_conn(conn, "job-added")
                    .await
                    .unwrap()
                    .unwrap();

                // Upstream still running → the added node is gated.
                assert!(
                    !is_job_ready_conn(conn, &added).await.unwrap(),
                    "added node must wait on its control dependency"
                );

                // Upstream completes → the added node is ready to claim.
                conn.execute(
                    "UPDATE jobs SET status = 'complete' WHERE id = 'job-up'",
                    (),
                )
                .await?;
                assert!(
                    is_job_ready_conn(conn, &added).await.unwrap(),
                    "added node becomes ready once its control dependency completes"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reports_orphaned_worktree_but_keeps_shared_path() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("keep", RecipeNodeType::Agent),
                        node("drop-own", RecipeNodeType::Agent),
                        node("drop-shared", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "keep")],
                );
                seed_project_issue_execution(conn, &snap).await;
                // Surviving job holds /tmp/wt-shared.
                seed_job(
                    conn,
                    "job-keep",
                    "keep",
                    "running",
                    Some("/tmp/wt-shared"),
                    true,
                )
                .await;
                // Removed failed job owns its own worktree exclusively.
                seed_job(
                    conn,
                    "job-own",
                    "drop-own",
                    "failed",
                    Some("/tmp/wt-own"),
                    true,
                )
                .await;
                // Removed blocked job shares the surviving job's worktree path.
                seed_job(
                    conn,
                    "job-shared",
                    "drop-shared",
                    "blocked",
                    Some("/tmp/wt-shared"),
                    true,
                )
                .await;

                let removed: HashSet<String> = ["drop-own".to_string(), "drop-shared".to_string()]
                    .into_iter()
                    .collect();
                let outcome = reconcile_removed_nodes_conn(conn, "exec-1", &removed)
                    .await
                    .unwrap();

                assert_eq!(
                    outcome.worktree_targets.len(),
                    1,
                    "only the exclusively-owned path"
                );
                let target = &outcome.worktree_targets[0];
                assert_eq!(target.worktree_path, "/tmp/wt-own");
                assert_eq!(target.repo_path, "/tmp/repo");
                assert_eq!(target.job_ids, vec!["job-own".to_string()]);
                // Both removed jobs are archived (cancelled), not deleted.
                assert_eq!(
                    job_status(conn, "job-own").await.as_deref(),
                    Some("cancelled")
                );
                assert_eq!(
                    job_status(conn, "job-shared").await.as_deref(),
                    Some("cancelled")
                );
                assert_eq!(
                    job_status(conn, "job-keep").await.as_deref(),
                    Some("running")
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_job_does_not_wedge_execution_status() {
        // An archived (cancelled) job is terminal: it must not count as a
        // non-terminal job keeping the execution "running", nor as a failure.
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("keep", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "keep")],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(conn, "job-keep", "keep", "complete", None, false).await;
                seed_job(conn, "job-cancel", "gone", "cancelled", None, false).await;

                crate::transitions::outcome::recompute_execution_status_conn(conn, "exec-1")
                    .await?;

                let mut rows = conn
                    .query("SELECT status FROM executions WHERE id = 'exec-1'", ())
                    .await?;
                let status = rows.next().await?.unwrap().text(0)?;
                assert_eq!(
                    status, "complete",
                    "a cancelled job is terminal and must not hold the execution running"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_status_survives_recompute() {
        // Stickiness: the status projection must never re-derive a cancelled job
        // back to a live status, even though it carries no terminal turn.
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("gone", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "gone")],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(conn, "job-cancel", "gone", "cancelled", None, true).await;

                let derived = recompute_job_status_conn(conn, "job-cancel").await?;
                assert_eq!(
                    derived,
                    JobStatus::Cancelled,
                    "recompute leaves a cancelled job cancelled"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
