//! Restart a node: archive its current attempt and create a fresh job for the
//! same recipe node, applying the node's edited snapshot config.
//!
//! A model/prompt edit cannot reach a live (or already-terminal) session in
//! place: the system prompt is fixed at session spawn and a model change rotates
//! the session rather than mutating the turn. Restart is the honest bridge — it
//! starts the node over with its current concrete config.
//!
//! The shape composes two already-tested primitives instead of inventing an
//! in-place reset. Job status is *derived from turn facts* (only `Cancelled` is a
//! sticky override), so flipping a failed job back to `pending` in place leaves
//! the failed turn as the latest fact and the projection re-derives `Failed`. A
//! fresh job starts from a clean fact slate and derives `Pending` deterministically:
//!
//! 1. Archive every prior attempt for the node via [`reconcile_removed_nodes_conn`]
//!    — it rejects running/complete nodes (immutable), flips the rest to
//!    `Cancelled` with their transcripts preserved, and returns the worktree
//!    paths now owned by nobody (for filesystem teardown by the caller).
//! 2. Materialize a fresh `pending` job for the same node from the (edited)
//!    snapshot via [`create_jobs_for_new_nodes_conn`]. It reads the concrete
//!    `selection.model` from the snapshot and inserts a job with a NULL worktree
//!    (allocated lazily at start), so “fresh worktree” comes for free while the
//!    archived attempt's branch and commits are preserved on disk-by-git.
//!
//! The caller (host command) then tears down the orphaned worktree and
//! re-advances the DAG; once the node's live job is the fresh `pending` one,
//! downstream cascade-failed descendants derive back to gated/pending and re-run.

use super::*;

/// Outcome of restarting a node: the archival reconcile result (cancelled job
/// ids + orphaned worktree teardown targets) and the freshly created job(s).
#[derive(Debug)]
pub struct RestartNodeOutcome {
    pub reconcile: RemovedNodesReconcile,
    pub created_jobs: Vec<Job>,
}

/// Connection-level restart: validate, archive the prior attempt, create a fresh
/// job — all in one transaction so a partial restart is never observable.
pub(crate) async fn restart_node_conn(
    conn: &turso::Connection,
    execution_id: &str,
    recipe_node_id: &str,
) -> DbResult<RestartNodeOutcome> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    if !snapshot
        .recipe
        .nodes
        .iter()
        .any(|node| node.id == recipe_node_id)
    {
        return Err(db_internal(format!(
            "Cannot restart node {recipe_node_id}: not present in execution snapshot"
        )));
    }

    let target: HashSet<String> = std::iter::once(recipe_node_id.to_string()).collect();
    // Archive every prior attempt for the node. Reuses the snapshot-edit
    // machinery, which rejects running/complete jobs before any mutation.
    let reconcile = reconcile_removed_nodes_conn(conn, execution_id, &target).await?;
    // Materialize one fresh pending job for the same node from the edited snapshot.
    let created_jobs =
        create_jobs_for_new_nodes_conn(conn, execution_id, &target, &snapshot).await?;

    Ok(RestartNodeOutcome {
        reconcile,
        created_jobs,
    })
}

/// Blocking wrapper over [`restart_node_conn`] for host command paths.
pub fn restart_node(
    db: Arc<LocalDb>,
    execution_id: &str,
    recipe_node_id: &str,
) -> Result<RestartNodeOutcome, String> {
    let execution_id = execution_id.to_string();
    let recipe_node_id = recipe_node_id.to_string();
    run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id.clone();
            let recipe_node_id = recipe_node_id.clone();
            Box::pin(async move { restart_node_conn(conn, &execution_id, &recipe_node_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, JobStatus, NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode,
        RecipeNodeType, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };
    use std::collections::HashMap;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("restart-node-test.db").await
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

    async fn seed_project_issue_execution(conn: &turso::Connection, snap: &ExecutionSnapshot) {
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
        let snapshot_json = snap.to_json().unwrap();
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
        created_at: i64,
    ) {
        conn.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, status, project_id, issue_id,
                 worktree_path, created_at, updated_at)
             VALUES (?1,'exec-1',?2,?3,'proj-1','issue-1',?4,?5,?5)",
            params![id, node_id, status, worktree_path, created_at],
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
            conn.execute(
                "UPDATE jobs SET current_session_id = ?1 WHERE id = ?2",
                params![format!("sess-{id}"), id],
            )
            .await
            .unwrap();
        }
    }

    async fn job_status(conn: &turso::Connection, id: &str) -> Option<String> {
        let mut rows = conn
            .query("SELECT status FROM jobs WHERE id = ?1", params![id])
            .await
            .unwrap();
        rows.next().await.unwrap().map(|row| row.text(0).unwrap())
    }

    async fn jobs_for_node(conn: &turso::Connection, node_id: &str) -> Vec<(String, String)> {
        let mut rows = conn
            .query(
                "SELECT id, status FROM jobs WHERE execution_id = 'exec-1' AND recipe_node_id = ?1
                 ORDER BY created_at ASC",
                params![node_id],
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            out.push((row.text(0).unwrap(), row.text(1).unwrap()));
        }
        out
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restart_archives_failed_job_and_creates_fresh_pending() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("work", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "work")],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(conn, "job-1", "work", "failed", Some("/tmp/wt-1"), true, 10).await;
                // A transcript row that must survive archival.
                conn.execute(
                    "INSERT INTO runs (id, job_id, status, created_at, updated_at)
                     VALUES ('run-1', 'job-1', 'exited', 1, 1)",
                    (),
                )
                .await?;

                let outcome = restart_node_conn(conn, "exec-1", "work").await.unwrap();

                // Old attempt archived, its transcript preserved.
                assert_eq!(
                    outcome.reconcile.cancelled_job_ids,
                    vec!["job-1".to_string()]
                );
                assert_eq!(
                    job_status(conn, "job-1").await.as_deref(),
                    Some("cancelled")
                );
                let mut srows = conn
                    .query("SELECT status FROM sessions WHERE job_id = 'job-1'", ())
                    .await?;
                assert_eq!(
                    srows.next().await?.unwrap().text(0)?,
                    "closed",
                    "archived job's session is closed but preserved"
                );
                let mut rrows = conn
                    .query("SELECT 1 FROM runs WHERE id = 'run-1'", ())
                    .await?;
                assert!(rrows.next().await?.is_some(), "transcript run preserved");

                // Exactly one fresh pending job created for the same node.
                assert_eq!(outcome.created_jobs.len(), 1);
                let fresh = &outcome.created_jobs[0];
                assert_ne!(fresh.id, "job-1");
                assert_eq!(fresh.status, JobStatus::Pending);
                assert!(
                    fresh.worktree_path.is_none(),
                    "fresh worktree allocated lazily"
                );

                // The live lookup now resolves to the fresh job, never the archive.
                let live =
                    crate::db_records::load_live_job_by_execution_node_conn(conn, "exec-1", "work")
                        .await?
                        .unwrap();
                assert_eq!(live.id, fresh.id);

                // Lineage: two attempts for the node (cancelled + pending).
                let attempts = jobs_for_node(conn, "work").await;
                assert_eq!(attempts.len(), 2);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restart_rejects_running_node() {
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
                    Some("/tmp/wt"),
                    true,
                    10,
                )
                .await;
                let err = restart_node_conn(conn, "exec-1", "busy").await.unwrap_err();
                assert!(
                    err.to_string().contains("immutable"),
                    "unexpected error: {err}"
                );
                // No fresh job created; the running job is untouched.
                assert_eq!(jobs_for_node(conn, "busy").await.len(), 1);
                assert_eq!(
                    job_status(conn, "job-busy").await.as_deref(),
                    Some("running")
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restart_rejects_complete_node() {
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
                seed_job(conn, "job-done", "done", "complete", None, false, 10).await;
                let err = restart_node_conn(conn, "exec-1", "done").await.unwrap_err();
                assert!(
                    err.to_string().contains("immutable"),
                    "unexpected error: {err}"
                );
                assert_eq!(jobs_for_node(conn, "done").await.len(), 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn per_node_lookup_prefers_live_over_cancelled() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("work", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "work")],
                );
                seed_project_issue_execution(conn, &snap).await;
                // An older cancelled archive and a newer live pending attempt.
                seed_job(conn, "old", "work", "cancelled", None, false, 10).await;
                seed_job(conn, "new", "work", "pending", None, false, 20).await;

                let live =
                    crate::db_records::load_live_job_by_execution_node_conn(conn, "exec-1", "work")
                        .await?
                        .unwrap();
                assert_eq!(live.id, "new", "live lookup skips the cancelled archive");

                let statuses =
                    crate::execution::queries::node_statuses_conn(conn, "exec-1").await?;
                assert_eq!(
                    statuses.get("work").map(String::as_str),
                    Some("pending"),
                    "node-status projection reflects the live attempt"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restart_clears_downstream_cascade() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("up", RecipeNodeType::Agent),
                        node("down", RecipeNodeType::Agent),
                    ],
                    vec![
                        control_edge("e1", "trigger", "up"),
                        control_edge("e2", "up", "down"),
                    ],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(conn, "job-up", "up", "failed", Some("/tmp/wt-up"), true, 10).await;
                seed_job(conn, "job-down", "down", "pending", None, false, 11).await;

                // Before restart: down cascades to Failed off the failed upstream.
                let before = recompute_job_status_conn(conn, "job-down").await?;
                assert_eq!(before, JobStatus::Failed, "down cascades off failed up");

                restart_node_conn(conn, "exec-1", "up").await.unwrap();

                // After restart: the live up job is fresh pending, so down derives
                // back to gated/pending and will re-run once up completes.
                let after = recompute_job_status_conn(conn, "job-down").await?;
                assert_eq!(after, JobStatus::Pending, "cascade cleared after restart");
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn list_node_attempts_returns_lineage_oldest_to_newest() {
        let db = migrated_db().await;
        db.write(|conn| {
            Box::pin(async move {
                let snap = snapshot(
                    vec![
                        node("trigger", RecipeNodeType::Trigger),
                        node("work", RecipeNodeType::Agent),
                    ],
                    vec![control_edge("e1", "trigger", "work")],
                );
                seed_project_issue_execution(conn, &snap).await;
                seed_job(conn, "attempt-1", "work", "cancelled", None, false, 10).await;
                seed_job(conn, "attempt-2", "work", "cancelled", None, false, 20).await;
                seed_job(conn, "attempt-3", "work", "running", None, true, 30).await;

                let attempts =
                    crate::execution::queries::list_node_attempts_conn(conn, "exec-1", "work")
                        .await?;
                let ids: Vec<&str> = attempts.iter().map(|a| a.id.as_str()).collect();
                assert_eq!(
                    ids,
                    vec!["attempt-1", "attempt-2", "attempt-3"],
                    "lineage includes archives, oldest→newest"
                );
                assert_eq!(
                    attempts[2].current_session_id.as_deref(),
                    Some("sess-attempt-3")
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
