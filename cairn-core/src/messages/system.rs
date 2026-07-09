//! System messages emitted on lifecycle events.
//!
//! These are auto-generated messages posted to the issue channel when
//! jobs start, complete, or fail — giving other agents passive awareness
//! of what's happening on the issue.

use std::sync::Arc;

use crate::models::ChannelType;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError, LocalDb, RowExt};
use cairn_db::turso::params;

/// Emit a system message for a job lifecycle event.
///
/// Looks up the job's issue context and posts to the issue channel.
/// No-op if the job isn't associated with an issue.
///
/// `run_id` identifies the run this event is about. When set, it becomes
/// the `sender_run_id` on the message so that delivery filters can exclude
/// the message from being shown back to the same agent.
pub fn emit_job_event(orch: &Orchestrator, job_id: &str, run_id: Option<&str>, event: JobEvent) {
    // Lifecycle messages live in the job's OWNING database (CAIRN-2197): a team
    // job's rows are in the synced replica, so looking up context and inserting
    // the message against the private DB would silently drop the message for
    // every team execution. Best-effort — a closed replica skips the message.
    let Some(db) = owning_db_for_job(orch, job_id) else {
        return;
    };
    let ctx = match lookup_job_context(db.clone(), job_id) {
        Some(ctx) => ctx,
        None => return, // No issue context — standalone job, skip
    };

    let label = match &ctx.uri {
        Some(uri) => format!("{} ({})", ctx.node_name, uri),
        None => ctx.node_name.clone(),
    };

    let content = match event {
        JobEvent::Started => format!("{} started working", label),
        JobEvent::Completed => format!("{} finished successfully", label),
        JobEvent::Failed => format!("{} failed", label),
        JobEvent::Blocked { reason } => match reason {
            Some(reason) => format!("{label} is blocked: {reason}"),
            None => format!("{label} is blocked waiting for approval"),
        },
    };

    let msg = super::db::insert_message(
        &db,
        &ChannelType::Issue,
        Some(&ctx.issue_key),
        run_id, // Identifies which agent this event is about
        "system",
        None,
        &content,
    );

    if msg.is_ok() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "messages", "action": "insert"}),
        );
    }
}

pub enum JobEvent {
    Started,
    Completed,
    Failed,
    /// Blocked. `reason` names a specific cause (e.g. missing required output);
    /// None falls back to the generic "waiting for approval" message.
    Blocked {
        reason: Option<String>,
    },
}

struct JobContext {
    /// Issue channel key in KEY/NUMBER format (e.g. "CRN/40")
    issue_key: String,
    node_name: String,
    uri: Option<String>,
}

/// Resolve the database that owns a job (CAIRN-2197): the synced replica for a
/// team job, the private DB for a local one. Best-effort — a closed team replica
/// yields None and the (best-effort) lifecycle message is simply skipped.
fn owning_db_for_job(orch: &Orchestrator, job_id: &str) -> Option<Arc<LocalDb>> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())
    })
    .ok()
}

/// Resolve the database that owns a run (CAIRN-2197), same contract as
/// [`owning_db_for_job`].
fn owning_db_for_run(orch: &Orchestrator, run_id: &str) -> Option<Arc<LocalDb>> {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())
    })
    .ok()
}

fn lookup_job_context(db: Arc<LocalDb>, job_id: &str) -> Option<JobContext> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { lookup_job_context_async(&db, &job_id).await })
        .ok()
        .flatten()
}

async fn lookup_job_context_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<JobContext>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            // Join the parent job so we can build the canonical URI for a
            // sub-task: `.../{seq}/{parent}/task/{segment}` instead of the
            // top-level `.../{seq}/{segment}`. Without this, lifecycle
            // messages ("X finished successfully") advertised wrong URIs for
            // any job with a parent, and downstream agents that tried to DM
            // those URIs failed to resolve a recipient.
            let mut rows = conn
                .query(
                    "SELECT j.uri_segment, j.node_name, p.key, i.number, e.seq,
                            parent.uri_segment AS parent_uri_segment
                     FROM jobs j
                     JOIN issues i ON j.issue_id = i.id
                     JOIN projects p ON i.project_id = p.id
                     LEFT JOIN executions e ON j.execution_id = e.id
                     LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                     WHERE j.id = ?1
                     LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;

            let Some(row) = rows.next().await? else {
                return Ok(None);
            };

            let uri_segment = row.opt_text(0)?;
            let node_name = row.opt_text(1)?.unwrap_or_else(|| "unknown".to_string());
            let project_key = row.text(2)?;
            let issue_number = row.i64(3)? as i32;
            let exec_seq = row.opt_i64(4)?.unwrap_or(1) as i32;
            let parent_uri_segment = row.opt_text(5)?;
            let issue_key = format!("{}/{}", project_key, issue_number);
            let uri = uri_segment.as_deref().map(|segment| {
                cairn_common::uri::build_job_base_uri(
                    &project_key,
                    issue_number,
                    exec_seq,
                    segment,
                    parent_uri_segment.as_deref(),
                )
            });

            Ok::<_, DbError>(Some(JobContext {
                issue_key,
                node_name,
                uri,
            }))
        })
    })
    .await
    .map_err(|error| format!("Failed to look up job message context: {error}"))
}

/// Emit a system message when a user creates a terminal for a job.
/// Tells the agent about the terminal's URI so it can read output.
pub fn emit_terminal_created(orch: &Orchestrator, job_id: &str, slug: &str, title: &str) {
    let Some(db) = owning_db_for_job(orch, job_id) else {
        return;
    };
    let ctx = match lookup_job_context(db.clone(), job_id) {
        Some(ctx) => ctx,
        None => return, // No issue context — standalone job, skip
    };

    // Build terminal URI from node URI
    let terminal_uri = match &ctx.uri {
        Some(node_uri) => format!("{}/terminal/{}", node_uri, slug),
        None => return,
    };

    let content = format!(
        "User opened terminal \"{}\" — read output with: {}",
        title, terminal_uri
    );

    let msg = super::db::insert_message(
        &db,
        &ChannelType::Issue,
        Some(&ctx.issue_key),
        None, // No sender_run_id — user-initiated, all agents should see it
        "system",
        None,
        &content,
    );

    if msg.is_ok() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "messages", "action": "insert"}),
        );
    }
}

/// Emit a system message for a run being continued (follow-up message sent).
pub fn emit_run_continued(orch: &Orchestrator, run_id: &str) {
    let Some(db) = owning_db_for_run(orch, run_id) else {
        return;
    };
    let job_id = lookup_run_job_id(db.clone(), run_id);

    if let Some(job_id) = job_id {
        let ctx = match lookup_job_context(db.clone(), &job_id) {
            Some(ctx) => ctx,
            None => return,
        };

        let label = match &ctx.uri {
            Some(uri) => format!("{} ({})", ctx.node_name, uri),
            None => ctx.node_name.clone(),
        };
        let content = format!("{} received a follow-up message", label);

        let msg = super::db::insert_message(
            &db,
            &ChannelType::Issue,
            Some(&ctx.issue_key),
            Some(run_id),
            "system",
            None,
            &content,
        );

        if msg.is_ok() {
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );
        }
    }
}

fn lookup_run_job_id(db: Arc<LocalDb>, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    run_db_blocking(move || async move { lookup_run_job_id_async(&db, &run_id).await })
        .ok()
        .flatten()
}

async fn lookup_run_job_id_async(db: &LocalDb, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    let job_id = db
        .read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                        params![run_id.as_str()],
                    )
                    .await?;

                crate::storage::next_opt_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|error| format!("Failed to look up run job: {error}"))?;

    Ok(job_id)
}

#[cfg(test)]
mod tests {
    //! Pins the URI shape produced by lookup_job_context_async for jobs with a
    //! parent (sub-tasks) vs without (top-level node jobs). emit_job_event and
    //! emit_terminal_created both feed JobContext.uri into channel messages
    //! that other agents try to DM, so the URI must round-trip back through
    //! the message-target parser and recipient resolver.

    use super::*;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("system-test.db").await
    }

    /// Seed a top-level builder job at exec 1 + a sub-task `review` nested
    /// under it. Returns (top_level_job_id, sub_task_job_id).
    async fn seed_node_and_subtask(db: &LocalDb) -> (String, String) {
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1', 'default', 'Test Project', 'PROJ', '/tmp/test-repo', 1, 1);
            INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
            INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-builder', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'complete', 1, 1);
            INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-review', 'exec-1', 'job-builder', 'issue-1', 'proj-1', 'review', 'review', 'complete', 1, 1);
            ",
        )
        .await
        .unwrap();
        ("job-builder".to_string(), "job-review".to_string())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn job_context_uri_for_subtask_uses_task_form() {
        let db = migrated_db().await;
        let (_top, sub) = seed_node_and_subtask(&db).await;

        let ctx = lookup_job_context_async(&db, &sub)
            .await
            .unwrap()
            .expect("sub-task job context");

        assert_eq!(ctx.node_name, "review");
        assert_eq!(ctx.issue_key, "PROJ/42");
        assert_eq!(
            ctx.uri.as_deref(),
            Some("cairn://p/PROJ/42/1/builder/task/review"),
            "sub-task URI must nest under its parent (was the bug — it used to be cairn://p/PROJ/42/1/review)"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn job_context_uri_for_top_level_node_unchanged() {
        let db = migrated_db().await;
        let (top, _sub) = seed_node_and_subtask(&db).await;

        let ctx = lookup_job_context_async(&db, &top)
            .await
            .unwrap()
            .expect("top-level job context");

        assert_eq!(ctx.node_name, "builder");
        assert_eq!(
            ctx.uri.as_deref(),
            Some("cairn://p/PROJ/42/1/builder"),
            "top-level job URI shape must not regress from the parent-join change"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn job_context_uri_round_trips_through_parse_uri() {
        // Pins the contract that matters for messaging: the URI we advertise
        // on lifecycle messages must parse back to a Task resource (or Node
        // for top-level jobs) so parse_message_target can route a DM to it.
        let db = migrated_db().await;
        let (top, sub) = seed_node_and_subtask(&db).await;

        let top_uri = lookup_job_context_async(&db, &top)
            .await
            .unwrap()
            .and_then(|c| c.uri)
            .expect("top-level URI");
        assert!(
            matches!(
                cairn_common::uri::parse_uri(&top_uri),
                Some(cairn_common::uri::CairnResource::Node { .. })
            ),
            "top-level URI should parse as Node: {top_uri}"
        );

        let sub_uri = lookup_job_context_async(&db, &sub)
            .await
            .unwrap()
            .and_then(|c| c.uri)
            .expect("sub-task URI");
        assert!(
            matches!(
                cairn_common::uri::parse_uri(&sub_uri),
                Some(cairn_common::uri::CairnResource::Task { .. })
            ),
            "sub-task URI should parse as Task: {sub_uri}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn job_context_uri_is_none_for_job_without_uri_segment() {
        // Defensive: jobs created before uri_segment became first-class store
        // NULL there. The function should still return a JobContext with
        // uri = None rather than failing.
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-x', 'default', 'X', 'X', '/tmp/x', 1, 1);
            INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-x', 'proj-x', 1, 't', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-x', 'recipe-default', 'issue-x', 'proj-x', 'running', 1, 1);
            INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at)
             VALUES ('job-x', 'exec-x', 'legacy', 'issue-x', 'proj-x', 'legacy', 'complete', 1, 1);
            ",
        )
        .await
        .unwrap();

        let ctx = lookup_job_context_async(&db, "job-x")
            .await
            .unwrap()
            .expect("legacy job context");
        assert_eq!(ctx.uri, None);
    }
}
