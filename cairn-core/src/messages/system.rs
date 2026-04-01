//! System messages emitted on lifecycle events.
//!
//! These are auto-generated messages posted to the issue channel when
//! jobs start, complete, or fail — giving other agents passive awareness
//! of what's happening on the issue.

use crate::models::ChannelType;
use crate::node_segments::visible_node_segment_or_name;
use crate::orchestrator::Orchestrator;
use crate::schema::{executions, issues, jobs, projects, runs};
use diesel::prelude::*;

/// Emit a system message for a job lifecycle event.
///
/// Looks up the job's issue context and posts to the issue channel.
/// No-op if the job isn't associated with an issue.
///
/// `run_id` identifies the run this event is about. When set, it becomes
/// the `sender_run_id` on the message so that delivery filters can exclude
/// the message from being shown back to the same agent.
pub fn emit_job_event(orch: &Orchestrator, job_id: &str, run_id: Option<&str>, event: JobEvent) {
    let ctx = match lookup_job_context(orch, job_id) {
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
        JobEvent::Blocked => format!("{} is blocked waiting for approval", label),
    };

    // Insert + deliver
    let conn_result = orch.db.conn.lock();
    let mut conn = match conn_result {
        Ok(c) => c,
        Err(_) => return,
    };

    let msg = super::db::insert_message(
        &mut conn,
        &ChannelType::Issue,
        Some(&ctx.issue_key),
        run_id, // Identifies which agent this event is about
        "system",
        None,
        &content,
    );

    if let Ok(msg) = msg {
        // Drop conn before delivery (delivery needs its own lock)
        drop(conn);
        super::delivery::deliver(orch, &msg);
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
    Blocked,
}

struct JobContext {
    /// Issue channel key in KEY/NUMBER format (e.g. "CRN/40")
    issue_key: String,
    node_name: String,
    uri: Option<String>,
}

fn lookup_job_context(orch: &Orchestrator, job_id: &str) -> Option<JobContext> {
    let mut conn = orch.db.conn.lock().ok()?;

    // Get job's issue_id, recipe node id, node_name, and execution_id (all nullable)
    let (issue_id, recipe_node_id, node_name, execution_id): (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = jobs::table
        .find(job_id)
        .select((
            jobs::issue_id,
            jobs::recipe_node_id,
            jobs::node_name,
            jobs::execution_id,
        ))
        .first(&mut *conn)
        .ok()?;

    let issue_id = issue_id?; // Skip if no issue
    let node_name = node_name.unwrap_or_else(|| "unknown".to_string());

    // Resolve issue UUID to KEY/NUMBER format
    let (project_key, issue_number): (String, i32) = issues::table
        .find(&issue_id)
        .inner_join(projects::table)
        .select((projects::key, issues::number))
        .first(&mut *conn)
        .ok()?;

    let issue_key = format!("{}/{}", project_key, issue_number);

    // Build cairn:// URI for identification
    let uri = build_node_uri(
        &mut conn,
        &issue_id,
        execution_id.as_deref(),
        recipe_node_id.as_deref(),
        &node_name,
    );

    Some(JobContext {
        issue_key,
        node_name,
        uri,
    })
}

/// Build a cairn://PROJECT/NUMBER/EXEC/NODE URI from components.
fn build_node_uri(
    conn: &mut diesel::SqliteConnection,
    issue_id: &str,
    execution_id: Option<&str>,
    recipe_node_id: Option<&str>,
    node_name: &str,
) -> Option<String> {
    let (project_key, issue_number): (String, i32) = issues::table
        .find(issue_id)
        .inner_join(projects::table)
        .select((projects::key, issues::number))
        .first(conn)
        .ok()?;

    let exec_seq = execution_id.and_then(|eid| {
        executions::table
            .find(eid)
            .select(executions::seq)
            .first::<Option<i32>>(conn)
            .ok()
            .flatten()
    });

    let visible_node = visible_node_segment_or_name(recipe_node_id, node_name);

    Some(format!(
        "cairn://{}/{}/{}/{}",
        project_key,
        issue_number,
        exec_seq.unwrap_or(1),
        visible_node
    ))
}

/// Emit a system message when a user creates a terminal for a job.
/// Tells the agent about the terminal's URI so it can read output.
pub fn emit_terminal_created(orch: &Orchestrator, job_id: &str, slug: &str, title: &str) {
    let ctx = match lookup_job_context(orch, job_id) {
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

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(_) => return,
    };

    let msg = super::db::insert_message(
        &mut conn,
        &ChannelType::Issue,
        Some(&ctx.issue_key),
        None, // No sender_run_id — user-initiated, all agents should see it
        "system",
        None,
        &content,
    );

    if let Ok(msg) = msg {
        drop(conn);
        super::delivery::deliver(orch, &msg);
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "messages", "action": "insert"}),
        );
    }
}

/// Emit a system message for a run being continued (follow-up message sent).
pub fn emit_run_continued(orch: &Orchestrator, run_id: &str) {
    let job_id: Option<String> = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(_) => return,
        };
        runs::table
            .find(run_id)
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
    };

    if let Some(job_id) = job_id {
        let ctx = match lookup_job_context(orch, &job_id) {
            Some(ctx) => ctx,
            None => return,
        };

        let label = match &ctx.uri {
            Some(uri) => format!("{} ({})", ctx.node_name, uri),
            None => ctx.node_name.clone(),
        };
        let content = format!("{} received a follow-up message", label);

        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(_) => return,
        };

        let msg = super::db::insert_message(
            &mut conn,
            &ChannelType::Issue,
            Some(&ctx.issue_key),
            Some(run_id),
            "system",
            None,
            &content,
        );

        if let Ok(msg) = msg {
            drop(conn);
            super::delivery::deliver(orch, &msg);
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );
        }
    }
}
