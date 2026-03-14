//! Issue resource MCP handlers.
//!
//! Provides read_issue_resource handler for cairn://* URIs.

use diesel::prelude::*;
use serde::Deserialize;

use crate::diesel_models::{DbArtifact, DbJob};
use crate::mcp::types::McpCallbackRequest;
use crate::models::ExecutionSnapshot;
use crate::orchestrator::Orchestrator;
use crate::schema::{
    action_runs, artifacts, comments, events, executions, file_changes, issues, job_terminals,
    jobs, pr_data, runs,
};
use cairn_common::uri::{parse_uri, CairnResource};

use super::lookup_project_by_key;

/// Payload for read_issue_resource request
#[derive(Debug, Clone, Deserialize)]
pub struct ReadIssueResourcePayload {
    pub uri: String,
}

// ============================================================================
// Handler
// ============================================================================

/// Handle read_issue_resource request - returns issue data for a given URI
pub async fn handle_read_issue_resource(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> String {
    let payload: ReadIssueResourcePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let resource = match parse_uri(&payload.uri) {
        Some(r) => r,
        None => return format!("Invalid issue resource URI: {}", payload.uri),
    };

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database error: {}", e),
    };

    match resource {
        CairnResource::Project { project } => read_project(&mut conn, &project),
        CairnResource::Issue { project, number } => read_issue(&mut conn, &project, number),
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => read_node(&mut conn, &project, number, exec_seq, &node_id),
        CairnResource::NodeChat {
            project,
            number,
            exec_seq,
            node_id,
        } => read_node_chat(&mut conn, &project, number, exec_seq, &node_id),
        CairnResource::NodeChatFull {
            project,
            number,
            exec_seq,
            node_id,
        } => read_node_chat_full(&mut conn, &project, number, exec_seq, &node_id),
        CairnResource::NodeChatEvent {
            project,
            number,
            exec_seq,
            node_id,
            run_seq,
            event_seq,
        } => read_node_chat_event(
            &mut conn, &project, number, exec_seq, &node_id, run_seq, event_seq,
        ),
        CairnResource::NodeArtifact {
            project,
            number,
            exec_seq,
            node_id,
        } => read_node_artifact(&mut conn, &project, number, exec_seq, &node_id),
        CairnResource::TaskChat {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => read_task_chat(&mut conn, &project, number, exec_seq, &node_id, &task_name),
        CairnResource::TaskChatFull {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => read_task_chat_full(&mut conn, &project, number, exec_seq, &node_id, &task_name),
        CairnResource::TaskChatEvent {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            run_seq,
            event_seq,
        } => read_task_chat_event(
            &mut conn, &project, number, exec_seq, &node_id, &task_name, run_seq, event_seq,
        ),
        CairnResource::TaskArtifact {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => read_task_artifact(&mut conn, &project, number, exec_seq, &node_id, &task_name),
        CairnResource::ProjectChat { project, name } => {
            read_project_chat(&mut conn, &project, &name)
        }
        CairnResource::Files { project, number } => read_issue_files(&mut conn, &project, number),
        CairnResource::NodeFiles {
            project,
            number,
            exec_seq,
            node_id,
        } => read_node_files(&mut conn, &project, number, exec_seq, &node_id),
        CairnResource::ProjectMessages { project } => {
            read_project_messages(&mut conn, &project, &payload.uri)
        }
        CairnResource::IssueMessages { project, number } => {
            read_issue_messages(&mut conn, &project, number, &payload.uri)
        }
        // Terminal URIs are routed to read_resource by the MCP binary and should never reach here
        CairnResource::NodeTerminal { .. } | CairnResource::ProjectTerminal { .. } => {
            "Terminal URIs are not handled by read_issue_resource".to_string()
        }
    }
}

// ============================================================================
// Resource Readers
// ============================================================================

fn read_issue(conn: &mut SqliteConnection, project_key: &str, number: i32) -> String {
    // Get project
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    // Get issue (description is Nullable)
    let issue: Result<(String, String, String, Option<String>, i32), _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select((
            issues::id,
            issues::title,
            issues::status,
            issues::description,
            issues::created_at,
        ))
        .first(conn);

    let (issue_id, title, status, description, created_at) = match issue {
        Ok(i) => i,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    let description = description.unwrap_or_default();

    // Format header
    let created_date = chrono::DateTime::from_timestamp(created_at as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "Unknown".to_string());

    let mut output = format!("# {}-{}: {}\n\n", project_key, number, title);
    output.push_str(&format!("Status: {} | Created: {}\n", status, created_date));

    // Get PR data for this issue (via action_runs)
    #[allow(clippy::type_complexity)]
    let pr: Option<(i32, String, String)> = pr_data::table
        .inner_join(action_runs::table.on(pr_data::action_run_id.eq(action_runs::id.nullable())))
        .filter(action_runs::issue_id.eq(&issue_id))
        .select((pr_data::pr_number, pr_data::pr_url, pr_data::pr_status))
        .first(conn)
        .ok();

    if let Some((pr_number, pr_url, pr_status)) = pr {
        output.push_str(&format!("PR: #{} ({}) {}\n", pr_number, pr_status, pr_url));
    }
    output.push('\n');

    // Description
    if !description.is_empty() {
        output.push_str("## Description\n\n");
        output.push_str(&description);
        output.push_str("\n\n");
    }

    // Comments (inlined)
    let comment_rows: Vec<(String, String, i32)> = comments::table
        .filter(comments::issue_id.eq(&issue_id))
        .order(comments::created_at.asc())
        .select((comments::content, comments::source, comments::created_at))
        .load(conn)
        .unwrap_or_default();

    if !comment_rows.is_empty() {
        output.push_str(&format!("## Comments ({})\n\n", comment_rows.len()));
        for (content, source, comment_created_at) in &comment_rows {
            let comment_date = chrono::DateTime::from_timestamp(*comment_created_at as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            output.push_str(&format!("[{}] {}\n", source, comment_date));
            output.push_str(content);
            output.push_str("\n\n");
        }
    }

    // Nodes - grouped by execution
    output.push_str("## Nodes\n\n");
    output.push_str(&format!("`cairn://{}/{}`\n", project_key, number));

    // Get executions ordered by started_at
    let exec_ids: Vec<String> = executions::table
        .filter(executions::issue_id.eq(&issue_id))
        .order(executions::started_at.asc())
        .select(executions::id)
        .load(conn)
        .unwrap_or_default();

    if exec_ids.is_empty() {
        output.push_str("  (no executions yet)\n");
        return output;
    }

    for (exec_seq, exec_id) in exec_ids.iter().enumerate() {
        let exec_num = exec_seq + 1;
        output.push_str(&format!("├─ `/{}`\n", exec_num));

        // Get jobs for this execution
        let exec_jobs: Vec<DbJob> = jobs::table
            .filter(jobs::execution_id.eq(exec_id))
            .order(jobs::created_at.asc())
            .load(conn)
            .unwrap_or_default();

        // Separate top-level and task jobs
        let top_level: Vec<&DbJob> = exec_jobs
            .iter()
            .filter(|j| j.parent_job_id.is_none())
            .collect();
        let tasks: Vec<&DbJob> = exec_jobs
            .iter()
            .filter(|j| j.parent_job_id.is_some())
            .collect();

        for (idx, job) in top_level.iter().enumerate() {
            let is_last_node = idx == top_level.len() - 1;
            let prefix = if is_last_node {
                "│  └─"
            } else {
                "│  ├─"
            };
            let child_prefix = if is_last_node {
                "│     "
            } else {
                "│  │  "
            };

            // Resolve node name: UUID → human-readable via execution snapshot
            let node_id = job
                .recipe_node_id
                .as_ref()
                .and_then(|rid| get_node_name_from_execution(conn, exec_id, rid))
                .or_else(|| job.agent_config_id.clone())
                .unwrap_or_else(|| "unknown".to_string());
            // Guard against UUIDs leaking into display (e.g. missing snapshot)
            let node_id = if looks_like_uuid(&node_id) {
                job.agent_config_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            } else {
                node_id
            };

            // Status icon
            let status = match job.status.as_str() {
                "complete" => "✓",
                "running" => "◐",
                "failed" => "✗",
                "pending" => "○",
                _ => "?",
            };

            // Resource indicators
            let has_artifact = has_artifact_for_job(conn, &job.id);
            let has_terminal = has_terminal_for_job(conn, &job.id);
            let mut indicators = String::new();
            if has_artifact {
                indicators.push_str(" 📄");
            }
            if has_terminal {
                indicators.push_str(" 🖥");
            }

            // Todo progress
            let todo = get_todo_progress(conn, &job.id)
                .map(|t| format!(" {}", t))
                .unwrap_or_default();

            output.push_str(&format!(
                "{} `cairn://{}/{}/{}/{}`[{}]{}{}\n",
                prefix, project_key, number, exec_num, node_id, status, todo, indicators
            ));

            // Child tasks — disambiguate duplicate names with -N suffix
            let child_tasks: Vec<&&DbJob> = tasks
                .iter()
                .filter(|t| t.parent_job_id.as_ref() == Some(&job.id))
                .collect();
            let mut task_name_counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for (tidx, task) in child_tasks.iter().enumerate() {
                let is_last_task = tidx == child_tasks.len() - 1;
                let task_prefix = if is_last_task { "└─" } else { "├─" };

                let base_name = task
                    .agent_config_id
                    .clone()
                    .unwrap_or_else(|| "task".to_string());
                let count = task_name_counts.entry(base_name.clone()).or_insert(0);
                *count += 1;
                let display_name = if *count == 1 {
                    base_name
                } else {
                    format!("{}-{}", base_name, count)
                };
                let task_status = match task.status.as_str() {
                    "complete" => "✓",
                    "running" => "◐",
                    "failed" => "✗",
                    _ => "○",
                };
                let task_has_artifact = has_artifact_for_job(conn, &task.id);
                let task_indicator = if task_has_artifact { " 📄" } else { "" };

                output.push_str(&format!(
                    "{}{} `.../task/{}`[{}]{}\n",
                    child_prefix, task_prefix, display_name, task_status, task_indicator
                ));
            }
        }
    }

    output
}

/// Get todo progress string like "3/5 todos"
fn get_todo_progress(conn: &mut SqliteConnection, job_id: &str) -> Option<String> {
    // Todos are stored as JSON in runs.todos column
    let todos_json: Option<String> = runs::table
        .filter(runs::job_id.eq(job_id))
        .order(runs::created_at.desc())
        .select(runs::todos)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    todos_json.and_then(|json| {
        let todos: Vec<serde_json::Value> = serde_json::from_str(&json).ok()?;
        if todos.is_empty() {
            return None;
        }
        let completed = todos
            .iter()
            .filter(|t| t.get("status").and_then(|s| s.as_str()) == Some("completed"))
            .count();
        Some(format!("{}/{} todos", completed, todos.len()))
    })
}

fn read_node(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find job by node name (looks up name in execution snapshot)
    let job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Format header
    let status_icon = match job.status.as_str() {
        "complete" => "✓",
        "running" => "◐",
        "failed" => "✗",
        "pending" => "○",
        _ => "?",
    };

    let mut output = format!("# {} [{}]\n\n", node_name, status_icon);

    // Timing info
    if let Some(started) = job.started_at {
        let started_str = chrono::DateTime::from_timestamp(started as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        output.push_str(&format!("Started: {}\n", started_str));
    }
    if let Some(completed) = job.completed_at {
        let completed_str = chrono::DateTime::from_timestamp(completed as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        output.push_str(&format!("Completed: {}\n", completed_str));
    }
    output.push('\n');

    // Build resource URIs for available sub-resources
    let base_uri = format!(
        "cairn://{}/{}/{}/{}",
        project_key, number, exec_seq, node_name
    );
    output.push_str("## Resources\n\n");
    output.push_str(&format!("- Chat: `{}/chat`\n", base_uri));

    // Check if there's an artifact
    let has_artifact = has_artifact_for_job(conn, &job.id);
    if has_artifact {
        output.push_str(&format!("- Artifact: `{}/artifact`\n", base_uri));
    }

    // Check for PR data via action_runs
    #[allow(clippy::type_complexity)]
    let pr: Option<(
        i32,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i32>,
        Option<i32>,
    )> = pr_data::table
        .inner_join(action_runs::table.on(pr_data::action_run_id.eq(action_runs::id.nullable())))
        .filter(action_runs::parent_job_id.eq(&job.id))
        .select((
            pr_data::pr_number,
            pr_data::pr_url,
            pr_data::pr_status,
            pr_data::review_decision,
            pr_data::mergeable,
            pr_data::checks_status,
            pr_data::additions,
            pr_data::deletions,
        ))
        .first(conn)
        .ok();

    if let Some((
        pr_number,
        pr_url,
        pr_status,
        review_decision,
        mergeable,
        checks_status,
        additions,
        deletions,
    )) = pr
    {
        output.push_str("\n## Pull Request\n\n");
        output.push_str(&format!("PR #{}: {}\n", pr_number, pr_url));
        output.push_str(&format!("Status: {}\n", pr_status));

        if let Some(review) = review_decision {
            output.push_str(&format!("Review: {}\n", review));
        }
        if let Some(merge) = mergeable {
            output.push_str(&format!("Mergeable: {}\n", merge));
        }
        if let Some(checks) = checks_status {
            output.push_str(&format!("Checks: {}\n", checks));
        }
        if additions.is_some() || deletions.is_some() {
            let adds = additions.unwrap_or(0);
            let dels = deletions.unwrap_or(0);
            output.push_str(&format!("Changes: +{} -{}\n", adds, dels));
        }
    }

    output
}

fn read_node_chat(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find job by node name
    let job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    let job_id = job.id;

    // Get all runs for this job
    let run_ids: Vec<String> = runs::table
        .filter(runs::job_id.eq(&job_id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    if run_ids.is_empty() {
        return "No runs found for this node.".to_string();
    }

    // Get all events for these runs, ordered by sequence
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::run_id.eq_any(&run_ids))
        .order((events::run_id.asc(), events::sequence.asc()))
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    // Format as markdown transcript with event URIs
    let base_uri = format!(
        "cairn://{}/{}/{}/{}/chat",
        project_key, number, exec_seq, node_name
    );
    format_transcript_with_context(&event_rows, &base_uri)
}

fn read_node_chat_full(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find job by node name
    let job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    let job_id = job.id;

    // Get all runs for this job
    let run_ids: Vec<String> = runs::table
        .filter(runs::job_id.eq(&job_id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    if run_ids.is_empty() {
        return "No runs found for this node.".to_string();
    }

    // Get all events for these runs, ordered by sequence
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::run_id.eq_any(&run_ids))
        .order((events::run_id.asc(), events::sequence.asc()))
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    // Format as full markdown transcript (no truncation)
    format_transcript_full(&event_rows)
}

fn read_node_artifact(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find job by node name
    let job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Get artifact for this job (or child jobs)
    match get_artifact_for_job(conn, &job.id) {
        Some(a) => a.data,
        None => format!(
            "No artifact found for node '{}' in issue {}-{}",
            node_name, project_key, number
        ),
    }
}

fn read_task_chat(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find the parent job by node name
    let parent_job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Find the task job by name (with potential -N suffix disambiguation)
    let task_job = match find_task_by_name(conn, &parent_job.id, task_name) {
        Ok(j) => j,
        Err(e) => return e,
    };

    // Get all runs for this task job
    let run_ids: Vec<String> = runs::table
        .filter(runs::job_id.eq(&task_job.id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    if run_ids.is_empty() {
        return "No runs found for this task.".to_string();
    }

    // Get all events for these runs
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::run_id.eq_any(&run_ids))
        .order((events::run_id.asc(), events::sequence.asc()))
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    // Format as markdown transcript with event URIs
    let base_uri = format!(
        "cairn://{}/{}/{}/{}/task/{}/chat",
        project_key, number, exec_seq, node_name, task_name
    );
    format_transcript_with_context(&event_rows, &base_uri)
}

fn read_task_chat_full(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find the parent job by node name
    let parent_job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Find the task job by name (with potential -N suffix disambiguation)
    let task_job = match find_task_by_name(conn, &parent_job.id, task_name) {
        Ok(j) => j,
        Err(e) => return e,
    };

    // Get all runs for this task job
    let run_ids: Vec<String> = runs::table
        .filter(runs::job_id.eq(&task_job.id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    if run_ids.is_empty() {
        return "No runs found for this task.".to_string();
    }

    // Get all events for these runs
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::run_id.eq_any(&run_ids))
        .order((events::run_id.asc(), events::sequence.asc()))
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    // Format as full markdown transcript (no truncation)
    format_transcript_full(&event_rows)
}

fn read_task_artifact(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find the parent job by node name
    let parent_job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Find the task job by name (with potential -N suffix disambiguation)
    let task_job = match find_task_by_name(conn, &parent_job.id, task_name) {
        Ok(j) => j,
        Err(e) => return e,
    };

    // Get artifact for this task
    let artifact: Result<DbArtifact, _> = artifacts::table
        .filter(artifacts::job_id.eq(&task_job.id))
        .order(artifacts::version.desc())
        .first(conn);

    match artifact {
        Ok(a) => a.data,
        Err(_) => format!(
            "No artifact found for task '{}' in node '{}' of issue {}-{}",
            task_name, node_name, project_key, number
        ),
    }
}

fn read_node_chat_event(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    run_seq: i32,
    event_seq: i32,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find job by node name
    let job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Get the Nth run (1-indexed run_seq)
    let run_id = match get_nth_run_id(conn, &job.id, run_seq) {
        Some(id) => id,
        None => return format!("Run {} not found for node '{}'", run_seq, node_name),
    };

    // Get the specific event
    get_single_event(conn, &run_id, event_seq)
}

#[allow(clippy::too_many_arguments)]
fn read_task_chat_event(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    run_seq: i32,
    event_seq: i32,
) -> String {
    // Get project and issue
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Find the parent job by node name
    let parent_job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    // Find the task job by name
    let task_job = match find_task_by_name(conn, &parent_job.id, task_name) {
        Ok(j) => j,
        Err(e) => return e,
    };

    // Get the Nth run (1-indexed run_seq)
    let run_id = match get_nth_run_id(conn, &task_job.id, run_seq) {
        Some(id) => id,
        None => {
            return format!(
                "Run {} not found for task '{}' in node '{}'",
                run_seq, task_name, node_name
            )
        }
    };

    // Get the specific event
    get_single_event(conn, &run_id, event_seq)
}

/// Get the Nth run for a job (1-indexed)
fn get_nth_run_id(conn: &mut SqliteConnection, job_id: &str, run_seq: i32) -> Option<String> {
    if run_seq < 1 {
        return None;
    }

    let run_ids: Vec<String> = runs::table
        .filter(runs::job_id.eq(job_id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    run_ids.into_iter().nth((run_seq - 1) as usize)
}

/// Get a single event by run_id and sequence, returning full formatted content
fn get_single_event(conn: &mut SqliteConnection, run_id: &str, event_seq: i32) -> String {
    let event: Result<(String, String), _> = events::table
        .filter(events::run_id.eq(run_id))
        .filter(events::sequence.eq(event_seq))
        .select((events::event_type, events::data))
        .first(conn);

    match event {
        Ok((event_type, data)) => format_single_event(&event_type, &data),
        Err(_) => format!("Event with sequence {} not found", event_seq),
    }
}

/// Format a single event with full content (no truncation)
fn format_single_event(event_type: &str, data: &str) -> String {
    let event_data: serde_json::Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(_) => return "Error parsing event data".to_string(),
    };

    let mut output = String::new();

    match event_type {
        "assistant" => {
            if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    output.push_str("**Assistant:** ");
                    output.push_str(content);
                    output.push_str("\n\n");
                }
            }

            // Handle tool_uses (tool calls)
            if let Some(tool_uses) = event_data.get("toolUses").and_then(|t| t.as_array()) {
                for tool in tool_uses {
                    let name = tool
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown");
                    let input = tool
                        .get("input")
                        .map(|i| {
                            if i.is_string() {
                                i.as_str().unwrap_or("").to_string()
                            } else {
                                serde_json::to_string_pretty(i).unwrap_or_default()
                            }
                        })
                        .unwrap_or_default();

                    output.push_str(&format!("**Tool Call ({}):**\n", name));
                    output.push_str(&input);
                    output.push_str("\n\n");
                }
            }
        }
        "user" => {
            if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    output.push_str("**User:** ");
                    output.push_str(content);
                    output.push_str("\n\n");
                }
            }
        }
        "result" => {
            if let Some(tool_name) = event_data.get("toolName").and_then(|t| t.as_str()) {
                if let Some(result) = event_data.get("toolResult").and_then(|r| r.as_str()) {
                    output.push_str(&format!("**Tool Result ({}):**\n", tool_name));
                    output.push_str(result);
                    output.push_str("\n\n");
                }
            }
        }
        _ => {
            // Return raw JSON for other event types
            output.push_str(&format!("**Event ({}):**\n", event_type));
            output.push_str(
                &serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| data.to_string()),
            );
        }
    }

    if output.is_empty() {
        "No content in this event.".to_string()
    } else {
        output
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Get node name from execution snapshot (node_id is the UUID)
fn get_node_name_from_execution(
    conn: &mut SqliteConnection,
    execution_id: &str,
    node_id: &str,
) -> Option<String> {
    let snapshot_json: Option<String> = executions::table
        .filter(executions::id.eq(execution_id))
        .select(executions::snapshot)
        .first(conn)
        .ok()?;

    let snapshot_json = snapshot_json?;
    let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json).ok()?;

    snapshot
        .recipe
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.name.clone())
}

/// Check if a string looks like a UUID (8-4-4-4-12 hex pattern)
fn looks_like_uuid(s: &str) -> bool {
    s.len() == 36
        && s.chars().filter(|c| *c == '-').count() == 4
        && s.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

/// Check if a job or any of its child jobs have an artifact
fn has_artifact_for_job(conn: &mut SqliteConnection, job_id: &str) -> bool {
    // Check the job itself
    let direct: bool = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .select(artifacts::id)
        .first::<String>(conn)
        .is_ok();

    if direct {
        return true;
    }

    // Check child jobs - get child job IDs first, then check for artifacts
    let child_job_ids: Vec<String> = jobs::table
        .filter(jobs::parent_job_id.eq(job_id))
        .select(jobs::id)
        .load(conn)
        .unwrap_or_default();

    if child_job_ids.is_empty() {
        return false;
    }

    artifacts::table
        .filter(artifacts::job_id.eq_any(&child_job_ids))
        .select(artifacts::id)
        .first::<String>(conn)
        .is_ok()
}

/// Check if a job has any terminals
fn has_terminal_for_job(conn: &mut SqliteConnection, job_id: &str) -> bool {
    job_terminals::table
        .filter(job_terminals::job_id.eq(job_id))
        .select(job_terminals::id)
        .first::<String>(conn)
        .is_ok()
}

/// Get artifact for a job (checking job itself and child jobs)
fn get_artifact_for_job(conn: &mut SqliteConnection, job_id: &str) -> Option<DbArtifact> {
    // Try the job itself first
    if let Ok(artifact) = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .order(artifacts::version.desc())
        .first::<DbArtifact>(conn)
    {
        return Some(artifact);
    }

    // Try child jobs - get child job IDs first, then get artifact
    let child_job_ids: Vec<String> = jobs::table
        .filter(jobs::parent_job_id.eq(job_id))
        .select(jobs::id)
        .load(conn)
        .unwrap_or_default();

    if child_job_ids.is_empty() {
        return None;
    }

    artifacts::table
        .filter(artifacts::job_id.eq_any(&child_job_ids))
        .order(artifacts::version.desc())
        .first(conn)
        .ok()
}

/// Find a task job by name, handling disambiguation suffix
/// e.g., "Explore" finds the first Explore task, "Explore-2" finds the second
fn find_task_by_name(
    conn: &mut SqliteConnection,
    parent_job_id: &str,
    task_name: &str,
) -> Result<DbJob, String> {
    // Parse task name to extract base name and optional index
    // e.g., "Explore-2" → ("Explore", 2), "Explore" → ("Explore", 1)
    let (base_name, target_index) = parse_task_name_with_index(task_name);

    // Get all child jobs with this base agent_config_id, ordered by creation
    let matching_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::parent_job_id.eq(parent_job_id))
        .filter(jobs::agent_config_id.eq(&base_name))
        .order(jobs::created_at.asc())
        .load(conn)
        .unwrap_or_default();

    if matching_jobs.is_empty() {
        return Err(format!("Task '{}' not found", task_name));
    }

    // Get the Nth occurrence (1-indexed)
    matching_jobs
        .into_iter()
        .nth(target_index - 1)
        .ok_or_else(|| {
            format!(
                "Task '{}' not found (only {} tasks with name '{}')",
                task_name,
                target_index - 1,
                base_name
            )
        })
}

/// Parse a task name that may have a disambiguation suffix
/// e.g., "Explore-2" → ("Explore", 2), "Explore" → ("Explore", 1)
fn parse_task_name_with_index(task_name: &str) -> (String, usize) {
    // Try to find a -N suffix where N is a number
    if let Some(dash_pos) = task_name.rfind('-') {
        let suffix = &task_name[dash_pos + 1..];
        if let Ok(index) = suffix.parse::<usize>() {
            if index > 0 {
                return (task_name[..dash_pos].to_string(), index);
            }
        }
    }
    // No valid suffix, treat as first occurrence
    (task_name.to_string(), 1)
}

/// Get the execution_id for a given exec_seq (1-based index stored in seq column)
fn get_execution_id_for_seq(
    conn: &mut SqliteConnection,
    issue_id: &str,
    exec_seq: i32,
) -> Option<String> {
    executions::table
        .filter(executions::issue_id.eq(issue_id))
        .filter(executions::seq.eq(exec_seq))
        .select(executions::id)
        .first(conn)
        .ok()
}

/// Find a job by node name (looking up name in execution snapshot)
/// exec_seq is required and identifies the specific execution to search
fn find_job_by_node_name(
    conn: &mut SqliteConnection,
    issue_id: &str,
    node_name: &str,
    exec_seq: i32,
) -> Option<DbJob> {
    // Resolve exec_seq to an execution_id
    let exec_id = get_execution_id_for_seq(conn, issue_id, exec_seq)?;

    // Get all jobs for this execution
    let job_rows: Vec<DbJob> = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .filter(jobs::execution_id.eq(&exec_id))
        .filter(jobs::recipe_node_id.is_not_null())
        .order(jobs::created_at.desc()) // Most recent first
        .load(conn)
        .ok()?;

    // Find the job whose node name matches
    for job in job_rows {
        if let Some(ref recipe_node_id) = &job.recipe_node_id {
            if let Some(name) = get_node_name_from_execution(conn, &exec_id, recipe_node_id) {
                if name == node_name {
                    return Some(job);
                }
            }
        }
    }

    None
}

// Truncation thresholds
const TOOL_INPUT_THRESHOLD: usize = 500;
const TOOL_INPUT_PREVIEW: usize = 200;
const TOOL_RESULT_THRESHOLD: usize = 500;
const TOOL_RESULT_PREVIEW: usize = 200;
const ASSISTANT_CONTENT_THRESHOLD: usize = 2000;
const ASSISTANT_CONTENT_PREVIEW: usize = 500;

/// Truncate text and return (display_text, was_truncated, original_len)
fn truncate_with_info(text: &str, threshold: usize, preview_len: usize) -> (String, bool, usize) {
    let original_len = text.len();
    if original_len <= threshold {
        (text.to_string(), false, original_len)
    } else {
        // Find a safe truncation point (don't break in middle of multi-byte char)
        let safe_len = text
            .char_indices()
            .take_while(|(i, _)| *i < preview_len)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        (format!("{}...", &text[..safe_len]), true, original_len)
    }
}

/// Context for generating event URIs during transcript formatting
struct TranscriptContext<'a> {
    /// Base URI like "cairn://CAIRN/123/builder-1/chat"
    base_uri: &'a str,
    /// Map from run_id to 1-indexed run sequence
    run_seq_map: std::collections::HashMap<&'a str, i32>,
}

impl<'a> TranscriptContext<'a> {
    fn new(base_uri: &'a str, events: &'a [(String, i32, String, String)]) -> Self {
        // Build map of run_id -> sequence number (1-indexed)
        let mut seen_runs: Vec<&str> = Vec::new();
        for (run_id, _, _, _) in events {
            if !seen_runs.contains(&run_id.as_str()) {
                seen_runs.push(run_id);
            }
        }

        let run_seq_map: std::collections::HashMap<&str, i32> = seen_runs
            .into_iter()
            .enumerate()
            .map(|(i, run_id)| (run_id, (i + 1) as i32))
            .collect();

        TranscriptContext {
            base_uri,
            run_seq_map,
        }
    }

    /// Get event URI for a specific event
    fn event_uri(&self, run_id: &str, event_seq: i32) -> String {
        let run_seq = self.run_seq_map.get(run_id).copied().unwrap_or(1);
        format!("{}/{}/{}", self.base_uri, run_seq, event_seq)
    }
}

/// Format events into a markdown transcript with truncation
fn format_transcript_with_context(
    events: &[(String, i32, String, String)],
    base_uri: &str,
) -> String {
    let ctx = TranscriptContext::new(base_uri, events);
    let mut transcript = String::new();

    for (run_id, seq, event_type, data) in events {
        // Parse the event data as JSON
        let event_data: serde_json::Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        match event_type.as_str() {
            "assistant" => {
                // Assistant message - content is directly in the event
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        let (display, truncated, original_len) = truncate_with_info(
                            content,
                            ASSISTANT_CONTENT_THRESHOLD,
                            ASSISTANT_CONTENT_PREVIEW,
                        );
                        transcript.push_str("**Assistant:** ");
                        transcript.push_str(&display);
                        if truncated {
                            transcript.push_str(&format!(
                                "\n[Truncated - {} chars] → {}\n\n",
                                original_len,
                                ctx.event_uri(run_id, *seq)
                            ));
                        } else {
                            transcript.push_str("\n\n");
                        }
                    }
                }

                // Handle tool_uses (tool calls)
                if let Some(tool_uses) = event_data.get("toolUses").and_then(|t| t.as_array()) {
                    for tool in tool_uses {
                        let name = tool
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("unknown");
                        let input = tool
                            .get("input")
                            .map(|i| {
                                if i.is_string() {
                                    i.as_str().unwrap_or("").to_string()
                                } else {
                                    serde_json::to_string(i).unwrap_or_default()
                                }
                            })
                            .unwrap_or_default();

                        let (display, truncated, original_len) =
                            truncate_with_info(&input, TOOL_INPUT_THRESHOLD, TOOL_INPUT_PREVIEW);

                        transcript.push_str(&format!("**Tool Call ({}):** ", name));
                        transcript.push_str(&display);
                        if truncated {
                            transcript.push_str(&format!(
                                "\n[Input truncated - {} chars] → {}\n\n",
                                original_len,
                                ctx.event_uri(run_id, *seq)
                            ));
                        } else {
                            transcript.push_str("\n\n");
                        }
                    }
                }
            }
            "user" => {
                // User message - content is directly in the event
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        transcript.push_str("**User:** ");
                        transcript.push_str(content);
                        transcript.push_str("\n\n");
                    }
                }
            }
            "result" => {
                // Tool result
                if let Some(tool_name) = event_data.get("toolName").and_then(|t| t.as_str()) {
                    if let Some(result) = event_data.get("toolResult").and_then(|r| r.as_str()) {
                        let (display, truncated, original_len) =
                            truncate_with_info(result, TOOL_RESULT_THRESHOLD, TOOL_RESULT_PREVIEW);

                        transcript.push_str(&format!("**Tool Result ({}):** ", tool_name));
                        transcript.push_str(&display);
                        if truncated {
                            transcript.push_str(&format!(
                                "\n[Truncated - {} chars] → {}\n\n",
                                original_len,
                                ctx.event_uri(run_id, *seq)
                            ));
                        } else {
                            transcript.push_str("\n\n");
                        }
                    }
                }
            }
            _ => {
                // Skip other event types for now
            }
        }
    }

    if transcript.is_empty() {
        "No conversation content found.".to_string()
    } else {
        transcript
    }
}

/// Format events into a full markdown transcript without truncation
fn format_transcript_full(events: &[(String, i32, String, String)]) -> String {
    let mut transcript = String::new();

    for (_run_id, _seq, event_type, data) in events {
        // Parse the event data as JSON
        let event_data: serde_json::Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        match event_type.as_str() {
            "assistant" => {
                // Assistant message - content is directly in the event
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        transcript.push_str("**Assistant:** ");
                        transcript.push_str(content);
                        transcript.push_str("\n\n");
                    }
                }

                // Handle tool_uses (tool calls) - full input
                if let Some(tool_uses) = event_data.get("toolUses").and_then(|t| t.as_array()) {
                    for tool in tool_uses {
                        let name = tool
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("unknown");
                        let input = tool
                            .get("input")
                            .map(|i| {
                                if i.is_string() {
                                    i.as_str().unwrap_or("").to_string()
                                } else {
                                    serde_json::to_string_pretty(i).unwrap_or_default()
                                }
                            })
                            .unwrap_or_default();

                        transcript.push_str(&format!("**Tool Call ({}):**\n", name));
                        transcript.push_str(&input);
                        transcript.push_str("\n\n");
                    }
                }
            }
            "user" => {
                // User message - content is directly in the event
                if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        transcript.push_str("**User:** ");
                        transcript.push_str(content);
                        transcript.push_str("\n\n");
                    }
                }
            }
            "result" => {
                // Tool result - full content
                if let Some(tool_name) = event_data.get("toolName").and_then(|t| t.as_str()) {
                    if let Some(result) = event_data.get("toolResult").and_then(|r| r.as_str()) {
                        transcript.push_str(&format!("**Tool Result ({}):**\n", tool_name));
                        transcript.push_str(result);
                        transcript.push_str("\n\n");
                    }
                }
            }
            _ => {
                // Skip other event types for now
            }
        }
    }

    if transcript.is_empty() {
        "No conversation content found.".to_string()
    } else {
        transcript
    }
}

// ============================================================================
// New Resource Readers (Project, ProjectChat)
// ============================================================================

/// Read project overview with list of issues and terminals
fn read_project(conn: &mut SqliteConnection, project_key: &str) -> String {
    use crate::schema::{job_terminals, projects};

    // Get project
    let project: Result<(String, String, Option<String>), _> = projects::table
        .filter(projects::key.eq(project_key.to_uppercase()))
        .select((projects::id, projects::name, projects::context))
        .first(conn);

    let (project_id, project_name, project_context): (String, String, Option<String>) =
        match project {
            Ok(p) => p,
            Err(_) => return format!("Project '{}' not found", project_key),
        };

    // Get recent issues for this project
    let project_issues: Vec<(i32, String, String, Option<String>, i32)> = issues::table
        .filter(issues::project_id.eq(&project_id))
        .order(issues::created_at.desc())
        .limit(50)
        .select((
            issues::number,
            issues::title,
            issues::status,
            issues::description,
            issues::created_at,
        ))
        .load(conn)
        .unwrap_or_default();

    // Get project-level terminals (not job terminals)
    let terminals: Vec<(String, Option<String>, Option<String>, String)> = job_terminals::table
        .filter(job_terminals::project_id.eq(&project_id))
        .filter(job_terminals::job_id.is_null())
        .order(job_terminals::created_at.desc())
        .select((
            job_terminals::slug.assume_not_null(),
            job_terminals::title,
            job_terminals::description,
            job_terminals::status,
        ))
        .load(conn)
        .unwrap_or_default();

    // Format as markdown
    let mut output = format!("# {}\n\n", project_name);

    if let Some(ctx) = project_context {
        if !ctx.is_empty() {
            output.push_str(&ctx);
            output.push_str("\n\n");
        }
    }

    // Project terminals section (if any)
    if !terminals.is_empty() {
        output.push_str("## Terminals\n\n");
        for (slug, title, _description, status) in &terminals {
            let status_icon = match status.as_str() {
                "running" => "🟢",
                "exited" => "⚫",
                _ => "○",
            };
            let display_name = title.as_ref().unwrap_or(slug);
            output.push_str(&format!(
                "- `cairn://{}/terminal/{}` {} {}\n",
                project_key.to_uppercase(),
                slug,
                status_icon,
                display_name
            ));
        }
        output.push('\n');
    }

    output.push_str("## Issues\n\n");

    if project_issues.is_empty() {
        output.push_str("No issues found.\n");
    } else {
        for (number, title, status, _description, _created_at) in project_issues {
            let status_indicator = match status.as_str() {
                "Active" => "◐",
                "Merged" => "✓",
                "Closed" => "✗",
                "Waiting" => "⏳",
                _ => "○", // Backlog
            };
            output.push_str(&format!(
                "- {}-{} [{}] {}\n",
                project_key.to_uppercase(),
                number,
                status_indicator,
                title
            ));
        }
    }

    output
}

/// Read project chat session
fn read_project_chat(conn: &mut SqliteConnection, project_key: &str, chat_name: &str) -> String {
    use crate::schema::{chats, projects};

    // Get project
    let project_id: Result<String, _> = projects::table
        .filter(projects::key.eq(project_key.to_uppercase()))
        .select(projects::id)
        .first(conn);

    let project_id = match project_id {
        Ok(id) => id,
        Err(_) => return format!("Project '{}' not found", project_key),
    };

    // Find the chat by ID (chat_name is the chat ID)
    let chat: Result<(String, String, i32), _> = chats::table
        .filter(chats::project_id.eq(&project_id))
        .filter(chats::id.eq(chat_name))
        .select((chats::id, chats::status, chats::updated_at))
        .first(conn);

    let (chat_id, status, updated_at): (String, String, i32) = match chat {
        Ok(c) => c,
        Err(_) => {
            return format!(
                "Chat '{}' not found in project '{}'",
                chat_name, project_key
            )
        }
    };

    // Get events for this chat (via runs)
    let chat_events: Vec<(String, i32, String, String)> = runs::table
        .filter(runs::chat_id.eq(&chat_id))
        .inner_join(events::table)
        .order((runs::created_at.asc(), events::sequence.asc()))
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    // Format output
    let mut output = format!("# Project Chat: {}\n\n", chat_id);
    output.push_str(&format!("Status: {}\n", status));
    output.push_str(&format!(
        "Last updated: {}\n\n",
        chrono::DateTime::from_timestamp(updated_at as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    ));

    if chat_events.is_empty() {
        output.push_str("No messages in this chat yet.\n");
    } else {
        output.push_str("## Conversation\n\n");
        let base_uri = format!("cairn://{}/chat/{}", project_key.to_uppercase(), chat_name);
        output.push_str(&format_transcript_with_context(&chat_events, &base_uri));
    }

    output
}

// ============================================================================
// File Changes Readers
// ============================================================================

/// Read all file changes for an issue (aggregated across all jobs)
fn read_issue_files(conn: &mut SqliteConnection, project_key: &str, number: i32) -> String {
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    // Get all jobs for this issue
    let job_ids: Vec<String> = jobs::table
        .filter(jobs::issue_id.eq(&issue_id))
        .select(jobs::id)
        .load(conn)
        .unwrap_or_default();

    if job_ids.is_empty() {
        return format!("No jobs found for issue {}-{}", project_key, number);
    }

    // Get file changes with agent name, grouped by job
    #[allow(clippy::type_complexity)]
    let results: Vec<(
        String,
        String,
        Option<i32>,
        Option<i32>,
        Option<String>,
        String,
    )> = file_changes::table
        .inner_join(jobs::table.on(file_changes::job_id.eq(jobs::id)))
        .filter(file_changes::job_id.eq_any(&job_ids))
        .select((
            file_changes::file_path,
            file_changes::status,
            file_changes::additions,
            file_changes::deletions,
            file_changes::previous_path,
            jobs::agent_config_id.assume_not_null(),
        ))
        .order((jobs::created_at.asc(), file_changes::file_path.asc()))
        .load(conn)
        .unwrap_or_default();

    if results.is_empty() {
        return format!(
            "No file changes recorded for issue {}-{}",
            project_key, number
        );
    }

    let mut output = format!("# Files Changed - {}-{}\n\n", project_key, number);
    let mut current_agent = String::new();

    for (file_path, status, additions, deletions, previous_path, agent_name) in &results {
        if *agent_name != current_agent {
            current_agent.clone_from(agent_name);
            output.push_str(&format!("\n## {}\n\n", agent_name));
            output.push_str("| File | Status | +/- |\n");
            output.push_str("|------|--------|-----|\n");
        }

        let changes = match (additions, deletions) {
            (Some(a), Some(d)) => format!("+{} -{}", a, d),
            _ => "-".to_string(),
        };

        let file_display = if let Some(prev) = previous_path {
            format!("{} → {}", prev, file_path)
        } else {
            file_path.clone()
        };

        output.push_str(&format!(
            "| {} | {} | {} |\n",
            file_display, status, changes
        ));
    }

    output
}

/// Read file changes for a specific node (job)
fn read_node_files(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let project_ctx = match lookup_project_by_key(conn, project_key) {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    let issue_id: Result<String, _> = issues::table
        .filter(issues::project_id.eq(&project_ctx.project_id))
        .filter(issues::number.eq(number))
        .select(issues::id)
        .first(conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", project_key, number),
    };

    let job = match find_job_by_node_name(conn, &issue_id, node_name, exec_seq) {
        Some(j) => j,
        None => {
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        }
    };

    #[allow(clippy::type_complexity)]
    let results: Vec<(String, String, Option<i32>, Option<i32>, Option<String>)> =
        file_changes::table
            .filter(file_changes::job_id.eq(&job.id))
            .select((
                file_changes::file_path,
                file_changes::status,
                file_changes::additions,
                file_changes::deletions,
                file_changes::previous_path,
            ))
            .order(file_changes::file_path.asc())
            .load(conn)
            .unwrap_or_default();

    if results.is_empty() {
        return format!(
            "No file changes recorded for node '{}' in issue {}-{}",
            node_name, project_key, number
        );
    }

    let mut output = format!(
        "# Files Changed - {}-{} / {}\n\n",
        project_key, number, node_name
    );
    output.push_str("| File | Status | +/- |\n");
    output.push_str("|------|--------|-----|\n");

    for (file_path, status, additions, deletions, previous_path) in &results {
        let changes = match (additions, deletions) {
            (Some(a), Some(d)) => format!("+{} -{}", a, d),
            _ => "-".to_string(),
        };

        let file_display = if let Some(prev) = previous_path {
            format!("{} → {}", prev, file_path)
        } else {
            file_path.clone()
        };

        output.push_str(&format!(
            "| {} | {} | {} |\n",
            file_display, status, changes
        ));
    }

    output
}

// ============================================================================
// Message Resource Readers
// ============================================================================

/// Parse query params from a raw URI string.
/// Returns (before, after, since, limit).
fn parse_message_query_params(
    uri: &str,
) -> (Option<String>, Option<String>, Option<i64>, Option<i64>) {
    let query_str = match uri.find('?') {
        Some(idx) => &uri[idx + 1..],
        None => return (None, None, None, None),
    };

    let mut before = None;
    let mut after = None;
    let mut since = None;
    let mut limit = None;

    for pair in query_str.split('&') {
        let mut kv = pair.splitn(2, '=');
        let key = kv.next().unwrap_or("");
        let val = kv.next().unwrap_or("");
        match key {
            "before" => before = Some(val.to_string()),
            "after" => after = Some(val.to_string()),
            "since" => since = val.parse().ok(),
            "limit" => limit = val.parse().ok(),
            _ => {}
        }
    }

    (before, after, since, limit)
}

fn format_messages(messages: &[crate::models::Message]) -> String {
    if messages.is_empty() {
        return "(no messages)".to_string();
    }

    let mut out = String::new();
    for msg in messages {
        let ts = chrono::DateTime::from_timestamp(msg.created_at, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| msg.created_at.to_string());
        out.push_str(&format!("[{}] {}: {}\n", ts, msg.sender_name, msg.content));
    }
    out
}

fn read_project_messages(conn: &mut SqliteConnection, project_key: &str, raw_uri: &str) -> String {
    // Validate project exists
    if lookup_project_by_key(conn, project_key).is_err() {
        return format!("Project '{}' not found", project_key);
    }

    let (before, after, since, limit) = parse_message_query_params(raw_uri);

    match crate::messages::db::query_channel(
        conn,
        &crate::models::ChannelType::Project,
        Some(project_key),
        before.as_deref(),
        after.as_deref(),
        since,
        limit,
    ) {
        Ok(messages) => {
            let mut out = format!("# Messages — {}\n\n", project_key);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_messages(&messages));
            out
        }
        Err(e) => format!("Failed to query messages: {}", e),
    }
}

fn read_issue_messages(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    raw_uri: &str,
) -> String {
    // Use KEY/NUMBER as channel_id (matches how messages are stored)
    let issue_key = format!("{}/{}", project_key, number);

    let (before, after, since, limit) = parse_message_query_params(raw_uri);

    match crate::messages::db::query_channel(
        conn,
        &crate::models::ChannelType::Issue,
        Some(&issue_key),
        before.as_deref(),
        after.as_deref(),
        since,
        limit,
    ) {
        Ok(messages) => {
            let mut out = format!("# Messages — {}-{}\n\n", project_key, number);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_messages(&messages));
            out
        }
        Err(e) => format!("Failed to query messages: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_with_info_no_truncation() {
        let (result, truncated, len) = truncate_with_info("short text", 100, 50);
        assert_eq!(result, "short text");
        assert!(!truncated);
        assert_eq!(len, 10);
    }

    #[test]
    fn test_truncate_with_info_with_truncation() {
        let long_text = "a".repeat(600);
        let (result, truncated, len) = truncate_with_info(&long_text, 500, 200);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 203); // 200 chars + "..."
        assert!(truncated);
        assert_eq!(len, 600);
    }

    #[test]
    fn test_truncate_with_info_unicode_safe() {
        // Test with multi-byte characters
        let text = "🎉".repeat(100); // 400 bytes (4 bytes per emoji)
        let (result, truncated, len) = truncate_with_info(&text, 100, 50);
        // Should truncate at a valid character boundary
        assert!(result.ends_with("..."));
        assert!(truncated);
        assert_eq!(len, 400);
        // Verify the result is valid UTF-8 (would panic otherwise)
        assert!(result.chars().all(|c| c.is_ascii() || c == '🎉'));
    }

    #[test]
    fn test_transcript_context_run_seq_map() {
        let events = vec![
            (
                "run-a".to_string(),
                1,
                "assistant".to_string(),
                "{}".to_string(),
            ),
            (
                "run-a".to_string(),
                2,
                "result".to_string(),
                "{}".to_string(),
            ),
            (
                "run-b".to_string(),
                1,
                "assistant".to_string(),
                "{}".to_string(),
            ),
            (
                "run-a".to_string(),
                3,
                "assistant".to_string(),
                "{}".to_string(),
            ),
        ];
        let ctx = TranscriptContext::new("cairn://CAIRN/123/node/chat", &events);

        // run-a should be 1 (first seen), run-b should be 2 (second seen)
        assert_eq!(ctx.run_seq_map.get("run-a"), Some(&1));
        assert_eq!(ctx.run_seq_map.get("run-b"), Some(&2));
    }

    #[test]
    fn test_transcript_context_event_uri() {
        let events = vec![(
            "run-a".to_string(),
            5,
            "assistant".to_string(),
            "{}".to_string(),
        )];
        let ctx = TranscriptContext::new("cairn://CAIRN/123/node/chat", &events);

        assert_eq!(ctx.event_uri("run-a", 5), "cairn://CAIRN/123/node/chat/1/5");
    }

    #[test]
    fn test_format_single_event_assistant() {
        let data = r#"{"content": "Hello, I can help you with that."}"#;
        let result = format_single_event("assistant", data);
        assert!(result.contains("**Assistant:**"));
        assert!(result.contains("Hello, I can help you with that."));
    }

    #[test]
    fn test_format_single_event_tool_result() {
        let data = r#"{"toolName": "read", "toolResult": "file contents here"}"#;
        let result = format_single_event("result", data);
        assert!(result.contains("**Tool Result (read):**"));
        assert!(result.contains("file contents here"));
    }

    #[test]
    fn test_format_single_event_with_tool_uses() {
        let data = r#"{"content": "Let me read that file.", "toolUses": [{"name": "read", "input": {"file": "test.txt"}}]}"#;
        let result = format_single_event("assistant", data);
        assert!(result.contains("**Assistant:**"));
        assert!(result.contains("**Tool Call (read):**"));
    }

    #[test]
    fn test_format_transcript_full_no_truncation() {
        // Create a long message that would be truncated in regular transcript
        let long_content = "a".repeat(3000);
        let events = vec![(
            "run-1".to_string(),
            1,
            "assistant".to_string(),
            format!(r#"{{"content": "{}"}}"#, long_content),
        )];
        let result = format_transcript_full(&events);
        // Should contain the full content without truncation
        assert!(result.contains(&long_content));
        assert!(!result.contains("[Truncated"));
    }
}
