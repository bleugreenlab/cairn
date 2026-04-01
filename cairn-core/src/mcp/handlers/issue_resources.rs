//! Issue resource MCP handlers.
//!
//! Provides read_issue_resource handler for cairn://* URIs.

use diesel::prelude::*;
use serde::Deserialize;

use crate::config::slugify;
use crate::diesel_models::{DbArtifact, DbJob};
use crate::mcp::types::McpCallbackRequest;
use crate::models::ExecutionSnapshot;
use crate::node_segments::visible_node_segment;
use crate::orchestrator::Orchestrator;
use crate::schema::{
    artifacts, comments, events, executions, file_changes, issues, job_terminals, jobs,
    merge_requests, runs, turns,
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
        CairnResource::NodeChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            turn_seq,
        } => read_node_chat_turn(&mut conn, &project, number, exec_seq, &node_id, turn_seq),
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
        CairnResource::TaskChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            turn_seq,
        } => read_task_chat_turn(
            &mut conn, &project, number, exec_seq, &node_id, &task_name, turn_seq,
        ),
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

fn visible_job_node_segment(conn: &mut SqliteConnection, job: &DbJob) -> String {
    let resolved_node_name = job.node_name.clone().or_else(|| {
        job.recipe_node_id.as_ref().and_then(|rid| {
            job.execution_id
                .as_deref()
                .and_then(|eid| get_node_name_from_execution(conn, eid, rid))
        })
    });

    visible_node_segment(job.recipe_node_id.as_deref(), resolved_node_name.as_deref())
        .or(resolved_node_name)
        .or_else(|| job.agent_config_id.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

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

    // Get PR data for this issue (via merge_requests)
    let pr: Option<(Option<i32>, Option<String>, String)> = merge_requests::table
        .filter(merge_requests::issue_id.eq(&issue_id))
        .select((
            merge_requests::github_pr_number,
            merge_requests::github_pr_url,
            merge_requests::status,
        ))
        .order(merge_requests::opened_at.desc())
        .first(conn)
        .ok();

    if let Some((Some(pr_number), Some(pr_url), pr_status)) = pr {
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

            let node_segment = visible_job_node_segment(conn, job);

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
                prefix, project_key, number, exec_num, node_segment, status, todo, indicators
            ));

            // Child tasks — use canonical node_name from DB (already disambiguated at creation)
            let child_tasks: Vec<&&DbJob> = tasks
                .iter()
                .filter(|t| t.parent_job_id.as_ref() == Some(&job.id))
                .collect();
            for (tidx, task) in child_tasks.iter().enumerate() {
                let is_last_task = tidx == child_tasks.len() - 1;
                let task_prefix = if is_last_task { "└─" } else { "├─" };

                let display_name = task
                    .node_name
                    .clone()
                    .or_else(|| task.agent_config_id.clone())
                    .unwrap_or_else(|| "task".to_string());
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
    crate::todos::get_todo_progress(conn, job_id)
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

    let visible_node_segment = visible_job_node_segment(conn, &job);

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
        project_key, number, exec_seq, visible_node_segment
    );
    output.push_str("## Resources\n\n");
    output.push_str(&format!("- Chat: `{}/chat`\n", base_uri));

    // Check if there's an artifact
    let has_artifact = has_artifact_for_job(conn, &job.id);
    if has_artifact {
        output.push_str(&format!("- Artifact: `{}/artifact`\n", base_uri));
    }

    // Check for PR data via merge_requests
    #[allow(clippy::type_complexity)]
    let pr: Option<(
        Option<i32>,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i32>,
        Option<i32>,
    )> = merge_requests::table
        .filter(merge_requests::job_id.eq(&job.id))
        .select((
            merge_requests::github_pr_number,
            merge_requests::github_pr_url,
            merge_requests::status,
            merge_requests::github_review,
            merge_requests::github_mergeable,
            merge_requests::checks_status,
            merge_requests::additions,
            merge_requests::deletions,
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
        if let (Some(pr_num), Some(url)) = (pr_number, pr_url) {
            output.push_str("\n## Pull Request\n\n");
            output.push_str(&format!("PR #{}: {}\n", pr_num, url));
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
    }

    // Todos section
    if let Ok(todos) = crate::todos::get_todos_for_job(conn, &job.id) {
        if !todos.is_empty() {
            let completed = todos.iter().filter(|t| t.status == "completed").count();
            output.push_str(&format!(
                "\n## Todos ({}/{} completed)\n\n",
                completed,
                todos.len()
            ));
            for todo in &todos {
                let icon = match todo.status.as_str() {
                    "completed" => "[✓]",
                    "in_progress" => "[→]",
                    _ => "[ ]",
                };
                let active = if todo.status == "in_progress" {
                    todo.active_form
                        .as_ref()
                        .map(|f| format!(" ({})", f))
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                output.push_str(&format!("- {} {}{}\n", icon, todo.content, active));
            }
        }
    }

    output
}

/// Load events for a job in correct temporal order.
///
/// Uses two-query strategy: loads run ordering from `runs.created_at`,
/// then sorts events by `(run_position, created_at, rowid)`.
///
/// - `run_position` fixes cross-run ordering (UUID v4 alphabetical != temporal)
/// - `created_at` separates events across seconds
/// - `rowid` (via insertion-order index) is the stable intra-second tiebreaker,
///   because `sequence` is unreliable on cold resume
fn load_job_events_ordered(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Vec<(String, i32, String, String)> {
    // 1. Load run_ids ordered by creation time
    let ordered_run_ids: Vec<String> = runs::table
        .filter(runs::job_id.eq(job_id))
        .order(runs::created_at.asc())
        .select(runs::id)
        .load(conn)
        .unwrap_or_default();

    if ordered_run_ids.is_empty() {
        return Vec::new();
    }

    // 2. Build run_id -> position map
    let run_position: std::collections::HashMap<&str, usize> = ordered_run_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();

    // 3. Load all events in rowid order (insertion order) with created_at for sort
    let event_rows: Vec<(String, i32, i32, String, String)> = events::table
        .filter(events::run_id.eq_any(&ordered_run_ids))
        .order(diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid"))
        .select((
            events::run_id,
            events::sequence,
            events::created_at,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    // 4. Build insertion-order index, sort by (run_position, created_at, insertion_index)
    let mut indexed: Vec<(usize, (String, i32, i32, String, String))> =
        event_rows.into_iter().enumerate().collect();

    indexed.sort_by(|(idx_a, a), (idx_b, b)| {
        let pos_a = run_position
            .get(a.0.as_str())
            .copied()
            .unwrap_or(usize::MAX);
        let pos_b = run_position
            .get(b.0.as_str())
            .copied()
            .unwrap_or(usize::MAX);
        pos_a.cmp(&pos_b).then(a.2.cmp(&b.2)).then(idx_a.cmp(idx_b))
    });

    // Strip created_at and index from output
    indexed
        .into_iter()
        .map(|(_, (run_id, seq, _created_at, event_type, data))| (run_id, seq, event_type, data))
        .collect()
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

    let event_rows = load_job_events_ordered(conn, &job.id);
    if event_rows.is_empty() {
        return "No runs found for this node.".to_string();
    }

    // Format as markdown transcript with event URIs
    let base_uri = format!(
        "cairn://{}/{}/{}/{}/chat",
        project_key,
        number,
        exec_seq,
        visible_job_node_segment(conn, &job)
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

    let event_rows = load_job_events_ordered(conn, &job.id);
    if event_rows.is_empty() {
        return "No runs found for this node.".to_string();
    }

    // Format as full markdown transcript (no truncation)
    crate::transcripts::format_transcript_full(&event_rows)
}

/// Read a turn-scoped transcript slice.
///
/// Resolves the turn via the job's first run's session_id. Known limitation:
/// if the job's session was rotated (due to failure), turns from successor
/// sessions are not addressable by this URI — only the initial session's
/// turn namespace is reachable.
fn read_node_chat_turn(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    turn_seq: i32,
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

    // Get session_id from the job's first run
    let session_id: Option<String> = runs::table
        .filter(runs::job_id.eq(&job.id))
        .order(runs::created_at.asc())
        .select(runs::session_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    let session_id = match session_id {
        Some(id) => id,
        None => return "No session found for this node.".to_string(),
    };

    // Find turn by (session_id, sequence = turn_seq)
    let turn_id: Result<String, _> = turns::table
        .filter(turns::session_id.eq(&session_id))
        .filter(turns::sequence.eq(turn_seq))
        .select(turns::id)
        .first(conn);

    let turn_id = match turn_id {
        Ok(id) => id,
        Err(_) => {
            return format!(
                "Turn {} not found for node '{}' in issue {}-{}",
                turn_seq, node_name, project_key, number
            )
        }
    };

    // Load events for this turn
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::turn_id.eq(&turn_id))
        .order(events::sequence.asc())
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    if event_rows.is_empty() {
        return format!(
            "No events found for turn {} in node '{}' of issue {}-{}",
            turn_seq, node_name, project_key, number
        );
    }

    // Format as full transcript (no truncation for focused turn slices)
    crate::transcripts::format_transcript_full(&event_rows)
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

    let event_rows = load_job_events_ordered(conn, &task_job.id);
    if event_rows.is_empty() {
        return "No runs found for this task.".to_string();
    }

    // Format as markdown transcript with event URIs
    let base_uri = format!(
        "cairn://{}/{}/{}/{}/task/{}/chat",
        project_key,
        number,
        exec_seq,
        visible_job_node_segment(conn, &parent_job),
        task_name
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

    let event_rows = load_job_events_ordered(conn, &task_job.id);
    if event_rows.is_empty() {
        return "No runs found for this task.".to_string();
    }

    // Format as full markdown transcript (no truncation)
    crate::transcripts::format_transcript_full(&event_rows)
}

fn read_task_chat_turn(
    conn: &mut SqliteConnection,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    turn_seq: i32,
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

    // Task turn URIs are only unambiguous when all runs share one session.
    let run_session_ids: Vec<Option<String>> = runs::table
        .filter(runs::job_id.eq(&task_job.id))
        .order(runs::created_at.asc())
        .select(runs::session_id)
        .load(conn)
        .unwrap_or_default();

    let mut session_ids = run_session_ids.into_iter().flatten();
    let session_id = match session_ids.next() {
        Some(id) => id,
        None => return "No session found for this task.".to_string(),
    };

    if session_ids.any(|id| id != session_id) {
        return format!(
            "Task turn URI is ambiguous for task '{}' in node '{}' of issue {}-{} because runs span multiple sessions",
            task_name, node_name, project_key, number
        );
    }

    // Find turn by (session_id, sequence = turn_seq)
    let turn_id: Result<String, _> = turns::table
        .filter(turns::session_id.eq(&session_id))
        .filter(turns::sequence.eq(turn_seq))
        .select(turns::id)
        .first(conn);

    let turn_id = match turn_id {
        Ok(id) => id,
        Err(_) => {
            return format!(
                "Turn {} not found for task '{}' in node '{}' of issue {}-{}",
                turn_seq, task_name, node_name, project_key, number
            )
        }
    };

    // Load events for this turn
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::turn_id.eq(&turn_id))
        .order(events::sequence.asc())
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(conn)
        .unwrap_or_default();

    if event_rows.is_empty() {
        return format!(
            "No events found for turn {} in task '{}' of node '{}' for issue {}-{}",
            turn_seq, task_name, node_name, project_key, number
        );
    }

    crate::transcripts::format_transcript_full(&event_rows)
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
        "result" | "tool_result" => {
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
pub(crate) fn get_node_name_from_execution(
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
    let matching_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::parent_job_id.eq(parent_job_id))
        .order(jobs::created_at.asc())
        .load(conn)
        .unwrap_or_default();

    matching_jobs
        .into_iter()
        .find(|job| {
            job.node_name
                .as_ref()
                .map(|name| name == task_name || slugify(name) == task_name)
                .unwrap_or(false)
                || job
                    .agent_config_id
                    .as_ref()
                    .map(|name| name == task_name || slugify(name) == task_name)
                    .unwrap_or(false)
        })
        .ok_or_else(|| format!("Task '{}' not found", task_name))
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

/// Find a job by node name (looking up name in execution snapshot, then node_name column)
/// exec_seq is required and identifies the specific execution to search
fn find_job_by_node_name(
    conn: &mut SqliteConnection,
    issue_id: &str,
    node_name: &str,
    exec_seq: i32,
) -> Option<DbJob> {
    // Resolve exec_seq to an execution_id
    let exec_id = get_execution_id_for_seq(conn, issue_id, exec_seq)?;

    // 1. Try recipe nodes (resolve name via execution snapshot)
    let recipe_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .filter(jobs::execution_id.eq(&exec_id))
        .filter(jobs::recipe_node_id.is_not_null())
        .order(jobs::created_at.desc())
        .load(conn)
        .ok()?;

    for job in recipe_jobs {
        if let Some(ref recipe_node_id) = &job.recipe_node_id {
            if recipe_node_id == node_name {
                return Some(job);
            }
            if let Some(name) = get_node_name_from_execution(conn, &exec_id, recipe_node_id) {
                if name == node_name || slugify(&name) == node_name {
                    return Some(job);
                }
            }
        }
    }

    // 2. Try node_name column (task agents + any job with stored name)
    let candidate_jobs: Vec<DbJob> = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .filter(jobs::execution_id.eq(&exec_id))
        .load(conn)
        .ok()?;

    candidate_jobs.into_iter().find(|job| {
        job.node_name
            .as_ref()
            .map(|name| name == node_name || slugify(name) == node_name)
            .unwrap_or(false)
    })
}

// Truncation thresholds
const ASSISTANT_CONTENT_THRESHOLD: usize = 2000;
const ASSISTANT_CONTENT_PREVIEW: usize = 500;
const TOOL_SUMMARY_PREVIEW: usize = 80;

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

fn cleaned_tool_name(tool_name: &str) -> &str {
    if tool_name.starts_with("mcp__") {
        tool_name.rsplit("__").next().unwrap_or(tool_name)
    } else {
        tool_name
    }
}

fn single_line_preview(text: &str, max_len: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let safe_len = trimmed
        .char_indices()
        .take_while(|(i, _)| *i < max_len)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(trimmed.len());

    if trimmed.len() > safe_len {
        format!("{}...", &trimmed[..safe_len])
    } else {
        trimmed.to_string()
    }
}

fn summarize_scalar(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => {
            let preview = single_line_preview(text, TOOL_SUMMARY_PREVIEW);
            if preview.is_empty() {
                None
            } else {
                Some(preview)
            }
        }
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

fn summarize_tool_input(tool_name: &str, input: &serde_json::Value) -> Option<String> {
    let obj = input.as_object()?;
    let cleaned = cleaned_tool_name(tool_name);

    match cleaned {
        "Bash" | "bash" => obj
            .get("description")
            .or_else(|| obj.get("command"))
            .and_then(summarize_scalar),
        "Read" | "read" => obj
            .get("file_path")
            .or_else(|| obj.get("path"))
            .and_then(summarize_scalar),
        "Edit" | "edit" | "filechange" => {
            if let Some(changes) = obj.get("changes").and_then(|v| v.as_array()) {
                let count = changes.len();
                Some(format!(
                    "{} file{}",
                    count,
                    if count == 1 { "" } else { "s" }
                ))
            } else {
                obj.get("file_path").and_then(summarize_scalar)
            }
        }
        "Write" | "write" => obj.get("file_path").and_then(summarize_scalar),
        "Grep" | "grep" => obj
            .get("pattern")
            .and_then(summarize_scalar)
            .map(|pattern| format!("\"{}\"", pattern)),
        "Glob" | "glob" => obj.get("pattern").and_then(summarize_scalar),
        "task" | "Task" => obj.get("description").and_then(summarize_scalar),
        "execute" => obj.get("code").and_then(|value| {
            value.as_str().and_then(|code| {
                code.lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty())
                    .map(|line| single_line_preview(line, TOOL_SUMMARY_PREVIEW))
                    .filter(|line| !line.is_empty())
            })
        }),
        "message" => obj.get("content").and_then(|value| {
            summarize_scalar(value).map(|content| match obj.get("to").and_then(|v| v.as_str()) {
                Some(target) if !target.is_empty() => format!("→ {} {}", target, content),
                _ => format!("→ project {}", content),
            })
        }),
        _ => obj
            .get("command")
            .or_else(|| obj.get("file_path"))
            .or_else(|| obj.get("path"))
            .or_else(|| obj.get("pattern"))
            .or_else(|| obj.get("query"))
            .or_else(|| obj.get("url"))
            .or_else(|| obj.get("title"))
            .or_else(|| obj.get("description"))
            .and_then(summarize_scalar),
    }
}

fn summarize_tool_result_payload(value: &serde_json::Value) -> Option<String> {
    let obj = value.as_object()?;

    if obj.get("isError").and_then(|v| v.as_bool()) == Some(true) {
        if let Some(message) = obj
            .get("error")
            .or_else(|| obj.get("message"))
            .or_else(|| obj.get("output"))
            .and_then(summarize_scalar)
        {
            return Some(format!("error: {}", message));
        }
        return Some("error".to_string());
    }

    obj.get("message")
        .or_else(|| obj.get("output"))
        .or_else(|| obj.get("status"))
        .or_else(|| obj.get("decision"))
        .or_else(|| obj.get("slug"))
        .or_else(|| obj.get("uri"))
        .or_else(|| obj.get("id"))
        .and_then(summarize_scalar)
}

fn summarize_tool_result(event_data: &serde_json::Value) -> Option<String> {
    if event_data.get("isError").and_then(|v| v.as_bool()) == Some(true) {
        return Some("error".to_string());
    }

    let raw_result = event_data.get("toolResult")?;

    if !raw_result.is_string() {
        return summarize_tool_result_payload(raw_result);
    }

    let result = raw_result.as_str().unwrap_or_default().trim();
    if result.is_empty() {
        return None;
    }

    serde_json::from_str::<serde_json::Value>(result)
        .ok()
        .and_then(|value| summarize_tool_result_payload(&value))
}

fn push_compact_tool_entry(
    transcript: &mut String,
    label: &str,
    tool_name: Option<&str>,
    summary: Option<String>,
    event_uri: &str,
) {
    transcript.push_str(&format!("**{}", label));
    if let Some(tool_name) = tool_name.filter(|tool_name| !tool_name.is_empty()) {
        transcript.push_str(&format!(" ({})", tool_name));
    }
    transcript.push_str(":**");
    if let Some(summary) = summary.filter(|summary| !summary.is_empty()) {
        transcript.push(' ');
        transcript.push_str(&summary);
    }
    transcript.push_str(&format!(" → {}\n\n", event_uri));
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
                        let summary = tool
                            .get("input")
                            .and_then(|input| summarize_tool_input(name, input));

                        push_compact_tool_entry(
                            &mut transcript,
                            "Tool Call",
                            Some(name),
                            summary,
                            &ctx.event_uri(run_id, *seq),
                        );
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
            "result" | "tool_result" => {
                let tool_name = event_data.get("toolName").and_then(|t| t.as_str());
                let summary = summarize_tool_result(&event_data);
                push_compact_tool_entry(
                    &mut transcript,
                    "Tool Result",
                    tool_name,
                    summary,
                    &ctx.event_uri(run_id, *seq),
                );
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
        let data = r#"{"content": "Let me read that file.", "toolUses": [{"name": "read", "input": {"file": "test.txt", "body": "complete payload"}}]}"#;
        let result = format_single_event("assistant", data);
        assert!(result.contains("**Assistant:**"));
        assert!(result.contains("**Tool Call (read):**"));
        assert!(result.contains("\"file\": \"test.txt\""));
        assert!(result.contains("\"body\": \"complete payload\""));
    }

    #[test]
    fn test_format_single_event_tool_result_alias_preserves_full_payload() {
        let data = serde_json::json!({
            "toolName": "read",
            "toolResult": serde_json::json!({
                "message": "detailed result",
                "body": "complete payload"
            })
            .to_string()
        })
        .to_string();

        let result = format_single_event("tool_result", &data);

        assert!(result.contains("**Tool Result (read):**"));
        assert!(result.contains("detailed result"));
        assert!(result.contains("complete payload"));
    }

    #[test]
    fn test_format_transcript_with_context_compacts_tool_calls() {
        let events = vec![(
            "run-1".to_string(),
            3,
            "assistant".to_string(),
            serde_json::json!({
                "content": "Let me read that file.",
                "toolUses": [{
                    "name": "read",
                    "input": {
                        "path": "/tmp/test.txt",
                        "unused": {"raw": "json body"}
                    }
                }]
            })
            .to_string(),
        )];

        let result = format_transcript_with_context(&events, "cairn://CAIRN/1062/1/builder/chat");

        assert!(result.contains("**Tool Call (read):** /tmp/test.txt"));
        assert!(result.contains("cairn://CAIRN/1062/1/builder/chat/1/3"));
        assert!(!result.contains("\"unused\""));
        assert!(!result.contains("raw"));
    }

    #[test]
    fn test_format_transcript_with_context_compacts_tool_results() {
        let events = vec![
            (
                "run-1".to_string(),
                4,
                "result".to_string(),
                serde_json::json!({
                    "toolName": "read",
                    "toolResult": "file contents here"
                })
                .to_string(),
            ),
            (
                "run-1".to_string(),
                5,
                "result".to_string(),
                serde_json::json!({
                    "toolName": "bash",
                    "toolResult": serde_json::json!({
                        "message": "Background terminal started: dev-server",
                        "uri": "cairn://CAIRN/1062/terminal/dev-server"
                    }).to_string()
                })
                .to_string(),
            ),
            (
                "run-1".to_string(),
                6,
                "tool_result".to_string(),
                serde_json::json!({
                    "toolName": null,
                    "toolResult": serde_json::json!({
                        "message": "Updated 5 todos"
                    }).to_string()
                })
                .to_string(),
            ),
        ];

        let result = format_transcript_with_context(&events, "cairn://CAIRN/1062/1/builder/chat");

        assert!(result.contains("**Tool Result (read):** → cairn://CAIRN/1062/1/builder/chat/1/4"));
        assert!(result.contains("**Tool Result (bash):** Background terminal started: dev-server → cairn://CAIRN/1062/1/builder/chat/1/5"));
        assert!(result
            .contains("**Tool Result:** Updated 5 todos → cairn://CAIRN/1062/1/builder/chat/1/6"));
        assert!(!result.contains("file contents here"));
        assert!(!result.contains("**Tool Result (tool):**"));
    }

    #[test]
    fn test_format_transcript_full_no_truncation() {
        // Create a long message that would be truncated in regular transcript
        let long_content = "a".repeat(3000);
        let tool_input = serde_json::json!({"path": "/tmp/full.txt", "body": "complete payload"});
        let tool_result =
            serde_json::json!({"message": "detailed result", "body": "complete payload"})
                .to_string();
        let events = vec![
            (
                "run-1".to_string(),
                1,
                "assistant".to_string(),
                format!(
                    r#"{{"content": "{}", "toolUses": [{{"name": "read", "input": {}}}]}}"#,
                    long_content, tool_input
                ),
            ),
            (
                "run-1".to_string(),
                2,
                "tool_result".to_string(),
                serde_json::json!({"toolName": "read", "toolResult": tool_result}).to_string(),
            ),
        ];
        let result = crate::transcripts::format_transcript_full(&events);
        // Should contain the full content without truncation
        assert!(result.contains(&long_content));
        assert!(result.contains("\"body\": \"complete payload\""));
        assert!(result.contains("detailed result"));
        assert!(!result.contains("[Truncated"));
    }

    // --- find_job_by_node_name tests ---

    use crate::diesel_models::{NewExecution, NewJob};
    use crate::models::{
        ExecutionSnapshot, NodePosition, RecipeNode, RecipeNodeType, RecipeSnapshot, RecipeTrigger,
        TriggerContext, TriggerType,
    };
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};

    fn setup_execution_with_snapshot(
        conn: &mut SqliteConnection,
        issue_id: &str,
        project_id: &str,
        nodes: Vec<RecipeNode>,
    ) -> String {
        let recipe_id = uuid::Uuid::new_v4().to_string();
        let execution_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        let snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: recipe_id.clone(),
                name: "Test".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes,
                edges: vec![],
            },
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            TriggerContext {
                issue_id: None,
                project_id: project_id.to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
            },
        );
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();

        diesel::insert_into(executions::table)
            .values(&NewExecution {
                id: &execution_id,
                recipe_id: &recipe_id,
                issue_id: Some(issue_id),
                project_id: Some(project_id),
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: Some(&snapshot_json),
                seq: Some(1),
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(conn)
            .unwrap();

        execution_id
    }

    fn make_node(id: &str, name: &str) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Agent,
            name: name.to_string(),
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

    fn insert_job(
        conn: &mut SqliteConnection,
        issue_id: &str,
        project_id: &str,
        execution_id: &str,
        recipe_node_id: Option<&str>,
        node_name: Option<&str>,
        parent_job_id: Option<&str>,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(jobs::table)
            .values(&NewJob {
                id: &id,
                execution_id: Some(execution_id),
                manager_id: None,
                recipe_node_id,
                parent_job_id,
                worktree_path: None,
                branch: None,
                base_commit: None,
                current_session_id: None,
                resume_session_id: None,
                status: "running",
                agent_config_id: None,
                issue_id: Some(issue_id),
                project_id,
                task_description: None,
                created_at: now,
                updated_at: now,
                completed_at: None,
                parent_tool_use_id: None,
                task_index: None,
                started_at: Some(now),
                model: None,
                node_name,
                base_branch: None,
                current_turn_id: None,
            })
            .execute(conn)
            .unwrap();
        id
    }

    #[test]
    fn read_node_resource_by_slugified_name() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node("node-1", "Builder")],
        );

        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some("54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67"),
            Some("Builder"),
            None,
        );

        let output = read_node(&mut conn, "TST", 1, 1, "builder");
        assert!(output.contains("## Resources"));
        assert!(output.contains("cairn://TST/1/1/builder/chat"));
    }

    #[test]
    fn read_node_resource_accepts_legacy_recipe_node_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let recipe_node_id = "54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67";
        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node("node-1", "Builder")],
        );

        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some(recipe_node_id),
            Some("Builder"),
            None,
        );

        let output = read_node(&mut conn, "TST", 1, 1, recipe_node_id);
        assert!(output.contains("## Resources"));
        assert!(output.contains("cairn://TST/1/1/builder/chat"));
    }

    #[test]
    fn find_job_by_node_name_recipe_node() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");

        let node_id = "node-1";
        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node(node_id, "Builder")],
        );

        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some(node_id),
            Some("Builder"),
            None,
        );

        let found = find_job_by_node_name(&mut conn, &issue_id, "Builder", 1);
        assert!(found.is_some(), "should find job via recipe node snapshot");
        assert_eq!(found.unwrap().recipe_node_id.as_deref(), Some(node_id));
    }

    #[test]
    fn find_job_by_node_name_falls_back_to_node_name_column() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");

        // Create execution with one recipe node (the parent)
        let parent_node_id = "parent-node";
        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node(parent_node_id, "Builder")],
        );

        let parent_job_id = insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some(parent_node_id),
            Some("Builder"),
            None,
        );

        // Task-spawned job: no recipe_node_id, but has node_name
        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            None,
            Some("Explore"),
            Some(&parent_job_id),
        );

        // Should find the task via node_name fallback
        let found = find_job_by_node_name(&mut conn, &issue_id, "Explore", 1);
        assert!(found.is_some(), "should find task job via node_name column");
        assert_eq!(found.unwrap().node_name.as_deref(), Some("Explore"));
    }

    #[test]
    fn find_job_by_node_name_disambiguated_task() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");

        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node("n1", "Builder")],
        );

        let parent_id = insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some("n1"),
            Some("Builder"),
            None,
        );

        // Two Explore tasks with disambiguated names (set at creation time)
        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            None,
            Some("Explore"),
            Some(&parent_id),
        );
        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            None,
            Some("Explore-2"),
            Some(&parent_id),
        );

        let found1 = find_job_by_node_name(&mut conn, &issue_id, "Explore", 1);
        assert!(found1.is_some());
        assert_eq!(found1.unwrap().node_name.as_deref(), Some("Explore"));

        let found2 = find_job_by_node_name(&mut conn, &issue_id, "Explore-2", 1);
        assert!(found2.is_some());
        assert_eq!(found2.unwrap().node_name.as_deref(), Some("Explore-2"));
    }

    #[test]
    fn find_job_by_node_name_no_match() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");

        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node("n1", "Builder")],
        );

        insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some("n1"),
            Some("Builder"),
            None,
        );

        let found = find_job_by_node_name(&mut conn, &issue_id, "NonExistent", 1);
        assert!(found.is_none(), "should return None for unknown node name");
    }

    // --- load_job_events_ordered tests ---

    use crate::diesel_models::{NewEvent, NewRun};

    fn insert_run_for_job(conn: &mut SqliteConnection, id: &str, job_id: &str, created_at: i32) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(runs::table)
            .values(&NewRun {
                id,
                issue_id: None,
                project_id: None,
                job_id: Some(job_id),
                chat_id: None,
                status: Some("exited"),
                session_id: None,
                error_message: None,
                started_at: None,
                exited_at: None,
                created_at,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: None,
            })
            .execute(conn)
            .unwrap();
    }

    fn insert_event_for_run(
        conn: &mut SqliteConnection,
        id: &str,
        run_id: &str,
        sequence: i32,
        event_type: &str,
        data: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(events::table)
            .values(&NewEvent {
                id,
                run_id,
                session_id: None,
                sequence,
                timestamp: now,
                event_type,
                data,
                parent_tool_use_id: None,
                created_at: now,
                input_tokens: None,
                cache_read_tokens: None,
                cache_create_tokens: None,
                output_tokens: None,
                turn_id: None,
            })
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_load_job_events_ordered_sorts_by_run_creation_time() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let exec_id = setup_execution_with_snapshot(
            &mut conn,
            &issue_id,
            &project_id,
            vec![make_node("n1", "Builder")],
        );
        let job_id = insert_job(
            &mut conn,
            &issue_id,
            &project_id,
            &exec_id,
            Some("n1"),
            Some("Builder"),
            None,
        );

        // Create two runs with the SAME created_at timestamp.
        // UUIDs are alphabetically ordered: run-a < run-b.
        // But run-b was created first (lower created_at wins tiebreak).
        // The old code sorted by run_id.asc() which would put run-a first — wrong.
        let same_time = chrono::Utc::now().timestamp() as i32;
        insert_run_for_job(&mut conn, "run-b", &job_id, same_time);
        insert_run_for_job(&mut conn, "run-a", &job_id, same_time + 1);

        // Events in run-b (first run)
        insert_event_for_run(&mut conn, "e1", "run-b", 1, "user", "{}");
        insert_event_for_run(&mut conn, "e2", "run-b", 2, "assistant", "{}");

        // Events in run-a (second run)
        insert_event_for_run(&mut conn, "e3", "run-a", 1, "user", "{}");
        insert_event_for_run(&mut conn, "e4", "run-a", 2, "assistant", "{}");

        let events = load_job_events_ordered(&mut conn, &job_id);
        assert_eq!(events.len(), 4);

        // Should be ordered by run creation time, not alphabetical run_id.
        // run-b (created first) events come before run-a events.
        assert_eq!(events[0].0, "run-b"); // run_id
        assert_eq!(events[0].1, 1); // sequence
        assert_eq!(events[1].0, "run-b");
        assert_eq!(events[1].1, 2);
        assert_eq!(events[2].0, "run-a");
        assert_eq!(events[2].1, 1);
        assert_eq!(events[3].0, "run-a");
        assert_eq!(events[3].1, 2);
    }

    #[test]
    fn test_load_job_events_ordered_no_runs() {
        let mut conn = test_diesel_conn();
        let events = load_job_events_ordered(&mut conn, "nonexistent-job");
        assert!(events.is_empty());
    }
}
