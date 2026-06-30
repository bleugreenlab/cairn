//! Node, task, todo, question, and node transcript resource readers.

use std::collections::HashMap;

use turso::params;

use super::common::{
    connect_and_find_node_job, connect_and_find_task_job, connect_for_read,
    find_action_run_by_node_name, find_job_by_node_name, find_query_value, get_artifact_for_job,
    get_direct_artifact_for_job, get_named_artifact_for_job, list_named_artifacts_for_job,
    reject_query_params, resolve_issue_id, visible_job_node_segment, ResourceActionRun,
};
use super::transcript::{
    format_transcript_digest, get_nth_run_id, get_single_event, load_job_events_ordered,
    load_turn_events, DigestMeta, EventRow,
};

use crate::storage::{LocalDb, RowExt};
use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_job_todos_uri, build_node_artifact_uri_named, build_node_chat_uri,
    build_node_permission_uri, build_node_question_uri, build_node_uri, build_node_wakes_uri,
    build_task_artifact_uri, build_task_artifact_uri_named, build_task_chat_uri,
    build_task_permission_uri,
};

/// Resolve the job_id that owns the todos addressed by a `JobTodos` URI.
///
/// `task_name: None` resolves a node job; `Some` resolves a sub-agent task job.
/// Shared by the todos read resolver and the `write` dispatch so both address
/// the same job from the same URI.
pub(crate) async fn resolve_todos_job_id(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
) -> Result<String, String> {
    match task_name {
        None => {
            let (_, job) =
                connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await?;
            Ok(job.id)
        }
        Some(task_name) => {
            let (_, _parent_job, task_job) =
                connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
                    .await?;
            Ok(task_job.id)
        }
    }
}

fn job_status_icon(status: &str) -> &'static str {
    match status {
        "complete" => "✓",
        "running" => "◐",
        "failed" => "✗",
        "pending" => "○",
        _ => "?",
    }
}

fn format_node_timestamp(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "Unknown".to_string())
}

fn append_optional_timestamp(output: &mut String, label: &str, timestamp: Option<i64>) {
    if let Some(timestamp) = timestamp {
        output.push_str(&format!(
            "{}: {}\n",
            label,
            format_node_timestamp(timestamp)
        ));
    }
}

fn render_job_summary_header(
    name: &str,
    status: &str,
    started_at: Option<i64>,
    completed_at: Option<i64>,
) -> String {
    let mut output = format!("# {} [{}]\n\n", name, job_status_icon(status));
    append_optional_timestamp(&mut output, "Started", started_at);
    append_optional_timestamp(&mut output, "Completed", completed_at);
    output.push('\n');
    output
}

/// `"{n} {noun}"` with a plural `s` for any count other than one.
fn count_phrase(n: i64, noun: &str) -> String {
    format!("{} {}{}", n, noun, if n == 1 { "" } else { "s" })
}

/// Collapse text to its first non-empty line, capped to one readable line. Used
/// for the node-summary "Latest" preview so a long assistant message becomes a
/// single scannable line.
fn activity_preview(text: &str) -> String {
    let line = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    const MAX_CHARS: usize = 160;
    if line.chars().count() > MAX_CHARS {
        let head: String = line.chars().take(MAX_CHARS).collect();
        format!("{}\u{2026}", head.trim_end())
    } else {
        line.to_string()
    }
}

/// The freshest assistant prose line for a node, drawn only from live (`full`)
/// rows. Archived assistant events store their body at git coordinates, not in
/// `data`, so they are excluded rather than reconstructed — a summary read must
/// stay cheap, and a completed/archived node has its artifact to read instead.
async fn latest_assistant_preview(conn: &turso::Connection, job_id: &str) -> Option<String> {
    let mut rows = conn
        .query(
            "
            SELECT e.data
            FROM events e
            JOIN runs r ON e.run_id = r.id
            WHERE r.job_id = ?1
              AND e.event_type = 'assistant'
              AND (e.storage_mode IS NULL OR e.storage_mode = 'full')
            ORDER BY e.created_at DESC, e.rowid DESC
            LIMIT 30
            ",
            (job_id,),
        )
        .await
        .ok()?;
    while let Ok(Some(row)) = rows.next().await {
        let Ok(data) = row.text(0) else { continue };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            continue;
        };
        if let Some(content) = value.get("content").and_then(|c| c.as_str()) {
            let preview = activity_preview(content);
            if !preview.is_empty() {
                return Some(preview);
            }
        }
    }
    None
}

/// Render the node summary's `## Activity` section: turn/run/event counts, the
/// last-activity timestamp, and the latest assistant line. Returns `None` when
/// the node has no events yet (nothing to summarize beyond the header), so a
/// just-started node stays terse. Counts and timing come from one cheap
/// aggregate that needs no event-body reconstruction.
async fn render_node_activity(conn: &turso::Connection, job_id: &str) -> Option<String> {
    let mut rows = conn
        .query(
            "
            SELECT COUNT(DISTINCT e.run_id), COUNT(DISTINCT e.turn_id),
                   COUNT(*), MAX(e.created_at)
            FROM events e
            JOIN runs r ON e.run_id = r.id
            WHERE r.job_id = ?1
            ",
            (job_id,),
        )
        .await
        .ok()?;
    let row = rows.next().await.ok().flatten()?;
    let runs = row.opt_i64(0).ok().flatten().unwrap_or(0);
    let turns = row.opt_i64(1).ok().flatten().unwrap_or(0);
    let events = row.opt_i64(2).ok().flatten().unwrap_or(0);
    let last_activity = row.opt_i64(3).ok().flatten();
    if events == 0 {
        return None;
    }

    let mut summary = format!(
        "{} \u{b7} {} \u{b7} {}",
        count_phrase(turns, "turn"),
        count_phrase(runs, "run"),
        count_phrase(events, "event"),
    );
    if let Some(ts) = last_activity {
        summary.push_str(&format!(
            " \u{b7} last active {}",
            format_node_timestamp(ts)
        ));
    }

    let mut output = format!("## Activity\n\n{}\n", summary);
    if let Some(preview) = latest_assistant_preview(conn, job_id).await {
        output.push_str(&format!("\nLatest: {}\n", preview));
    }
    Some(output)
}

fn format_job_todos(
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    todos: &[crate::models::TodoItem],
) -> String {
    let label = match task_name {
        Some(task_name) => format!("{}/task/{}", node_name, task_name),
        None => node_name.to_string(),
    };
    let todos_uri = build_job_todos_uri(project_key, number, exec_seq, node_name, task_name);

    if todos.is_empty() {
        return format!("# Todos — {}\n\nNo todos yet.\n\n`{}`\n", label, todos_uri);
    }

    let completed = todos.iter().filter(|t| t.status == "completed").count();
    let mut output = format!(
        "# Todos — {} ({}/{} completed)\n\n",
        label,
        completed,
        todos.len()
    );
    for todo in todos {
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
        let priority = todo
            .priority
            .as_ref()
            .map(|p| format!(" [{}]", p))
            .unwrap_or_default();
        output.push_str(&format!(
            "- [{}] {} {}{}{}\n",
            todo.id, icon, todo.content, active, priority
        ));
    }
    output.push_str(&format!("\n`{}`\n", todos_uri));
    output
}

pub(super) async fn read_job_todos(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
) -> String {
    let job_id =
        match resolve_todos_job_id(db, project_key, number, exec_seq, node_name, task_name).await {
            Ok(job_id) => job_id,
            Err(error) => return error,
        };

    let todos = match crate::todos::get_todos_for_job(db, &job_id).await {
        Ok(todos) => todos,
        Err(error) => return error,
    };

    format_job_todos(project_key, number, exec_seq, node_name, task_name, &todos)
}

pub(super) async fn read_node_wakes(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let job_id =
        match resolve_todos_job_id(db, project_key, number, exec_seq, node_name, None).await {
            Ok(job_id) => job_id,
            Err(error) => return error,
        };
    let subscriptions =
        match crate::orchestrator::wakes::list_subscriptions_for_job(db, &job_id).await {
            Ok(subscriptions) => subscriptions,
            Err(error) => return error,
        };
    let pending =
        match crate::orchestrator::wakes::peek_pending_suppressed_for_job(db, &job_id).await {
            Ok(pending) => pending,
            Err(error) => return error,
        };
    let uri = build_node_wakes_uri(project_key, number, exec_seq, node_name);
    let mut out = format!("# Wakes — {node_name}\n\n`{uri}`\n\n");
    if subscriptions.is_empty() {
        out.push_str("No wake subscriptions.\n");
    } else {
        out.push_str("## Subscriptions\n\n");
        for sub in &subscriptions {
            let facts = sub
                .fact_kinds
                .as_ref()
                .map(|k| k.join(", "))
                .unwrap_or_else(|| "all".to_string());
            let source = sub.source_ref.as_deref().unwrap_or("*");
            out.push_str(&format!(
                "- `{}` {} `{}` facts=[{}] created_by={}\n",
                sub.state.as_str(),
                sub.source_kind,
                source,
                facts,
                sub.created_by
            ));
        }
    }
    out.push_str("\n## Pending digest\n\n");
    if pending.is_empty() {
        out.push_str("No suppressed wakes pending delivery.\n");
    } else {
        out.push_str(&crate::orchestrator::wakes::SuppressedWake::render_digest(
            &pending,
        ));
        out.push('\n');
    }
    out.push_str("\n## Mutations\n\n- append `{subscribe:{kind,ref?,factKinds?}}`\n- append `{subscribe:{kind:\"terminal\", ref:\"cairn:~/terminal/<slug>\", on:\"exit\"}}` — end your turn and resume when the terminal exits (fires immediately if it already exited)\n- append `{subscribe:{kind:\"terminal\", ref:\"cairn:~/terminal/<slug>\", on:\"output\", phrase:\"ready\"}}` — resume when a literal phrase appears in the running terminal's output (case-sensitive, one-shot; also wakes if the terminal exits first; survives terminal restarts)\n- append `{mute:{kind,ref?,factKinds?}, until?:{kind,ref?}}`\n- patch `{unmute:{kind,ref?}}`\n- delete `{unsubscribe:{kind,ref?}}`\n");
    out
}

pub(super) async fn read_node_tasks(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let mut tasks: Vec<(String, Option<String>, Option<String>, String)> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT uri_segment, agent_config_id, task_description, status
            FROM jobs
            WHERE parent_job_id = ?1
            ORDER BY task_index ASC, created_at ASC
            ",
            (job.id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(segment), Ok(agent), Ok(description), Ok(status)) = (
                row.opt_text(0),
                row.opt_text(1),
                row.opt_text(2),
                row.text(3),
            ) else {
                continue;
            };
            let segment = segment.unwrap_or_else(|| "task".to_string());
            tasks.push((segment, agent, description, status));
        }
    }

    let mut output = format!("# Tasks — {}\n\n", node_name);
    if tasks.is_empty() {
        output.push_str("No delegated tasks yet.\n\n");
    } else {
        output.push_str(&format!("{} task(s)\n\n", tasks.len()));
        for (segment, agent, description, status) in &tasks {
            let icon = match status.as_str() {
                "complete" => "✓",
                "running" => "◐",
                "failed" => "✗",
                "pending" => "○",
                _ => "?",
            };
            let label = description
                .clone()
                .filter(|value| !value.is_empty())
                .or_else(|| agent.clone())
                .unwrap_or_default();
            output.push_str(&format!(
                "- [{}]({}) [{}]{} — chat: {} · artifact: {}\n",
                segment,
                build_task_chat_uri(project_key, number, exec_seq, node_name, segment),
                icon,
                if label.is_empty() {
                    String::new()
                } else {
                    format!(" {}", label)
                },
                build_task_chat_uri(project_key, number, exec_seq, node_name, segment),
                build_task_artifact_uri(project_key, number, exec_seq, node_name, segment),
            ));
        }
        output.push('\n');
    }
    output
}

/// Summarize a stored questions JSON array into a single human-readable line.
fn summarize_questions_json(questions_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(questions_json)
        .ok()
        .and_then(|value| {
            value.as_array().map(|items| {
                items
                    .iter()
                    .filter_map(|q| q.get("question").and_then(|text| text.as_str()))
                    .collect::<Vec<_>>()
                    .join("; ")
            })
        })
        .filter(|summary| !summary.is_empty())
        .unwrap_or_else(|| "(question)".to_string())
}

struct NodeQuestionRow {
    segment: String,
    questions_json: String,
    response: Option<String>,
    answered: bool,
}

async fn load_node_questions(conn: &turso::Connection, job_id: &str) -> Vec<NodeQuestionRow> {
    let mut questions = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT p.uri_segment, p.questions, p.response, p.answered_at
            FROM prompts p
            JOIN runs r ON p.run_id = r.id
            WHERE r.job_id = ?1
            ORDER BY p.created_at ASC, p.id ASC
            ",
            (job_id,),
        )
        .await
    {
        let mut fallback_ordinal = 0;
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(segment), Ok(questions_json), Ok(response), Ok(answered_at)) = (
                row.opt_text(0),
                row.text(1),
                row.opt_text(2),
                row.opt_i64(3),
            ) else {
                continue;
            };
            fallback_ordinal += 1;
            questions.push(NodeQuestionRow {
                segment: segment.unwrap_or_else(|| format!("q-{}", fallback_ordinal)),
                questions_json,
                response,
                answered: answered_at.is_some(),
            });
        }
    }
    questions
}

pub(super) async fn read_node_questions(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let questions = load_node_questions(&conn, &job.id).await;

    let mut output = format!("# Questions — {}\n\n", node_name);
    if questions.is_empty() {
        output.push_str("No questions asked yet.\n\n");
    } else {
        output.push_str(&format!("{} question(s)\n\n", questions.len()));
        for question in &questions {
            let icon = if question.answered { "✓" } else { "⏳" };
            output.push_str(&format!(
                "- [{}]({}) [{}] {}\n",
                question.segment,
                build_node_question_uri(
                    project_key,
                    number,
                    exec_seq,
                    node_name,
                    &question.segment
                ),
                icon,
                summarize_questions_json(&question.questions_json),
            ));
            if let Some(response) = question.response.as_deref().filter(|r| !r.is_empty()) {
                output.push_str(&format!("  → {}\n", response));
            }
        }
        output.push('\n');
    }
    output
}

pub(super) async fn read_node_question(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    segment: &str,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let questions = load_node_questions(&conn, &job.id).await;
    let Some(question) = questions.into_iter().find(|q| q.segment == segment) else {
        return format!(
            "Question '{}' not found for node '{}' in issue {}-{}",
            segment, node_name, project_key, number
        );
    };

    let status = if question.answered {
        "answered"
    } else {
        "pending"
    };
    let mut output = format!("# Question {} — {}\n\n", segment, node_name);
    output.push_str(&format!("Status: {}\n\n", status));

    if let Ok(serde_json::Value::Array(items)) =
        serde_json::from_str::<serde_json::Value>(&question.questions_json)
    {
        for item in &items {
            if let Some(text) = item.get("question").and_then(|value| value.as_str()) {
                output.push_str(&format!("## {}\n\n", text));
            }
            if let Some(options) = item.get("options").and_then(|value| value.as_array()) {
                for option in options {
                    if let Some(label) = option.get("label").and_then(|value| value.as_str()) {
                        let description = option
                            .get("description")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default();
                        output.push_str(&format!("- **{}**: {}\n", label, description));
                    }
                }
                output.push('\n');
            }
        }
    }

    match question.response.as_deref().filter(|r| !r.is_empty()) {
        Some(response) => output.push_str(&format!("## Response\n\n{}\n\n", response)),
        None => output.push_str("## Response\n\n(awaiting answer)\n\n"),
    }

    output
}

struct NodePermissionRow {
    segment: String,
    /// Crossing summary (fence) or the tool name (legacy prompt).
    summary: String,
    status: String,
    response: Option<String>,
}

async fn load_node_permissions(conn: &turso::Connection, job_id: &str) -> Vec<NodePermissionRow> {
    let mut requests = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT pr.uri_segment, pr.tool_name, pr.tool_input, pr.status, pr.response
            FROM permission_requests pr
            JOIN runs r ON pr.run_id = r.id
            WHERE COALESCE(pr.job_id, r.job_id) = ?1
            ORDER BY pr.created_at ASC, pr.id ASC
            ",
            (job_id,),
        )
        .await
    {
        let mut fallback_ordinal = 0;
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(segment), Ok(tool_name), Ok(tool_input), Ok(status), Ok(response)) = (
                row.opt_text(0),
                row.text(1),
                row.text(2),
                row.text(3),
                row.opt_text(4),
            ) else {
                continue;
            };
            fallback_ordinal += 1;
            // A fence crossing stores a `summary`; a legacy prompt does not, so
            // fall back to the tool name.
            let summary = serde_json::from_str::<serde_json::Value>(&tool_input)
                .ok()
                .and_then(|v| {
                    v.get("summary")
                        .and_then(|s| s.as_str())
                        .map(ToString::to_string)
                })
                .unwrap_or(tool_name);
            requests.push(NodePermissionRow {
                segment: segment.unwrap_or_else(|| format!("perm-{}", fallback_ordinal)),
                summary,
                status,
                response,
            });
        }
    }
    requests
}

pub(super) async fn read_node_permissions(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let requests = load_node_permissions(&conn, &job.id).await;

    let mut output = format!("# Permissions — {}\n\n", node_name);
    if requests.is_empty() {
        output.push_str("No permission requests yet.\n\n");
    } else {
        output.push_str(&format!("{} permission request(s)\n\n", requests.len()));
        for request in &requests {
            let icon = if request.status == "pending" {
                "\u{23f3}"
            } else {
                "\u{2713}"
            };
            output.push_str(&format!(
                "- [{}]({}) [{}] {}\n",
                request.segment,
                build_node_permission_uri(
                    project_key,
                    number,
                    exec_seq,
                    node_name,
                    &request.segment
                ),
                icon,
                request.summary,
            ));
            if request.status != "pending" {
                output.push_str(&format!("  → {}\n", request.status));
            }
        }
        output.push('\n');
    }
    output
}

pub(super) async fn read_node_permission(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    segment: &str,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let requests = load_node_permissions(&conn, &job.id).await;
    let Some(request) = requests.into_iter().find(|r| r.segment == segment) else {
        return format!(
            "Permission '{}' not found for node '{}' in issue {}-{}",
            segment, node_name, project_key, number
        );
    };

    let mut output = format!("# Permission {} — {}\n\n", segment, node_name);
    output.push_str(&format!("Status: {}\n\n", request.status));
    output.push_str(&format!("## Request\n\n{}\n\n", request.summary));
    match request.response.as_deref().filter(|r| !r.is_empty()) {
        Some(response) => output.push_str(&format!("## Resolution\n\n{}\n\n", response)),
        None => output.push_str("## Resolution\n\n(awaiting answer)\n\n"),
    }

    output
}

/// Permissions raised by a sub-agent task job. The task analogue of
/// [`read_node_permissions`]: resolves the task job (parent node + task
/// segment) and reuses the same job-id-keyed permission loader, so a sub-task's
/// permissions resolve at the canonical `.../task/{task}/permissions` URI
/// instead of an unresolvable top-level-node URI (issue #143).
pub(super) async fn read_task_permissions(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let requests = load_node_permissions(&conn, &task_job.id).await;

    let mut output = format!("# Permissions — {}/{}\n\n", node_name, task_name);
    if requests.is_empty() {
        output.push_str("No permission requests yet.\n\n");
    } else {
        output.push_str(&format!("{} permission request(s)\n\n", requests.len()));
        for request in &requests {
            let icon = if request.status == "pending" {
                "\u{23f3}"
            } else {
                "\u{2713}"
            };
            output.push_str(&format!(
                "- [{}]({}) [{}] {}\n",
                request.segment,
                build_task_permission_uri(
                    project_key,
                    number,
                    exec_seq,
                    node_name,
                    task_name,
                    &request.segment
                ),
                icon,
                request.summary,
            ));
            if request.status != "pending" {
                output.push_str(&format!("  → {}\n", request.status));
            }
        }
        output.push('\n');
    }
    output
}

pub(super) async fn read_task_permission(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    segment: &str,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let requests = load_node_permissions(&conn, &task_job.id).await;
    let Some(request) = requests.into_iter().find(|r| r.segment == segment) else {
        return format!(
            "Permission '{}' not found for task '{}' in node '{}' of issue {}-{}",
            segment, task_name, node_name, project_key, number
        );
    };

    let mut output = format!("# Permission {} — {}/{}\n\n", segment, node_name, task_name);
    output.push_str(&format!("Status: {}\n\n", request.status));
    output.push_str(&format!("## Request\n\n{}\n\n", request.summary));
    match request.response.as_deref().filter(|r| !r.is_empty()) {
        Some(response) => output.push_str(&format!("## Resolution\n\n{}\n\n", response)),
        None => output.push_str("## Resolution\n\n(awaiting answer)\n\n"),
    }

    output
}

pub(super) async fn read_node(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    diff_full: bool,
) -> String {
    let routed_db = orch.db.for_project(project_key).await;
    let db = &*routed_db;
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };

    // Get project and issue
    let (_, issue_id) = match resolve_issue_id(&conn, project_key, number).await {
        Ok(resolved) => resolved,
        Err(e) => return e,
    };

    // Find job by node name (looks up name in execution snapshot)
    let job = match find_job_by_node_name(&conn, &issue_id, node_name, exec_seq).await {
        Some(j) => j,
        None => {
            // No agent job matches this segment. It may be an action node (e.g.
            // a `pr` node), which is an action_run, not a job. Resolve and
            // render that instead (CAIRN-1222).
            if let Some(action_run) =
                find_action_run_by_node_name(&conn, &issue_id, node_name, exec_seq).await
            {
                return render_action_node(
                    orch,
                    project_key,
                    number,
                    exec_seq,
                    node_name,
                    &action_run,
                    diff_full,
                )
                .await;
            }
            return format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            );
        }
    };

    let visible_node_segment = visible_job_node_segment(&conn, &job).await;

    let mut output = render_job_summary_header(
        node_name,
        &job.status,
        job.started_at.map(i64::from),
        job.completed_at.map(i64::from),
    );

    // Activity gives the summary something to decide on before an artifact
    // exists: how much has happened (turns/runs/events), when it last moved, and
    // the latest assistant line. Skipped entirely for a node with no events yet.
    if let Some(activity) = render_node_activity(&conn, &job.id).await {
        output.push_str(&activity);
        output.push('\n');
    }

    // Build resource URIs for available sub-resources
    let base_uri = build_node_uri(project_key, number, exec_seq, &visible_node_segment);
    output.push_str("## Resources\n\n");
    output.push_str(&format!("- Chat: `{}/chat`\n", base_uri));

    // Surface every named artifact this node has produced at its canonical,
    // schema-named URI (e.g. `/plan`, `/create-pr`) — never the generic
    // `/artifact` alias (CAIRN-1219). Each distinct addressed name owns its own
    // version chain, so a node may surface several; a single-artifact node lists
    // exactly one, rendered identically to before.
    let mut named_artifacts = list_named_artifacts_for_job(&conn, &job.id).await;
    if named_artifacts.is_empty() {
        // The node's own artifact may live on a child job (a delegated task);
        // fall back to the child-inclusive lookup so it still surfaces.
        named_artifacts.extend(get_artifact_for_job(&conn, &job.id).await);
    }
    let artifact_uris: Vec<String> = named_artifacts
        .iter()
        .map(|artifact| {
            build_node_artifact_uri_named(
                project_key,
                number,
                exec_seq,
                &visible_node_segment,
                artifact.schema_name(),
            )
        })
        .collect();
    match artifact_uris.as_slice() {
        [] => {}
        [single] => output.push_str(&format!("- Artifact: `{}`\n", single)),
        many => {
            output.push_str("- Artifacts:\n");
            for uri in many {
                output.push_str(&format!("  - `{}`\n", uri));
            }
        }
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
        String,
        String,
        String,
    )> = match conn
        .query(
            "
            SELECT mr.github_pr_number, mr.github_pr_url, mr.status, mr.github_review,
                   mr.github_mergeable, mr.checks_status, mr.additions, mr.deletions,
                   mr.source_branch, mr.target_branch, p.repo_path
            FROM merge_requests mr
            JOIN projects p ON mr.project_id = p.id
            WHERE mr.job_id = ?1
            LIMIT 1
            ",
            (job.id.as_str(),),
        )
        .await
    {
        Ok(mut rows) => rows.next().await.ok().flatten().and_then(|row| {
            Some((
                row.opt_i64(0).ok()?.map(|value| value as i32),
                row.opt_text(1).ok()?,
                row.text(2).ok()?,
                row.opt_text(3).ok()?,
                row.opt_text(4).ok()?,
                row.opt_text(5).ok()?,
                row.opt_i64(6).ok()?.map(|value| value as i32),
                row.opt_i64(7).ok()?.map(|value| value as i32),
                row.text(8).ok()?,
                row.text(9).ok()?,
                row.text(10).ok()?,
            ))
        }),
        Err(_) => None,
    };

    if let Some((
        pr_number,
        pr_url,
        pr_status,
        review_decision,
        mergeable,
        checks_status,
        additions,
        deletions,
        source_branch,
        target_branch,
        repo_path,
    )) = pr
    {
        if let (Some(pr_num), Some(url)) = (pr_number, pr_url) {
            output.push_str("\n## Pull Request\n\n");
            output.push_str(&format!("PR #{}: {}\n", pr_num, url));
            output.push_str(&format!("Status: {}\n", pr_status));

            // A jj-conflicted source carries garbage GitHub mergeable/diff state
            // that can read as a clean, mergeable change here even when the branch
            // cannot merge. The conflict can arise out-of-band (base advance /
            // reconcile) with no subsequent PR refresh, so a cached bit would go
            // stale; for an open PR, consult the jj store directly (its single
            // source of truth) so the coordinator sees the blocker BEFORE
            // attempting a merge.
            let source_conflict = if pr_status == "open" {
                crate::pr_data::actions::source_conflict_report(
                    &orch.jj_binary_path,
                    &orch.config_dir,
                    &repo_path,
                    &source_branch,
                    Some(&target_branch),
                )
            } else {
                None
            };

            if let Some(review) = review_decision {
                output.push_str(&format!("Review: {}\n", review));
            }
            if source_conflict.is_some() {
                output.push_str("Mergeable: CONFLICTING\n");
            } else if let Some(merge) = mergeable {
                output.push_str(&format!("Mergeable: {}\n", merge));
            }
            if let Some(checks) = checks_status {
                output.push_str(&format!("Checks: {}\n", checks));
            }
            if additions.is_some() || deletions.is_some() {
                let adds = additions.unwrap_or(0);
                let dels = deletions.unwrap_or(0);
                if source_conflict.is_some() {
                    output.push_str(&format!(
                        "Changes: +{} -{} (stale — branch carries conflicts; resolve before trusting)\n",
                        adds, dels
                    ));
                } else {
                    output.push_str(&format!("Changes: +{} -{}\n", adds, dels));
                }
            }
            if let Some(report) = &source_conflict {
                output.push_str("\n⛔ Conflicted history — cannot merge:\n");
                output.push_str(&crate::pr_data::actions::format_conflicted_commits(
                    &report.commits,
                ));
                output.push('\n');
                output.push_str(&crate::pr_data::actions::conflict_recovery_hint(
                    &source_branch,
                    Some(&target_branch),
                ));
                output.push('\n');
            }
        }
    }

    // Todos section
    if let Ok(todos) = crate::todos::get_todos_for_job(db, &job.id).await {
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

/// Render an action node (an `action_run`, e.g. a `pr` node) at its bare node
/// URI. When the action_run owns a PR (a `merge_requests` row keyed on its id),
/// append the live PR view + merge/close/refresh actions, targeting the bare
/// node URI so a driver patches `.../EXEC/pr` directly. Otherwise emit a minimal
/// status summary. The PR *is* this node's content — there is no artifact row
/// and no output schema (CAIRN-1222).
async fn render_action_node(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    action_run: &ResourceActionRun,
    diff_full: bool,
) -> String {
    let status_icon = match action_run.status.as_str() {
        "complete" => "✓",
        "running" => "◐",
        "failed" => "✗",
        "blocked" => "◼",
        "pending" => "○",
        _ => "?",
    };
    let mut output = format!("# {} [{}]\n\n", node_name, status_icon);
    if action_run.status == "blocked" {
        output.push_str("Awaiting merge/close.\n\n");
    }

    let fmt_ts = |ts: i64| {
        chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    };
    output.push_str(&format!("Created: {}\n", fmt_ts(action_run.created_at)));
    if let Some(started) = action_run.started_at {
        output.push_str(&format!("Started: {}\n", fmt_ts(started)));
    }
    if let Some(completed) = action_run.completed_at {
        output.push_str(&format!("Completed: {}\n", fmt_ts(completed)));
    }
    output.push('\n');

    // The bare node URI is both the content address and the action target.
    let node_uri = build_node_uri(project_key, number, exec_seq, node_name);

    // If this action_run owns a PR, append the live PR section (state / review /
    // mergeable / CI / file stats) plus a merge/close/refresh actions block
    // while the PR is open. Owner-generic: keyed on the action_run id, the same
    // owner key the write path uses (CAIRN-1220).
    match crate::pr_data::actions::render_live_pr_section(
        orch,
        &action_run.id,
        &node_uri,
        diff_full,
    )
    .await
    {
        Some(section) => {
            output.push_str(&section);
        }
        None => {
            output.push_str(&format!("`{}`\n", node_uri));
        }
    }

    output
}

/// Map each turn in the job's primary session to its `turns.sequence` so digest
/// turn numbers and `chat/turn/{n}` addressing agree. Only the first run's
/// session is addressable by the turn URI; turns from rotated successor sessions
/// are intentionally absent and render with appearance-order numbering and no
/// drill-down link.
async fn load_primary_session_turn_sequences(
    conn: &turso::Connection,
    job_id: &str,
) -> HashMap<String, i32> {
    let mut map = HashMap::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT id, sequence
            FROM turns
            WHERE session_id = (
                SELECT session_id FROM runs WHERE job_id = ?1 ORDER BY created_at ASC LIMIT 1
            )
            ",
            (job_id,),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            if let (Ok(id), Ok(seq)) = (row.text(0), row.i64(1)) {
                map.insert(id, seq as i32);
            }
        }
    }
    map
}

pub(super) async fn read_node_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    params: &[QueryParam],
) -> String {
    // `latest=true` flips turn order (newest turn first; events within a turn
    // stay chronological). `offset`/`limit` page whole turns rather than rendered
    // lines, so consume them here instead of letting the shared line view slice
    // through a turn.
    let latest = matches!(find_query_value(params, "latest"), Some("true") | Some("1"));
    let turn_offset =
        find_query_value(params, "offset").and_then(|value| value.parse::<i64>().ok());
    let turn_limit =
        find_query_value(params, "limit").and_then(|value| value.parse::<usize>().ok());
    let leftover: Vec<QueryParam> = params
        .iter()
        .filter(|param| param.key != "latest" && param.key != "offset" && param.key != "limit")
        .cloned()
        .collect();
    if let Some(error) = reject_query_params("node chat", &leftover) {
        return error;
    }

    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let event_rows = load_job_events_ordered(
        &conn,
        &job.id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    if event_rows.is_empty() {
        return "No runs found for this node.".to_string();
    }

    let base_uri = build_node_chat_uri(
        project_key,
        number,
        exec_seq,
        &visible_job_node_segment(&conn, &job).await,
    );
    let turn_sequences = load_primary_session_turn_sequences(&conn, &job.id).await;
    let meta = DigestMeta {
        label: node_name,
        project: project_key,
        number,
        exec_seq,
        status: &job.status,
    };
    format_transcript_digest(
        &event_rows,
        &base_uri,
        &meta,
        &turn_sequences,
        latest,
        turn_offset,
        turn_limit,
    )
}

pub(super) async fn read_node_chat_raw(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let event_rows = load_job_events_ordered(
        &conn,
        &job.id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    if event_rows.is_empty() {
        return "No runs found for this node.".to_string();
    }

    // The raw stream is the verbose, unsummarized transcript — the digest's
    // programmatic/grep fallback.
    let rows: Vec<crate::transcripts::TranscriptRow> =
        event_rows.iter().map(EventRow::to_transcript_row).collect();
    crate::transcripts::format_transcript_full(&rows)
}

/// Read a turn-scoped transcript slice.
///
/// Resolves the turn via the job's first run's session_id. Known limitation:
/// if the job's session was rotated (due to failure), turns from successor
/// sessions are not addressable by this URI — only the initial session's
/// turn namespace is reachable.
pub(super) async fn read_node_chat_turn(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    turn_seq: i32,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    // Get session_id from the job's first run
    let session_id: Option<String> = match conn
        .query(
            "
            SELECT session_id
            FROM runs
            WHERE job_id = ?1
            ORDER BY created_at ASC
            LIMIT 1
            ",
            (job.id.as_str(),),
        )
        .await
    {
        Ok(mut rows) => rows
            .next()
            .await
            .ok()
            .flatten()
            .and_then(|row| row.opt_text(0).ok().flatten()),
        Err(_) => None,
    };

    let session_id = match session_id {
        Some(id) => id,
        None => return "No session found for this node.".to_string(),
    };

    // Find turn by (session_id, sequence = turn_seq)
    let turn_id = match conn
        .query(
            "
            SELECT id
            FROM turns
            WHERE session_id = ?1 AND sequence = ?2
            LIMIT 1
            ",
            params![session_id.as_str(), turn_seq as i64],
        )
        .await
    {
        Ok(mut rows) => match rows
            .next()
            .await
            .ok()
            .flatten()
            .and_then(|row| row.text(0).ok())
        {
            Some(id) => id,
            None => {
                return format!(
                    "Turn {} not found for node '{}' in issue {}-{}",
                    turn_seq, node_name, project_key, number
                )
            }
        },
        Err(_) => {
            return format!(
                "Turn {} not found for node '{}' in issue {}-{}",
                turn_seq, node_name, project_key, number
            )
        }
    };

    // Load events for this turn
    let event_rows = load_turn_events(
        &conn,
        &turn_id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;

    if event_rows.is_empty() {
        return format!(
            "No events found for turn {} in node '{}' of issue {}-{}",
            turn_seq, node_name, project_key, number
        );
    }

    // Format as full transcript (no truncation for focused turn slices)
    crate::transcripts::format_transcript_full(&event_rows)
}

/// Render a stored artifact JSON payload as readable markdown.
///
/// Artifacts are stored as a JSON object of typed fields, but an agent reading
/// one wants the prose, not the envelope. Introspect the payload directly (no
/// schema lookup): a single string field unwraps to its raw multi-line value; a
/// `title` + body pair (e.g. create-pr) renders as `# {title}` then the body;
/// any non-object or non-textual payload falls back to the raw JSON.
pub(super) fn render_artifact_markdown(data: &str) -> String {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(data) else {
        return data.to_string();
    };

    // serde_json::Map is a sorted BTreeMap (no `preserve_order` feature), so
    // field iteration is deterministic and alphabetical.
    let string_fields: Vec<(&String, &str)> = map
        .iter()
        .filter_map(|(key, value)| value.as_str().map(|text| (key, text)))
        .collect();

    match string_fields.len() {
        // No textual content: surface the raw payload rather than an empty body.
        0 => data.to_string(),
        // A single string field (any name) is the document itself, unwrapped to
        // its real lines.
        1 => string_fields[0].1.to_string(),
        _ => {
            let mut out = String::new();
            if let Some(title) = map.get("title").and_then(|value| value.as_str()) {
                out.push_str(&format!("# {title}\n\n"));
            }
            // Remaining string fields as titled sections, alphabetical, with the
            // primary body field (content, else body) emitted last as prose.
            let body_key = if map.contains_key("content") {
                Some("content")
            } else if map.contains_key("body") {
                Some("body")
            } else {
                None
            };
            for (key, value) in &string_fields {
                if key.as_str() == "title" || Some(key.as_str()) == body_key {
                    continue;
                }
                out.push_str(&format!("## {key}\n\n{value}\n\n"));
            }
            if let Some(key) = body_key {
                if let Some(value) = map.get(key).and_then(|value| value.as_str()) {
                    out.push_str(value);
                }
            }
            out.trim_end_matches('\n').to_string()
        }
    }
}

/// Build the schema-aware affordance block for a node/task artifact, deriving
/// the `create` example from the schema the addressed name actually validates
/// against (shared with the write path via `resolve_artifact_contract`). Returns
/// `None` when no schema resolves, leaving the caller to fall back to the static
/// contract block. This is what makes a copied artifact example a valid write
/// even for a custom schema (CAIRN #170).
#[allow(clippy::too_many_arguments)]
pub(super) async fn artifact_affordance_block(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    artifact_name: Option<&str>,
    kind: cairn_common::contract::ResourceKind,
) -> Option<String> {
    let db = orch.db.for_project(project_key).await;
    let job_id = resolve_todos_job_id(&db, project_key, number, exec_seq, node_name, task_name)
        .await
        .ok()?;
    let contract = crate::mcp::handlers::implementation::resolve_artifact_contract(
        orch,
        &job_id,
        task_name,
        artifact_name,
    )
    .await;
    let schema = contract.validation_schema?;
    let schema_value =
        crate::output_schemas::resolve_output_schema(orch.schema_dir.as_deref(), &schema).ok()?;
    super::common::artifact_affordance_with_schema(kind, artifact_name, &schema_value)
}

pub(super) async fn read_node_artifact(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    artifact_name: Option<&str>,
    diff_full: bool,
) -> String {
    let routed_db = orch.db.for_project(project_key).await;
    let db = &*routed_db;
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    // A named read (`.../{node}/plan`) returns that name's own version chain; the
    // generic `/artifact` alias (name: None) keeps the latest-overall behavior
    // for back-compat reads.
    let artifact = match artifact_name {
        Some(name) => get_named_artifact_for_job(&conn, &job.id, name).await,
        None => get_artifact_for_job(&conn, &job.id).await,
    };
    let body = match &artifact {
        Some(a) => render_artifact_markdown(&a.data),
        None => format!(
            "No artifact found for node '{}' in issue {}-{}",
            node_name, project_key, number
        ),
    };

    // Address the artifact at its canonical, schema-named URI (e.g. `/plan`,
    // `/create-pr`). The stored schema name wins over whatever alias the caller
    // read through, so the confirm / request-changes / PR affordances never
    // point at the generic `/artifact` alias (CAIRN-1219).
    let resolved_name = artifact
        .as_ref()
        .and_then(|a| a.schema_name())
        .or(artifact_name);
    let artifact_uri =
        build_node_artifact_uri_named(project_key, number, exec_seq, node_name, resolved_name);

    // While the producing job is blocked, this artifact is a user gate awaiting
    // a human (or external driver). The `confirmed` flag can't be plumbed into
    // the read here, but `blocked` is the precise "gate open" signal — surface
    // how to resolve it via `write`. This block is stateful, so it lives here
    // rather than in the contract-derived `affordance_for_kind`.
    let mut output = if job.status == "blocked" {
        let node_uri = format!(
            "cairn://p/{}/{}/{}/{}",
            project_key, number, exec_seq, node_name
        );
        format!(
            "{body}\n\n## actions\n- [confirm]({artifact_uri}): patch with confirmed:true to approve this gated artifact and advance the DAG. e.g. write({{changes:[{{target:\"{artifact_uri}\",mode:\"patch\",payload:{{confirmed:true}}}}]}})\n- [continue]({node_uri}): append a message to the producing node; it resumes with your feedback and revises (re-blocking on a new version). e.g. write({{changes:[{{target:\"{node_uri}\",mode:\"append\",payload:{{content:\"...\"}}}}]}})"
        )
    } else {
        body
    };

    // If this artifact's job owns a PR (merge_requests row), append the live PR
    // section: body/state/review/mergeable/CI + per-file stats, plus a
    // merge/close/refresh actions block while the PR is open. Non-PR artifacts
    // (no merge_requests row) get nothing extra.
    if let Some(section) =
        crate::pr_data::actions::render_live_pr_section(orch, &job.id, &artifact_uri, diff_full)
            .await
    {
        output.push_str("\n\n");
        output.push_str(&section);
    }

    output
}

/// Read a sub-agent task job's base summary — the task analogue of `read_node`.
pub(super) async fn read_task(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let mut output = render_job_summary_header(
        task_name,
        &task_job.status,
        task_job.started_at.map(i64::from),
        task_job.completed_at.map(i64::from),
    );

    let chat_uri = build_task_chat_uri(project_key, number, exec_seq, node_name, task_name);
    output.push_str("## Resources\n\n");
    output.push_str(&format!("- Chat: `{}`\n", chat_uri));
    // Surface the task artifact at its canonical, schema-named URI rather than
    // the generic `/artifact` alias (CAIRN-1219).
    if let Some(artifact) = get_direct_artifact_for_job(&conn, &task_job.id).await {
        output.push_str(&format!(
            "- Artifact: `{}`\n",
            build_task_artifact_uri_named(
                project_key,
                number,
                exec_seq,
                node_name,
                task_name,
                artifact.schema_name(),
            )
        ));
    }

    output
}

pub(super) async fn read_task_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    params: &[QueryParam],
) -> String {
    // `latest=true` flips turn order; `offset`/`limit` page whole turns.
    let latest = matches!(find_query_value(params, "latest"), Some("true") | Some("1"));
    let turn_offset =
        find_query_value(params, "offset").and_then(|value| value.parse::<i64>().ok());
    let turn_limit =
        find_query_value(params, "limit").and_then(|value| value.parse::<usize>().ok());
    let leftover: Vec<QueryParam> = params
        .iter()
        .filter(|param| param.key != "latest" && param.key != "offset" && param.key != "limit")
        .cloned()
        .collect();
    if let Some(error) = reject_query_params("task chat", &leftover) {
        return error;
    }

    let (conn, parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let event_rows = load_job_events_ordered(
        &conn,
        &task_job.id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    if event_rows.is_empty() {
        return "No runs found for this task.".to_string();
    }

    let base_uri = build_task_chat_uri(
        project_key,
        number,
        exec_seq,
        &visible_job_node_segment(&conn, &parent_job).await,
        task_name,
    );
    let turn_sequences = load_primary_session_turn_sequences(&conn, &task_job.id).await;
    let meta = DigestMeta {
        label: task_name,
        project: project_key,
        number,
        exec_seq,
        status: &task_job.status,
    };
    format_transcript_digest(
        &event_rows,
        &base_uri,
        &meta,
        &turn_sequences,
        latest,
        turn_offset,
        turn_limit,
    )
}

pub(super) async fn read_task_chat_raw(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let event_rows = load_job_events_ordered(
        &conn,
        &task_job.id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    if event_rows.is_empty() {
        return "No runs found for this task.".to_string();
    }

    let rows: Vec<crate::transcripts::TranscriptRow> =
        event_rows.iter().map(EventRow::to_transcript_row).collect();
    crate::transcripts::format_transcript_full(&rows)
}

pub(super) async fn read_task_chat_turn(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    turn_seq: i32,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    // Task turn URIs are only unambiguous when all runs share one session.
    let mut run_session_ids: Vec<Option<String>> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT session_id
            FROM runs
            WHERE job_id = ?1
            ORDER BY created_at ASC
            ",
            (task_job.id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(session_id) = row.opt_text(0) {
                run_session_ids.push(session_id);
            }
        }
    }

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
    let turn_id = match conn
        .query(
            "
            SELECT id
            FROM turns
            WHERE session_id = ?1 AND sequence = ?2
            LIMIT 1
            ",
            params![session_id.as_str(), turn_seq as i64],
        )
        .await
    {
        Ok(mut rows) => match rows
            .next()
            .await
            .ok()
            .flatten()
            .and_then(|row| row.text(0).ok())
        {
            Some(id) => id,
            None => {
                return format!(
                    "Turn {} not found for task '{}' in node '{}' of issue {}-{}",
                    turn_seq, task_name, node_name, project_key, number
                )
            }
        },
        Err(_) => {
            return format!(
                "Turn {} not found for task '{}' in node '{}' of issue {}-{}",
                turn_seq, task_name, node_name, project_key, number
            )
        }
    };

    // Load events for this turn
    let event_rows = load_turn_events(
        &conn,
        &turn_id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;

    if event_rows.is_empty() {
        return format!(
            "No events found for turn {} in task '{}' of node '{}' for issue {}-{}",
            turn_seq, task_name, node_name, project_key, number
        );
    }

    crate::transcripts::format_transcript_full(&event_rows)
}

pub(super) async fn read_task_artifact(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    // Get artifact for this task
    match get_direct_artifact_for_job(&conn, &task_job.id).await {
        Some(artifact) => render_artifact_markdown(&artifact.data),
        None => format!(
            "No artifact found for task '{}' in node '{}' of issue {}-{}",
            task_name, node_name, project_key, number
        ),
    }
}

pub(super) async fn read_node_chat_event(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    run_seq: i32,
    event_seq: i32,
) -> String {
    let (conn, job) =
        match connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    // Get the Nth run (1-indexed run_seq)
    let run_id = match get_nth_run_id(&conn, &job.id, run_seq).await {
        Some(id) => id,
        None => return format!("Run {} not found for node '{}'", run_seq, node_name),
    };

    // Get the specific event
    get_single_event(
        &conn,
        &run_id,
        event_seq,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await
}

#[cfg(test)]
mod task_permission_tests {
    use super::{read_node_permission, read_task_permission};
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("task-perm.db");
        std::mem::forget(temp);
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// Seed an issue with a top-level `builder` node and a `review` sub-agent
    /// task under it, plus a pending fence-crossing permission on the task.
    async fn seed(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',2,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-parent','e','i','p','running','builder','Builder',1,1)",
            "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-task','e','j-parent','i','p','running','review','Review',1,1)",
            "INSERT INTO runs (id, job_id, issue_id, created_at, updated_at) VALUES ('run-task','j-task','i',1,1)",
            "INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input, status, uri_segment, created_at) VALUES ('pr1','run-task','j-task','tu1','bash','{\"summary\":\"cross the fence\"}','pending','perm-1',1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    /// A sub-task permission resolves at its canonical
    /// `.../{parent}/task/{task}/permissions/{seg}` URI, while the old flat
    /// top-level form names a node that does not exist (issue #143).
    #[tokio::test]
    async fn task_permission_resolves_under_parent_while_flat_node_does_not() {
        let db = test_db().await;
        seed(&db).await;

        let resolved = read_task_permission(&db, "PRJ", 2, 1, "builder", "review", "perm-1").await;
        assert!(
            resolved.contains("Permission perm-1") && resolved.contains("cross the fence"),
            "task permission should resolve: {resolved}"
        );
        assert!(
            !resolved.contains("not found"),
            "task permission should resolve: {resolved}"
        );

        // The pre-fix URI named the task's own segment as a top-level node.
        let broken = read_node_permission(&db, "PRJ", 2, 1, "review", "perm-1").await;
        assert!(
            broken.contains("not found"),
            "flat sub-task node URI must not resolve: {broken}"
        );
    }
}

#[cfg(test)]
mod artifact_render_tests {
    use super::render_artifact_markdown;

    #[test]
    fn single_string_field_unwraps_to_real_lines() {
        // The live `[lines 1–1 of 1]` single-line-JSON bug: a stored
        // `{"content":"...multi-line..."}` must render as the real lines.
        let rendered = render_artifact_markdown("{\"content\":\"line1\\nline2\\nline3\"}");
        assert_eq!(rendered, "line1\nline2\nline3");
        assert_eq!(rendered.lines().count(), 3);
    }

    #[test]
    fn title_and_body_render_titled_sections() {
        // create-pr shape: title heading then the body as trailing prose.
        let rendered = render_artifact_markdown(
            "{\"title\":\"My PR\",\"body\":\"Body line one\\nBody line two\"}",
        );
        assert_eq!(rendered, "# My PR\n\nBody line one\nBody line two");
    }

    #[test]
    fn extra_string_fields_become_alphabetical_sections() {
        let rendered = render_artifact_markdown(
            "{\"title\":\"T\",\"content\":\"main body\",\"summary\":\"a summary\"}",
        );
        // title first, then the non-body sections alphabetically, then the
        // primary body field last as prose.
        assert_eq!(rendered, "# T\n\n## summary\n\na summary\n\nmain body");
    }

    #[test]
    fn non_textual_payload_falls_back_to_raw_json() {
        let raw = "{\"count\":3,\"ok\":true}";
        assert_eq!(render_artifact_markdown(raw), raw);
    }

    #[test]
    fn non_object_payload_is_verbatim() {
        assert_eq!(render_artifact_markdown("plain text"), "plain text");
    }
}

#[cfg(test)]
mod activity_tests {
    use super::{activity_preview, count_phrase};

    #[test]
    fn count_phrase_pluralizes_around_one() {
        assert_eq!(count_phrase(1, "turn"), "1 turn");
        assert_eq!(count_phrase(0, "turn"), "0 turns");
        assert_eq!(count_phrase(3, "run"), "3 runs");
    }

    #[test]
    fn activity_preview_takes_first_nonempty_line() {
        assert_eq!(
            activity_preview("\n  \nFirst real line\nsecond line"),
            "First real line"
        );
    }

    #[test]
    fn activity_preview_truncates_long_lines_with_ellipsis() {
        let long = "x".repeat(400);
        let preview = activity_preview(&long);
        assert!(preview.ends_with('\u{2026}'));
        // 160 retained chars plus the ellipsis.
        assert_eq!(preview.chars().count(), 161);
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn read_task_chat_event(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    run_seq: i32,
    event_seq: i32,
) -> String {
    let (conn, _parent_job, task_job) =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    // Get the Nth run (1-indexed run_seq)
    let run_id = match get_nth_run_id(&conn, &task_job.id, run_seq).await {
        Some(id) => id,
        None => {
            return format!(
                "Run {} not found for task '{}' in node '{}'",
                run_seq, task_name, node_name
            )
        }
    };

    // Get the specific event
    get_single_event(
        &conn,
        &run_id,
        event_seq,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await
}
