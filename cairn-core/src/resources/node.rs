//! Node, task, todo, question, and node transcript resource readers.

use std::collections::HashMap;

use cairn_db::turso::params;

use super::common::{
    connect_and_find_node_job, connect_and_find_task_job, connect_for_read,
    find_action_run_by_node_name, find_job_by_node_name, find_query_value, get_artifact_for_job,
    get_direct_artifact_for_job, get_named_artifact_for_job, has_artifact_for_job,
    list_named_artifacts_for_job, reject_query_params, resolve_issue_id, visible_job_node_segment,
    ResourceActionRun,
};
use super::transcript::{
    format_raw_transcript, format_transcript_digest_with, get_nth_run_id, get_single_event,
    load_job_events_ordered, load_turn_events, parse_raw_transcript_format, DigestMeta,
    DigestOptions, RawTranscriptFormat,
};

use crate::storage::{LocalDb, RowExt};
use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_job_todos_uri, build_node_artifact_uri_named, build_node_chat_uri, build_node_checks_uri,
    build_node_permission_uri, build_node_question_uri, build_node_uri, build_node_wakes_uri,
    build_task_artifact_uri, build_task_artifact_uri_named, build_task_chat_uri,
    build_task_checks_uri, build_task_permission_uri,
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
async fn latest_assistant_preview(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Option<String> {
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
async fn render_node_activity(conn: &cairn_db::turso::Connection, job_id: &str) -> Option<String> {
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

/// Read the `/checks` projection for a node: the turn-end (`when:idle`/`when:review`)
/// project-check surface. Renders the live log tail while a suite is in flight and
/// the cached per-check verdicts for the node's current sealed tree. This is the
/// resolvable `content_ref` the failure-resume push inlines, and a general
/// human-readable surface independent of a PR.
pub(super) async fn read_node_checks(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let (_conn, job) =
        match connect_and_find_node_job(&orch.db.local, project_key, number, exec_seq, node_name)
            .await
        {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };
    let uri = build_node_checks_uri(project_key, number, exec_seq, node_name);
    let mut out = format!("# Checks \u{2014} {node_name}\n\n`{uri}`\n");
    match crate::execution::checks_turn_end::render_turn_end_checks_section(orch, &job.id).await {
        Some(section) => {
            out.push('\n');
            out.push_str(section.trim_start_matches('\n'));
        }
        None => out.push_str("\nNo turn-end check results yet for the current tree.\n"),
    }
    out
}

/// Read the checks projection for a delegated task job.
pub(super) async fn read_task_checks(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    let (_conn, _parent_job, task_job) = match connect_and_find_task_job(
        &orch.db.local,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return error,
    };
    let uri = build_task_checks_uri(project_key, number, exec_seq, node_name, task_name);
    let mut out = format!("# Checks — {task_name}\n\n{uri}\n");
    match crate::execution::checks_turn_end::render_turn_end_checks_section(orch, &task_job.id)
        .await
    {
        Some(section) => {
            out.push('\n');
            out.push_str(section.trim_start_matches('\n'));
        }
        None => out.push_str("\nNo turn-end check results yet for the current tree.\n"),
    }
    out
}

/// Render a node's ephemeral calls (CAIRN-2481): the caller's child jobs that
/// carry a persisted output contract. Calls share the `parent_job_id` linkage
/// with delegated tasks but are distinguished by a non-NULL `output_contract`.
pub(super) async fn read_node_calls(
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

    let mut calls: Vec<(String, Option<String>, Option<String>, String)> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT uri_segment, agent_config_id, task_description, status
            FROM jobs
            WHERE parent_job_id = ?1 AND output_contract IS NOT NULL
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
            let segment = segment.unwrap_or_else(|| "call".to_string());
            calls.push((segment, agent, description, status));
        }
    }

    let mut output = format!("# Calls — {}\n\n", node_name);
    if calls.is_empty() {
        output.push_str("No ephemeral calls yet.\n\n");
    } else {
        output.push_str(&format!("{} call(s)\n\n", calls.len()));
        for (segment, agent, description, status) in &calls {
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
                .unwrap_or_else(|| segment.clone());
            output.push_str(&format!("- {icon} {label} ({segment})\n"));
        }
        output.push('\n');
    }
    output
}

struct TaskListItem {
    job_id: String,
    segment: String,
    agent: Option<String>,
    description: Option<String>,
    status: String,
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

    let mut tasks: Vec<TaskListItem> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT id, uri_segment, agent_config_id, task_description, status
            FROM jobs
            WHERE parent_job_id = ?1
            ORDER BY task_index ASC, created_at ASC
            ",
            (job.id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(task_id), Ok(segment), Ok(agent), Ok(description), Ok(status)) = (
                row.text(0),
                row.opt_text(1),
                row.opt_text(2),
                row.opt_text(3),
                row.text(4),
            ) else {
                continue;
            };
            let segment = segment.unwrap_or_else(|| "task".to_string());
            tasks.push(TaskListItem {
                job_id: task_id,
                segment,
                agent,
                description,
                status,
            });
        }
    }

    let mut output = format!("# Tasks — {}\n\n", node_name);
    if tasks.is_empty() {
        output.push_str("No delegated tasks yet.\n\n");
    } else {
        output.push_str(&format!("{} task(s)\n\n", tasks.len()));
        for task in &tasks {
            let TaskListItem {
                job_id,
                segment,
                agent,
                description,
                status,
            } = task;
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
            let chat_uri = build_task_chat_uri(project_key, number, exec_seq, node_name, segment);
            let mut detail = format!(
                "- [{}]({}) [{}]{} — chat: {}",
                segment,
                chat_uri,
                icon,
                if label.is_empty() {
                    String::new()
                } else {
                    format!(" {}", label)
                },
                chat_uri,
            );
            if has_artifact_for_job(&conn, job_id).await {
                detail.push_str(&format!(
                    " · artifact: {}",
                    build_task_artifact_uri(project_key, number, exec_seq, node_name, segment),
                ));
            } else if status == "failed" || status == "cancelled" {
                detail.push_str(&format!(" — {status}, no verdict"));
            }
            detail.push('\n');
            output.push_str(&detail);
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

async fn load_node_questions(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Vec<NodeQuestionRow> {
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

async fn load_node_permissions(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Vec<NodePermissionRow> {
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
    conn: &cairn_db::turso::Connection,
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
    // Opt-in reseed-fidelity knobs: `messages=full` renders messages unabridged;
    // `diffs=true` inlines each file write's change body beneath its row. Both
    // default off so the digest stays byte-identical for the common case.
    let unabridged = matches!(find_query_value(params, "messages"), Some("full"));
    let inline_diffs = matches!(find_query_value(params, "diffs"), Some("true") | Some("1"));
    let leftover: Vec<QueryParam> = params
        .iter()
        .filter(|param| {
            !matches!(
                param.key.as_str(),
                "latest" | "offset" | "limit" | "messages" | "diffs"
            )
        })
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

    let base_uri = build_node_chat_uri(
        project_key,
        number,
        exec_seq,
        &visible_job_node_segment(&conn, &job).await,
    );
    let meta = DigestMeta {
        label: node_name,
        project: project_key,
        number,
        exec_seq,
        status: &job.status,
    };
    render_job_chat_digest(
        db,
        &conn,
        &job.id,
        &base_uri,
        &meta,
        &DigestOptions {
            latest,
            turn_offset,
            turn_limit,
            unabridged,
            inline_diffs,
        },
    )
    .await
}

/// Canonical `/chat` digest render for an already-resolved job.
///
/// Both the node-chat read resolver and the cold-resume reseed path
/// (CAIRN-2534) route through this, so the digest is produced by exactly one
/// code path rather than re-rendered through a parallel one. `base_uri`/`meta`
/// are caller-built (the cosmetic header line plus truncation drill-down link
/// scaffolding).
async fn render_job_chat_digest(
    db: &LocalDb,
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    base_uri: &str,
    meta: &DigestMeta<'_>,
    opts: &DigestOptions,
) -> String {
    let event_rows = load_job_events_ordered(
        conn,
        job_id,
        db.content_store().map(|s| s.as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    if event_rows.is_empty() {
        return "No runs found for this node.".to_string();
    }
    let turn_sequences = load_primary_session_turn_sequences(conn, job_id).await;
    format_transcript_digest_with(&event_rows, base_uri, meta, &turn_sequences, opts)
}

/// Render the cold-resume seed digest for a job (CAIRN-2534): its `/chat`
/// transcript at full reseed fidelity (`messages=full` + `diffs=true`) in
/// chronological order, framed as the reconstructed prior context.
///
/// Reached from the reseed gate in `continue_job_impl`, which holds only the
/// job — not the node's project/number/exec coordinates. Those are cosmetic
/// here: under `unabridged` no truncation drill-down links are emitted, so a
/// best-effort base_uri/meta suffices while the digest *body* stays exact.
/// Returns the "No runs found" sentinel when the job has no transcript.
pub(crate) async fn render_reseed_digest(db: &LocalDb, job: &crate::db_records::DbJob) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let label = job
        .node_name
        .as_deref()
        .or(job.uri_segment.as_deref())
        .unwrap_or("session");
    let base_uri = build_node_chat_uri(&job.project_id, 0, 0, label);
    let meta = DigestMeta {
        label,
        project: &job.project_id,
        number: 0,
        exec_seq: 0,
        status: &job.status,
    };
    render_job_chat_digest(
        db,
        &conn,
        &job.id,
        &base_uri,
        &meta,
        &DigestOptions {
            latest: false,
            turn_offset: None,
            turn_limit: None,
            unabridged: true,
            inline_diffs: true,
        },
    )
    .await
}

pub(super) async fn read_node_chat_raw(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    params: &[QueryParam],
) -> String {
    let format = match parse_raw_transcript_format(params) {
        Ok(format) => format,
        Err(error) => return error,
    };
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
        return match format {
            RawTranscriptFormat::Markdown => "No runs found for this node.".to_string(),
            RawTranscriptFormat::Json => String::new(),
        };
    }

    format_raw_transcript(&event_rows, format)
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
/// schema lookup): a lone string field unwraps to its raw multi-line value; a
/// `title` + body pair (e.g. create-pr) renders as `# {title}` then the body;
/// a non-object or field-less-of-prose payload falls back to the raw JSON.
/// Otherwise every field is surfaced as a titled section — string fields as
/// prose, and non-string fields (arrays, objects) via `render_json_md` — so a
/// structured report never loses its findings, sources, or stats (CAIRN-2560).
pub(super) fn render_artifact_markdown(data: &str) -> String {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(data) else {
        return data.to_string();
    };

    // A lone single field that is a string is the document itself, unwrapped to
    // its real multi-line value (the plan / single-body convention).
    if map.len() == 1 {
        if let Some(text) = map.values().next().and_then(|value| value.as_str()) {
            return text.to_string();
        }
    }

    // A payload with no textual fields at all (e.g. a pure stats object) has no
    // prose to anchor titled sections; surface the raw JSON, which is
    // deterministic and loses nothing.
    if !map.values().any(|value| value.is_string()) {
        return data.to_string();
    }

    // Multi-field render. serde_json::Map is a sorted BTreeMap (no
    // `preserve_order` feature), so field iteration is deterministic and
    // alphabetical. Every field is surfaced: the `title` string becomes the
    // document heading, the primary body field (content, else body) is emitted
    // last as trailing prose, and every other field — string OR structured —
    // renders as its own titled section. Non-string fields (arrays, objects)
    // are rendered legibly rather than dropped, so a structured report's
    // findings, sources, and stats survive the render (CAIRN-2560).
    let title_heading = map.get("title").and_then(|value| value.as_str());
    // Only treat content/body as trailing prose when it is actually a string;
    // otherwise it renders as a normal structured section like any other field.
    let body_key = if map
        .get("content")
        .and_then(|value| value.as_str())
        .is_some()
    {
        Some("content")
    } else if map.get("body").and_then(|value| value.as_str()).is_some() {
        Some("body")
    } else {
        None
    };

    let mut out = String::new();
    if let Some(title) = title_heading {
        out.push_str(&format!("# {title}\n\n"));
    }
    for (key, value) in &map {
        // Skip the fields rendered elsewhere: the title heading (only when it
        // was consumed as the heading) and the trailing body field.
        if (key.as_str() == "title" && title_heading.is_some()) || Some(key.as_str()) == body_key {
            continue;
        }
        let rendered = match value.as_str() {
            Some(text) => text.to_string(),
            None => render_json_md(value, 0),
        };
        out.push_str(&format!("## {key}\n\n{rendered}\n\n"));
    }
    if let Some(key) = body_key {
        if let Some(value) = map.get(key).and_then(|value| value.as_str()) {
            out.push_str(value);
        }
    }
    out.trim_end_matches('\n').to_string()
}

/// How deep `render_json_md` recurses before falling back to a fenced JSON
/// block. Deep-research reports nest at most a couple of levels
/// (report → findings[] → sources[]); the budget only guards pathological or
/// unrecognizable shapes so nothing is ever silently dropped.
const MAX_JSON_MD_DEPTH: usize = 8;

/// Legibly render a non-string JSON value (array, object, or scalar) as
/// deterministic markdown for an artifact section body, so structured fields
/// like a deep-research report's `findings`/`sources` are surfaced instead of
/// dropped. Scalars become inline text; objects render as `**key**: value`
/// lines with nested containers indented beneath a `**key**:` header; arrays
/// render as bullet lists, with an array of objects nesting each object's
/// fields under its bullet. Anything nested past `MAX_JSON_MD_DEPTH` falls back
/// to a fenced JSON block, which is verbose but still lossless.
fn render_json_md(value: &serde_json::Value, depth: usize) -> String {
    use serde_json::Value;
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(items) => {
            if items.is_empty() {
                return "_(none)_".to_string();
            }
            if depth >= MAX_JSON_MD_DEPTH {
                return fenced_json(value);
            }
            items
                .iter()
                .map(|item| bullet(&render_json_md(item, depth + 1)))
                .collect::<Vec<_>>()
                .join("\n")
        }
        Value::Object(fields) => {
            if fields.is_empty() {
                return "_(none)_".to_string();
            }
            if depth >= MAX_JSON_MD_DEPTH {
                return fenced_json(value);
            }
            let mut lines = Vec::new();
            for (key, field) in fields {
                if field.is_string() || field.is_number() || field.is_boolean() || field.is_null() {
                    lines.push(format!("**{key}**: {}", render_json_md(field, depth + 1)));
                } else {
                    // Container value: header line, then the nested block
                    // indented two spaces so it reads as belonging to this key.
                    lines.push(format!("**{key}**:"));
                    for line in render_json_md(field, depth + 1).lines() {
                        lines.push(format!("  {line}"));
                    }
                }
            }
            lines.join("\n")
        }
    }
}

/// Prefix the first line of a rendered block with a `- ` bullet and indent every
/// continuation line two spaces so a multi-line item stays visually grouped
/// under its bullet.
fn bullet(rendered: &str) -> String {
    let mut out = String::new();
    for (index, line) in rendered.lines().enumerate() {
        if index == 0 {
            out.push_str(&format!("- {line}"));
        } else {
            out.push_str(&format!("\n  {line}"));
        }
    }
    out
}

/// Render a JSON value as a fenced ```json block — the lossless fallback for
/// shapes too deep or irregular to render as prose.
fn fenced_json(value: &serde_json::Value) -> String {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    format!("```json\n{pretty}\n```")
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
    let contract = crate::mcp::handlers::comments_artifacts::resolve_artifact_contract(
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

fn render_gated_artifact_actions(
    body: &str,
    artifact_uri: &str,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let messages_uri = format!(
        "cairn://p/{}/{}/{}/{}/messages",
        project_key, number, exec_seq, node_name
    );
    format!(
        "{body}\n\n## actions\n- [confirm]({artifact_uri}): patch with confirmed:true to approve this gated artifact and advance the DAG. e.g. write({{changes:[{{target:\"{artifact_uri}\",mode:\"patch\",payload:{{confirmed:true}}}}]}})\n- [continue]({messages_uri}): append a message to the producing node; it resumes with your feedback and revises (re-blocking on a new version). e.g. write({{changes:[{{target:\"{messages_uri}\",mode:\"append\",payload:{{content:\"...\"}}}}]}})"
    )
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
        render_gated_artifact_actions(
            &body,
            &artifact_uri,
            project_key,
            number,
            exec_seq,
            node_name,
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
    // Opt-in reseed-fidelity knobs, mirrored from the node chat digest.
    let unabridged = matches!(find_query_value(params, "messages"), Some("full"));
    let inline_diffs = matches!(find_query_value(params, "diffs"), Some("true") | Some("1"));
    let leftover: Vec<QueryParam> = params
        .iter()
        .filter(|param| {
            !matches!(
                param.key.as_str(),
                "latest" | "offset" | "limit" | "messages" | "diffs"
            )
        })
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
    format_transcript_digest_with(
        &event_rows,
        &base_uri,
        &meta,
        &turn_sequences,
        &DigestOptions {
            latest,
            turn_offset,
            turn_limit,
            unabridged,
            inline_diffs,
        },
    )
}

pub(super) async fn read_task_chat_raw(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    params: &[QueryParam],
) -> String {
    let format = match parse_raw_transcript_format(params) {
        Ok(format) => format,
        Err(error) => return error,
    };
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
        return match format {
            RawTranscriptFormat::Markdown => "No runs found for this task.".to_string(),
            RawTranscriptFormat::Json => String::new(),
        };
    }

    format_raw_transcript(&event_rows, format)
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

/// Sentinel body a `?wait` read returns when the call is still running past the
/// long-poll budget. The harness re-issues the read; agents ignore it.
const CALL_WAIT_PENDING: &str = "__CAIRN_CALL_PENDING__";
/// Sentinel body a `?wait` read returns when the call finished without a usable
/// artifact (a failed/crashed run, or a terminal run with no artifact). The
/// harness maps it to a `null` `agent()` result.
pub(crate) const CALL_WAIT_FAILED: &str = "__CAIRN_CALL_FAILED__";

/// Long-poll budget for a single `?wait` read, kept under common proxy/gateway
/// idle timeouts; the harness re-issues on the pending sentinel.
const CALL_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(290);

/// Block until the delegated call/task behind a task-artifact URI reaches a
/// terminal run status, then return its raw artifact JSON (the harness parses
/// it). Returns [`CALL_WAIT_PENDING`] if the budget expires first (the caller
/// re-issues) and [`CALL_WAIT_FAILED`] on a failed/crashed run or a terminal run
/// with no artifact. This is the script-facing await behind the harness
/// `agent()`: spawn a background call, then long-poll its result URI here.
pub(super) async fn wait_for_task_artifact(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> String {
    // Resolve the call job once, then DROP the resolution transaction: it opened
    // with `BEGIN` (a pinned snapshot), so re-reading status through it would
    // never observe the call's completion. Each terminal check below opens a
    // fresh read instead.
    let job_id =
        match connect_and_find_task_job(db, project_key, number, exec_seq, node_name, task_name)
            .await
        {
            Ok((_conn, _parent_job, task_job)) => task_job.id,
            Err(error) => return error,
        };

    // Subscribe BEFORE the first status read so a completion that fires between
    // the check and the wait is not missed (subscribe-then-check ordering).
    let mut rx = orch.run_completions.subscribe();
    let deadline = tokio::time::Instant::now() + CALL_WAIT_BUDGET;

    loop {
        if let Some(body) = terminal_call_body(db, &job_id).await {
            return body;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return CALL_WAIT_PENDING.to_string();
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            // A run finished (this call's or another's); re-check our job status.
            Ok(Ok(_run_id)) => continue,
            // Dropped completions under load; re-check unconditionally.
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            // The broadcast closed (host shutting down) or the budget expired.
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) | Err(_) => {
                return CALL_WAIT_PENDING.to_string()
            }
        }
    }
}

/// Terminal-status body for a call/task job: `Some(raw artifact JSON)` on a
/// successful terminal run, `Some(CALL_WAIT_FAILED)` on failure or a terminal
/// run with no artifact, and `None` while the latest run is still non-terminal.
/// Reads through `db` (a FRESH connection each call) so it observes the call's
/// committed status change — a held read transaction would pin a snapshot.
async fn terminal_call_body(db: &LocalDb, job_id: &str) -> Option<String> {
    let status = latest_run_status(db, job_id).await?;
    if !matches!(
        status.as_str(),
        "complete" | "completed" | "exited" | "failed" | "crashed"
    ) {
        // Latest run still non-terminal — keep the caller parked.
        return None;
    }
    // The return artifact IS the call's completion contract: once written, the
    // call is complete with that payload no matter how its run terminated — a
    // clean exit, a CLI stream that crashed AFTER the artifact landed, or a retry
    // whose latest run crashed (artifacts are job-scoped, so an earlier run's
    // result still resolves). Believing the run's exit type over the artifact is
    // exactly what returned `null` for a finished call and made fan-out look
    // broken (CAIRN-2677 bug 1). Only a terminal run with NO artifact is a genuine
    // failure the harness maps to a `null` `agent()` result. Scoped to the call's
    // CONTRACTED return artifact (not any artifact the run wrote) so an unrelated
    // named artifact can never masquerade as the call's result (CAIRN-2677).
    match crate::artifacts::queries::contracted_return_artifact_data(db, job_id).await {
        Some(data) => Some(data),
        None => Some(CALL_WAIT_FAILED.to_string()),
    }
}

/// Status of the most recent run for a job, or `None` when no run exists yet.
async fn latest_run_status(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    // `rowid DESC` tiebreaks the second-resolution `created_at`
                    // so the newest-inserted run always wins: a call restart
                    // that lands in the same second as its predecessor must
                    // resolve the await against the replacement, not the killed
                    // old run (CAIRN-2516).
                    "SELECT status FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
        })
    })
    .await
    .ok()
    .flatten()
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

    #[test]
    fn deep_research_report_surfaces_findings_and_source_urls() {
        // The CAIRN-2560 regression: a report with several string fields plus
        // structured arrays/objects. The array-of-objects `findings` (each
        // carrying a nested `sources` URL array) and the `stats` object used to
        // be silently dropped once >=2 string fields were present.
        let data = r#"{
            "summary": "The sky is blue from Rayleigh scattering.",
            "question": "Why is the sky blue?",
            "caveats": "A simplified account.",
            "findings": [
                {
                    "claim": "Rayleigh scattering dominates",
                    "confidence": "high",
                    "evidence": "Shorter wavelengths scatter more strongly",
                    "sources": ["https://example.com/rayleigh", "https://example.com/optics"]
                },
                {
                    "claim": "Sunset reddening is the same effect",
                    "confidence": "medium",
                    "evidence": "Longer atmospheric path length",
                    "sources": ["https://example.com/sunset"]
                }
            ],
            "stats": { "sourcesConsulted": 12, "verified": 8 }
        }"#;
        let rendered = render_artifact_markdown(data);

        // Every string field is still present with its section heading.
        assert!(rendered.contains("## summary"), "{rendered}");
        assert!(rendered.contains("The sky is blue from Rayleigh scattering."));
        assert!(rendered.contains("## question"));
        assert!(rendered.contains("## caveats"));

        // The structured findings are surfaced: claim, confidence, evidence.
        assert!(rendered.contains("## findings"), "{rendered}");
        assert!(rendered.contains("Rayleigh scattering dominates"));
        assert!(rendered.contains("**confidence**: high"));
        assert!(rendered.contains("Longer atmospheric path length"));

        // The citations — the whole point — appear in the rendered output.
        assert!(
            rendered.contains("https://example.com/rayleigh"),
            "{rendered}"
        );
        assert!(
            rendered.contains("https://example.com/optics"),
            "{rendered}"
        );
        assert!(
            rendered.contains("https://example.com/sunset"),
            "{rendered}"
        );

        // The stats object is surfaced too.
        assert!(rendered.contains("## stats"));
        assert!(rendered.contains("**sourcesConsulted**: 12"));
        assert!(rendered.contains("**verified**: 8"));
    }

    #[test]
    fn single_string_plus_structured_field_keeps_the_structured_field() {
        // Exactly one string field alongside a structured field used to drop the
        // array (the old 1-string-field branch returned only the string).
        let rendered =
            render_artifact_markdown(r#"{"summary":"S","sources":["https://a","https://b"]}"#);
        assert!(rendered.contains("## summary"), "{rendered}");
        assert!(rendered.contains("S"));
        assert!(rendered.contains("## sources"), "{rendered}");
        assert!(rendered.contains("- https://a"), "{rendered}");
        assert!(rendered.contains("- https://b"), "{rendered}");
    }

    #[test]
    fn array_of_scalars_renders_as_bullet_list() {
        let rendered =
            render_artifact_markdown(r#"{"summary":"S","tags":["alpha","beta","gamma"]}"#);
        assert_eq!(
            rendered,
            "## summary\n\nS\n\n## tags\n\n- alpha\n- beta\n- gamma"
        );
    }

    #[test]
    fn nested_object_field_renders_key_value_lines() {
        let rendered =
            render_artifact_markdown(r#"{"summary":"S","meta":{"model":"opus","turns":3}}"#);
        assert_eq!(
            rendered,
            "## meta\n\n**model**: opus\n**turns**: 3\n\n## summary\n\nS"
        );
    }

    #[test]
    fn empty_containers_are_marked_not_dropped() {
        let rendered = render_artifact_markdown(r#"{"summary":"S","items":[],"meta":{}}"#);
        assert!(rendered.contains("## items\n\n_(none)_"), "{rendered}");
        assert!(rendered.contains("## meta\n\n_(none)_"), "{rendered}");
    }
}

#[cfg(test)]
mod wait_artifact_tests {
    use super::{terminal_call_body, wait_for_task_artifact, CALL_WAIT_FAILED};
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open(tempfile::tempdir().unwrap().keep().join("wait.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    /// Seed a workflow node job (`flow`) with one call sub-job (`c1`) whose
    /// latest run has `run_status`, optionally with an output artifact.
    async fn seed_call(db: &LocalDb, run_status: &str, artifact: Option<&str>) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',3,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-wf','e','i','p','running','flow','Flow',1,1)",
            "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-call','e','j-wf','i','p','complete','c1','Call','Explore',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        db.execute(
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-call','j-call','i',?1,1,1)",
            (run_status,),
        )
        .await
        .unwrap();
        if let Some(data) = artifact {
            db.execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, data, created_at, updated_at) VALUES ('a1','j-call','answer',?1,1,1)",
                (data,),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn wait_returns_raw_artifact_json_for_terminal_call() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "exited", Some(r#"{"answer":"42"}"#)).await;
        // Terminal on the first check: returns immediately with the raw JSON the
        // harness parses (no markdown rendering, no affordance).
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, r#"{"answer":"42"}"#);
    }

    #[tokio::test]
    async fn wait_returns_failed_sentinel_for_failed_call() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "failed", None).await;
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, CALL_WAIT_FAILED);
    }

    #[tokio::test]
    async fn wait_returns_failed_sentinel_when_terminal_without_artifact() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "exited", None).await;
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, CALL_WAIT_FAILED);
    }

    #[tokio::test]
    async fn wait_returns_artifact_for_crashed_call_that_wrote_it() {
        // The return artifact IS the completion contract: a CLI stream that
        // crashed AFTER the artifact landed must still resolve the call with that
        // payload, not `null` (CAIRN-2677 bug 1). Before the fix a `crashed`
        // latest run short-circuited to CALL_WAIT_FAILED without ever looking at
        // the artifact — the exact loss that made fan-out's Blue return `null`.
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "crashed", Some(r#"{"answer":"blue"}"#)).await;
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, r#"{"answer":"blue"}"#);
    }

    #[tokio::test]
    async fn wait_returns_artifact_for_failed_call_that_wrote_it() {
        // Same invariant on a `failed` terminal run: the written artifact wins.
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "failed", Some(r#"{"answer":"red"}"#)).await;
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, r#"{"answer":"red"}"#);
    }

    #[tokio::test]
    async fn wait_returns_failed_sentinel_for_crashed_call_without_artifact() {
        // A genuinely failed call (terminal run, no artifact) still resolves to
        // the failure sentinel — the fix only changes the artifact-present case.
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "crashed", None).await;
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, CALL_WAIT_FAILED);
    }

    #[tokio::test]
    async fn wait_ignores_a_non_return_artifact_under_a_return_contract() {
        // A call contracted to write `return` that wrote ONLY a different named
        // artifact (never `return`) must resolve to the failure sentinel, not the
        // unrelated artifact — the result read is scoped to the contract, the same
        // way the reducer and restart paths are (CAIRN-2677 review).
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "exited", None).await;
        let contract =
            crate::execution::delegation::resolve_delegated_output_contract(None).unwrap();
        let json = serde_json::to_string(&contract).unwrap();
        db.execute(
            "UPDATE jobs SET output_contract = ?1 WHERE id = 'j-call'",
            (json.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO artifacts (id, job_id, artifact_type, data, output_name, created_at, updated_at) \
             VALUES ('a1','j-call','plan','{\"x\":1}','plan',1,1)",
            (),
        )
        .await
        .unwrap();
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 3, 1, "flow", "c1").await;
        assert_eq!(body, CALL_WAIT_FAILED);
    }

    /// A fast call restart lands the replacement run in the SAME second as the
    /// killed predecessor, so their `created_at` ties. The wait path must still
    /// select the replacement (live) run — not the stopped predecessor — or the
    /// workflow's `agent()` resolves `null` prematurely (CAIRN-2516). The
    /// `rowid DESC` tiebreak guarantees the newest-inserted run wins.
    #[tokio::test]
    async fn same_second_restart_selects_replacement_run_not_stopped_predecessor() {
        let (_orch, db) = orch_with_db().await;
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',3,'T','active',1,1)",
            "INSERT INTO jobs (id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-call','i','p','running','c1','Call',1,1)",
            // Predecessor first (lower rowid), replacement second (higher rowid),
            // identical `created_at` (a same-second restart).
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-old','j-call','i','exited',5,5)",
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-new','j-call','i','live',5,5)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        // The replacement (live) run is the latest, so the await stays parked
        // (None) instead of resolving against the stopped predecessor's failure.
        assert_eq!(terminal_call_body(&db, "j-call").await, None);
    }

    #[tokio::test]
    async fn wait_resolves_a_call_under_a_child_workflow_node() {
        let (orch, db) = orch_with_db().await;
        // A top-level caller node `builder`, a workflow `wf` that is a CHILD of
        // the caller (agent_config_id='workflow'), and a call `c1` under the
        // workflow. The call resolves only because a workflow job is
        // node-addressable by its segment despite having a parent.
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',4,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-builder','e','i','p','running','builder','Builder',1,1)",
            "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-wf','e','j-builder','i','p','running','wf','WF','workflow',1,1)",
            "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-c1','e','j-wf','i','p','complete','c1','Call','Explore',1,1)",
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-c1','j-c1','i','exited',1,1)",
            "INSERT INTO artifacts (id, job_id, artifact_type, data, created_at, updated_at) VALUES ('a','j-c1','answer','{\"v\":1}',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let body = wait_for_task_artifact(&orch, &db, "PRJ", 4, 1, "wf", "c1").await;
        assert_eq!(body, r#"{"v":1}"#);
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

#[cfg(test)]
mod gated_artifact_tests {
    use super::render_gated_artifact_actions;

    #[test]
    fn gated_artifact_continue_targets_node_messages_collection() {
        let output = render_gated_artifact_actions(
            "body",
            "cairn://p/CAIRN/2160/1/planner/plan",
            "CAIRN",
            2160,
            1,
            "planner",
        );

        assert!(output.contains("[continue](cairn://p/CAIRN/2160/1/planner/messages)"));
        assert!(output.contains("target:\"cairn://p/CAIRN/2160/1/planner/messages\""));
        assert!(!output.contains("target:\"cairn://p/CAIRN/2160/1/planner\""));
    }
}
