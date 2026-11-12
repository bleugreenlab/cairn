//! Node, task, todo, question, and node transcript resource readers.

use std::collections::{HashMap, HashSet};

use crate::mcp::handlers::durable_suspend::{stuck_resolutions, StuckPhase, StuckResolution};

use super::common::{
    find_action_run_by_node_name, find_job_by_node_name, find_node_job, find_query_value,
    find_task_job, get_direct_artifact_for_job, get_named_artifact_for_job, has_artifact_for_job,
    list_named_artifacts_for_job, load_node_job, load_task_job, read_resource, reject_query_params,
    resolve_issue_id, visible_job_node_segment, ResourceActionRun, ResourceArtifact, ResourceJob,
};
use super::transcript::{
    finish_turn_rows, format_raw_transcript, format_transcript_digest_with, gather_single_event,
    gather_turn_rows, get_nth_run_id, grep_transcript_content, load_job_events_ordered,
    parse_raw_transcript_format, render_single_event, transcript_from_rows, DigestCoordinate,
    DigestMeta, EventRow, RawTranscriptFormat, FIRST_JOB_SESSION,
};
use crate::mcp::handlers::search::{GrepPayload, GrepWindow};
use cairn_models::continuation::DigestOptions;
use futures_util::future::BoxFuture;

use crate::storage::{LocalDb, RowExt, TrackedConnection};

/// One short read that resolves a node's job and runs `rows`' statements
/// against it on the same connection, returning the job and what `rows` read.
/// `rows` is a read closure like any other: statements only, owned results.
#[track_caller]
fn read_node_rows<'s, T: Send + 'static>(
    db: &'s LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    rows: impl for<'a> FnOnce(&'a TrackedConnection, &'a ResourceJob) -> BoxFuture<'a, T>
        + Send
        + 'static,
) -> impl std::future::Future<Output = Result<(ResourceJob, T), String>> + 's {
    let (project_key, node_name) = (project_key.to_owned(), node_name.to_owned());
    read_resource(db, move |conn| {
        Box::pin(async move {
            let job = find_node_job(conn, &project_key, number, exec_seq, &node_name).await?;
            let found = rows(conn, &job).await;
            Ok((job, found))
        })
    })
}

/// [`read_node_rows`] for a delegated task: resolves the parent and task jobs,
/// then runs `rows` with both.
#[track_caller]
fn read_task_rows<'s, T: Send + 'static>(
    db: &'s LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    rows: impl for<'a> FnOnce(&'a TrackedConnection, &'a ResourceJob, &'a ResourceJob) -> BoxFuture<'a, T>
        + Send
        + 'static,
) -> impl std::future::Future<Output = Result<(ResourceJob, ResourceJob, T), String>> + 's {
    let (project_key, node_name, task_name) = (
        project_key.to_owned(),
        node_name.to_owned(),
        task_name.to_owned(),
    );
    read_resource(db, move |conn| {
        Box::pin(async move {
            let (parent_job, task_job) =
                find_task_job(conn, &project_key, number, exec_seq, &node_name, &task_name).await?;
            let found = rows(conn, &parent_job, &task_job).await;
            Ok((parent_job, task_job, found))
        })
    })
}
use cairn_common::query::QueryParam;
use cairn_uri::{
    build_job_todos_uri, build_node_artifact_uri_named, build_node_chat_uri, build_node_checks_uri,
    build_node_permission_uri, build_node_question_uri, build_node_uri, build_node_wakes_uri,
    build_task_artifact_uri, build_task_artifact_uri_named, build_task_chat_uri,
    build_task_checks_uri, build_task_permission_uri,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArtifactFormat {
    Markdown,
    Json,
}

pub(super) fn parse_artifact_format(
    params: &[QueryParam],
    allow_diff: bool,
) -> Result<(ArtifactFormat, bool), String> {
    let unexpected: Vec<&str> = params
        .iter()
        .filter(|param| param.key != "format" && !(allow_diff && param.key == "diff"))
        .map(|param| param.key.as_str())
        .collect();
    if !unexpected.is_empty() {
        return Err(format!(
            "Query parameters are not supported on artifact resources: {}",
            unexpected.join(", ")
        ));
    }

    let format = match params.iter().rev().find(|param| param.key == "format") {
        None => ArtifactFormat::Markdown,
        Some(param) if param.value == "json" => ArtifactFormat::Json,
        Some(param) => {
            return Err(format!(
                "Unsupported artifact format '{}'; expected json",
                param.value
            ))
        }
    };
    let diff = params.iter().rev().find(|param| param.key == "diff");
    if let Some(param) = diff {
        if param.value != "full" {
            return Err(format!(
                "Unsupported artifact diff '{}'; expected full",
                param.value
            ));
        }
        if format == ArtifactFormat::Json {
            return Err(
                "format=json cannot be combined with diff=full; diff is Markdown-only".to_string(),
            );
        }
    }
    Ok((format, diff.is_some()))
}

#[cfg(test)]
mod artifact_format_tests {
    use super::*;

    fn param(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn artifact_format_defaults_and_rejects_invalid_combinations() {
        assert_eq!(
            parse_artifact_format(&[], true).unwrap(),
            (ArtifactFormat::Markdown, false)
        );
        assert_eq!(
            parse_artifact_format(&[param("format", "json")], false).unwrap(),
            (ArtifactFormat::Json, false)
        );
        assert!(parse_artifact_format(&[param("format", "yaml")], true)
            .unwrap_err()
            .contains("expected json"));
        assert!(parse_artifact_format(&[param("other", "x")], false)
            .unwrap_err()
            .contains("other"));
        assert!(
            parse_artifact_format(&[param("format", "json"), param("diff", "full")], true)
                .unwrap_err()
                .contains("Markdown-only")
        );
        assert!(parse_artifact_format(&[param("diff", "full")], false)
            .unwrap_err()
            .contains("diff"));
    }
}

/// Resolve the job id a node- or task-scoped URI addresses, read-only.
///
/// `task_name: None` resolves a node job; `Some` resolves a sub-agent task job.
/// The one resolver every node-scoped READ surface shares -- todos, progress,
/// artifacts, memories, messages, wakes, checks -- so a URI names the same job
/// whichever collection hangs off it.
///
/// A thread that has never run resolves to nothing here, reported as the
/// degradation that points at its overview. Minting the session job is a write,
/// and belongs to [`resolve_node_or_task_job_id`] alone: a read must never
/// bring into existence the thing it was asked to look at.
pub(crate) async fn resolve_node_or_task_job_id_for_read(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
) -> Result<String, String> {
    crate::jobs::queries::job_id_for_node_coordinate(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| match task_name {
        Some(task_name) => match cairn_uri::NodeAddress::new(number, exec_seq, node_name) {
            cairn_uri::NodeAddress::Thread { name } => {
                format!("Task '{task_name}' not found under thread '{name}'")
            }
            cairn_uri::NodeAddress::Node { .. } => {
                format!("Task '{task_name}' not found under node '{node_name}'")
            }
        },
        None => super::common::node_job_not_found_message(project_key, number, exec_seq, node_name),
    })
}

/// Resolve the job id a node- or task-scoped URI addresses, for a WRITE.
///
/// Identical to [`resolve_node_or_task_job_id_for_read`] but for one asymmetry:
/// writing to a thread's own resource mints its session job when the thread has
/// never run, because that write IS the thread starting. Everything else --
/// including every task coordinate -- resolves read-only.
pub(crate) async fn resolve_node_or_task_job_id(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
) -> Result<String, String> {
    if let (cairn_uri::NodeAddress::Thread { name }, None) = (
        cairn_uri::NodeAddress::new(number, exec_seq, node_name),
        task_name,
    ) {
        let (project_key, name) = (project_key.to_string(), name.to_string());
        return db
            .write(move |conn| {
                let project_key = project_key.clone();
                let name = name.clone();
                Box::pin(async move {
                    let thread_id =
                        crate::threads::thread_id_for_coordinate_conn(conn, &project_key, &name)
                            .await?;
                    crate::threads::ensure_thread_session_conn(conn, &thread_id, None).await
                })
            })
            .await
            .map_err(|error| error.to_string());
    }
    resolve_node_or_task_job_id_for_read(db, project_key, number, exec_seq, node_name, task_name)
        .await
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
    crate::clock::stamp(timestamp).unwrap_or_else(|| "Unknown".to_string())
}

/// The node summary's paragraph for one of its calls stuck mid-resolution.
/// Only a phase that is actually failing says so; a resolver waiting on another
/// continuation has delivered its result and is merely slow.
fn describe_stuck_resolution(stuck: &StuckResolution) -> String {
    let since = format_node_timestamp(stuck.since_ms / 1_000);
    let (label, id) = (&stuck.label, &stuck.wait_id);
    match stuck.phase {
        StuckPhase::Delivering => format!(
            "**Stuck delivering a result.** Its `{label}` call settled at {since}, but handing the result back to the agent has failed since then (wait `{id}`). Cairn retries automatically every 30 seconds; the agent stays parked until it lands."
        ),
        StuckPhase::Resuming => format!(
            "**Stuck resuming.** Its `{label}` call settled at {since} and the result reached the agent's transcript, but resuming its turn has failed since then (wait `{id}`). Cairn retries automatically every 30 seconds; the agent stays parked until its turn resumes."
        ),
        StuckPhase::AwaitingContinuation => format!(
            "**Waiting for another continuation to start.** Its `{label}` call settled at {since} and the result reached the agent's transcript; its turn resumes through another continuation that has not started yet (wait `{id}`)."
        ),
    }
}

/// How a parent's task or call listing names a child stuck mid-resolution.
fn stuck_status(stuck: &StuckResolution) -> String {
    let since = format_node_timestamp(stuck.since_ms / 1_000);
    match stuck.phase {
        StuckPhase::Delivering => format!("running, stuck delivering a result since {since}"),
        StuckPhase::Resuming => format!("running, stuck resuming since {since}"),
        StuckPhase::AwaitingContinuation => {
            format!("running, waiting for another continuation to start since {since}")
        }
    }
}

/// The stuck resolutions of `job_ids`, or none if they cannot be read: a
/// listing is better without the notice than not rendered at all.
async fn load_stuck_resolutions(
    db: &LocalDb,
    job_ids: &[String],
    live_resolvers: &HashSet<String>,
) -> Vec<StuckResolution> {
    stuck_resolutions(
        db,
        job_ids,
        chrono::Utc::now().timestamp_millis(),
        live_resolvers,
    )
    .await
    .unwrap_or_else(|error| {
        log::warn!("could not read stuck resolutions: {error}");
        Vec::new()
    })
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

/// The summary's title and timestamps. A job held on its own pending work
/// carries that hold beside its status icon and in a sentence beneath, because
/// `running` alone reads exactly like a node mid-turn.
fn render_job_summary_header(
    name: &str,
    status: &str,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    hold: Option<&crate::messages::delivery::Hold>,
) -> String {
    let icon = job_status_icon(status);
    let mut output = match (status, hold) {
        ("failed", _) => format!("# {name} [{icon} Failed]\n\n"),
        (_, Some(hold)) => format!("# {name} [{icon} {}]\n\n", hold.qualifier()),
        (_, None) => format!("# {name} [{icon}]\n\n"),
    };
    append_optional_timestamp(&mut output, "Started", started_at);
    append_optional_timestamp(&mut output, "Completed", completed_at);
    output.push('\n');
    if let Some(hold) = hold {
        output.push_str(&format!("Not taking a turn: {}.\n\n", hold.render()));
    }
    output
}

/// The job's placement default line, active or cleared by the fleet, beneath
/// the summary header. Nothing when it has none.
async fn push_placement_default(output: &mut String, db: &LocalDb, job_id: &str) {
    use crate::fleet::placement_default::{load, DefaultOwner};
    let Ok(owner) = DefaultOwner::for_job(db, job_id).await else {
        return;
    };
    if let Ok(Some(default)) = load(db, &owner).await {
        output.push_str(&default.line());
        output.push_str("\n\n");
    }
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
    conn: &cairn_db::storage::TrackedConnection,
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

/// What the node summary's `## Activity` section says.
struct NodeActivity {
    runs: i64,
    turns: i64,
    events: i64,
    last_activity: Option<i64>,
    latest: Option<String>,
}

/// Read the node summary's activity: turn/run/event counts, the last-activity
/// timestamp, and the latest assistant line. Returns `None` when the node has
/// no events yet (nothing to summarize beyond the header), so a just-started
/// node stays terse. Counts and timing come from one cheap aggregate that needs
/// no event-body reconstruction.
async fn load_node_activity(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> Option<NodeActivity> {
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
    Some(NodeActivity {
        runs,
        turns,
        events,
        last_activity,
        latest: latest_assistant_preview(conn, job_id).await,
    })
}

/// Render the node summary's `## Activity` section.
fn render_node_activity(activity: &NodeActivity) -> String {
    let mut summary = format!(
        "{} \u{b7} {} \u{b7} {}",
        count_phrase(activity.turns, "turn"),
        count_phrase(activity.runs, "run"),
        count_phrase(activity.events, "event"),
    );
    if let Some(ts) = activity.last_activity {
        summary.push_str(&format!(
            " \u{b7} last active {}",
            format_node_timestamp(ts)
        ));
    }

    let mut output = format!("## Activity\n\n{}\n", summary);
    if let Some(preview) = &activity.latest {
        output.push_str(&format!("\nLatest: {}\n", preview));
    }
    output
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
    let job_id = match resolve_node_or_task_job_id_for_read(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await
    {
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
    let job_id = match resolve_node_or_task_job_id_for_read(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        None,
    )
    .await
    {
        Ok(job_id) => job_id,
        Err(error) => return error,
    };
    render_job_wakes(
        db,
        node_name,
        &build_node_wakes_uri(project_key, number, exec_seq, node_name),
        &job_id,
    )
    .await
}

/// A node or task job's batches that outlived their calls, or one by request
/// id: the handle a detached call answered with.
#[allow(clippy::too_many_arguments)]
pub(super) async fn read_node_batches(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    request_id: Option<&str>,
) -> String {
    use crate::mcp::handlers::run::detached;
    let job_id = match resolve_node_or_task_job_id_for_read(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await
    {
        Ok(job_id) => job_id,
        Err(error) => return error,
    };
    let label = task_name.unwrap_or(node_name);
    let home = match crate::jobs::queries::home_uri_for_job(db, &job_id).await {
        Ok(Some(home)) => home,
        Ok(None) => return "Could not resolve this job's batch home.".to_string(),
        Err(error) => return format!("Could not resolve this job's batch home: {error}"),
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let in_flight = match open_batch_waits(db, &job_id).await {
        Ok(waits) => waits,
        Err(error) => return format!("Could not read this job's in-flight batches: {error}"),
    };
    match request_id {
        Some(request_id) => match detached::load(db, request_id).await {
            Ok(Some(batch)) if batch.job_id == job_id => {
                let mut out = detached::render(&batch, &detached::live_state(orch, &batch));
                if matches!(
                    batch.state,
                    detached::DetachedState::Parked | detached::DetachedState::Running
                ) {
                    if let Some(progress) = detached::live_progress(orch, request_id) {
                        out.push_str(&format!("Progress: {}.\n", progress.describe(now_ms)));
                    }
                    out.push_str(&cancel_line(&home, request_id));
                }
                out
            }
            Ok(None) if in_flight.iter().any(|wait| wait.request_id == request_id) => {
                let wait = in_flight
                    .iter()
                    .find(|wait| wait.request_id == request_id)
                    .expect("checked above");
                render_in_flight(orch, &home, wait, now_ms)
            }
            Ok(None)
                if detached::live_owner(request_id)
                    .is_some_and(|owner| owner.job_id == job_id) =>
            {
                let progress = detached::live_progress(orch, request_id)
                    .map(|progress| format!("Progress: {}.\n", progress.describe(now_ms)))
                    .unwrap_or_default();
                format!(
                    "# Run batch `{request_id}`\n\nState: running; its call is still waiting on it inline.\n{progress}{}",
                    cancel_line(&home, request_id)
                )
            }
            Ok(Some(batch)) => {
                match crate::jobs::queries::home_uri_for_job(db, &batch.job_id).await {
                    Ok(Some(owner)) => format!(
                        "Run batch `{request_id}` belongs to a different job. Read `{owner}/batches/{request_id}`. This collection lists only batches owned by `{home}`, not its tasks."
                    ),
                    _ => format!("Run batch `{request_id}` exists but belongs to a different job, not `{home}`."),
                }
            }
            Ok(None) => format!(
                "No detached run batch `{request_id}` was found. This collection lists only batches owned by `{home}`, not its tasks. A batch whose call answered with its own result has no handle here."
            ),
            Err(error) => format!("Could not read run batch `{request_id}`: {error}"),
        },
        None => match detached::for_job(db, &job_id).await {
            Ok(batches) if batches.is_empty() && in_flight.is_empty() => format!(
                "# Batches ({label})\n\nNo run batch owned by this job is in flight or has kept running past its call. Tasks' batches live under each task's own `batches` collection.\n"
            ),
            Ok(batches) => {
                let mut out = format!("# Batches ({label})\n\nOnly this job's batches; tasks have their own `batches` collections.\n\n");
                if !in_flight.is_empty() {
                    out.push_str("## In flight, their calls suspended\n\n");
                    for wait in &in_flight {
                        let progress = detached::live_progress(orch, &wait.request_id)
                            .map(|progress| format!("; {}", progress.describe(now_ms)))
                            .unwrap_or_default();
                        out.push_str(&format!(
                            "- `{home}/batches/{}` '{}'{progress}\n",
                            wait.request_id,
                            wait.label.as_deref().unwrap_or("unnamed")
                        ));
                    }
                    out.push('\n');
                }
                if !batches.is_empty() {
                    out.push_str("## Outlived their calls\n\n");
                }
                for batch in batches {
                    let live = detached::live_state(orch, &batch);
                    out.push_str(&format!(
                        "- {}\n",
                        detached::summary_line(&batch, &home, &live)
                    ));
                }
                out.push_str("\nCancel one in flight with an `action:\"cancel\"` patch on its own `batches/<request_id>`.\n");
                out
            }
            Err(error) => format!("Could not read this job's batches: {error}"),
        },
    }
}

/// A run batch whose call is suspended on it: its open run-batch wait.
struct OpenBatchWait {
    request_id: String,
    label: Option<String>,
    created_at: i64,
    state: String,
}

/// The job's run batches whose calls are suspended on them, newest first.
async fn open_batch_waits(db: &LocalDb, job_id: &str) -> Result<Vec<OpenBatchWait>, String> {
    db.query_all(
        "SELECT json_extract(condition_json, '$.request_id'), json_extract(condition_json, '$.label'), created_at, state
           FROM agent_waits
          WHERE job_id = ?1 AND state IN ('pending', 'resolving')
            AND json_extract(condition_json, '$.kind') = 'run_batch'
          ORDER BY created_at DESC",
        (job_id.to_string(),),
        |row| {
            Ok(OpenBatchWait {
                request_id: row.text(0)?,
                label: row.opt_text(1)?,
                created_at: row.get::<i64>(2)?,
                state: row.text(3)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

fn cancel_line(home: &str, request_id: &str) -> String {
    format!(
        "\nCancel this batch only: `write({{changes:[{{target:\"{home}/batches/{request_id}\",mode:\"patch\",payload:{{action:\"cancel\"}}}}]}})`. A call waiting on it resumes with a cancelled result; a commit it is already writing stands.\n"
    )
}

/// The page of a batch whose call is suspended on it.
fn render_in_flight(
    orch: &crate::orchestrator::Orchestrator,
    home: &str,
    wait: &OpenBatchWait,
    now_ms: i64,
) -> String {
    use crate::mcp::handlers::run::detached;
    let since = crate::clock::stamp_millis(wait.created_at)
        .map(|stamp| format!(" since {stamp}"))
        .unwrap_or_default();
    let state = if wait.state == "resolving" {
        "settled; its result is being delivered to its call".to_string()
    } else {
        format!("in flight, its call suspended on it{since}")
    };
    let progress = match detached::live_progress(orch, &wait.request_id) {
        Some(progress) => format!("Progress: {}.\n", progress.describe(now_ms)),
        None => "Progress: this runner holds no task for it; it settles from its machine's own report.\n".to_string(),
    };
    format!(
        "# Run batch `{}`\n\nLabel: {}\nState: {state}\n{progress}{}",
        wait.request_id,
        wait.label.as_deref().unwrap_or("unnamed"),
        cancel_line(home, &wait.request_id)
    )
}

/// Deliveries owed to this node's recipient that have waited past their bound:
/// each with its age, why it is owed and why the last attempt did not
/// deliver, so nothing waits invisibly.
async fn render_held_receipts(db: &LocalDb, job_id: &str) -> String {
    let Ok(recipient) = cairn_attention::wakes::stable_recipient_for_job(db, job_id).await else {
        return String::new();
    };
    let now = chrono::Utc::now().timestamp_millis();
    let held = cairn_attention::wakes::overdue_receipts(
        db,
        &recipient,
        now - cairn_attention::wakes::RECEIPT_OVERDUE_MS,
    )
    .await
    .unwrap_or_default();
    // A held firing dropped at its horizon stays listed for as long again.
    let dropped = cairn_attention::wakes::dropped_receipts(
        db,
        &recipient,
        crate::orchestrator::wakes::REBUILD_WAKE_KEY_PREFIX,
        now - crate::routes::HELD_HORIZON_MS,
    )
    .await
    .unwrap_or_default();
    let mut out = String::new();
    if !dropped.is_empty() {
        out.push_str("## Dropped deliveries\n\nOwed to you and given up on:\n\n");
        for receipt in &dropped {
            out.push_str(&format!(
                "- `{}`: {}\n",
                receipt.fact_key,
                receipt.last_refusal.as_deref().unwrap_or("dropped")
            ));
        }
        out.push('\n');
    }
    if held.is_empty() {
        return out;
    }
    out.push_str("## Held deliveries\n\nOwed to you and not yet delivered:\n\n");
    for receipt in held {
        let reasons = receipt
            .reasons
            .iter()
            .map(|reached| {
                serde_json::to_value(&reached.reason)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("kind")
                            .and_then(|kind| kind.as_str())
                            .map(str::to_owned)
                    })
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "- `{}` at {}, waiting {} min (owed for: {reasons}; last attempt: {})\n",
            receipt.fact_key,
            receipt.level.as_str(),
            (now - receipt.created_at) / 60_000,
            receipt
                .last_refusal
                .as_deref()
                .unwrap_or("not attempted yet"),
        ));
    }
    out.push('\n');
    out
}

pub(super) async fn render_job_wakes(db: &LocalDb, label: &str, uri: &str, job_id: &str) -> String {
    let subscriptions =
        match crate::orchestrator::wakes::list_subscriptions_for_job(db, job_id).await {
            Ok(subscriptions) => subscriptions,
            Err(error) => return error,
        };
    let pending =
        match crate::orchestrator::wakes::peek_pending_suppressed_for_job(db, job_id).await {
            Ok(pending) => pending,
            Err(error) => return error,
        };
    // The child issues this node coordinates carry no subscription row: their
    // attention is routed by deriving the parent issue's current driver at wake
    // time (CAIRN-3293), so this section is the only place the watch is visible.
    let coordinated =
        match crate::orchestrator::wakes::coordinated_child_issue_uris_for_job(db, job_id).await {
            Ok(coordinated) => coordinated,
            Err(error) => return error,
        };
    let mut out = format!("# Wakes — {label}\n\n`{uri}`\n\n");
    out.push_str(&render_held_receipts(db, job_id).await);
    if let Ok(thread_id) = crate::orchestrator::wakes::thread_home_for_job(db, job_id).await {
        match crate::orchestrator::wakes::list_wake_schedules(db, &thread_id).await {
            Ok(schedules) if !schedules.is_empty() => {
                out.push_str("## Schedules\n\n");
                let now = chrono::Utc::now().timestamp();
                for schedule in schedules {
                    let next = schedule
                        .next_due_at(now)
                        .map(|at| crate::clock::stamp(at).unwrap_or_else(|| at.to_string()))
                        .unwrap_or_else(|| "unavailable".into());
                    let evaluated = schedule
                        .last_evaluated_at
                        .map(|at| crate::clock::stamp(at).unwrap_or_else(|| at.to_string()))
                        .unwrap_or_else(|| "never".into());
                    let fired = schedule
                        .last_fired_occurrence_at
                        .map(|at| crate::clock::stamp(at).unwrap_or_else(|| at.to_string()))
                        .unwrap_or_else(|| "never".into());
                    out.push_str(&format!(
                        "- `{}` {} every={}ms next={} last_evaluated={} last_fired={} reason={}\n",
                        schedule.id,
                        schedule.state,
                        schedule.every_ms,
                        next,
                        evaluated,
                        fired,
                        schedule.reason
                    ));
                }
                out.push('\n');
            }
            Ok(_) => {}
            Err(error) => return error,
        }
        match crate::orchestrator::wakes::conditions::list_wake_conditions(db, &thread_id).await {
            Ok(conditions) if !conditions.is_empty() => {
                out.push_str("## Conditions\n\n");
                for condition in conditions {
                    let evaluated = condition
                        .last_evaluated_at
                        .map(|at| crate::clock::stamp(at).unwrap_or_else(|| at.to_string()))
                        .unwrap_or_else(|| "never".into());
                    let fired = condition
                        .last_fired_at
                        .map(|at| crate::clock::stamp(at).unwrap_or_else(|| at.to_string()))
                        .unwrap_or_else(|| "never".into());
                    let mut sql = condition.sql.replace('\n', " ");
                    if sql.chars().count() > 120 {
                        sql = format!("{}…", sql.chars().take(119).collect::<String>());
                    }
                    out.push_str(&format!("- `{}` {} once={} last_outcome={} last_evaluated={} last_fired={} reason={} sql=`{}`\n", condition.id, condition.state, condition.once, condition.last_outcome.as_deref().unwrap_or("never"), evaluated, fired, condition.reason, sql));
                }
                out.push('\n');
            }
            Ok(_) => {}
            Err(error) => return error,
        }
    }
    if subscriptions.is_empty() {
        out.push_str("No wake subscriptions.\n");
    } else {
        out.push_str("## Subscriptions\n\n");
        for sub in &subscriptions {
            if let Some(line) = crate::orchestrator::wakes::describe_rebuild_subscription(sub) {
                out.push_str(&line);
                out.push('\n');
                continue;
            }
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
    if !coordinated.is_empty() {
        out.push_str(
            "\n## Coordinated child issues\n\nYou drive these children's parent issue, so their question, permission, review, resolved, and message facts wake you. Derived from the parent edge — no subscription needed, and it moves to the next coordinator execution on its own.\n\n",
        );
        for child_uri in &coordinated {
            out.push_str(&format!("- `{child_uri}`\n"));
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
    out.push_str("\n## Mutations\n\n- append `{subscribe:{kind,ref?,factKinds?}}`\n- append `{schedule:{every:\"6h\",reason:\"…\"}}` (thread homes only)\n- append `{condition:{sql:\"SELECT …\",reason:\"…\",once?:true}}` (thread homes only; evaluated after committed `cairn:~/db` writes)\n- append `{subscribe:{kind:\"terminal\", ref:\"cairn:~/terminal/<slug>\", on:\"exit\"}}` — end your turn and resume when the terminal exits (fires immediately if it already exited)\n- append `{subscribe:{kind:\"terminal\", ref:\"cairn:~/terminal/<slug>\", on:\"output\", phrase:\"ready\"}}` — resume when a literal phrase appears in the running terminal's output (case-sensitive, one-shot; also wakes if the terminal exits first; survives terminal restarts)\n- append `{subscribe:{kind:\"rebuild\", contains:\"<40-hex commit> | cairn://p/<project>/<issue>\"}}` — one-shot: resume when the runner boots with a build whose source commit contains the target by store ancestry (an issue resolves to its merge commit, waiting for it to merge); fires at once if the current boot already contains it; a build with no recorded source commit never satisfies it\n- append `{mute:{kind,ref?,factKinds?}, until?:{kind,ref?}}`\n- append `{mute:{kind:\"checks\",ref?}}` — mute this node's checks by default, or the referenced node's checks; verdicts become passive ride-alongs rather than being discarded\n- patch `{unmute:{kind,ref?}}` (including `kind:\"checks\"`)\n- delete `{unsubscribe:{kind,ref?}}`\n");
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
    let job = match load_node_job(&orch.db.local, project_key, number, exec_seq, node_name).await {
        Ok(job) => job,
        Err(error) => return error,
    };
    let uri = build_node_checks_uri(project_key, number, exec_seq, node_name);
    let mut out = format!("# Checks \u{2014} {node_name}\n\n`{uri}`\n");
    match crate::execution::checks_turn_end::render_turn_end_checks_section(orch, &job.id).await {
        Some(section) => {
            out.push('\n');
            out.push_str(section.trim_start_matches('\n'));
        }
        None => {
            let why =
                crate::execution::checks_turn_end::turn_end_checks_absence(orch, &job.id).await;
            out.push_str(&format!("\n{why}\n"));
        }
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
    let (_parent_job, task_job) = match load_task_job(
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
        None => {
            let why =
                crate::execution::checks_turn_end::turn_end_checks_absence(orch, &task_job.id)
                    .await;
            out.push_str(&format!("\n{why}\n"));
        }
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
    live_resolvers: &HashSet<String>,
) -> String {
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move {
            let mut calls: Vec<CallListItem> = Vec::new();
            if let Ok(mut rows) = conn
                .query(
                    "
                    SELECT uri_segment, agent_config_id, task_description, status, id
                    FROM jobs
                    WHERE parent_job_id = ?1 AND output_contract IS NOT NULL
                    ORDER BY task_index ASC, created_at ASC
                    ",
                    (job.id.as_str(),),
                )
                .await
            {
                while let Ok(Some(row)) = rows.next().await {
                    let (Ok(segment), Ok(agent), Ok(description), Ok(status), Ok(job_id)) = (
                        row.opt_text(0),
                        row.opt_text(1),
                        row.opt_text(2),
                        row.text(3),
                        row.text(4),
                    ) else {
                        continue;
                    };
                    calls.push(CallListItem {
                        job_id,
                        segment: segment.unwrap_or_else(|| "call".to_string()),
                        agent,
                        description,
                        status,
                    });
                }
            }
            calls
        })
    });
    let calls = match read.await {
        Ok((_, calls)) => calls,
        Err(error) => return error,
    };

    let mut output = format!("# Calls — {}\n\n", node_name);
    if calls.is_empty() {
        output.push_str("No ephemeral calls yet.\n\n");
    } else {
        output.push_str(&format!("{} call(s)\n\n", calls.len()));
        let call_ids: Vec<String> = calls.iter().map(|call| call.job_id.clone()).collect();
        let stuck = load_stuck_resolutions(db, &call_ids, live_resolvers).await;
        for CallListItem {
            job_id,
            segment,
            agent,
            description,
            status,
        } in &calls
        {
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
            let stuck_suffix = stuck
                .iter()
                .find(|stuck| &stuck.job_id == job_id)
                .map(|stuck| format!(" — {}", stuck_status(stuck)))
                .unwrap_or_default();
            output.push_str(&format!("- {icon} {label} ({segment}){stuck_suffix}\n"));
        }
        output.push('\n');
    }
    output
}

struct CallListItem {
    job_id: String,
    segment: String,
    agent: Option<String>,
    description: Option<String>,
    status: String,
}

struct TaskListItem {
    job_id: String,
    segment: String,
    agent: Option<String>,
    description: Option<String>,
    status: String,
    has_artifact: bool,
}

pub(super) async fn read_node_tasks(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    live_resolvers: &HashSet<String>,
) -> String {
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move {
            let mut listed: Vec<(String, TaskListItem)> = Vec::new();
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
                    listed.push((
                        task_id.clone(),
                        TaskListItem {
                            job_id: task_id,
                            segment,
                            agent,
                            description,
                            status,
                            has_artifact: false,
                        },
                    ));
                }
            }
            let mut tasks = Vec::with_capacity(listed.len());
            for (task_id, mut task) in listed {
                task.has_artifact = has_artifact_for_job(conn, &task_id).await;
                tasks.push(task);
            }
            tasks
        })
    });
    let tasks = match read.await {
        Ok((_, tasks)) => tasks,
        Err(error) => return error,
    };

    let mut output = format!("# Tasks — {}\n\n", node_name);
    if tasks.is_empty() {
        output.push_str("No delegated tasks yet.\n\n");
    } else {
        output.push_str(&format!("{} task(s)\n\n", tasks.len()));
        let task_ids: Vec<String> = tasks.iter().map(|task| task.job_id.clone()).collect();
        let stuck = load_stuck_resolutions(db, &task_ids, live_resolvers).await;
        for task in &tasks {
            let TaskListItem {
                job_id,
                segment,
                agent,
                description,
                status,
                has_artifact,
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
            if *has_artifact {
                detail.push_str(&format!(
                    " · artifact: {}",
                    build_task_artifact_uri(project_key, number, exec_seq, node_name, segment),
                ));
            } else if status == "failed" || status == "cancelled" {
                detail.push_str(&format!(" — {status}, no verdict"));
            }
            if let Some(stuck) = stuck.iter().find(|stuck| &stuck.job_id == job_id) {
                detail.push_str(&format!(" — {}", stuck_status(stuck)));
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
    prompt_id: String,
    segment: String,
    questions_json: String,
    response: Option<String>,
    answered: bool,
    resolution: Option<cairn_db::models::ResolutionReceipt>,
    /// `Some` when withdrawn, holding the stored cause if there is one.
    withdrawn: Option<Option<String>>,
    /// Why the answer did not resume the job, when it did not.
    resume_error: Option<String>,
}

impl NodeQuestionRow {
    /// Where the question stands. A string rather than a flag, so states
    /// beyond pending, answered and withdrawn have somewhere to go.
    fn state(&self) -> &'static str {
        if self.answered {
            "answered"
        } else if self.withdrawn.is_some() {
            "withdrawn"
        } else {
            "pending"
        }
    }
}

async fn load_node_questions(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> Vec<NodeQuestionRow> {
    let mut questions = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT p.uri_segment, p.questions, p.response, p.answered_at,
                   p.resolution_id, p.resolution_surface, p.resolution_provider,
                   p.resolution_conversation, p.resolution_actor, p.created_at,
                   p.withdrawn_at, p.withdrawn_cause, p.resume_error, p.id
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
                prompt_id: row.text(13).unwrap_or_default(),
                segment: segment.unwrap_or_else(|| format!("q-{}", fallback_ordinal)),
                questions_json,
                response,
                answered: answered_at.is_some(),
                resolution: row.opt_text(5).ok().flatten().map(|surface| {
                    cairn_db::models::ResolutionReceipt {
                        id: row.opt_text(4).ok().flatten(),
                        surface,
                        provider: row.opt_text(6).ok().flatten(),
                        conversation: row.opt_text(7).ok().flatten(),
                        actor: row.opt_text(8).ok().flatten(),
                        resolved_at: answered_at.unwrap_or_else(|| row.i64(9).unwrap_or_default()),
                    }
                }),
                withdrawn: row
                    .opt_i64(10)
                    .ok()
                    .flatten()
                    .map(|_| row.opt_text(11).ok().flatten()),
                resume_error: row.opt_text(12).ok().flatten(),
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
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move { load_node_questions(conn, &job.id).await })
    });
    let questions = match read.await {
        Ok((_, questions)) => questions,
        Err(error) => return error,
    };

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
                let answer = displayed_answer(&question.questions_json, response);
                output.push_str(&format!("  → {}\n", answer.replace('\n', "\n    ")));
            }
            if let Some(receipt) = &question.resolution {
                output.push_str(&format!("  {}\n", format_resolution_receipt(receipt)));
            }
            if let Some(line) = resume_refusal_line(question.resume_error.as_deref()) {
                output.push_str(&format!("  \u{26a0} {line}\n"));
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
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move { load_node_questions(conn, &job.id).await })
    });
    let questions = match read.await {
        Ok((_, questions)) => questions,
        Err(error) => return error,
    };
    let Some(question) = questions.into_iter().find(|q| q.segment == segment) else {
        return format!(
            "Question '{}' not found for node '{}' in issue {}/{}",
            segment, node_name, project_key, number
        );
    };

    let mut output = format!("# Question {} — {}\n\n", segment, node_name);
    output.push_str(&format!("Status: {}\n\n", question.state()));

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
        Some(response) => output.push_str(&format!(
            "## Response\n\n{}\n\n",
            displayed_answer(&question.questions_json, response)
        )),
        None => output.push_str("## Response\n\n(awaiting answer)\n\n"),
    }
    if let Some(line) = resume_refusal_line(question.resume_error.as_deref()) {
        output.push_str(&format!("## Resume\n\n{line}\n\n"));
    }

    output
}

/// Block until a question is no longer pending, then return where it stands
/// as one line of JSON: `{"state":"answered","answers":[{index,answer}],"text"}`,
/// `{"state":"withdrawn","cause"}`, or, when the long-poll budget runs out
/// first, `{"state":"pending"}` (the caller reads again). The script-facing
/// await behind the harness `ask()`, as [`wait_for_task_artifact`] is behind
/// `agent()`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn wait_for_question(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    segment: &str,
    reader_run_id: Option<&str>,
) -> String {
    // Answers are broadcast; a withdrawal is not, so the wait also re-reads
    // on a short tick.
    const TICK: std::time::Duration = std::time::Duration::from_secs(5);
    let mut rx = orch.prompt_responses.subscribe();
    let deadline = tokio::time::Instant::now() + CALL_WAIT_BUDGET;
    loop {
        let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
            Box::pin(async move { load_node_questions(conn, &job.id).await })
        });
        let question = match read.await {
            Ok((_, questions)) => questions.into_iter().find(|q| q.segment == segment),
            Err(error) => {
                return serde_json::json!({ "state": "error", "error": error }).to_string()
            }
        };
        let Some(question) = question else {
            return serde_json::json!({
                "state": "error",
                "error": format!("question {segment} not found for node {node_name}"),
            })
            .to_string();
        };
        match question.state() {
            "pending" => {}
            "withdrawn" => {
                return serde_json::json!({
                    "state": "withdrawn",
                    "cause": question.withdrawn.flatten(),
                })
                .to_string()
            }
            state => {
                if let Some(run_id) = reader_run_id {
                    if let Err(error) = crate::messages::queued::take_question_answer_for_reader(
                        db,
                        run_id,
                        &question.prompt_id,
                    )
                    .await
                    {
                        log::warn!("{error}; the answer may also arrive at a tool boundary");
                    }
                }
                let response = question.response.unwrap_or_default();
                let questions: Vec<crate::models::question::Question> =
                    serde_json::from_str(&question.questions_json).unwrap_or_default();
                let stored = crate::models::question::PromptAnswers::decode_stored(&response);
                let text = stored.render(&questions);
                let answers = stored
                    .into_answers(questions.len())
                    .map(|answers| answers.into_answers())
                    .unwrap_or_default();
                return serde_json::json!({ "state": state, "answers": answers, "text": text })
                    .to_string();
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return serde_json::json!({ "state": "pending" }).to_string();
        }
        if let Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) =
            tokio::time::timeout(remaining.min(TICK), rx.recv()).await
        {
            tokio::time::sleep(remaining.min(TICK)).await
        }
    }
}

/// A stored answer as a reader sees it: indexed answers rendered against the
/// prompt's questions, an answer stored before answers were indexed verbatim.
fn displayed_answer(questions_json: &str, response: &str) -> String {
    let questions: Vec<crate::models::question::Question> =
        serde_json::from_str(questions_json).unwrap_or_default();
    crate::models::question::PromptAnswers::decode_stored(response).render(&questions)
}

struct NodePermissionRow {
    segment: String,
    /// Crossing summary (fence) or the tool name (legacy prompt).
    summary: String,
    status: String,
    response: Option<String>,
    resolution: Option<cairn_db::models::ResolutionReceipt>,
    /// Why the answer did not resume the job, when it did not.
    resume_error: Option<String>,
}

/// The line an answered permission or question carries when its answer was
/// recorded but the job could not continue, so the refusal is visible where the
/// answer is.
fn resume_refusal_line(resume_error: Option<&str>) -> Option<String> {
    resume_error.map(|reason| format!("answered, but the resume was refused: {reason}"))
}

fn format_resolution_receipt(receipt: &cairn_db::models::ResolutionReceipt) -> String {
    let mut origin = receipt.surface.clone();
    if let Some(provider) = receipt.provider.as_deref() {
        origin.push_str(&format!(" via {provider}"));
    }
    if let Some(conversation) = receipt.conversation.as_deref() {
        origin.push_str(&format!(" in {conversation}"));
    }
    if let Some(actor) = receipt.actor.as_deref() {
        origin.push_str(&format!(" by {actor}"));
    }
    format!(
        "Resolved {origin} {}",
        crate::clock::age(receipt.resolved_at)
    )
}

async fn load_node_permissions(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> Vec<NodePermissionRow> {
    let mut requests = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT pr.uri_segment, pr.tool_name, pr.tool_input, pr.status, pr.response,
                   pr.resolution_id, pr.resolution_surface, pr.resolution_provider,
                   pr.resolution_conversation, pr.resolution_actor,
                   pr.responded_at, pr.created_at, pr.resume_error
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
                resolution: row.opt_text(6).ok().flatten().map(|surface| {
                    cairn_db::models::ResolutionReceipt {
                        id: row.opt_text(5).ok().flatten(),
                        surface,
                        provider: row.opt_text(7).ok().flatten(),
                        conversation: row.opt_text(8).ok().flatten(),
                        actor: row.opt_text(9).ok().flatten(),
                        resolved_at: row
                            .opt_i64(10)
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| row.i64(11).unwrap_or_default()),
                    }
                }),
                resume_error: row.opt_text(12).ok().flatten(),
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
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move { load_node_permissions(conn, &job.id).await })
    });
    let requests = match read.await {
        Ok((_, requests)) => requests,
        Err(error) => return error,
    };

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
                output.push_str(&format!(
                    "  → {}\n",
                    permission_status_text(&request.status, request.response.as_deref())
                ));
            }
            if let Some(receipt) = &request.resolution {
                output.push_str(&format!("  {}\n", format_resolution_receipt(receipt)));
            }
            if let Some(line) = resume_refusal_line(request.resume_error.as_deref()) {
                output.push_str(&format!("  \u{26a0} {line}\n"));
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
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move { load_node_permissions(conn, &job.id).await })
    });
    let requests = match read.await {
        Ok((_, requests)) => requests,
        Err(error) => return error,
    };
    let Some(request) = requests.into_iter().find(|r| r.segment == segment) else {
        return format!(
            "Permission '{}' not found for node '{}' in issue {}/{}",
            segment, node_name, project_key, number
        );
    };

    let mut output = format!("# Permission {} — {}\n\n", segment, node_name);
    output.push_str(&format!("Status: {}\n\n", request.status));
    output.push_str(&format!("## Request\n\n{}\n\n", request.summary));
    push_permission_resolution(&mut output, &request.status, request.response.as_deref());
    if let Some(line) = resume_refusal_line(request.resume_error.as_deref()) {
        output.push_str(&format!("## Resume\n\n{line}\n\n"));
    }

    output
}

/// A permission's status as a reader should see it: a withdrawn request says
/// why, since it was never answered and never will be.
fn permission_status_text(status: &str, response: Option<&str>) -> String {
    if status == "withdrawn" {
        let cause = crate::mcp::handlers::permission::withdrawn_cause(response)
            .map(|cause| cause.sentence())
            .unwrap_or("its recorded cause could not be read");
        format!("withdrawn unanswered: {cause}")
    } else {
        status.to_string()
    }
}

fn push_permission_resolution(output: &mut String, status: &str, response: Option<&str>) {
    if status == "withdrawn" {
        output.push_str(&format!(
            "## Resolution\n\n{}. It accepts no answer.\n\n",
            permission_status_text(status, response)
        ));
        return;
    }
    match response.filter(|r| !r.is_empty()) {
        Some(response) => output.push_str(&format!("## Resolution\n\n{}\n\n", response)),
        None => output.push_str("## Resolution\n\n(awaiting answer)\n\n"),
    }
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
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        |conn, _parent_job, task_job| {
            Box::pin(async move { load_node_permissions(conn, &task_job.id).await })
        },
    );
    let requests = match read.await {
        Ok((_, _, requests)) => requests,
        Err(error) => return error,
    };

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
                output.push_str(&format!(
                    "  → {}\n",
                    permission_status_text(&request.status, request.response.as_deref())
                ));
            }
            if let Some(line) = resume_refusal_line(request.resume_error.as_deref()) {
                output.push_str(&format!("  \u{26a0} {line}\n"));
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
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        |conn, _parent_job, task_job| {
            Box::pin(async move { load_node_permissions(conn, &task_job.id).await })
        },
    );
    let requests = match read.await {
        Ok((_, _, requests)) => requests,
        Err(error) => return error,
    };
    let Some(request) = requests.into_iter().find(|r| r.segment == segment) else {
        return format!(
            "Permission '{}' not found for task '{}' in node '{}' of issue {}/{}",
            segment, task_name, node_name, project_key, number
        );
    };

    let mut output = format!("# Permission {} — {}/{}\n\n", segment, node_name, task_name);
    output.push_str(&format!("Status: {}\n\n", request.status));
    output.push_str(&format!("## Request\n\n{}\n\n", request.summary));
    push_permission_resolution(&mut output, &request.status, request.response.as_deref());
    if let Some(line) = resume_refusal_line(request.resume_error.as_deref()) {
        output.push_str(&format!("## Resume\n\n{line}\n\n"));
    }

    output
}

#[cfg(test)]
mod withdrawn_permission_rendering_tests {
    use super::*;

    #[test]
    fn a_withdrawn_permission_reads_as_retired_with_its_cause() {
        let response = r#"{"withdrawn":{"cause":"batch_cancelled"}}"#;
        assert_eq!(
            permission_status_text("withdrawn", Some(response)),
            "withdrawn unanswered: the batch that raised it was cancelled"
        );
        let mut output = String::new();
        push_permission_resolution(&mut output, "withdrawn", Some(response));
        assert!(output.contains("It accepts no answer."), "{output}");
        assert!(!output.contains("awaiting answer"), "{output}");
        assert_eq!(permission_status_text("denied", None), "denied");
    }
}

/// Everything a node summary reads from the database, owned, so the network,
/// jj and admission work the summary also does runs after the read releases.
enum NodeSummaryRows {
    Job(Box<JobSummaryRows>),
    /// No agent job matches the segment, but an action node (e.g. a `pr`
    /// node) does: an action_run, not a job (CAIRN-1222).
    Action(ResourceActionRun),
    NotFound,
}

struct JobSummaryRows {
    job: ResourceJob,
    failure_reason: Option<String>,
    visible_node_segment: String,
    activity: Option<NodeActivity>,
    named_artifacts: Vec<ResourceArtifact>,
    pr: Option<NodePrRow>,
}

/// The `merge_requests` row a node summary reports on.
struct NodePrRow {
    number: Option<i32>,
    url: Option<String>,
    status: String,
    review_decision: Option<String>,
    mergeable: Option<String>,
    checks_status: Option<String>,
    additions: Option<i32>,
    deletions: Option<i32>,
    source_branch: String,
    target_branch: String,
    repo_path: String,
}

async fn load_node_pr_row(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> Option<NodePrRow> {
    let mut rows = conn
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
            (job_id,),
        )
        .await
        .ok()?;
    let row = rows.next().await.ok().flatten()?;
    Some(NodePrRow {
        number: row.opt_i64(0).ok()?.map(|value| value as i32),
        url: row.opt_text(1).ok()?,
        status: row.text(2).ok()?,
        review_decision: row.opt_text(3).ok()?,
        mergeable: row.opt_text(4).ok()?,
        checks_status: row.opt_text(5).ok()?,
        additions: row.opt_i64(6).ok()?.map(|value| value as i32),
        deletions: row.opt_i64(7).ok()?.map(|value| value as i32),
        source_branch: row.text(8).ok()?,
        target_branch: row.text(9).ok()?,
        repo_path: row.text(10).ok()?,
    })
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
    let (key, segment) = (project_key.to_owned(), node_name.to_owned());
    let read = read_resource(db, move |conn| {
        Box::pin(async move {
            let (_, issue_id) = resolve_issue_id(conn, &key, number).await?;
            // Find job by node name (looks up name in execution snapshot)
            let Some(job) = find_job_by_node_name(conn, &issue_id, &segment, exec_seq).await else {
                return Ok(
                    match find_action_run_by_node_name(conn, &issue_id, &segment, exec_seq).await {
                        Some(action_run) => NodeSummaryRows::Action(action_run),
                        None => NodeSummaryRows::NotFound,
                    },
                );
            };
            let visible_node_segment = visible_job_node_segment(conn, &job).await;
            let activity = load_node_activity(conn, &job.id).await;
            let named_artifacts = list_named_artifacts_for_job(conn, &job.id).await;
            let pr = load_node_pr_row(conn, &job.id).await;
            let failure_reason = if job.status == "failed" {
                cairn_attention::delivery::job_failure_reason_conn(conn, &job.id)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            Ok(NodeSummaryRows::Job(Box::new(JobSummaryRows {
                job,
                failure_reason,
                visible_node_segment,
                activity,
                named_artifacts,
                pr,
            })))
        })
    });
    let JobSummaryRows {
        job,
        failure_reason,
        visible_node_segment,
        activity,
        named_artifacts,
        pr,
    } = match read.await {
        Ok(NodeSummaryRows::Job(rows)) => *rows,
        Ok(NodeSummaryRows::Action(action_run)) => {
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
        Ok(NodeSummaryRows::NotFound) => {
            return format!(
                "Node '{}' not found for issue {}/{}",
                node_name, project_key, number
            );
        }
        Err(error) => return error,
    };

    let hold = crate::messages::delivery::describe_hold(db, &job.id).await;
    let mut output = render_job_summary_header(
        node_name,
        &job.status,
        job.started_at.map(i64::from),
        job.completed_at.map(i64::from),
        hold.as_ref(),
    );
    if let Some(reason) = failure_reason {
        output.push_str(&format!("Failure: {reason}\n\n"));
    }
    push_placement_default(&mut output, db, &job.id).await;
    if job.status == "pending" && job.started_at.is_none() {
        use crate::jobs::queries::PendingDispatch;
        let pending = if job.delegated {
            PendingDispatch::Delegated
        } else {
            PendingDispatch::AfterRecipe
        };
        output.push_str(&crate::jobs::queries::pending_dispatch_description(pending));
        output.push_str(".\n\n");
    }
    // A launch waiting for a session slot has a run and no process, which reads
    // like a hang unless the summary says where it is in line and why.
    let launch = crate::execution::jobs::describe_job_launch(orch, &job.id).await;
    if let Some(launch) = launch.as_deref() {
        output.push_str(launch);
        output.push('\n');
    }
    // Branch preparation queues for the project store before any run exists,
    // so without this line a new job reads as stalled for as long as it waits.
    let preparation = orch.branch_preparation_wait(&job.id);
    if let Some((status, position, since_ms)) = preparation.as_ref() {
        output.push_str(&crate::orchestrator::describe_preparation_wait(
            status,
            *position,
            &format_node_timestamp(since_ms / 1_000),
        ));
        output.push_str("\n\n");
    }
    if job.status == "running"
        && launch.is_none()
        && preparation.is_none()
        && crate::jobs::queries::latest_run_id_for_job(db, &job.id)
            .await
            .is_none()
    {
        output.push_str(crate::jobs::queries::PREPARING_DESCRIPTION);
        output.push_str("\n\n");
    }
    // A call that settled but whose result never made it back reads like a
    // healthy `running` node with an outstanding call unless the summary says
    // otherwise.
    let live = crate::mcp::handlers::durable_suspend::live_resolvers(orch);
    for stuck in load_stuck_resolutions(db, std::slice::from_ref(&job.id), &live).await {
        output.push_str(&describe_stuck_resolution(&stuck));
        output.push_str("\n\n");
    }

    // Activity gives the summary something to decide on before an artifact
    // exists: how much has happened (turns/runs/events), when it last moved, and
    // the latest assistant line. Skipped entirely for a node with no events yet.
    if let Some(activity) = &activity {
        output.push_str(&render_node_activity(activity));
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

    if let Some(NodePrRow {
        number: pr_number,
        url: pr_url,
        status: pr_status,
        review_decision,
        mergeable,
        checks_status,
        additions,
        deletions,
        source_branch,
        target_branch,
        repo_path,
    }) = pr
    {
        // A non-positive number is not a pull-request binding; see
        // `bound_pr_number` in `pr_data::actions::context`.
        let bound = pr_number.filter(|number| *number > 0).zip(pr_url);
        if bound.is_none() && pr_status == "open" {
            // The row exists but nothing is bound to it. Saying nothing here
            // reads as "this node has no change", which is how a failed open
            // stayed invisible; say what is actually true instead.
            output.push_str(&format!(
                "\n## Pull Request\n\nNot published: no pull request has been opened for `{source_branch}` yet. Refresh the node's PR artifact to re-check GitHub.\n"
            ));
        }
        if let Some((pr_num, url)) = bound {
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
            // The store is read through one pinned view, so this read never
            // settles a split operation log; an unreadable store says so rather
            // than passing the cached GitHub bit off as a verdict.
            let source_conflict: Option<crate::pr_data::actions::SourceConflicts> =
                (pr_status == "open").then(|| {
                    let view = crate::pr_data::actions::StoreView::capture(
                        &orch.jj_binary_path,
                        &orch.config_dir,
                        &repo_path,
                    );
                    crate::pr_data::actions::source_conflict_report(
                        &view,
                        &source_branch,
                        Some(&target_branch),
                    )
                });
            let conflicted = source_conflict.as_ref().and_then(|c| c.report());
            let unknown = source_conflict.as_ref().and_then(|c| c.unknown());

            if let Some(review) = review_decision {
                output.push_str(&format!("Review: {}\n", review));
            }
            if conflicted.is_some() {
                output.push_str("Mergeable: CONFLICTING\n");
            } else if unknown.is_some() {
                match &mergeable {
                    Some(merge) => output.push_str(&format!(
                        "Mergeable: UNKNOWN (GitHub last reported {merge}, which cannot see a \
                         conflict recorded in the branch store)\n"
                    )),
                    None => output.push_str("Mergeable: UNKNOWN\n"),
                }
            } else if let Some(merge) = mergeable {
                output.push_str(&format!("Mergeable: {}\n", merge));
            }
            if let Some(checks) = checks_status {
                output.push_str(&format!("Checks: {}\n", checks));
            }
            if additions.is_some() || deletions.is_some() {
                let adds = additions.unwrap_or(0);
                let dels = deletions.unwrap_or(0);
                if conflicted.is_some() {
                    output.push_str(&format!(
                        "Changes: +{} -{} (stale — branch carries conflicts; resolve before trusting)\n",
                        adds, dels
                    ));
                } else {
                    output.push_str(&format!("Changes: +{} -{}\n", adds, dels));
                }
            }
            if let Some(reason) = unknown {
                output.push('\n');
                output.push_str(&crate::pr_data::actions::unknown_conflict_note(reason));
                output.push('\n');
            }
            if let Some(report) = conflicted {
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

    output.push_str(&format!(
        "Created: {}\n",
        format_node_timestamp(action_run.created_at)
    ));
    if let Some(started) = action_run.started_at {
        output.push_str(&format!("Started: {}\n", format_node_timestamp(started)));
    }
    if let Some(completed) = action_run.completed_at {
        output.push_str(&format!(
            "Completed: {}\n",
            format_node_timestamp(completed)
        ));
    }
    output.push('\n');

    // The bare node URI is both the content address and the action target.
    let node_uri = build_node_uri(project_key, number, exec_seq, node_name);

    match render_pr_publish_history(orch, &action_run.id, &node_uri).await {
        Ok(Some(history)) => output.push_str(&history),
        Ok(None) => {}
        Err(error) => output.push_str(&format!(
            "Publication attempt history unavailable: {error}\n\n"
        )),
    }

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

async fn render_pr_publish_history(
    orch: &crate::orchestrator::Orchestrator,
    action_run_id: &str,
    node_uri: &str,
) -> Result<Option<String>, String> {
    use crate::pr_data::publish_attempts::{
        is_pr_publish_retry_eligible, list_publish_attempts, PR_PUBLISH_MAX_ATTEMPTS,
    };

    let action =
        crate::execution::actions::get_action_run_for_orchestrator(orch, action_run_id).await?;
    if action.action_config_id != "builtin:pr" {
        return Ok(None);
    }
    let node =
        crate::execution::actions::load_action_node(orch, action_run_id, &action.execution_id)
            .await?;
    if node.node_type != crate::models::RecipeNodeType::Pr {
        return Ok(None);
    }
    let attempts = list_publish_attempts(orch, action_run_id).await?;
    let mut output = String::from("## Publication attempts\n\n");
    for attempt in &attempts {
        output.push_str(&format!(
            "- Cycle {}, attempt {}: {}. {}\n",
            attempt.cycle,
            attempt.attempt,
            attempt.outcome.as_deref().unwrap_or("in flight"),
            attempt
                .detail
                .as_deref()
                .unwrap_or("No terminal cause recorded yet."),
        ));
    }
    if attempts.is_empty() {
        output.push_str("No publication attempt has been recorded for this node.\n");
    }
    if action.status == crate::models::ActionRunStatus::Failed {
        if let Some(error) = &action.error_message {
            output.push_str(&format!("\nLast publication error: {error}\n"));
        }
        let current = crate::execution::actions::pr_publish_input_revision(orch, &action).await?;
        let last = attempts
            .iter()
            .max_by_key(|attempt| (attempt.cycle, attempt.attempt));
        let last_revision = last.and_then(|attempt| attempt.input_revision.as_deref());
        let count = last.map_or(0, |attempt| attempt.attempt);
        let reason = if attempts.is_empty() {
            "Automatic retry has no attempt history from before publication journaling. Use action:\"retry\" once to establish a baseline."
        } else if count >= PR_PUBLISH_MAX_ATTEMPTS {
            "The automatic retry limit has been reached."
        } else if current.is_none() {
            "Automatic retry is waiting for resolvable Create-PR artifact and implementation branch inputs."
        } else if last_revision.is_none() {
            "Automatic retry has no recorded input revision to compare against."
        } else if is_pr_publish_retry_eligible(
            current.as_deref(),
            last_revision,
            count,
            PR_PUBLISH_MAX_ATTEMPTS,
        ) {
            "A new input revision is retry-eligible on the next workflow advance."
        } else {
            "Automatic retry is waiting on a new Create-PR handoff or a changed implementation branch head."
        };
        output.push_str(&format!(
            "\n{reason} Explicit retry starts a fresh attempt cycle without requiring changed inputs.\n\nRetry this failed PR node in place: `write({{changes:[{{target:\"{node_uri}\",mode:\"patch\",payload:{{action:\"retry\"}}}}]}})`.\n",
        ));
    } else if action.status == crate::models::ActionRunStatus::Blocked {
        output.push_str("\nPublication succeeded; use `action:\"refresh\"` to refresh the existing PR, not retry.\n");
    }
    output.push('\n');
    Ok(Some(output))
}

/// Map each turn in the job's primary session to its `turns.sequence` so digest
/// turn numbers and `chat/turn/{n}` addressing agree. Only the first run's
/// session is addressable by the turn URI; turns from rotated successor sessions
/// are intentionally absent and render with appearance-order numbering and no
/// drill-down link.
fn primary_session_turns_sql() -> String {
    format!("SELECT id, sequence FROM turns WHERE session_id = ({FIRST_JOB_SESSION})")
}

async fn load_primary_session_turn_sequences(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> HashMap<String, i32> {
    let mut map = HashMap::new();
    if let Ok(mut rows) = conn.query(&primary_session_turns_sql(), (job_id,)).await {
        while let Ok(Some(row)) = rows.next().await {
            if let (Ok(id), Ok(seq)) = (row.text(0), row.i64(1)) {
                map.insert(id, seq as i32);
            }
        }
    }
    map
}

/// The digest options a node or task `/chat` read takes, or the rejection of a
/// param it does not.
///
/// `latest=true` flips turn order (newest turn first; events within a turn stay
/// chronological). `offset`/`limit` page whole turns rather than rendered lines,
/// so they are consumed here instead of letting the shared line view slice
/// through a turn. `messages=full` renders messages unabridged and `diffs=true`
/// inlines each file write's change body beneath its row; both are opt-in
/// reseed-fidelity knobs, off by default so the digest stays byte-identical for
/// the common case.
fn chat_digest_options(resource: &str, params: &[QueryParam]) -> Result<DigestOptions, String> {
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
    if let Some(error) = reject_query_params(resource, &leftover) {
        return Err(error);
    }
    Ok(DigestOptions {
        latest: matches!(find_query_value(params, "latest"), Some("true") | Some("1")),
        turn_offset: find_query_value(params, "offset").and_then(|value| value.parse().ok()),
        turn_limit: find_query_value(params, "limit").and_then(|value| value.parse().ok()),
        unabridged: matches!(find_query_value(params, "messages"), Some("full")),
        inline_diffs: matches!(find_query_value(params, "diffs"), Some("true") | Some("1")),
    })
}

/// A node's or task's whole transcript, loaded for one `/chat` read, with what
/// a rendering of it addresses: `base_uri` is the `/chat` URI its turn links and
/// header extend.
struct LoadedChat {
    rows: Vec<EventRow>,
    turn_sequences: HashMap<String, i32>,
    base_uri: String,
    status: String,
}

impl LoadedChat {
    fn raw_uri(&self) -> String {
        format!("{}/raw", self.base_uri)
    }
}

/// Load a node's transcript for `/chat`. A transcript with no events is an
/// `Err`, so neither the digest nor a grep can present it as content.
///
/// The job resolves in its own short read, which has released before the
/// transcript loads, so nothing holds a transaction across the load and the
/// render (tenet 1260).
async fn load_node_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> Result<LoadedChat, String> {
    let read = read_node_rows(db, project_key, number, exec_seq, node_name, |conn, job| {
        Box::pin(async move {
            (
                visible_job_node_segment(conn, job).await,
                load_primary_session_turn_sequences(conn, &job.id).await,
            )
        })
    });
    let (job, (visible_segment, turn_sequences)) = read.await?;
    let rows = load_job_events_ordered(db, &job.id)
        .await
        .map_err(|error| format!("Failed to read this node's transcript: {error}"))?;
    if rows.is_empty() {
        return Err("No runs found for this node.".to_string());
    }
    Ok(LoadedChat {
        rows,
        turn_sequences,
        base_uri: build_node_chat_uri(project_key, number, exec_seq, &visible_segment),
        status: job.status,
    })
}

pub(super) async fn read_node_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    params: &[QueryParam],
) -> String {
    let opts = match chat_digest_options("node chat", params) {
        Ok(opts) => opts,
        Err(error) => return error,
    };
    let chat = match load_node_chat(db, project_key, number, exec_seq, node_name).await {
        Ok(chat) => chat,
        Err(error) => return error,
    };
    let meta = DigestMeta {
        label: node_name,
        project: project_key,
        coordinate: DigestCoordinate::Issue { number, exec_seq },
        status: &chat.status,
    };
    format_transcript_digest_with(
        &chat.rows,
        &chat.base_uri,
        &meta,
        &chat.turn_sequences,
        crate::clock::host(),
        &opts,
    )
}

/// `grep` on a node's `/chat`: matches over the transcript's full content (see
/// [`grep_transcript_content`]), across the turns the digest params select.
pub(super) async fn grep_node_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    params: &[QueryParam],
    payload: &GrepPayload,
) -> Result<GrepWindow, String> {
    let opts = chat_digest_options("node chat", params)?;
    let chat = load_node_chat(db, project_key, number, exec_seq, node_name).await?;
    grep_transcript_content(
        &chat.rows,
        &chat.turn_sequences,
        crate::clock::host(),
        &opts,
        payload,
        &chat.raw_uri(),
    )
}

/// `grep` over one turn's events, headed by the turn's own number.
fn grep_turn_rows(
    rows: &[EventRow],
    turn_seq: i32,
    payload: &GrepPayload,
    raw_uri: &str,
) -> Result<GrepWindow, String> {
    let turn_sequences: HashMap<String, i32> = rows
        .iter()
        .filter_map(|row| row.turn_id.clone())
        .map(|turn_id| (turn_id, turn_seq))
        .collect();
    grep_transcript_content(
        rows,
        &turn_sequences,
        crate::clock::host(),
        &DigestOptions::default(),
        payload,
        raw_uri,
    )
}

/// What this job's transcript weighs verbatim: the baseline a cold native resume
/// re-sends, against which a digest reseed is priced (CAIRN-4757).
///
/// The seed and the baseline are one neutral measure: both read the same
/// ordered events through [`transcript_from_rows`], the seed rendered by
/// `cairn_models::continuation::render_digest` and the baseline measured by
/// `cairn_models::continuation::verbatim_bytes`, so the two sides of the
/// comparison describe the same history in the same terms.
///
/// `None` when the job has no transcript to measure, or it could not be read,
/// which leaves the caller on native resume rather than rebuilding against a
/// number it does not have.
pub(crate) async fn reseed_verbatim_baseline_bytes(db: &LocalDb, job_id: &str) -> Option<i64> {
    let event_rows = load_job_events_ordered(db, job_id).await.ok()?;
    if event_rows.is_empty() {
        return None;
    }
    Some(cairn_models::continuation::verbatim_bytes(
        &transcript_from_rows(&event_rows, "", &HashMap::new()),
    ))
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
    let job = match load_node_job(db, project_key, number, exec_seq, node_name).await {
        Ok(job) => job,
        Err(error) => return error,
    };

    render_job_chat_raw(db, &job.id, format).await
}

/// Takes no connection, for the same reason as [`load_node_chat`].
pub(super) async fn render_job_chat_raw(
    db: &LocalDb,
    job_id: &str,
    format: RawTranscriptFormat,
) -> String {
    let event_rows = match load_job_events_ordered(db, job_id).await {
        Ok(rows) => rows,
        Err(error) => return format!("Failed to read this node's transcript: {error}"),
    };
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
    match load_node_chat_turn(db, project_key, number, exec_seq, node_name, turn_seq).await {
        // The full transcript projection: a focused turn slice is not truncated.
        Ok(rows) => crate::transcripts::format_transcript_full(
            &rows
                .iter()
                .map(EventRow::to_transcript_row)
                .collect::<Vec<_>>(),
        ),
        Err(error) => error,
    }
}

/// `grep` on a node's `/chat/turn/N`. A turn that does not resolve is an `Err`,
/// never a zero-match.
pub(super) async fn grep_node_chat_turn(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    turn_seq: i32,
    payload: &GrepPayload,
) -> Result<GrepWindow, String> {
    let rows = load_node_chat_turn(db, project_key, number, exec_seq, node_name, turn_seq).await?;
    let raw_uri = format!(
        "{}/raw",
        build_node_chat_uri(project_key, number, exec_seq, node_name)
    );
    grep_turn_rows(&rows, turn_seq, payload, &raw_uri)
}

/// The reconstructed events of turn `turn_seq` in a node's primary session, or
/// the message for a turn that does not resolve or holds no events.
pub(super) async fn load_node_chat_turn(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    turn_seq: i32,
) -> Result<Vec<EventRow>, String> {
    let label = node_name.to_owned();
    let hot_only = crate::storage::events::history::HistoryReader::new(db).is_hot_only();
    let read = read_node_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        move |conn, job| {
            Box::pin(async move {
                let turn_id = job_turn_id(conn, &job.id, &label, turn_seq).await?;
                let hot = gather_turn_rows(conn, &turn_id, hot_only).await?;
                Ok::<_, String>((turn_id, hot))
            })
        },
    );
    let (turn_id, hot) = read.await?.1?;
    let event_rows = finish_turn_rows(db, &turn_id, hot).await?;

    if event_rows.is_empty() {
        return Err(format!(
            "No events found for turn {} in node '{}'",
            turn_seq, node_name
        ));
    }
    Ok(event_rows)
}

/// The id of turn `turn_seq` in a job's primary session. Statements only. `Err`
/// is the message for a job with no session or no such turn.
async fn job_turn_id(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
    label: &str,
    turn_seq: i32,
) -> Result<String, String> {
    use super::transcript::{chat_turn_target, ChatTurnTarget};
    match chat_turn_target(conn, job_id, turn_seq, false).await? {
        ChatTurnTarget::Turn(turn_id) => Ok(turn_id),
        ChatTurnTarget::NoSession => Err("No session found for this node.".into()),
        _ => Err(format!("Turn {} not found for node '{}'", turn_seq, label)),
    }
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

/// Build the schema-aware affordance spec for a node/task artifact, deriving the
/// `create` payload keys and example from the schema the addressed name actually
/// validates against (shared with the write path via `resolve_artifact_contract`).
/// Returns `None` when no schema resolves, leaving the caller to fall back to the
/// static contract spec. This is what makes a copied artifact example a valid
/// write even for a custom schema (CAIRN #170).
#[allow(clippy::too_many_arguments)]
pub(super) async fn artifact_affordance_spec(
    orch: &crate::orchestrator::Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    artifact_name: Option<&str>,
    current: &cairn_uri::CairnResource,
) -> Option<cairn_common::read::AffordanceSpec> {
    let db = orch.db.for_project(project_key).await;
    let job_id = resolve_node_or_task_job_id_for_read(
        &db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await
    .ok()?;
    job_artifact_affordance_spec(orch, &job_id, task_name, artifact_name, current).await
}

/// [`artifact_affordance_spec`] for a job that is already resolved — a thread's
/// session job, which is addressed by thread name rather than by node
/// coordinates. Same contract, same schema, so an artifact advertises on read
/// what it enforces on write.
pub(super) async fn job_artifact_affordance_spec(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    task_name: Option<&str>,
    artifact_name: Option<&str>,
    current: &cairn_uri::CairnResource,
) -> Option<cairn_common::read::AffordanceSpec> {
    let contract = crate::mcp::handlers::comments_artifacts::resolve_artifact_contract(
        orch,
        job_id,
        task_name,
        artifact_name,
    )
    .await;
    let schema = contract.validation_schema?;
    let schema_value =
        crate::output_schemas::resolve_output_schema(orch.schema_dir.as_deref(), &schema).ok()?;
    super::common::artifact_affordance_spec_with_schema(
        current.kind(),
        artifact_name,
        &schema_value,
        Some(current),
    )
}

fn render_gated_artifact_actions(
    body: &str,
    artifact_uri: &str,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    deferral: Option<&crate::execution::approvals::OpenDeferral>,
) -> String {
    let messages_uri = format!(
        "cairn://p/{}/{}/{}/{}/messages",
        project_key, number, exec_seq, node_name
    );
    let deferred = deferral
        .map(|deferral| {
            let since = chrono::DateTime::from_timestamp(deferral.created_at, 0)
                .map(|at| at.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| deferral.created_at.to_string());
            format!(
                "\n\n_Deferred until {} since {since}._",
                deferral.until_rendered
            )
        })
        .unwrap_or_default();
    let facts = crate::execution::gates::ask_conditions::deferrable_facts().join(", ");
    format!(
        "{body}{deferred}\n\n## actions\n- [confirm]({artifact_uri}): patch with confirmed:true to approve this gated artifact and advance the DAG. e.g. write({{changes:[{{target:\"{artifact_uri}\",mode:\"patch\",payload:{{confirmed:true}}}}]}})\n- [defer]({artifact_uri}): patch with defer:{{until:[...]}} to set it aside until a cause comes true, in the `after:` gate vocabulary ({facts}); you are woken once when every clause holds or one can never hold, and deferring again replaces the last deferral. e.g. write({{changes:[{{target:\"{artifact_uri}\",mode:\"patch\",payload:{{defer:{{until:[{{fact:\"issue_resolution\",issue:\"cairn://p/PROJECT/N\",resolution:\"merged\"}}]}}}}}}]}})\n- [continue]({messages_uri}): append a message to the producing node; it resumes with your feedback and revises (re-blocking on a new version). e.g. write({{changes:[{{target:\"{messages_uri}\",mode:\"append\",payload:{{content:\"...\"}}}}]}})"
    )
}

pub(super) struct ArtifactCoordinates<'a> {
    pub project_key: &'a str,
    pub number: i32,
    pub exec_seq: i32,
    pub node_name: &'a str,
}

pub(super) async fn read_node_artifact(
    orch: &crate::orchestrator::Orchestrator,
    coordinates: ArtifactCoordinates<'_>,
    artifact_name: Option<&str>,
    format: ArtifactFormat,
    diff_full: bool,
) -> String {
    let ArtifactCoordinates {
        project_key,
        number,
        exec_seq,
        node_name,
    } = coordinates;
    let routed_db = orch.db.for_project(project_key).await;
    let db = &*routed_db;
    // A named read (`.../{node}/plan`) returns that name's own version chain; the
    // generic `/artifact` alias (name: None) keeps the latest-overall behavior
    // for back-compat reads.
    let wanted = artifact_name.map(str::to_owned);
    let read = read_node_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        move |conn, job| {
            Box::pin(async move {
                match wanted {
                    Some(name) => get_named_artifact_for_job(conn, &job.id, &name).await,
                    None => get_direct_artifact_for_job(conn, &job.id).await,
                }
            })
        },
    );
    let (job, artifact) = match read.await {
        Ok(read) => read,
        Err(error) => return error,
    };
    // A thread keeps no artifacts. Its session's addresses still resolve, to
    // the refusal that says where its record lives and then, as history, the
    // last version stored there (the retired arc, which tenet evidence, posts
    // and eras still cite).
    if format != ArtifactFormat::Json
        && matches!(
            cairn_uri::NodeAddress::new(number, exec_seq, node_name),
            cairn_uri::NodeAddress::Thread { .. }
        )
    {
        let refusal = crate::threads::thread_artifact_refusal(artifact_name);
        return match &artifact {
            Some(a) => format!(
                "{refusal}\n\n## History: the last version stored here (read-only)\n\n{}",
                render_artifact_markdown(&a.data)
            ),
            None => refusal,
        };
    }
    let body = match &artifact {
        Some(a) if format == ArtifactFormat::Json => a.data.clone(),
        Some(a) => render_artifact_markdown(&a.data),
        None => format!(
            "No artifact found for node '{}' in issue {}/{}",
            node_name, project_key, number
        ),
    };

    if format == ArtifactFormat::Json {
        return body;
    }

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

    // While this exact version waits on its approver, surface how to resolve
    // it via `write`. Keyed on the one pending-approval definition, not on
    // `blocked`: a planner whose plan waits can be stored `running`. This
    // block is stateful, so it lives here rather than in the contract-derived
    // `affordance_for_kind`.
    let pending = crate::execution::approvals::pending_approvals(
        db,
        crate::execution::approvals::ApprovalScope::Jobs(vec![job.id.clone()]),
    )
    .await
    .ok()
    .and_then(|approvals| approvals.into_iter().next())
    // The pending version is the newest of its chain, and a read shows a
    // chain's newest, so the chain's name identifies it. An unnamed checkpoint
    // chain is the only artifact its job has.
    .filter(|approval| {
        artifact.as_ref().is_some_and(|shown| {
            approval
                .output_name
                .as_deref()
                .is_none_or(|name| shown.schema_name() == Some(name))
        })
    });
    let mut output = match pending {
        Some(approval) => render_gated_artifact_actions(
            &body,
            &artifact_uri,
            project_key,
            number,
            exec_seq,
            node_name,
            approval.deferral.as_ref(),
        ),
        None => body,
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
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        |conn, parent_job, task_job| {
            Box::pin(async move {
                let parent_segment =
                    if task_job.status == "pending" && task_job.started_at.is_none() {
                        Some(visible_job_node_segment(conn, parent_job).await)
                    } else {
                        None
                    };
                let artifact = get_direct_artifact_for_job(conn, &task_job.id).await;
                (parent_segment, artifact)
            })
        },
    );
    let (_, task_job, (parent_segment, artifact)) = match read.await {
        Ok(read) => read,
        Err(error) => return error,
    };

    let hold = crate::messages::delivery::describe_hold(db, &task_job.id).await;
    let mut output = render_job_summary_header(
        task_name,
        &task_job.status,
        task_job.started_at.map(i64::from),
        task_job.completed_at.map(i64::from),
        hold.as_ref(),
    );
    push_placement_default(&mut output, db, &task_job.id).await;
    if let Some(parent_segment) = parent_segment {
        use crate::jobs::queries::PendingDispatch;
        let pending = if task_job.delegated {
            PendingDispatch::Delegated
        } else {
            PendingDispatch::AfterParent(&parent_segment)
        };
        output.push_str(&crate::jobs::queries::pending_dispatch_description(pending));
        output.push_str(".\n\n");
    } else if task_job.status == "running"
        && crate::jobs::queries::latest_run_id_for_job(db, &task_job.id)
            .await
            .is_none()
    {
        output.push_str(crate::jobs::queries::PREPARING_DESCRIPTION);
        output.push_str("\n\n");
    }

    let chat_uri = build_task_chat_uri(project_key, number, exec_seq, node_name, task_name);
    output.push_str("## Resources\n\n");
    output.push_str(&format!("- Chat: `{}`\n", chat_uri));
    // Surface the task artifact at its canonical, schema-named URI rather than
    // the generic `/artifact` alias (CAIRN-1219).
    if let Some(artifact) = artifact {
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
    let opts = match chat_digest_options("task chat", params) {
        Ok(opts) => opts,
        Err(error) => return error,
    };
    let chat = match load_task_chat(db, project_key, number, exec_seq, node_name, task_name).await {
        Ok(chat) => chat,
        Err(error) => return error,
    };
    let meta = DigestMeta {
        label: task_name,
        project: project_key,
        coordinate: DigestCoordinate::Issue { number, exec_seq },
        status: &chat.status,
    };
    format_transcript_digest_with(
        &chat.rows,
        &chat.base_uri,
        &meta,
        &chat.turn_sequences,
        crate::clock::host(),
        &opts,
    )
}

/// `grep` on a task's `/chat`; see [`grep_node_chat`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn grep_task_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    params: &[QueryParam],
    payload: &GrepPayload,
) -> Result<GrepWindow, String> {
    let opts = chat_digest_options("task chat", params)?;
    let chat = load_task_chat(db, project_key, number, exec_seq, node_name, task_name).await?;
    grep_transcript_content(
        &chat.rows,
        &chat.turn_sequences,
        crate::clock::host(),
        &opts,
        payload,
        &chat.raw_uri(),
    )
}

/// [`load_node_chat`] for a delegated task.
async fn load_task_chat(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> Result<LoadedChat, String> {
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        |conn, parent_job, task_job| {
            Box::pin(async move {
                (
                    visible_job_node_segment(conn, parent_job).await,
                    load_primary_session_turn_sequences(conn, &task_job.id).await,
                )
            })
        },
    );
    let (_, task_job, (parent_segment, turn_sequences)) = read.await?;
    let rows = load_job_events_ordered(db, &task_job.id)
        .await
        .map_err(|error| format!("Failed to read this task's transcript: {error}"))?;
    if rows.is_empty() {
        return Err("No runs found for this task.".to_string());
    }
    Ok(LoadedChat {
        rows,
        turn_sequences,
        base_uri: build_task_chat_uri(project_key, number, exec_seq, &parent_segment, task_name),
        status: task_job.status,
    })
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
    let (_parent_job, task_job) =
        match load_task_job(db, project_key, number, exec_seq, node_name, task_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let event_rows = match load_job_events_ordered(db, &task_job.id).await {
        Ok(rows) => rows,
        Err(error) => return format!("Failed to read this task's transcript: {error}"),
    };
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
    match load_task_chat_turn(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        turn_seq,
    )
    .await
    {
        Ok(rows) => crate::transcripts::format_transcript_full(
            &rows
                .iter()
                .map(EventRow::to_transcript_row)
                .collect::<Vec<_>>(),
        ),
        Err(error) => error,
    }
}

/// `grep` on a task's `/chat/turn/N`; see [`grep_node_chat_turn`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn grep_task_chat_turn(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    turn_seq: i32,
    payload: &GrepPayload,
) -> Result<GrepWindow, String> {
    let rows = load_task_chat_turn(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        turn_seq,
    )
    .await?;
    let raw_uri = format!(
        "{}/raw",
        build_task_chat_uri(project_key, number, exec_seq, node_name, task_name)
    );
    grep_turn_rows(&rows, turn_seq, payload, &raw_uri)
}

/// The reconstructed events of a task's turn `turn_seq`, or the message for a
/// turn that does not resolve or holds no events.
pub(super) async fn load_task_chat_turn(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
    turn_seq: i32,
) -> Result<Vec<EventRow>, String> {
    let hot_only = crate::storage::events::history::HistoryReader::new(db).is_hot_only();
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        move |conn, _parent_job, task_job| {
            Box::pin(async move {
                let turn = task_turn_id(conn, &task_job.id, turn_seq).await;
                let hot = match &turn {
                    TaskTurn::Found(id) => gather_turn_rows(conn, id, hot_only).await,
                    _ => Ok(None),
                };
                (turn, hot)
            })
        },
    );
    let (turn, hot) = read.await?.2;
    let turn_id = match turn {
        TaskTurn::Found(turn_id) => turn_id,
        TaskTurn::NoSession => return Err("No session found for this task.".to_string()),
        TaskTurn::Ambiguous => {
            return Err(format!(
                "Task turn URI is ambiguous for task '{}' in node '{}' of issue {}/{} because runs span multiple sessions",
                task_name, node_name, project_key, number
            ))
        }
        TaskTurn::TurnMissing => {
            return Err(format!(
                "Turn {} not found for task '{}' in node '{}' of issue {}/{}",
                turn_seq, task_name, node_name, project_key, number
            ))
        }
    };

    let event_rows = finish_turn_rows(db, &turn_id, hot?).await?;

    if event_rows.is_empty() {
        return Err(format!(
            "No events found for turn {} in task '{}' of node '{}' for issue {}/{}",
            turn_seq, task_name, node_name, project_key, number
        ));
    }
    Ok(event_rows)
}

/// What a task turn URI resolves to.
enum TaskTurn {
    NoSession,
    /// The task's runs span more than one session, so a turn number names no
    /// single turn.
    Ambiguous,
    TurnMissing,
    Found(String),
}

/// The id of turn `turn_seq` in a task's session. Statements only.
async fn task_turn_id(
    conn: &cairn_db::storage::TrackedConnection,
    task_job_id: &str,
    turn_seq: i32,
) -> TaskTurn {
    use super::transcript::{chat_turn_target, ChatTurnTarget};
    match chat_turn_target(conn, task_job_id, turn_seq, true).await {
        Ok(ChatTurnTarget::Turn(turn_id)) => TaskTurn::Found(turn_id),
        Ok(ChatTurnTarget::NoSession) => TaskTurn::NoSession,
        Ok(ChatTurnTarget::Ambiguous) => TaskTurn::Ambiguous,
        _ => TaskTurn::TurnMissing,
    }
}

pub(super) async fn read_task_artifact(
    db: &LocalDb,
    coordinates: ArtifactCoordinates<'_>,
    task_name: &str,
    artifact_name: Option<&str>,
    format: ArtifactFormat,
) -> String {
    let ArtifactCoordinates {
        project_key,
        number,
        exec_seq,
        node_name,
    } = coordinates;
    // Named task reads follow that artifact's version chain. The generic alias
    // retains latest-overall behavior.
    let wanted = artifact_name.map(str::to_owned);
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        move |conn, _parent_job, task_job| {
            Box::pin(async move {
                match wanted {
                    Some(name) => get_named_artifact_for_job(conn, &task_job.id, &name).await,
                    None => get_direct_artifact_for_job(conn, &task_job.id).await,
                }
            })
        },
    );
    let artifact = match read.await {
        Ok((_, _, artifact)) => artifact,
        Err(error) => return error,
    };
    match artifact {
        Some(artifact) if format == ArtifactFormat::Json => artifact.data,
        Some(artifact) => render_artifact_markdown(&artifact.data),
        None => format!(
            "No artifact found for task '{}' in node '{}' of issue {}/{}",
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
    // Resolve the call job once in its own short read. The loop below holds no
    // connection: a read pins a snapshot, so re-reading status through one would
    // never observe the call's completion. Each terminal check opens a fresh
    // read instead.
    let job_id = match load_task_job(db, project_key, number, exec_seq, node_name, task_name).await
    {
        Ok((_parent_job, task_job)) => task_job.id,
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
    let hot_only = crate::storage::events::history::HistoryReader::new(db).is_hot_only();
    let read = read_node_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        move |conn, job| {
            Box::pin(async move {
                let run_id = get_nth_run_id(conn, &job.id, run_seq).await;
                let hot = match &run_id {
                    Some(id) => gather_single_event(conn, id, event_seq, hot_only).await,
                    None => Ok(None),
                };
                (run_id, hot)
            })
        },
    );
    let (run_id, hot) = match read.await {
        Ok((_, (Some(run_id), hot))) => (run_id, hot),
        Ok((_, (None, _))) => return format!("Run {} not found for node '{}'", run_seq, node_name),
        Err(error) => return error,
    };

    render_single_event(db, &run_id, event_seq, hot).await
}

#[cfg(test)]
mod task_permission_tests {
    use super::{
        read_node_permission, read_node_question, read_node_questions, read_task_permission,
        read_task_permissions,
    };
    use crate::storage::LocalDb;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("task-perm.db");
        std::mem::forget(temp);
        let db = LocalDb::open_migrated_for_test(path).await.unwrap();
        db
    }

    /// Seed an issue with a top-level `builder` node and a `review` sub-agent
    /// task under it, plus a pending fence-crossing permission on the task.
    async fn seed(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
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

        let resolved = read_task_permission(&db, "prj", 2, 1, "builder", "review", "perm-1").await;
        assert!(
            resolved.contains("Permission perm-1") && resolved.contains("cross the fence"),
            "task permission should resolve: {resolved}"
        );
        assert!(
            !resolved.contains("not found"),
            "task permission should resolve: {resolved}"
        );

        // The pre-fix URI named the task's own segment as a top-level node.
        let broken = read_node_permission(&db, "prj", 2, 1, "review", "perm-1").await;
        assert!(
            broken.contains("not found"),
            "flat sub-task node URI must not resolve: {broken}"
        );
    }

    /// An answer whose resume was refused says so on the permission row, in the
    /// list and the single read, rather than only in the host log.
    #[tokio::test]
    async fn a_refused_resume_is_rendered_on_the_permission() {
        let db = test_db().await;
        seed(&db).await;
        db.execute(
            "UPDATE permission_requests SET status = 'denied', resume_error = 'session closed' WHERE id = 'pr1'",
            (),
        )
        .await
        .unwrap();

        let list = read_task_permissions(&db, "prj", 2, 1, "builder", "review").await;
        assert!(
            list.contains("answered, but the resume was refused: session closed"),
            "{list}"
        );
        let single = read_task_permission(&db, "prj", 2, 1, "builder", "review", "perm-1").await;
        assert!(
            single.contains("## Resume") && single.contains("session closed"),
            "{single}"
        );
    }

    /// The same for a question: answered, and the refused resume beside it.
    #[tokio::test]
    async fn a_refused_resume_is_rendered_on_the_question() {
        let db = test_db().await;
        seed(&db).await;
        for sql in [
            "INSERT INTO runs (id, job_id, issue_id, created_at, updated_at) VALUES ('run-parent','j-parent','i',1,1)",
            "INSERT INTO prompts (id, run_id, job_id, questions, response, created_at, answered_at, uri_segment, resume_error) VALUES ('p1','run-parent','j-parent','[{\"question\":\"Which?\",\"options\":[]}]','the first',1,2,'q-1','session closed')",
        ] {
            db.execute(sql, ()).await.unwrap();
        }

        let list = read_node_questions(&db, "prj", 2, 1, "builder").await;
        assert!(
            list.contains("answered, but the resume was refused: session closed"),
            "{list}"
        );
        let single = read_node_question(&db, "prj", 2, 1, "builder", "q-1").await;
        assert!(
            single.contains("Status: answered")
                && single.contains("## Resume")
                && single.contains("session closed"),
            "{single}"
        );
    }
}

#[cfg(test)]
mod coordinated_child_wakes_tests {
    use super::read_node_wakes;
    use crate::storage::LocalDb;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("coordinated-child-wakes.db");
        std::mem::forget(temp);
        let db = LocalDb::open_migrated_for_test(path).await.unwrap();
        db
    }

    /// A coordinator node on issue 1 with a live session, and a child issue 2
    /// parented to it.
    async fn seed(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('parent','p',1,'Parent','active',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, parent_issue_id, created_at, updated_at) VALUES ('child','p',2,'Child','active','parent',2,2)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','parent','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_session_id, uri_segment, node_name, created_at, updated_at) VALUES ('coord','e','parent','p','running','sess','coordinator','Coordinator',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    /// The derived child watch has no subscription row, so `/wakes` is the only
    /// surface that can tell a coordinator which children wake it.
    #[tokio::test]
    async fn wakes_lists_the_children_a_coordinator_drives() {
        let db = test_db().await;
        seed(&db).await;

        let rendered = read_node_wakes(&db, "prj", 1, 1, "coordinator").await;

        assert!(
            rendered.contains("## Coordinated child issues"),
            "got: {rendered}"
        );
        assert!(
            rendered.contains("`cairn://p/prj/2`"),
            "the coordinated child must be named: {rendered}"
        );
        assert!(
            rendered.contains("No wake subscriptions."),
            "and it must be visible without any subscription row: {rendered}"
        );
    }

    /// A node that drives no children omits the section entirely rather than
    /// rendering an empty heading.
    #[tokio::test]
    async fn wakes_omits_the_section_when_a_node_drives_no_children() {
        let db = test_db().await;
        seed(&db).await;
        db.execute(
            "UPDATE issues SET parent_issue_id = NULL WHERE id = 'child'",
            (),
        )
        .await
        .unwrap();

        let rendered = read_node_wakes(&db, "prj", 1, 1, "coordinator").await;

        assert!(
            !rendered.contains("Coordinated child issues"),
            "got: {rendered}"
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
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("wait.db"))
                .await
                .unwrap(),
        );
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
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
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
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
        assert_eq!(body, r#"{"answer":"42"}"#);
    }

    #[tokio::test]
    async fn wait_returns_failed_sentinel_for_failed_call() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "failed", None).await;
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
        assert_eq!(body, CALL_WAIT_FAILED);
    }

    #[tokio::test]
    async fn wait_returns_failed_sentinel_when_terminal_without_artifact() {
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "exited", None).await;
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
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
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
        assert_eq!(body, r#"{"answer":"blue"}"#);
    }

    #[tokio::test]
    async fn wait_returns_artifact_for_failed_call_that_wrote_it() {
        // Same invariant on a `failed` terminal run: the written artifact wins.
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "failed", Some(r#"{"answer":"red"}"#)).await;
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
        assert_eq!(body, r#"{"answer":"red"}"#);
    }

    #[tokio::test]
    async fn wait_returns_failed_sentinel_for_crashed_call_without_artifact() {
        // A genuinely failed call (terminal run, no artifact) still resolves to
        // the failure sentinel — the fix only changes the artifact-present case.
        let (orch, db) = orch_with_db().await;
        seed_call(&db, "crashed", None).await;
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
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
        let body = wait_for_task_artifact(&orch, &db, "prj", 3, 1, "flow", "c1").await;
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
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
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
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
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
        let body = wait_for_task_artifact(&orch, &db, "prj", 4, 1, "wf", "c1").await;
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
    let hot_only = crate::storage::events::history::HistoryReader::new(db).is_hot_only();
    let read = read_task_rows(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
        move |conn, _parent_job, task_job| {
            Box::pin(async move {
                let run_id = get_nth_run_id(conn, &task_job.id, run_seq).await;
                let hot = match &run_id {
                    Some(id) => gather_single_event(conn, id, event_seq, hot_only).await,
                    None => Ok(None),
                };
                (run_id, hot)
            })
        },
    );
    let (run_id, hot) = match read.await {
        Ok((_, _, (Some(run_id), hot))) => (run_id, hot),
        Ok((_, _, (None, _))) => {
            return format!(
                "Run {} not found for task '{}' in node '{}'",
                run_seq, task_name, node_name
            )
        }
        Err(error) => return error,
    };

    render_single_event(db, &run_id, event_seq, hot).await
}

#[cfg(test)]
mod gated_artifact_tests {
    use super::render_gated_artifact_actions;

    #[test]
    fn gated_artifact_continue_targets_node_messages_collection() {
        let output = render_gated_artifact_actions(
            "body",
            "cairn://p/cairn/2160/1/planner/plan",
            "cairn",
            2160,
            1,
            "planner",
            None,
        );

        assert!(output.contains("[continue](cairn://p/cairn/2160/1/planner/messages)"));
        assert!(output.contains("target:\"cairn://p/cairn/2160/1/planner/messages\""));
        assert!(!output.contains("target:\"cairn://p/cairn/2160/1/planner\""));
    }

    #[test]
    fn gated_artifact_actions_offer_defer_with_a_copy_paste_example() {
        let output = render_gated_artifact_actions(
            "body",
            "cairn://p/cairn/2160/1/planner/plan",
            "cairn",
            2160,
            1,
            "planner",
            None,
        );
        assert!(
            output.contains("- [defer](cairn://p/cairn/2160/1/planner/plan)"),
            "{output}"
        );
        assert!(
            output.contains(
                "write({changes:[{target:\"cairn://p/cairn/2160/1/planner/plan\",mode:\"patch\",payload:{defer:{until:[{fact:\"issue_resolution\",issue:\"cairn://p/PROJECT/N\",resolution:\"merged\"}]}}}]})"
            ),
            "{output}"
        );
        for fact in [
            "issue_resolution",
            "pr_state",
            "artifact_confirmed",
            "clock",
        ] {
            assert!(output.contains(fact), "{fact}: {output}");
        }
        assert!(!output.contains("Deferred until"));
    }

    #[test]
    fn an_open_deferral_is_shown_with_its_cause() {
        let deferral = crate::execution::approvals::OpenDeferral {
            condition_id: "c".into(),
            until: Vec::new(),
            until_rendered: "issue cairn://p/cairn/4526 merged".into(),
            created_at: 1_786_000_000,
        };
        let output = render_gated_artifact_actions(
            "body",
            "cairn://p/cairn/4527/1/planner/plan",
            "cairn",
            4527,
            1,
            "planner",
            Some(&deferral),
        );
        assert!(
            output.contains("_Deferred until issue cairn://p/cairn/4526 merged since 2026-08-06._"),
            "{output}"
        );
    }
}

#[cfg(test)]
mod stuck_resolution_tests {
    use super::{describe_stuck_resolution, read_node_calls, read_node_tasks};
    use crate::mcp::handlers::durable_suspend::{StuckPhase, StuckResolution};
    use crate::storage::LocalDb;
    use std::collections::HashSet;

    /// A thread session job, as the node renderers resolve it, with one
    /// delegated child `research` whose `run` call is parked in `resolving`,
    /// entered `age_ms` ago with no result delivered.
    async fn parent_with_parked_child(age_ms: i64) -> LocalDb {
        let db = crate::storage::migrated_test_db("stuck-resolutions.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-threads', 'default', 'Threads', 'thr', '/tmp/threads', 1, 1);
             INSERT INTO threads (id, project_id, name, jurisdiction, created_at, updated_at)
             VALUES ('t-design', 'p-threads', 'design-review', 'Own architecture decisions', 2, 3);",
        )
        .await
        .unwrap();
        let parent = crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, output_contract, created_at, updated_at) VALUES ('j-task', ?1, 'p-threads', 'running', 'research', 'research', NULL, 7, 8)",
            (parent.as_str(),),
        )
        .await
        .unwrap();
        db.execute_script(
            "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at) VALUES ('s-task', 'j-task', 'open', 'h', 1, 1);
             INSERT INTO runs (id, project_id, job_id, status, session_id, created_at, updated_at) VALUES ('r-task', 'p-threads', 'j-task', 'live', 's-task', 1, 1);
             INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('t-task', 's-task', 'r-task', 'j-task', 1, 'yielded', 'initial', 1, 1);",
        )
        .await
        .unwrap();
        let at = chrono::Utc::now().timestamp_millis() - age_ms;
        db.execute(
            r#"INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, resolution_json, created_at, resolving_at) VALUES ('w-task', 'j-task', 'r-task', 's-task', 't-task', 'tool-1', '{"kind":"run_batch","request_id":"req","commits":false,"label":"cargo test"}', 'resolving', 'batch output', ?1, ?1)"#,
            (at,),
        )
        .await
        .unwrap();
        db
    }

    async fn tasks_of(db: &LocalDb) -> String {
        read_node_tasks(db, "thr", 0, 0, "design-review", &HashSet::new()).await
    }

    #[tokio::test]
    async fn a_child_stuck_delivering_reads_as_stuck_in_its_parents_task_listing() {
        let db = parent_with_parked_child(3 * 60_000).await;
        let tasks = tasks_of(&db).await;
        assert!(
            tasks.contains("running, stuck delivering a result since"),
            "{tasks}"
        );
    }

    #[tokio::test]
    async fn a_child_that_only_just_settled_reads_as_plain_running() {
        let db = parent_with_parked_child(0).await;
        let tasks = tasks_of(&db).await;
        assert!(tasks.contains("research"), "{tasks}");
        assert!(!tasks.contains("stuck"), "{tasks}");
    }

    #[tokio::test]
    async fn a_delegated_task_is_not_listed_among_calls() {
        let db = parent_with_parked_child(3 * 60_000).await;
        let calls = read_node_calls(&db, "thr", 0, 0, "design-review", &HashSet::new()).await;
        assert!(!calls.contains("stuck"), "{calls}");
    }

    fn stuck(phase: StuckPhase) -> StuckResolution {
        StuckResolution {
            job_id: "j".into(),
            wait_id: "w-1".into(),
            label: "run: cargo test".into(),
            since_ms: 1_793_570_000_000,
            phase,
        }
    }

    #[test]
    fn the_summary_names_a_failing_phase_as_failing() {
        for phase in [StuckPhase::Delivering, StuckPhase::Resuming] {
            let text = describe_stuck_resolution(&stuck(phase));
            assert!(text.contains("`run: cargo test`"), "{text}");
            assert!(text.contains("has failed since then"), "{text}");
            assert!(text.contains("`w-1`"), "{text}");
        }
        assert!(describe_stuck_resolution(&stuck(StuckPhase::Delivering))
            .starts_with("**Stuck delivering a result.**"));
    }

    /// A live resolver in its collision backoff delivered its result already;
    /// the notice must not claim a failure that did not happen.
    #[test]
    fn the_summary_does_not_call_a_guarded_collision_wait_a_failure() {
        let text = describe_stuck_resolution(&stuck(StuckPhase::AwaitingContinuation));
        assert!(
            text.starts_with("**Waiting for another continuation to start.**"),
            "{text}"
        );
        assert!(!text.contains("failed"), "{text}");
    }
}

/// Every transcript resource reads the same through a pack as it reads hot.
#[cfg(test)]
mod history_parity_tests {
    use super::*;
    use crate::storage::history_pack::testing::{
        evict_all, publish_for_test, read_both, seed_fixture_execution,
    };

    /// Give the fixture execution an issue, a node and a delegated task, so
    /// resource URIs resolve to it.
    async fn seed_node_and_task(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
               VALUES ('issue-1', 'p', 7, 'An issue', 1, 1);
             UPDATE executions SET issue_id = 'issue-1', seq = 1 WHERE id = 'exec-1';
             UPDATE jobs SET issue_id = 'issue-1', node_name = 'builder', uri_segment = 'builder'
               WHERE id = 'job-1';
             INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, node_name,
                               uri_segment, status, created_at, updated_at)
               VALUES ('job-task', 'exec-1', 'job-1', 'issue-1', 'p', 'review', 'review',
                       'complete', 30, 30);
             INSERT INTO sessions (id, job_id, status, created_at, updated_at)
               VALUES ('sess-task', 'job-task', 'active', 30, 30);
             INSERT INTO runs (id, project_id, job_id, session_id, status, created_at, updated_at)
               VALUES ('run-task', 'p', 'job-task', 'sess-task', 'exited', 30, 30);
             INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
               VALUES ('turn-task', 'sess-task', 'run-task', 'job-task', 1, 'complete', 30, 30);
             INSERT INTO events (id, run_id, session_id, turn_id, sequence, timestamp, event_type,
                                 data, created_at)
               VALUES ('task-1', 'run-task', 'sess-task', 'turn-task', 1, 30, 'user',
                       '{\"eventType\":\"user\",\"content\":\"the delegated task\"}', 30),
                      ('task-2', 'run-task', 'sess-task', 'turn-task', 2, 31, 'assistant',
                       '{\"eventType\":\"assistant\",\"content\":\"task done\"}', 31);",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn turn_session_runlists_keep_job_indexed_plans() {
        let db = crate::storage::migrated_test_db("node-session-runlist-plans").await;
        let primary = primary_session_turns_sql();
        for sql in [FIRST_JOB_SESSION, primary.as_str()] {
            let plan = crate::storage::query_plan(&db, sql, vec!["job".into()]).await;
            assert!(
                plan.contains("SEARCH runs") && plan.contains("job_id"),
                "{plan}"
            );
            assert!(!plan.contains("SCAN runs"), "{plan}");
        }
    }

    #[tokio::test]
    async fn node_and_task_turn_detail_keep_the_resolving_checkout() {
        let db = crate::storage::migrated_test_db("node-hot-checkouts").await;
        seed_fixture_execution(&db, "node-hot-checkouts").await;
        seed_node_and_task(&db).await;
        for cataloged in [false, true] {
            if cataloged {
                publish_for_test(&db, "exec-1").await;
            }
            for task in [false, true] {
                let statements = db.read_statement_count();
                let before = db.read_checkout_count();
                let turn = if task {
                    read_task_chat_turn(&db, "proj", 7, 1, "builder", "review", 1).await
                } else {
                    read_node_chat_turn(&db, "proj", 7, 1, "builder", 1).await
                };
                assert!(
                    turn.contains(if task {
                        "task done"
                    } else {
                        "the restored body"
                    }),
                    "{turn}"
                );
                assert_eq!(
                    db.read_checkout_count() - before,
                    1,
                    "turn task={task}, catalog={cataloged}"
                );
                assert_eq!(
                    db.read_statement_count() - statements,
                    if task { 8 } else { 7 },
                    "metadata + session + turn + events, unchanged main shape"
                );
                let statements = db.read_statement_count();
                let before = db.read_checkout_count();
                let detail = if task {
                    read_task_chat_event(&db, "proj", 7, 1, "builder", "review", 1, 2).await
                } else {
                    read_node_chat_event(&db, "proj", 7, 1, "builder", 1, 2).await
                };
                assert!(
                    detail.contains(if task {
                        "task done"
                    } else {
                        "the restored body"
                    }),
                    "{detail}"
                );
                assert_eq!(
                    db.read_checkout_count() - before,
                    1,
                    "detail task={task}, catalog={cataloged}"
                );
                assert_eq!(
                    db.read_statement_count() - statements,
                    if task { 7 } else { 6 },
                    "metadata + run ordinal + event, unchanged main shape"
                );
            }
        }
    }

    #[tokio::test]
    async fn every_transcript_resource_reads_the_same_through_a_pack() {
        let db = crate::storage::migrated_test_db("resource-parity.db").await;
        seed_fixture_execution(&db, "resource-parity").await;
        seed_node_and_task(&db).await;
        publish_for_test(&db, "exec-1").await;

        let (hot, forced) = read_both(&db, || every_resource(&db)).await;
        assert_eq!(hot, forced);

        // The reads are about something: the archived body restored, the tool
        // name resolved, the task's own transcript.
        let body = |name: &str| &hot.iter().find(|(label, _)| *label == name).unwrap().1;
        assert!(
            body("chat/raw").contains("the restored body"),
            "{}",
            body("chat/raw")
        );
        assert!(body("chat/1/2 assistant").contains("the restored body"));
        assert!(
            body("chat/1/4 tool result").contains("**Tool Result (mcp__cairn__read):**"),
            "{}",
            body("chat/1/4 tool result")
        );
        assert_eq!(
            body("chat/1/99 missing"),
            "Event with sequence 99 not found"
        );
        assert!(body("task chat/raw").contains("the delegated task"));
        assert!(body("task chat/1/2").contains("task done"));

        // With the hot rows gone, only the pack can answer.
        evict_all(&db).await;
        let evicted = every_resource(&db).await;
        assert_eq!(hot, evicted);
    }

    /// Every transcript resource of the fixture node and its task, rendered.
    async fn every_resource(db: &LocalDb) -> Vec<(&'static str, String)> {
        let (key, number, exec) = ("proj", 7, 1);
        let json = json_format();
        vec![
            (
                "chat",
                read_node_chat(db, key, number, exec, "builder", &[]).await,
            ),
            (
                "chat/raw",
                read_node_chat_raw(db, key, number, exec, "builder", &[]).await,
            ),
            (
                "chat/raw json",
                read_node_chat_raw(db, key, number, exec, "builder", &json).await,
            ),
            (
                "chat/turn/1",
                read_node_chat_turn(db, key, number, exec, "builder", 1).await,
            ),
            (
                "chat/1/2 assistant",
                read_node_chat_event(db, key, number, exec, "builder", 1, 2).await,
            ),
            // A tool result that does not carry its tool's name, so this also
            // reads the run's assistant events to resolve it.
            (
                "chat/1/4 tool result",
                read_node_chat_event(db, key, number, exec, "builder", 1, 4).await,
            ),
            (
                "chat/1/99 missing",
                read_node_chat_event(db, key, number, exec, "builder", 1, 99).await,
            ),
            (
                "task chat",
                read_task_chat(db, key, number, exec, "builder", "review", &[]).await,
            ),
            (
                "task chat/raw",
                read_task_chat_raw(db, key, number, exec, "builder", "review", &[]).await,
            ),
            (
                "task chat/raw json",
                read_task_chat_raw(db, key, number, exec, "builder", "review", &json).await,
            ),
            (
                "task chat/turn/1",
                read_task_chat_turn(db, key, number, exec, "builder", "review", 1).await,
            ),
            (
                "task chat/1/2",
                read_task_chat_event(db, key, number, exec, "builder", "review", 1, 2).await,
            ),
        ]
    }

    fn json_format() -> Vec<QueryParam> {
        vec![QueryParam {
            key: "format".to_string(),
            value: "json".to_string(),
        }]
    }
}

#[cfg(test)]
mod hold_header_tests {
    use super::render_job_summary_header;
    use crate::messages::delivery::Hold;

    /// `running` alone reads exactly like a node mid-turn, so a held node's
    /// title carries the hold and the summary says what releases it.
    #[test]
    fn a_held_node_names_its_hold_beside_the_status_and_beneath_it() {
        let limit = Hold::ProviderLimit {
            retry_not_before: Some(1_790_000_000),
        };
        let header = render_job_summary_header("builder", "running", None, None, Some(&limit));
        assert!(
            header.starts_with(&format!("# builder [◐ {}]", limit.qualifier())),
            "{header}"
        );
        assert!(
            header.contains("[◐ held: provider limit until "),
            "{header}"
        );
        assert!(
            header.contains("Not taking a turn: it is held for a provider usage limit until "),
            "{header}"
        );

        let wait = Hold::Wait {
            labels: vec!["waitFor: duration".into()],
            supersedable: true,
        };
        let header = render_job_summary_header("builder", "running", None, None, Some(&wait));
        assert!(
            header.starts_with("# builder [◐ waiting: waitFor: duration]"),
            "{header}"
        );
        assert!(header.contains("escalate:true"), "{header}");

        let idle = render_job_summary_header("builder", "running", None, None, None);
        assert!(idle.starts_with("# builder [◐]\n"), "{idle}");
        assert!(!idle.contains("Not taking a turn"), "{idle}");
    }
}

#[cfg(test)]
mod failed_projection_tests {
    async fn emitted_work(db: &crate::storage::LocalDb) -> (i64, i64, i64) {
        use crate::storage::RowExt;
        db.query_one(
            "SELECT (SELECT COUNT(*) FROM messages),
                    (SELECT COUNT(*) FROM attention_pushes),
                    (SELECT COUNT(*) FROM effect_outbox)",
            (),
            |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn failed_projection_node_summary_names_failed_and_persisted_reason() {
        let env = crate::execution::gates::sweep::tests::env("node-failed-projection.db").await;
        env.db
            .execute_script(
                "UPDATE jobs SET status = 'failed', completed_at = 30 WHERE id = 'coord';
             UPDATE runs SET status = 'crashed', error_message = 'Backend launch refused',
                             updated_at = 30 WHERE id = 'run-coord';",
            )
            .await
            .unwrap();
        let before = emitted_work(&env.db).await;
        let page = super::read_node(&env.orch, "prj", 9, 1, "coordinator", false).await;
        assert!(page.contains("# coordinator [✗ Failed]"), "{page}");
        assert!(page.contains("Backend launch refused"), "{page}");
        assert_eq!(
            emitted_work(&env.db).await,
            before,
            "a passive mark emits no work"
        );
    }

    #[tokio::test]
    async fn failed_projection_node_reason_is_bounded_and_absent_on_live_work() {
        let env =
            crate::execution::gates::sweep::tests::env("node-failed-projection-bound.db").await;
        let error = format!("Backend refused\n{}", "é".repeat(600));
        env.db
            .execute(
                "UPDATE runs SET status='crashed', error_message=?1 WHERE id='run-coord'",
                (error,),
            )
            .await
            .unwrap();
        let live = super::read_node(&env.orch, "prj", 9, 1, "coordinator", false).await;
        assert!(!live.contains("Failure:"), "{live}");
        env.db
            .execute_script("UPDATE jobs SET status='failed' WHERE id='coord';")
            .await
            .unwrap();
        let failed = super::read_node(&env.orch, "prj", 9, 1, "coordinator", false).await;
        let reason = failed
            .lines()
            .find_map(|line| line.strip_prefix("Failure: "))
            .unwrap();
        assert!(reason.starts_with("Backend refused "), "{reason}");
        assert!(reason.chars().count() <= 241, "{reason}");
        assert!(reason.ends_with('…'), "{reason}");
    }

    #[test]
    fn failed_projection_header_names_failed_even_without_error_evidence() {
        let page = super::render_job_summary_header("builder", "failed", None, None, None);
        assert!(page.contains("[✗ Failed]"), "{page}");
    }
}
