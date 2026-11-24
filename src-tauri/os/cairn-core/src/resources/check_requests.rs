//! `cairn://p/{project}/check-requests/{id}` and the check requests section of a
//! node's `/checks`: check requests rendered from their durable rows
//! ([`crate::execution::checks::requests`]).
//!
//! A queued request explains itself wherever it is read: since when it has
//! waited, the machine it is pinned to, and that machine's latest refusal in
//! the machine's own words, which names the running commands it waits behind.

use crate::execution::checks::requests::{self, CheckRequestRow};
use crate::mcp::handlers::run_context::project_id_by_key;
use crate::orchestrator::Orchestrator;

/// Read one request, as Markdown or (`json`) as the object `cairn check
/// watch` renders.
pub(super) async fn read_check_request(
    orch: &Orchestrator,
    project: &str,
    id: &str,
    json: bool,
) -> String {
    let Some(id) = super::check_results::request_handle(id) else {
        return format!("Invalid check request id `{id}`: expected the UUID a filing returned");
    };
    let db = orch.db.for_project(project).await;
    let project_id = match project_id_by_key(&db, project).await {
        Ok(project_id) => project_id,
        Err(error) => return error,
    };
    let request_db = match requests::request_db(orch, &project_id).await {
        Ok(db) => db,
        Err(error) => return error,
    };
    let row = match requests::load_request(&request_db, &project_id, &id).await {
        Ok(Some(row)) => row,
        Ok(None) => return format!("Check request {id} is unknown in this project"),
        Err(error) => return error,
    };
    let value = match requests::request_value(orch, request_db, project, &row).await {
        Ok(value) => value,
        Err(error) => return format!("Unable to read check request {id}: {error}"),
    };
    if json {
        return value.to_string();
    }
    render(project, &row, &value)
}

fn executor_link(name: &str) -> String {
    format!("[{name}](cairn://executors/{name})")
}

fn clock(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%Y-%m-%d %H:%M:%SZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

fn consumer_label(consumer: &str) -> &str {
    match consumer {
        "requested" => "requested",
        "write" => "write cadence",
        "turn_end" => "review cadence",
        "main_cadence" => "main cadence",
        other => other,
    }
}

fn render(project: &str, row: &CheckRequestRow, value: &serde_json::Value) -> String {
    let uri = requests::request_uri(project, &row.id);
    let mut lines = vec![
        format!("# Check request {}", row.id),
        String::new(),
        format!("- Suite: {}", row.suite),
        format!("- Consumer: {}", consumer_label(&row.consumer)),
        format!("- Requested by: {}", row.requester_ref),
        format!(
            "- Revision: {}",
            row.revision_spec
                .as_deref()
                .unwrap_or("the node's own head")
        ),
    ];
    if let Some(commit) = &row.commit_sha {
        lines.push(format!("- Commit: {commit}"));
    }
    if let Some(machine) = &row.pinned_executor {
        lines.push(format!("- Pinned to: {}", executor_link(machine)));
    }
    lines.push(format!("- Filed: {}", clock(row.accepted_at_ms)));
    if row.is_open() {
        lines.push(format!("- State: {}", requests::standing(row)));
        if let Some(machine) = row.executor.as_deref().filter(|_| row.state == "running") {
            lines.push(format!("- Running on: {}", executor_link(machine)));
        }
        if let Some(held) = &row.held_by {
            lines.push(format!(
                "- Latest refusal ({} seen, at {}): {held}",
                row.held_count.max(1),
                clock(row.held_at_ms.unwrap_or(row.updated_ms))
            ));
        }
        if let Some(diagnostic) = value["observation"]["diagnostic"]
            .as_str()
            .filter(|_| row.held_by.is_none())
        {
            lines.push(format!("- Progress: {diagnostic}"));
        }
        if let Some(waiting) = &row.waiting_to_learn {
            lines.push(format!("- Waiting to learn: {waiting}"));
        }
        if let Some(request) = &row.request_id {
            lines.push(format!("- Fleet request: {request}"));
        }
        if let Some(by) = &row.cancel_requested_by {
            lines.push(format!("- Cancel requested by: {by}"));
        }
        lines.push(String::new());
        lines.push(format!(
            "No verdict yet. Wait for it without holding anything: `{}`. Withdraw it: write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"cancel\"}}}}]}})",
            requests::wait_line(&uri)
        ));
    } else {
        lines.extend(super::check_results::render_answer_lines(
            &value["observation"],
        ));
    }
    lines.join("\n")
}

/// The check requests section of a node's `/checks`: the requests it filed that
/// are still open, then the few most recently settled. `None` when it filed
/// none.
pub(super) async fn requested_section(
    orch: &Orchestrator,
    project_key: &str,
    requester_job_id: &str,
) -> Option<String> {
    let rows = requests::requests_of_requester(orch, requester_job_id, 5)
        .await
        .ok()?;
    if rows.is_empty() {
        return None;
    }
    let mut out = String::from("\n## Check requests\n\n");
    let mut open = false;
    for row in &rows {
        open |= row.is_open();
        let revision = row
            .revision_spec
            .as_deref()
            .unwrap_or("own head")
            .to_string();
        out.push_str(&format!(
            "- [{}]({}) ({}) on {revision}: {}\n",
            row.suite,
            requests::request_uri(project_key, &row.id),
            consumer_label(&row.consumer),
            requests::standing(row)
        ));
    }
    if open {
        out.push_str(
            "\nWait for one: run waitFor {kind:\"checks\", ref:<its URI>, on:\"verdict\"}; for every open one: ref:\"cairn:~/checks/requests\", on:\"settled\". Nothing is held while you wait.\n",
        );
    }
    Some(out)
}

/// `<job>/checks/requests`: the requests a node or task filed that are still
/// open, read from [`requests::open_requests_of_requester`], the same query a
/// `checks` wait `on:"settled"` at this address arms on.
pub(super) async fn read_open_check_requests(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> String {
    let db = orch.db.for_project(project).await;
    let job_id = match super::node::resolve_node_or_task_job_id_for_read(
        &db, project, number, exec_seq, node_id, task_name,
    )
    .await
    {
        Ok(job_id) => job_id,
        Err(error) => return error,
    };
    match requests::open_requests_of_requester(orch, &job_id).await {
        Ok(rows) => render_open(
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            &rows,
            now_ms(),
        ),
        Err(error) => format!("Unable to read open check requests: {error}"),
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn age(ms: i64) -> String {
    let seconds = (ms / 1000).max(0);
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h {}m", seconds / 3600, seconds % 3600 / 60),
        _ => format!("{}d {}h", seconds / 86400, seconds % 86400 / 3600),
    }
}

fn render_open(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
    rows: &[CheckRequestRow],
    now: i64,
) -> String {
    let (uri, checks, who) = match task_name {
        Some(task) => (
            cairn_uri::build_task_check_requests_uri(project, number, exec_seq, node_id, task),
            cairn_uri::build_task_checks_uri(project, number, exec_seq, node_id, task),
            task,
        ),
        None => (
            cairn_uri::build_node_check_requests_uri(project, number, exec_seq, node_id),
            cairn_uri::build_node_checks_uri(project, number, exec_seq, node_id),
            node_id,
        ),
    };
    let mut out = format!("# Open check requests \u{2014} {who}\n\n`{uri}`\n\n");
    if rows.is_empty() {
        out.push_str(&format!(
            "None open. File one with write({{changes:[{{target:\"{checks}\",mode:\"append\",payload:{{suite:\"<suite>\"}}}}]}}); settled requests and their verdicts are listed at `{checks}`.\n"
        ));
        return out;
    }
    out.push_str("| Request | Suite | Revision | State | Age |\n|---|---|---|---|---|\n");
    for row in rows {
        let link = requests::request_uri(project, &row.id);
        out.push_str(&format!(
            "| [{}]({link}) | {} | {} | {} | {} |\n",
            row.id,
            row.suite,
            row.revision_spec.as_deref().unwrap_or("own head"),
            requests::standing(row),
            age(now - row.accepted_at_ms),
        ));
    }
    out.push_str(&format!(
        "\nWait for all of these to settle without holding anything: run({{commands:[{{waitFor:{{kind:\"checks\",ref:\"{uri}\",on:\"settled\"}}}}]}}). For one verdict, wait on its own URI with on:\"verdict\".\n"
    ));
    out
}

/// Requests queued on the runner and pinned to `executor_name`: what waits
/// for this machine that its own queue does not hold.
pub(crate) async fn pinned_section(orch: &Orchestrator, executor_name: &str) -> Option<String> {
    let rows = requests::requests_pinned_to(orch, executor_name)
        .await
        .ok()?;
    if rows.is_empty() {
        return None;
    }
    let mut out = format!(
        "\n## Check requests pinned to this machine ({}, queued on the runner)\n\n",
        rows.len()
    );
    for (db, row) in rows {
        let project_key = db
            .query_opt_text(
                "SELECT key FROM projects WHERE id=?1",
                (row.project_id.clone(),),
            )
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| row.project_id.clone());
        out.push_str(&format!(
            "- {} {} ({}) on {} for {}: {}\n",
            requests::request_uri(&project_key, &row.id),
            row.suite,
            consumer_label(&row.consumer),
            row.revision_spec
                .as_deref()
                .unwrap_or("its node's own head"),
            row.requester_ref,
            requests::standing(&row)
        ));
    }
    Some(out)
}
