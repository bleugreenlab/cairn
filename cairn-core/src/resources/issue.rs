//! Issue detail resource reader.

use cairn_db::turso::params;

use super::common::{
    connect_for_read, get_todo_progress, has_artifact_for_job, has_terminal_for_job,
    lookup_project_by_key, resource_job_from_row, storage_error, visible_job_node_segment,
    ResourceJob, JOB_COLUMNS,
};

use crate::issues::relations;
use crate::models::{ExecutionSnapshot, RecipeNodeType};
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::uri::{build_issue_comment_uri, build_issue_uri, build_node_uri};

async fn render_dependencies_block(conn: &cairn_db::turso::Connection, issue_id: &str) -> String {
    let dependencies = match relations::list_dependency_uris(conn, issue_id).await {
        Ok(dependencies) => dependencies,
        Err(_) => return String::new(),
    };
    if dependencies.is_empty() {
        return String::new();
    }

    let mut output = String::from("## dependencies\n");
    for uri in dependencies {
        match relations::resolve_issue_uri(conn, &uri).await {
            Ok(Some(resolved)) => {
                let marker = if relations::is_complete_status(&resolved.status) {
                    "✓"
                } else {
                    "○"
                };
                output.push_str(&format!(
                    "- [{}-{}]({}) [{}] {}\n",
                    resolved.project_key, resolved.number, resolved.uri, marker, resolved.title
                ));
            }
            _ => output.push_str(&format!("- [{}]({}) [?] unresolved\n", uri, uri)),
        }
    }
    output.push('\n');
    output
}

async fn render_family_block(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    parent_issue_id: Option<&str>,
) -> String {
    let mut lines = Vec::new();
    if let Some(parent_issue_id) = parent_issue_id {
        if let Ok(parent_uri) = relations::issue_uri_for_id(conn, parent_issue_id).await {
            lines.push(format!("Parent: `{parent_uri}`"));
        }
    }

    let mut rows = match conn
        .query(
            "
            SELECT p.key, i.number, i.title
            FROM issues i
            JOIN projects p ON p.id = i.project_id
            WHERE i.parent_issue_id = ?1
            ORDER BY i.number DESC
            ",
            params![issue_id],
        )
        .await
    {
        Ok(rows) => rows,
        Err(_) => return String::new(),
    };
    let mut children = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        let (Ok(project_key), Ok(number), Ok(title)) = (row.text(0), row.i64(1), row.text(2))
        else {
            continue;
        };
        children.push(format!(
            "- [{}-{}]({}) {}",
            project_key,
            number,
            build_issue_uri(&project_key, number as i32),
            title
        ));
    }
    if !children.is_empty() {
        lines.push(format!("Children:\n{}", children.join("\n")));
    }

    if lines.is_empty() {
        String::new()
    } else {
        format!("## family\n{}\n\n", lines.join("\n"))
    }
}

async fn render_possibly_related_block(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> String {
    let mut rows = match conn
        .query(
            "
            SELECT p.key, i.number, i.title, COUNT(DISTINCT fc.file_path) AS overlap
            FROM file_changes fc
            JOIN jobs j ON j.id = fc.job_id
            JOIN issues i ON i.id = j.issue_id
            JOIN projects p ON p.id = i.project_id
            WHERE i.id != ?1
              AND fc.file_path IN (
                SELECT fc2.file_path
                FROM file_changes fc2
                JOIN jobs j2 ON j2.id = fc2.job_id
                WHERE j2.issue_id = ?1
              )
            GROUP BY i.id, p.key, i.number, i.title
            ORDER BY overlap DESC, i.updated_at DESC
            LIMIT 5
            ",
            params![issue_id],
        )
        .await
    {
        Ok(rows) => rows,
        Err(_) => return String::new(),
    };

    let mut entries = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        let (Ok(project_key), Ok(number), Ok(title), Ok(overlap)) =
            (row.text(0), row.i64(1), row.text(2), row.i64(3))
        else {
            continue;
        };
        entries.push(format!(
            "- [{}]({}) — {} file{} overlap\n",
            title,
            build_issue_uri(&project_key, number as i32),
            overlap,
            if overlap == 1 { "" } else { "s" }
        ));
    }

    if entries.is_empty() {
        String::new()
    } else {
        format!("## possibly related\n{}\n", entries.join(""))
    }
}

pub(super) async fn read_issue(db: &LocalDb, project_key: &str, number: i32) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };

    // Get project
    let project_ctx = match lookup_project_by_key(&conn, project_key).await {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };

    // Get issue (description is Nullable)
    let mut issue_rows = match conn
        .query(
            "
            SELECT id, title, status, description, created_at, parent_issue_id
            FROM issues
            WHERE project_id = ?1 AND number = ?2
            LIMIT 1
            ",
            params![project_ctx.project_id.as_str(), number as i64],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => return storage_error("Failed to load issue", error.into()),
    };

    let (issue_id, title, status, description, created_at, parent_issue_id) =
        match issue_rows.next().await {
            Ok(Some(row)) => {
                let parsed: DbResult<_> = (|| {
                    Ok((
                        row.text(0)?,
                        row.text(1)?,
                        row.text(2)?,
                        row.opt_text(3)?,
                        row.i64(4)? as i32,
                        row.opt_text(5)?,
                    ))
                })();
                match parsed {
                    Ok(issue) => issue,
                    Err(error) => return storage_error("Failed to decode issue", error),
                }
            }
            Ok(None) => return format!("Issue {}-{} not found", project_key, number),
            Err(error) => return storage_error("Failed to load issue", error.into()),
        };

    let description = description.unwrap_or_default();
    let labels = crate::labels::attach::list_labels_for_issue(&conn, &issue_id)
        .await
        .unwrap_or_default();

    // Format header
    let created_date = chrono::DateTime::from_timestamp(created_at as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "Unknown".to_string());

    let mut output = format!("# {}-{}: {}\n\n", project_key, number, title);
    output.push_str(&format!("Status: {} | Created: {}\n", status, created_date));
    if !labels.is_empty() {
        output.push_str(&format!(
            "Labels: {}\n",
            labels
                .iter()
                .map(|label| label.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    // Get PR data for this issue (via merge_requests)
    let pr: Option<(Option<i32>, Option<String>, String)> = match conn
        .query(
            "
            SELECT github_pr_number, github_pr_url, status
            FROM merge_requests
            WHERE issue_id = ?1
            ORDER BY opened_at DESC
            LIMIT 1
            ",
            (issue_id.as_str(),),
        )
        .await
    {
        Ok(mut rows) => rows.next().await.ok().flatten().and_then(|row| {
            Some((
                row.opt_i64(0).ok()?.map(|value| value as i32),
                row.opt_text(1).ok()?,
                row.text(2).ok()?,
            ))
        }),
        Err(_) => None,
    };

    if let Some((Some(pr_number), Some(pr_url), pr_status)) = pr {
        output.push_str(&format!("PR: #{} ({}) {}\n", pr_number, pr_status, pr_url));
    }
    output.push('\n');

    output.push_str(&render_family_block(&conn, &issue_id, parent_issue_id.as_deref()).await);

    // Description
    if !description.is_empty() {
        output.push_str("## Description\n\n");
        output.push_str(&description);
        output.push_str("\n\n");
    }

    // Comments (inlined)
    let mut comment_rows: Vec<(i64, String, String, i32)> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT seq, content, source, created_at
            FROM comments
            WHERE issue_id = ?1
            ORDER BY created_at ASC
            ",
            (issue_id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(seq), Ok(content), Ok(source), Ok(created_at)) =
                (row.i64(0), row.text(1), row.text(2), row.i64(3))
            else {
                continue;
            };
            comment_rows.push((seq, content, source, created_at as i32));
        }
    }

    if !comment_rows.is_empty() {
        output.push_str(&format!("## Comments ({})\n\n", comment_rows.len()));
        for (seq, content, source, comment_created_at) in &comment_rows {
            let comment_date = chrono::DateTime::from_timestamp(*comment_created_at as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            // `[#N]` is the per-issue comment seq: the addressable id for
            // `cairn://p/PROJECT/NUMBER/comments/N` (edit/delete).
            output.push_str(&format!("[#{}] [{}] {}\n", seq, source, comment_date));
            output.push_str(content);
            output.push_str("\n\n");
        }
    }

    output.push_str(&render_dependencies_block(&conn, &issue_id).await);
    output.push_str(&render_possibly_related_block(&conn, &issue_id).await);

    // Nodes - grouped by execution
    output.push_str("## Nodes\n\n");
    output.push_str(&format!("`{}`\n", build_issue_uri(project_key, number)));

    // Get executions ordered by started_at, carrying the `seq` column. Every
    // node URI is addressed by `seq` (not positional index) so the tree agrees
    // with the read resolver and watch `detail_uri`, which both key on
    // `executions.seq` (CAIRN-1222).
    let mut executions: Vec<(String, i64)> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT id, seq
            FROM executions
            WHERE issue_id = ?1
            ORDER BY started_at ASC
            ",
            (issue_id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            if let (Ok(id), Ok(seq)) = (row.text(0), row.i64(1)) {
                executions.push((id, seq));
            }
        }
    }

    if executions.is_empty() {
        output.push_str("  (no executions yet)\n\n");
        return output;
    }

    for (exec_id, exec_seq) in &executions {
        let exec_seq = *exec_seq as i32;
        output.push_str(&format!("├─ `/{}`\n", exec_seq));

        // Get jobs for this execution
        let job_sql = format!(
            "
            SELECT {JOB_COLUMNS}
            FROM jobs
            WHERE execution_id = ?1
            ORDER BY created_at ASC
            "
        );
        let mut exec_jobs: Vec<ResourceJob> = Vec::new();
        if let Ok(mut rows) = conn.query(job_sql.as_str(), (exec_id.as_str(),)).await {
            while let Ok(Some(row)) = rows.next().await {
                if let Ok(job) = resource_job_from_row(&row) {
                    exec_jobs.push(job);
                }
            }
        }

        // Separate top-level and task jobs
        let top_level: Vec<&ResourceJob> = exec_jobs
            .iter()
            .filter(|j| j.parent_job_id.is_none())
            .collect();
        let tasks: Vec<&ResourceJob> = exec_jobs
            .iter()
            .filter(|j| j.parent_job_id.is_some())
            .collect();

        // Top-level action_runs (e.g. a `pr` node) are nodes too — enumerate
        // them beside the job nodes so the tree URIs match the read resolver
        // (CAIRN-1222).
        let mut action_nodes: Vec<(String, String)> = Vec::new();
        if let Ok(mut rows) = conn
            .query(
                "
                SELECT uri_segment, status
                FROM action_runs
                WHERE execution_id = ?1
                  AND uri_segment IS NOT NULL
                ORDER BY created_at ASC
                ",
                (exec_id.as_str(),),
            )
            .await
        {
            while let Ok(Some(row)) = rows.next().await {
                if let (Ok(Some(segment)), Ok(status)) = (row.opt_text(0), row.text(1)) {
                    action_nodes.push((segment, status));
                }
            }
        }

        let total_nodes = top_level.len() + action_nodes.len();

        for (idx, job) in top_level.iter().enumerate() {
            let is_last_node = idx == total_nodes - 1;
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

            let node_segment = visible_job_node_segment(&conn, job).await;

            // Status icon
            let status = match job.status.as_str() {
                "complete" => "✓",
                "running" => "◐",
                "failed" => "✗",
                "pending" => "○",
                _ => "?",
            };

            // Resource indicators
            let has_artifact = has_artifact_for_job(&conn, &job.id).await;
            let has_terminal = has_terminal_for_job(&conn, &job.id).await;
            let mut indicators = String::new();
            if has_artifact {
                indicators.push_str(" 📄");
            }
            if has_terminal {
                indicators.push_str(" 🖥");
            }

            // Todo progress
            let todo = get_todo_progress(&conn, &job.id)
                .await
                .map(|t| format!(" {}", t))
                .unwrap_or_default();

            output.push_str(&format!(
                "{} `{}`[{}]{}{}\n",
                prefix,
                build_node_uri(project_key, number, exec_seq, &node_segment),
                status,
                todo,
                indicators
            ));

            let child_tasks: Vec<&&ResourceJob> = tasks
                .iter()
                .filter(|t| t.parent_job_id.as_ref() == Some(&job.id))
                .collect();
            for (tidx, task) in child_tasks.iter().enumerate() {
                let is_last_task = tidx == child_tasks.len() - 1;
                let task_prefix = if is_last_task { "└─" } else { "├─" };

                let uri_segment = task
                    .uri_segment
                    .clone()
                    .unwrap_or_else(|| "task".to_string());
                let task_status = match task.status.as_str() {
                    "complete" => "✓",
                    "running" => "◐",
                    "failed" => "✗",
                    _ => "○",
                };
                let task_has_artifact = has_artifact_for_job(&conn, &task.id).await;
                let task_indicator = if task_has_artifact { " 📄" } else { "" };

                output.push_str(&format!(
                    "{}{} `.../task/{}`[{}]{}\n",
                    child_prefix, task_prefix, uri_segment, task_status, task_indicator
                ));
            }
        }

        // Render action_run node lines after the job nodes, addressed by the
        // same `(seq, uri_segment)` key.
        for (aidx, (segment, status)) in action_nodes.iter().enumerate() {
            let is_last_node = top_level.len() + aidx == total_nodes - 1;
            let prefix = if is_last_node {
                "│  └─"
            } else {
                "│  ├─"
            };
            let status_icon = match status.as_str() {
                "complete" => "✓",
                "running" => "◐",
                "failed" => "✗",
                "blocked" => "◼",
                "pending" => "○",
                _ => "?",
            };
            output.push_str(&format!(
                "{} `{}`[{}]\n",
                prefix,
                build_node_uri(project_key, number, exec_seq, segment),
                status_icon,
            ));
        }
    }

    output.push('\n');

    output
}

/// Read an issue's executions collection. Lists each execution's seq, status,
/// and trigger, and advertises the append-to-start action.
pub(super) async fn read_issue_executions(db: &LocalDb, project_key: &str, number: i32) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project_ctx = match lookup_project_by_key(&conn, project_key).await {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };
    let issue_id = match conn
        .query(
            "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2 LIMIT 1",
            params![project_ctx.project_id.as_str(), number as i64],
        )
        .await
    {
        Ok(mut rows) => match rows.next().await {
            Ok(Some(row)) => match row.text(0) {
                Ok(id) => id,
                Err(e) => return storage_error("Failed to decode issue", e),
            },
            Ok(None) => return format!("Issue {}-{} not found", project_key, number),
            Err(e) => return storage_error("Failed to load issue", e.into()),
        },
        Err(e) => return storage_error("Failed to load issue", e.into()),
    };

    let mut output = format!("# Executions for {}-{}\n\n", project_key, number);
    let mut found = false;
    if let Ok(mut rows) = conn
        .query(
            "SELECT seq, status, triggered_by FROM executions
             WHERE issue_id = ?1 ORDER BY seq ASC",
            (issue_id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            found = true;
            let seq = row.opt_i64(0).ok().flatten().unwrap_or(0);
            let status = row.text(1).unwrap_or_default();
            let triggered_by = row.opt_text(2).ok().flatten().unwrap_or_default();
            output.push_str(&format!(
                "- execution {} [{}] (triggered: {})\n",
                seq, status, triggered_by
            ));
        }
    }
    if !found {
        output.push_str("(no executions yet)\n");
    }
    output
}

async fn resolve_issue_id_for_comments(
    db: &LocalDb,
    project_key: &str,
    number: i32,
) -> Result<String, String> {
    match relations::issue_id_for_project_number(db, project_key, number).await {
        Ok(Some(issue_id)) => Ok(issue_id),
        Ok(None) => Err(format!(
            "Issue {}-{} not found",
            project_key.to_uppercase(),
            number
        )),
        Err(error) => Err(format!("Failed to resolve issue: {error}")),
    }
}

fn render_comment(comment: &crate::models::Comment, uri: &str) -> String {
    let date = chrono::DateTime::from_timestamp(comment.created_at, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "Unknown".to_string());
    let mut out = format!("### comment {}\n", comment.seq);
    // Surface the addressable member URI inline so a reader of the collection
    // can patch (edit content) or delete this specific comment.
    out.push_str(&format!(
        "[{}] {} · edit/delete: {}\n",
        comment.source, date, uri
    ));
    out.push_str(&comment.content);
    out.push_str("\n\n");
    out
}

/// Render an issue's stored comments, each with its stable id, source
/// (user or agent), and timestamp so a caller can pick one to edit or delete.
/// Read-only: creating a comment stays on the issue-URI append.
pub(super) async fn read_issue_comments(db: &LocalDb, project_key: &str, number: i32) -> String {
    let issue_id = match resolve_issue_id_for_comments(db, project_key, number).await {
        Ok(issue_id) => issue_id,
        Err(message) => return message,
    };
    let comments = match crate::issues::comments::list(db, &issue_id).await {
        Ok(comments) => comments,
        Err(error) => return format!("Failed to load comments: {error}"),
    };
    let project_upper = project_key.to_uppercase();
    let mut out = format!("# Comments — {}-{}\n\n", project_upper, number);
    out.push_str(&format!("{} comment(s)\n\n", comments.len()));
    out.push_str(
        "Edit a comment: patch its `content`; delete it: delete its URI below. \
Post a new comment by appending to the issue URI.\n\n",
    );
    for comment in &comments {
        let uri = build_issue_comment_uri(&project_upper, number, comment.seq as i32);
        out.push_str(&render_comment(comment, &uri));
    }
    out
}

/// Render a single issue comment, addressed by its stable id. The comment must
/// belong to the named issue; a mismatch or unknown id reports not-found.
pub(super) async fn read_issue_comment(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    comment_seq: i32,
) -> String {
    let issue_id = match resolve_issue_id_for_comments(db, project_key, number).await {
        Ok(issue_id) => issue_id,
        Err(message) => return message,
    };
    let comments = match crate::issues::comments::list(db, &issue_id).await {
        Ok(comments) => comments,
        Err(error) => return format!("Failed to load comments: {error}"),
    };
    match comments
        .iter()
        .find(|comment| comment.seq == comment_seq as i64)
    {
        Some(comment) => {
            let project_upper = project_key.to_uppercase();
            let uri = build_issue_comment_uri(&project_upper, number, comment.seq as i32);
            let mut out = format!(
                "# Comment {} on {}-{}\n\n",
                comment.seq, project_upper, number
            );
            out.push_str(&render_comment(comment, &uri));
            out
        }
        None => format!(
            "Comment {comment_seq} not found on issue {}-{number}",
            project_key.to_uppercase()
        ),
    }
}

/// Render a single execution's frozen snapshot: the recipe graph, every agent
/// snapshot (selection, fence, tools, skills), and the skill set. This is the
/// read half of the execution-snapshot resource; the agent-edit half lives in
/// the mutation dispatch.
pub(super) async fn read_issue_execution(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project_ctx = match lookup_project_by_key(&conn, project_key).await {
        Ok(ctx) => ctx,
        Err(e) => return e,
    };
    let issue_id = match conn
        .query(
            "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2 LIMIT 1",
            params![project_ctx.project_id.as_str(), number as i64],
        )
        .await
    {
        Ok(mut rows) => match rows.next().await {
            Ok(Some(row)) => match row.text(0) {
                Ok(id) => id,
                Err(e) => return storage_error("Failed to decode issue", e),
            },
            Ok(None) => return format!("Issue {}-{} not found", project_key, number),
            Err(e) => return storage_error("Failed to load issue", e.into()),
        },
        Err(e) => return storage_error("Failed to load issue", e.into()),
    };

    let snapshot_json = match conn
        .query(
            "SELECT snapshot FROM executions WHERE issue_id = ?1 AND seq = ?2 LIMIT 1",
            params![issue_id.as_str(), exec_seq as i64],
        )
        .await
    {
        Ok(mut rows) => match rows.next().await {
            Ok(Some(row)) => match row.opt_text(0) {
                Ok(Some(json)) => json,
                Ok(None) => {
                    return format!(
                        "Execution {}-{}/{} has no snapshot",
                        project_key, number, exec_seq
                    )
                }
                Err(e) => return storage_error("Failed to decode snapshot", e),
            },
            Ok(None) => {
                return format!(
                    "Execution {}-{}/{} not found",
                    project_key, number, exec_seq
                )
            }
            Err(e) => return storage_error("Failed to load execution", e.into()),
        },
        Err(e) => return storage_error("Failed to load execution", e.into()),
    };

    let snapshot = match crate::config::snapshot_migrate::load(&snapshot_json) {
        Ok(snapshot) => snapshot,
        Err(e) => return format!("Failed to parse snapshot: {e}"),
    };

    render_execution_snapshot(project_key, number, exec_seq, &snapshot)
}

fn render_execution_snapshot(
    project_key: &str,
    number: i32,
    exec_seq: i32,
    snapshot: &ExecutionSnapshot,
) -> String {
    let mut output = format!(
        "# Execution snapshot {}-{} / execution {}\n\n",
        project_key, number, exec_seq
    );

    let recipe = &snapshot.recipe;
    output.push_str("## recipe\n");
    output.push_str(&format!("name: {} ({})\n", recipe.name, recipe.id));
    output.push_str(&format!("trigger: {}\n", recipe.trigger));
    output.push_str("nodes:\n");
    for node in &recipe.nodes {
        let agent = if node.node_type == RecipeNodeType::Agent {
            node.agent_config
                .as_ref()
                .and_then(|c| c.agent_config_id.as_deref())
                .map(|id| format!(" agent={id}"))
                .unwrap_or_default()
        } else {
            String::new()
        };
        output.push_str(&format!(
            "- {} [{:?}] {}{}\n",
            node.id, node.node_type, node.name, agent
        ));
    }
    if !recipe.edges.is_empty() {
        output.push_str("edges:\n");
        for edge in &recipe.edges {
            output.push_str(&format!(
                "- {} -> {}\n",
                edge.source_node_id, edge.target_node_id
            ));
        }
    }
    output.push('\n');

    output.push_str("## agents\n");
    let mut agent_ids: Vec<&String> = snapshot.agents.keys().collect();
    agent_ids.sort();
    for agent_id in agent_ids {
        let agent = &snapshot.agents[agent_id];
        output.push_str(&format!("### {}\n", agent_id));
        output.push_str(&format!("name: {}\n", agent.name));
        let selection = match &agent.selection {
            Some(sel) => format!("{}/{}", sel.backend, sel.model.as_str()),
            None => "(unresolved)".to_string(),
        };
        output.push_str(&format!("selection: {}\n", selection));
        output.push_str(&format!("fence: {:?}\n", agent.fence.unwrap_or_default()));
        output.push_str(&format!("tools: {}\n", agent.tools.len()));
        let skills = match &agent.skills {
            Some(skills) if !skills.is_empty() => skills.join(", "),
            Some(_) => "(none)".to_string(),
            None => "(all available)".to_string(),
        };
        output.push_str(&format!("skills: {}\n", skills));
    }
    output.push('\n');

    output.push_str("## skills\n");
    if snapshot.skills.is_empty() {
        output.push_str("(none)\n");
    } else {
        let mut skill_ids: Vec<&String> = snapshot.skills.keys().collect();
        skill_ids.sort();
        for skill_id in skill_ids {
            let skill = &snapshot.skills[skill_id];
            output.push_str(&format!("- {}: {}\n", skill_id, skill.name));
        }
    }

    output
}

#[cfg(test)]
mod execution_snapshot_read_tests {
    use super::*;
    use crate::models::Fence;
    use crate::models::{
        AgentSnapshot, Model, ModelSelection, RecipeSnapshot, RecipeTrigger, SkillSnapshot,
        TriggerContext, TriggerType,
    };
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use cairn_db::turso::params;
    use std::collections::HashMap;

    async fn exec(db: &LocalDb, sql: &'static str) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn test_db() -> LocalDb {
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn snapshot_json() -> String {
        let recipe = RecipeSnapshot {
            id: "r".to_string(),
            name: "My Recipe".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![],
            edges: vec![],
        };
        let agent = AgentSnapshot {
            id: "builder".to_string(),
            name: "Builder".to_string(),
            description: String::new(),
            prompt: "p".to_string(),
            tools: vec!["read".to_string(), "write".to_string()],
            tier: None,
            backend_preference: None,
            selection: Some(ModelSelection {
                backend: "claude".to_string(),
                model: Model::new(Model::SONNET),
            }),
            disallowed_tools: None,
            skills: None,
            fence: Some(Fence::Ask),
            sandbox: None,
            on_escape: None,
            model: None,
            resolved_backend: None,
            extras: None,
        };
        let mut agents = HashMap::new();
        agents.insert("builder".to_string(), agent);
        let mut skills = HashMap::new();
        skills.insert(
            "ui".to_string(),
            SkillSnapshot {
                id: "ui".to_string(),
                name: "UI Skill".to_string(),
                description: String::new(),
                prompt: String::new(),
                allowed_tools: None,
            },
        );
        let snap = ExecutionSnapshot::new(
            recipe,
            agents,
            skills,
            TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        );
        snap.to_json().unwrap()
    }

    async fn seed(db: &LocalDb) {
        exec(
            db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1','default','Cairn','CAIRN','/tmp/repo',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1','proj-1',1,'T','active',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1','r','issue-1','proj-1','running',1,3)",
        )
        .await;
        let json = snapshot_json();
        db.write(|conn| {
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE executions SET snapshot = ?1 WHERE id = 'exec-1'",
                    params![json.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn renders_seeded_snapshot() {
        let db = test_db().await;
        seed(&db).await;
        let out = read_issue_execution(&db, "CAIRN", 1, 3).await;
        assert!(
            out.contains("# Execution snapshot CAIRN-1 / execution 3"),
            "{out}"
        );
        assert!(out.contains("name: My Recipe (r)"), "{out}");
        assert!(out.contains("trigger: manual"), "{out}");
        assert!(out.contains("### builder"), "{out}");
        assert!(out.contains("selection: claude/sonnet"), "{out}");
        assert!(out.contains("fence: Ask"), "{out}");
        assert!(out.contains("tools: 2"), "{out}");
        assert!(out.contains("- ui: UI Skill"), "{out}");
    }

    #[tokio::test]
    async fn missing_execution_reports_clearly() {
        let db = test_db().await;
        seed(&db).await;
        let out = read_issue_execution(&db, "CAIRN", 1, 99).await;
        assert!(out.contains("not found"), "{out}");
    }
}
