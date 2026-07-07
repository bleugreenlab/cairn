use crate::models::{AgentConfig, ExecutionSnapshot};
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::protocol::CallbackResponse;
use cairn_common::uri::build_task_artifact_uri;

use super::common::{select_optional_i64, select_optional_text, ParentRunContext};

pub(super) async fn build_task_callback_response(
    db: &LocalDb,
    parent_ctx: &ParentRunContext,
    run_id: &str,
    node_id: &str,
    agent_config: &AgentConfig,
    task_description: &str,
    expected_artifact_type: &str,
) -> CallbackResponse {
    let final_status = select_optional_text(db, "SELECT status FROM runs WHERE id = ?1", run_id)
        .await
        .ok()
        .flatten();

    match final_status.as_deref() {
        Some("complete") | Some("completed") | Some("exited") => {
            let result =
                match latest_nonempty_artifact_content(db, node_id, Some(expected_artifact_type))
                    .await
                {
                    Some(content) => content,
                    None => match latest_nonempty_artifact_content(db, node_id, None).await {
                        Some(content) => content,
                        None => latest_nonempty_assistant_content(db, run_id)
                            .await
                            .unwrap_or_else(|| "Task completed.".to_string()),
                    },
                };

            let artifact_uri = compute_artifact_uri(db, parent_ctx, node_id).await;

            CallbackResponse {
                result,
                artifact_uri,
                ..Default::default()
            }
        }
        Some("failed") | Some("crashed") => CallbackResponse {
            result: format!(
                "Agent '{}' failed.\n\nTask: {}\n\nThe agent encountered an error.",
                agent_config.name, task_description
            ),
            ..Default::default()
        },
        _ => CallbackResponse {
            result: format!(
                "Agent '{}' finished with unknown status.\n\nTask: {}",
                agent_config.name, task_description
            ),
            ..Default::default()
        },
    }
}

/// The task artifact URI for a delegated child job, derived entirely from the
/// child's own job row. The background-completion push uses this for a
/// single-task batch, where there is no live `ParentRunContext` in hand (the
/// finalize path holds only the settled child job id). It reconstructs the
/// minimal context [`compute_artifact_uri`] reads — project key, issue number,
/// and execution seq — and delegates, so both paths build the same URI.
pub(super) async fn compute_artifact_uri_for_child_job(
    db: &LocalDb,
    child_job_id: &str,
) -> Option<String> {
    let (project_key, issue_number, exec_seq) = job_context_fields(db, child_job_id).await?;
    // Only project_key / issue_number / exec_seq are read by
    // `compute_artifact_uri`; the remaining fields are inert placeholders.
    let parent_ctx = ParentRunContext {
        run_id: String::new(),
        job_id: String::new(),
        execution_id: None,
        exec_seq: Some(exec_seq),
        issue_id: None,
        issue_number: Some(issue_number),
        project_id: String::new(),
        project_key,
    };
    compute_artifact_uri(db, &parent_ctx, child_job_id).await
}

/// `(project_key, issue_number, exec_seq)` for a job, joining issue, project, and
/// execution. `exec_seq` defaults to 1 when the job has no execution.
async fn job_context_fields(db: &LocalDb, job_id: &str) -> Option<(String, i32, i32)> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number, COALESCE(e.seq, 1)
                     FROM jobs j
                     JOIN issues i ON i.id = j.issue_id
                     JOIN projects p ON p.id = i.project_id
                     LEFT JOIN executions e ON e.id = j.execution_id
                     WHERE j.id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.text(0)?, row.i64(1)? as i32, row.i64(2)? as i32)))
                .transpose()
        })
    })
    .await
    .ok()
    .flatten()
}

pub(super) async fn compute_artifact_uri(
    db: &LocalDb,
    parent_ctx: &ParentRunContext,
    task_job_id: &str,
) -> Option<String> {
    let issue_number = parent_ctx.issue_number?;
    let exec_seq = if let Some(exec_seq) = parent_ctx.exec_seq {
        exec_seq
    } else {
        let execution_id = parent_ctx.execution_id.as_deref()?;
        select_optional_i64(db, "SELECT seq FROM executions WHERE id = ?1", execution_id)
            .await
            .ok()
            .flatten()? as i32
    };

    let parent_job_id = select_optional_text(
        db,
        "SELECT parent_job_id FROM jobs WHERE id = ?1",
        task_job_id,
    )
    .await
    .ok()
    .flatten()?;
    let parent_segment = node_uri_segment_for_job(db, &parent_job_id).await?;
    let task_segment = task_uri_segment_for_job(db, task_job_id).await?;

    Some(build_task_artifact_uri(
        &parent_ctx.project_key,
        issue_number,
        exec_seq,
        &parent_segment,
        &task_segment,
    ))
}

async fn job_uri_fields(
    db: &LocalDb,
    job_id: &str,
) -> DbResult<
    Option<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )>,
> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT uri_segment, recipe_node_id, node_name, agent_config_id, execution_id
                    FROM jobs
                    WHERE id = ?1
                    LIMIT 1
                    ",
                    (job_id.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok((
                        row.opt_text(0)?,
                        row.opt_text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                    ))
                })
                .transpose()
        })
    })
    .await
}

async fn load_execution_snapshot(db: &LocalDb, execution_id: &str) -> Option<ExecutionSnapshot> {
    select_optional_text(
        db,
        "SELECT snapshot FROM executions WHERE id = ?1",
        execution_id,
    )
    .await
    .ok()
    .flatten()
    .and_then(|json| serde_json::from_str(&json).ok())
}

async fn node_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let (uri_segment, recipe_node_id, node_name, agent_config_id, execution_id) =
        job_uri_fields(db, job_id).await.ok().flatten()?;
    if uri_segment
        .as_deref()
        .is_some_and(|segment| !segment.is_empty())
    {
        return uri_segment;
    }

    let resolved_name = match node_name {
        Some(name) => Some(name),
        None => match (execution_id.as_deref(), recipe_node_id.as_deref()) {
            (Some(eid), Some(rid)) => load_execution_snapshot(db, eid).await.and_then(|snapshot| {
                snapshot
                    .recipe
                    .nodes
                    .iter()
                    .find(|node| node.id == rid)
                    .map(|node| node.name.clone())
            }),
            _ => None,
        },
    };

    if let Some(name) = resolved_name.as_deref() {
        let slug = crate::config::slugify_resource_segment(name);
        if !slug.is_empty() {
            return Some(slug);
        }
    }

    if let Some(rid) = recipe_node_id.as_deref().filter(|id| !id.is_empty()) {
        return Some(rid.to_string());
    }

    agent_config_id
        .as_deref()
        .map(crate::config::slugify_resource_segment)
        .filter(|slug| !slug.is_empty())
}

async fn task_uri_segment_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let (uri_segment, _recipe_node_id, node_name, agent_config_id, _execution_id) =
        job_uri_fields(db, job_id).await.ok().flatten()?;
    if uri_segment
        .as_deref()
        .is_some_and(|segment| !segment.is_empty())
    {
        return uri_segment;
    }

    if let Some(slug) = node_name
        .as_deref()
        .map(crate::config::slugify_resource_segment)
        .filter(|slug| !slug.is_empty())
    {
        return Some(slug);
    }

    if let Some(slug) = agent_config_id
        .as_deref()
        .map(crate::config::slugify_resource_segment)
        .filter(|slug| !slug.is_empty())
    {
        return Some(slug);
    }

    Some("task".to_string())
}

fn normalize_result_text(text: String) -> Option<String> {
    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

fn parse_nonempty_artifact_content(data_json: &str) -> Option<String> {
    let data: serde_json::Value = serde_json::from_str(data_json).ok()?;
    // A freeform `return` artifact carries its result in a `content` string.
    if let Some(content) = data.get("content").and_then(|value| value.as_str()) {
        return normalize_result_text(content.to_string());
    }
    // A structured artifact (a preset like `review` or an inline custom schema)
    // has no `content` field; render the whole validated JSON so the parent
    // receives the structured result instead of falling back to chat text.
    match &data {
        serde_json::Value::Object(map) if !map.is_empty() => serde_json::to_string_pretty(&data)
            .ok()
            .and_then(normalize_result_text),
        _ => None,
    }
}

fn parse_nonempty_assistant_content(data_json: &str) -> Option<String> {
    serde_json::from_str::<crate::agent_process::stream::TranscriptEvent>(data_json)
        .ok()
        .and_then(|event| event.content)
        .and_then(normalize_result_text)
}

async fn latest_nonempty_artifact_content(
    db: &LocalDb,
    job_id: &str,
    artifact_type: Option<&str>,
) -> Option<String> {
    let job_id = job_id.to_string();
    let artifact_type = artifact_type.map(str::to_string);
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = if let Some(artifact_type) = artifact_type.as_deref() {
                conn.query(
                    "
                    SELECT data FROM artifacts
                    WHERE job_id = ?1 AND artifact_type = ?2
                    ORDER BY version DESC
                    LIMIT 10
                    ",
                    (job_id.as_str(), artifact_type),
                )
                .await?
            } else {
                conn.query(
                    "
                    SELECT data FROM artifacts
                    WHERE job_id = ?1
                    ORDER BY version DESC
                    LIMIT 10
                    ",
                    (job_id.as_str(),),
                )
                .await?
            };
            let mut data = Vec::new();
            while let Some(row) = rows.next().await? {
                data.push(row.text(0)?);
            }
            Ok(data)
        })
    })
    .await
    .ok()
    .and_then(|rows| {
        rows.into_iter()
            .filter_map(|data_json| parse_nonempty_artifact_content(&data_json))
            .next()
    })
}

async fn latest_nonempty_assistant_content(db: &LocalDb, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT data FROM events
                    WHERE run_id = ?1 AND event_type = 'assistant'
                    ORDER BY sequence DESC
                    LIMIT 20
                    ",
                    (run_id.as_str(),),
                )
                .await?;
            let mut data = Vec::new();
            while let Some(row) = rows.next().await? {
                data.push(row.text(0)?);
            }
            Ok(data)
        })
    })
    .await
    .ok()
    .and_then(|rows| {
        rows.into_iter()
            .filter_map(|data_json| parse_nonempty_assistant_content(&data_json))
            .next()
    })
}

pub(super) async fn latest_nonempty_artifact_content_arc(
    db: std::sync::Arc<LocalDb>,
    job_id: String,
    artifact_type: Option<String>,
) -> Option<String> {
    latest_nonempty_artifact_content(&db, &job_id, artifact_type.as_deref()).await
}

pub(super) async fn latest_nonempty_assistant_content_arc(
    db: std::sync::Arc<LocalDb>,
    run_id: String,
) -> Option<String> {
    latest_nonempty_assistant_content(&db, &run_id).await
}

#[cfg(test)]
mod result_render_tests {
    use super::parse_nonempty_artifact_content;

    #[test]
    fn freeform_return_artifact_uses_content_field() {
        let out = parse_nonempty_artifact_content(r#"{"content":"the result"}"#);
        assert_eq!(out.as_deref(), Some("the result"));
    }

    #[test]
    fn structured_artifact_renders_full_json() {
        let out = parse_nonempty_artifact_content(r#"{"approval":"approved","summary":"ok"}"#)
            .expect("structured artifact renders");
        assert!(out.contains("approval"));
        assert!(out.contains("approved"));
    }

    #[test]
    fn empty_artifact_object_is_none() {
        assert!(parse_nonempty_artifact_content("{}").is_none());
    }
}
