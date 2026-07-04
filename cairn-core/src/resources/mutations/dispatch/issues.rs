//! Issue and comment resource mutation dispatch, relocated from dispatch.rs.

use super::super::{
    build_failure, payload_non_empty_str, payload_str, payload_trimmed_non_empty_str,
    payload_value, ResourceMutationResult,
};
use super::append_payload;
use crate::mcp::handlers::{comments_artifacts, issues};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::ProjectIssues { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let title = payload_trimmed_non_empty_str(payload, "title", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.title is required and must be a non-empty string",
                )
            })?;
            if let Some(description) = payload.get("description") {
                if !description.is_string() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.description must be a string",
                    ));
                }
            }
            let parent = if let Some(parent) = payload.get("parent") {
                let Some(parent) = parent
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.parent must be a non-empty string",
                    ));
                };
                Some(parent.to_string())
            } else {
                None
            };
            let execution = parse_create_execution_spec(index, item, payload)?;
            let labels = parse_string_array_field(index, item, payload, "labels", &[])?;
            if dry_run {
                match &execution {
                    Some(spec) => format!(
                        "Would create issue in project {project}: {title} and start an execution{}",
                        spec.recipe
                            .as_deref()
                            .map(|r| format!(" (recipe '{r}')"))
                            .unwrap_or_default()
                    ),
                    None => format!("Would create issue in project {project}: {title}"),
                }
            } else {
                let description = payload_str(payload, "description", &[]).map(ToOwned::to_owned);
                let outcome = issues::create_issue_in_project(
                    orch,
                    project,
                    title.to_string(),
                    description,
                    labels,
                    execution,
                    parent,
                    request.run_id.clone(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                *applied_data = Some(serde_json::json!({
                    "projectKey": outcome.project_key,
                    "number": outcome.number,
                    "uri": outcome.uri,
                }));
                outcome.summary
            }
        }
        (
            CairnResource::IssueComment {
                project,
                number,
                comment_seq,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!("Would edit comment {comment_seq} on issue {project}-{number}")
            } else {
                // Route to the owning project's database (CAIRN-2132); a local
                // project resolves to the private DB, a shared one to its replica.
                let db = orch.db.for_project(project).await;
                let comment_id =
                    resolve_issue_comment_id(&db, index, item, project, *number, *comment_seq)
                        .await?;
                crate::issues::comments::update(&db, &comment_id, content)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                format!("Edited comment {comment_seq} on issue {project}-{number}")
            }
        }
        (
            CairnResource::IssueComment {
                project,
                number,
                comment_seq,
            },
            ChangeMode::Delete,
        ) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            if dry_run {
                format!("Would delete comment {comment_seq} on issue {project}-{number}")
            } else {
                let db = orch.db.for_project(project).await;
                let comment_id =
                    resolve_issue_comment_id(&db, index, item, project, *number, *comment_seq)
                        .await?;
                crate::issues::comments::delete(&db, &comment_id)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                format!("Deleted comment {comment_seq} on issue {project}-{number}")
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if let Some(title) = payload.get("title") {
                if !title.is_string() {
                    return Err(build_failure(index, item, "payload.title must be a string"));
                }
            }
            if let Some(description) = payload.get("description") {
                if !description.is_string() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.description must be a string",
                    ));
                }
            }
            let depends_on =
                parse_string_array_field(index, item, payload, "depends_on", &["dependsOn"])?;
            let labels = parse_string_array_field(index, item, payload, "labels", &[])?;
            // `status` is the resolution the UI's IssueMenu sets. Only the two
            // settable resolutions are accepted here: `backlog` is a derived
            // state, not a value a user sets, so it is rejected like any other.
            let status = match payload.get("status") {
                None | Some(serde_json::Value::Null) => None,
                Some(value) => {
                    let status = value.as_str().ok_or_else(|| {
                        build_failure(index, item, "payload.status must be a string")
                    })?;
                    if !matches!(status, "merged" | "closed") {
                        return Err(build_failure(
                            index,
                            item,
                            format!("Invalid status '{status}'. Allowed values: merged, closed"),
                        ));
                    }
                    Some(status.to_string())
                }
            };
            // Re-parenting: absent leaves the parent untouched, null/empty
            // orphans the issue, a string adopts it under that canonical issue
            // URI. Existence, same-project, and cycle checks happen in the txn.
            let parent = match payload.get("parent") {
                None => None,
                Some(serde_json::Value::Null) => Some(None),
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            "payload.parent must be an issue URI string or null",
                        )
                    })?;
                    if raw.trim().is_empty() {
                        Some(None)
                    } else {
                        let canonical = crate::issues::relations::canonicalize_issue_uri(raw)
                            .map_err(|e| build_failure(index, item, e))?;
                        Some(Some(canonical))
                    }
                }
            };
            if dry_run {
                let mut details = Vec::new();
                if let Some(status) = status.as_deref() {
                    details.push(format!("status={status}"));
                }
                match &parent {
                    None => {}
                    Some(None) => details.push("parent=cleared".to_string()),
                    Some(Some(uri)) => details.push(format!("parent={uri}")),
                }
                if details.is_empty() {
                    format!("Would patch issue {project}-{number}")
                } else {
                    format!(
                        "Would patch issue {project}-{number} ({})",
                        details.join(", ")
                    )
                }
            } else {
                let title = payload_str(payload, "title", &[]).map(ToOwned::to_owned);
                let description = payload_str(payload, "description", &[]).map(ToOwned::to_owned);
                issues::update_issue_by_project_number(
                    orch,
                    request,
                    project,
                    *number,
                    issues::IssuePatchFields {
                        title,
                        description,
                        depends_on,
                        labels,
                        status,
                        parent,
                    },
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Delete) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            // Resolve against the owning DB (CAIRN-2181): a team project's issue
            // row lives in its team replica, so the lookup must route there or a
            // team-project delete would falsely report "not found".
            let owning_db = orch.db.for_project(project).await;
            let issue_id =
                crate::issues::relations::issue_id_for_project_number(&owning_db, project, *number)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?
                    .ok_or_else(|| {
                        build_failure(index, item, format!("Issue {project}-{number} not found"))
                    })?;
            if dry_run {
                format!("Would delete issue {project}-{number}")
            } else {
                crate::issues::delete::delete_issue(orch, &issue_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!("Deleted issue {project}-{number}")
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars as a comment to issue {project}-{number}",
                    content.len()
                )
            } else {
                comments_artifacts::append_issue_comment(orch, request, project, *number, content)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

fn parse_string_array_field(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> ResourceMutationResult<Option<Vec<String>>> {
    let value = payload_value(payload, key, aliases);
    let Some(value) = value else {
        return Ok(None);
    };
    let values = value.as_array().ok_or_else(|| {
        build_failure(
            index,
            item,
            format!("payload.{key} must be an array of non-empty strings"),
        )
    })?;
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let item_value = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.{key} must be an array of non-empty strings"),
                )
            })?;
        parsed.push(item_value.to_string());
    }
    Ok(Some(parsed))
}

/// Resolve a per-issue comment `seq` to its stable comment id, mapping a missing
/// issue or comment to a clean not-found failure. The member URI embeds the
/// issue number, so a seq that belongs to a different issue is rejected too.
async fn resolve_issue_comment_id(
    db: &crate::storage::LocalDb,
    index: usize,
    item: &ChangeItem,
    project: &str,
    number: i32,
    comment_seq: i32,
) -> ResourceMutationResult<String> {
    let issue_id = crate::issues::relations::issue_id_for_project_number(db, project, number)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| build_failure(index, item, format!("Issue {project}-{number} not found")))?;
    crate::issues::comments::id_for_issue_seq(db, &issue_id, comment_seq as i64)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("Comment {comment_seq} not found on issue {project}-{number}"),
            )
        })
}

/// Parse the optional `execution` object on an issue-create payload into a
/// create+start spec. Absent or null -> None (create only). When present it must
/// be an object whose `recipe`/`backend`, if set, are strings.
fn parse_create_execution_spec(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Option<issues::CreateExecutionSpec>> {
    let value = match payload.get("execution") {
        None | Some(serde_json::Value::Null) => return Ok(None),
        Some(value) => value,
    };
    let obj = value.as_object().ok_or_else(|| {
        build_failure(
            index,
            item,
            "payload.execution must be an object {recipe?, backend?}",
        )
    })?;
    let str_field = |key: &str| -> ResourceMutationResult<Option<String>> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => value.as_str().map(|s| Some(s.to_string())).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.execution.{key} must be a string"),
                )
            }),
        }
    };
    Ok(Some(issues::CreateExecutionSpec {
        recipe: str_field("recipe")?,
        backend: str_field("backend")?,
    }))
}
