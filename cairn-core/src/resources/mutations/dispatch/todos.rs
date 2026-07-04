//! Todo list resource mutation dispatch, relocated from dispatch.rs.

use super::super::{build_failure, mode_name, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;
use serde::de::DeserializeOwned;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::JobTodos {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            mode @ (ChangeMode::Replace | ChangeMode::Append | ChangeMode::Patch),
        ) => {
            let payload = item.payload.as_ref().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires payload", mode_name(mode)),
                )
            })?;
            let label = match task_name.as_deref() {
                Some(task_name) => format!("{}/task/{}", node_id, task_name),
                None => node_id.clone(),
            };
            if dry_run {
                match mode {
                    ChangeMode::Patch => {
                        let updates = parse_todo_update_items(index, item, payload)?;
                        format!("Would patch {} todos for {}", updates.len(), label)
                    }
                    ChangeMode::Append => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        format!("Would append {} todos for {}", items.len(), label)
                    }
                    _ => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        format!(
                            "Would replace todos for {} with {} items",
                            label,
                            items.len()
                        )
                    }
                }
            } else {
                // Route todos writes to the owning project's database (CAIRN-2132);
                // resolution and all three mutations share the routed handle.
                let db = orch.db.for_project(project).await;
                let job_id = crate::resources::resolve_todos_job_id(
                    &db,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    task_name.as_deref(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;

                let summary = match mode {
                    ChangeMode::Replace => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        let todos = crate::todos::replace_todos(&db, &job_id, &items)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                        let completed = todos.iter().filter(|t| t.status == "completed").count();
                        *applied_data = Some(serde_json::to_value(&todos).unwrap_or_default());
                        format!(
                            "Replaced {} todos for {} ({} completed)\n{}",
                            todos.len(),
                            label,
                            completed,
                            crate::todos::format_todos_compact(&todos)
                        )
                    }
                    ChangeMode::Append => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        let appended = items.len();
                        let todos = crate::todos::append_todos(&db, &job_id, &items)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                        *applied_data = Some(serde_json::to_value(&todos).unwrap_or_default());
                        format!(
                            "Appended {} todos for {} ({} total)\n{}",
                            appended,
                            label,
                            todos.len(),
                            crate::todos::format_todos_compact(&todos)
                        )
                    }
                    ChangeMode::Patch => {
                        let updates = parse_todo_update_items(index, item, payload)?;
                        let patched = updates.len();
                        let todos = crate::todos::update_todos(&db, &job_id, &updates)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                        let completed = todos.iter().filter(|t| t.status == "completed").count();
                        *applied_data = Some(serde_json::to_value(&todos).unwrap_or_default());
                        format!(
                            "Patched {} todos for {} ({}/{} completed)\n{}",
                            patched,
                            label,
                            completed,
                            todos.len(),
                            crate::todos::format_todos_compact(&todos)
                        )
                    }
                    _ => unreachable!("mode guarded by match pattern"),
                };

                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "todos", "action": mode_name(mode)}),
                );

                summary
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

pub(super) fn parse_todo_write_items(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Vec<crate::todos::TodoWriteItem>> {
    parse_required_payload_field_describing(
        index,
        item,
        payload,
        "todos",
        crate::todos::TODO_WRITE_ITEM_KEYS,
    )
}

/// Parse a required payload field, enumerating the item's accepted keys when
/// deserialization fails. A mis-keyed array item (e.g. a todo keyed `title`
/// instead of `content`) yields a serde error that names at most the first
/// missing field; appending the accepted-key list turns the rejection into a
/// complete, self-correcting message (CAIRN #164).
fn parse_required_payload_field_describing<T>(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
    key: &str,
    accepted_keys: &str,
) -> ResourceMutationResult<T>
where
    T: DeserializeOwned,
{
    let value = payload
        .get(key)
        .ok_or_else(|| build_failure(index, item, format!("payload.{key} is required")))?;
    serde_json::from_value(value.clone()).map_err(|e| {
        build_failure(
            index,
            item,
            format!("Invalid payload.{key}: {e}. Each item accepts: {accepted_keys}."),
        )
    })
}

fn parse_todo_update_items(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Vec<crate::todos::TodoUpdateItem>> {
    parse_required_payload_field_describing(
        index,
        item,
        payload,
        "updates",
        crate::todos::TODO_UPDATE_ITEM_KEYS,
    )
}
