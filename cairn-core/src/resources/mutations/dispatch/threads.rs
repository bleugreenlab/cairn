//! First-class thread resource mutations.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::thread_name::validate_thread_name;
use cairn_common::uri::CairnResource;
use cairn_db::models::CreateThread;

async fn project_id(
    orch: &Orchestrator,
    project: &str,
) -> Result<(std::sync::Arc<cairn_db::storage::LocalDb>, String), String> {
    let db = orch.db.for_project(project).await;
    let id = crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
    Ok((db, id))
}

async fn thread_by_name(
    db: &cairn_db::storage::LocalDb,
    project_id: &str,
    name: &str,
) -> Result<cairn_db::models::Thread, String> {
    crate::threads::crud::list(db, project_id)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|thread| thread.name == name)
        .ok_or_else(|| format!("Thread '{name}' not found"))
}

// A thread has exactly one identifier: `name`. A payload still naming the
// retired `title` is refused by the contract gate, whose rejection enumerates
// the accepted keys and so points at `name` — no local check needed.

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::ProjectThreads { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let name = payload_non_empty_str(payload, "name", &[])
                .ok_or_else(|| build_failure(index, item, "payload.name is required"))?;
            validate_thread_name(name).map_err(|e| build_failure(index, item, e))?;
            let jurisdiction = payload
                .get("jurisdiction")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let content = payload
                .get("content")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            if dry_run {
                format!("Would create thread {project}/{name}")
            } else {
                let (db, project_id) = project_id(orch, project)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                let thread = crate::threads::crud::create(
                    &db,
                    CreateThread {
                        project_id,
                        name: Some(name.to_string()),
                        jurisdiction,
                        definition: None,
                        migrated_from_number: None,
                        model: None,
                    },
                )
                .await
                .map_err(|e| build_failure(index, item, e.to_string()))?;
                crate::channels::ensure_discord_thread_surface(
                    orch,
                    &db,
                    &thread.project_id,
                    &thread.name,
                )
                .await
                .map_err(|e| build_failure(index, item, e))?;
                if let Some(content) = content.filter(|value| !value.trim().is_empty()) {
                    crate::mcp::handlers::messages::append_thread_message(
                        orch, request, project, &thread.id, &content,
                    )
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                }
                format!("Created thread cairn://p/{project}/{}", thread.name)
            }
        }
        (
            CairnResource::Thread {
                project,
                name,
                path,
            },
            mode,
        ) if path.is_empty() => {
            let (db, project_id) = project_id(orch, project)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            let thread = thread_by_name(&db, &project_id, name)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            match mode {
                ChangeMode::Delete => {
                    if dry_run {
                        format!("Would delete thread {project}/{name}")
                    } else {
                        crate::threads::crud::delete(&db, &thread.id)
                            .await
                            .map_err(|e| build_failure(index, item, e.to_string()))?;
                        format!("Deleted thread {project}/{name}")
                    }
                }
                ChangeMode::Append => {
                    let payload = append_payload(index, item)?;
                    let content = payload_non_empty_str(payload, "content", &[])
                        .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
                    if dry_run {
                        format!(
                            "Would append {} chars to thread {project}/{name}",
                            content.len()
                        )
                    } else {
                        crate::mcp::handlers::messages::append_thread_message(
                            orch, request, project, &thread.id, content,
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e))?;
                        format!("Appended message to thread {project}/{name}")
                    }
                }
                ChangeMode::Patch => {
                    let payload = item
                        .payload
                        .as_ref()
                        .and_then(|v| v.as_object())
                        .ok_or_else(|| {
                            build_failure(index, item, "mode=patch requires an object payload")
                        })?;
                    if let Some(new_name) = payload.get("name").and_then(|v| v.as_str()) {
                        validate_thread_name(new_name)
                            .map_err(|e| build_failure(index, item, e))?;
                    }
                    let status = payload
                        .get("status")
                        .and_then(|v| v.as_str())
                        .map(cairn_db::models::ThreadStatus::parse)
                        .transpose()
                        .map_err(|e| build_failure(index, item, e))?;
                    if let Some(definition) = payload.get("definition").and_then(|v| v.as_str()) {
                        crate::threads::resolve_thread_definition(Some(definition))
                            .map_err(|e| build_failure(index, item, e))?;
                    }
                    let model = payload
                        .get("model")
                        .map(|value| {
                            value
                                .as_str()
                                .filter(|model| !model.trim().is_empty())
                                .ok_or_else(|| {
                                    build_failure(
                                        index,
                                        item,
                                        "payload.model must be a non-empty model name",
                                    )
                                })
                        })
                        .transpose()?;
                    if dry_run {
                        format!("Would patch thread {project}/{name}")
                    } else {
                        // One update operation, the same one the desktop command
                        // builds. The resource boundary validates and resolves
                        // names; persistence — metadata, definition, status, and
                        // the session establishment they imply — belongs to
                        // `crud::update` alone, so an agent-side patch and a
                        // desktop edit cannot reach different end states.
                        let updated = crate::threads::crud::update(
                            &db,
                            cairn_db::models::UpdateThread {
                                id: thread.id.clone(),
                                name: payload
                                    .get("name")
                                    .and_then(|v| v.as_str())
                                    .map(str::to_string),
                                jurisdiction: payload
                                    .get("jurisdiction")
                                    .and_then(|v| v.as_str())
                                    .map(|value| Some(value.to_string())),
                                definition: payload
                                    .get("definition")
                                    .and_then(|v| v.as_str())
                                    .map(|value| Some(value.to_string())),
                                status,
                                // The payload names only the model; the provider
                                // it resolves to is derived the single canonical
                                // way, the same one turn start uses to decide
                                // whether the live session still matches.
                                model: model.map(|model| {
                                    cairn_db::models::ModelSelection::new(
                                        crate::backends::resolved_backend_for_model(model),
                                        model.into(),
                                    )
                                }),
                            },
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e.to_string()))?;
                        format!("Patched thread cairn://p/{project}/{}", updated.name)
                    }
                }
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
