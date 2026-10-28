//! First-class thread resource mutations.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::thread_name::validate_thread_name;
use cairn_common::uri::CairnResource;
use cairn_db::models::CreateThread;

fn thread_db_refusal(project: &str, name: &str) -> String {
    format!(
        "cairn://p/{project}/{name}/db is thread '{name}'s own store, writable only by that thread and the tasks it spawns. You can read it with `?sql=`."
    )
}

fn statement_params(
    value: Option<&serde_json::Value>,
    statement_count: usize,
) -> Result<Vec<Vec<cairn_db::turso::Value>>, String> {
    let Some(value) = value else {
        return Ok(vec![Vec::new(); statement_count]);
    };
    let serde_json::Value::Array(values) = value else {
        return Err("payload.params must be an array".into());
    };

    let array_count = values.iter().filter(|value| value.is_array()).count();
    if array_count != 0 && array_count != values.len() {
        return Err("payload.params cannot mix parameter values and per-statement arrays; pass one array per statement: params:[[...],[...],[...]]".into());
    }
    if array_count == 0 {
        if statement_count != 1 {
            return Err(format!(
                "payload.params is a flat array, but SQL contains {statement_count} statements; pass one array per statement: params:[[...],[...],[...]]"
            ));
        }
        return values
            .iter()
            .map(sql_param)
            .collect::<Result<Vec<_>, _>>()
            .map(|params| vec![params]);
    }
    if values.len() != statement_count {
        return Err(format!(
            "payload.params contains {} statement parameter arrays, but SQL contains {statement_count} statements",
            values.len()
        ));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_array()
                .expect("all values were classified as arrays")
                .iter()
                .map(sql_param)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

fn sql_param(value: &serde_json::Value) -> Result<cairn_db::turso::Value, String> {
    Ok(match value {
        serde_json::Value::Null => cairn_db::turso::Value::Null,
        serde_json::Value::Bool(value) => cairn_db::turso::Value::Integer(i64::from(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                cairn_db::turso::Value::Integer(value)
            } else if let Some(value) = value.as_u64().and_then(|value| i64::try_from(value).ok()) {
                cairn_db::turso::Value::Integer(value)
            } else if let Some(value) = value.as_f64() {
                cairn_db::turso::Value::Real(value)
            } else {
                return Err("payload.params contains a number SQLite cannot represent".into());
            }
        }
        serde_json::Value::String(value) => cairn_db::turso::Value::Text(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            return Err("payload.params values must be null, booleans, numbers, or strings".into());
        }
    })
}

async fn owning_thread_id(
    db: &cairn_db::storage::LocalDb,
    job_id: &str,
) -> Result<Option<String>, String> {
    db.query_text(
        "SELECT COALESCE(j.thread_id, parent.thread_id) FROM jobs j LEFT JOIN jobs parent ON parent.id = j.parent_job_id WHERE j.id = ?1",
        (job_id.to_string(),),
    )
    .await
    .map_err(|error| error.to_string())
}

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
        (CairnResource::ThreadBranches { project, thread }, ChangeMode::Append) => {
            let _payload = append_payload(index, item)?;
            if dry_run {
                format!("Would render minted branch for {project}/{thread}")
            } else {
                // Minting is deliberately upstream in the write pre-scan so the ref,
                // backing job, file commit, and optional PR remain one destination.
                // This resource arm only reports the entity that pre-scan created.
                let (db, project_id) = project_id(orch, project)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                let thread_row = thread_by_name(&db, &project_id, thread)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                let conn = db
                    .connect()
                    .await
                    .map_err(|e| build_failure(index, item, e.to_string()))?;
                let mut rows = conn.query(
                    "SELECT number, git_ref, stage FROM thread_branches WHERE thread_id = ?1 ORDER BY number DESC LIMIT 1",
                    cairn_db::turso::params![thread_row.id.as_str()],
                ).await.map_err(|e| build_failure(index, item, e.to_string()))?;
                let row = rows.next().await.map_err(|e| build_failure(index, item, e.to_string()))?
                    .ok_or_else(|| build_failure(index, item, "No pre-created thread branch was found; branch minting must run before resource mutation dispatch"))?;
                use cairn_db::storage::RowExt;
                let number = row
                    .i64(0)
                    .map_err(|e| build_failure(index, item, e.to_string()))?;
                let git_ref = row
                    .text(1)
                    .map_err(|e| build_failure(index, item, e.to_string()))?;
                let stage = row
                    .text(2)
                    .map_err(|e| build_failure(index, item, e.to_string()))?;
                format!(
                    "Minted cairn://p/{project}/{thread}/branches/{number} ({git_ref}, {stage})"
                )
            }
        }
        (
            CairnResource::ThreadBranch {
                project,
                thread,
                number,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .and_then(|value| value.as_object())
                .ok_or_else(|| {
                    build_failure(index, item, "mode=patch requires an object payload")
                })?;
            if let Some(stage) = payload.get("stage").and_then(|value| value.as_str()) {
                if stage != "abandoned" {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.stage only accepts 'abandoned'",
                    ));
                }
            }
            let action = payload.get("action").and_then(|value| value.as_str());
            if let Some(action) = action {
                if !matches!(action, "merge" | "close") {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.action only accepts 'merge' or 'close'",
                    ));
                }
                if payload.contains_key("title") || payload.contains_key("stage") {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.action is mutually exclusive with title and stage",
                    ));
                }
                if action == "close" && payload.contains_key("method") {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.method is only accepted with action='merge'",
                    ));
                }
            } else if payload.contains_key("method") {
                return Err(build_failure(
                    index,
                    item,
                    "payload.method requires action='merge'",
                ));
            }
            let precondition = crate::pr_data::actions::parse_merge_precondition(
                payload.get("precondition"),
                action.unwrap_or_default(),
            )
            .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                let gate = precondition
                    .map(|_| " (precondition=pinBump)")
                    .unwrap_or_default();
                format!("Would patch thread branch {project}/{thread}/{number}{gate}")
            } else {
                let (db, project_id) = project_id(orch, project)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                let thread_row = thread_by_name(&db, &project_id, thread)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                if let Some(action) = action {
                    let job_id = db
                        .query_text(
                            "SELECT job_id FROM thread_branches WHERE thread_id = ?1 AND number = ?2",
                            cairn_db::turso::params![thread_row.id.as_str(), *number],
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e.to_string()))?
                        .ok_or_else(|| {
                            build_failure(
                                index,
                                item,
                                format!("Thread branch {thread}/{number} not found"),
                            )
                        })?;
                    let attribution =
                        Some(crate::pr_data::actions::PrResolutionAttribution::agent(
                            format!("cairn://p/{project}/{thread}"),
                        ));
                    let result = if action == "merge" {
                        crate::pr_data::actions::merge_pr_for_job(
                            orch,
                            &job_id,
                            payload
                                .get("method")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string),
                            attribution,
                            precondition,
                            crate::pr_data::actions::OperatorOverrides::default(),
                        )
                        .await
                    } else {
                        crate::pr_data::actions::close_pr_for_job(orch, &job_id, attribution).await
                    }
                    .map_err(|e| build_failure(index, item, e))?;
                    return Ok(Some(format!(
                        "{result}: cairn://p/{project}/{thread}/branches/{number}"
                    )));
                }
                let conn = db
                    .connect()
                    .await
                    .map_err(|e| build_failure(index, item, e.to_string()))?;
                let changed = conn.execute(
                    "UPDATE thread_branches SET title = CASE WHEN ?1 IS NULL THEN title ELSE ?1 END, stage = CASE WHEN ?2 IS NULL THEN stage ELSE ?2 END, moved_at = CASE WHEN ?2 IS NULL THEN moved_at ELSE unixepoch() END WHERE thread_id = ?3 AND number = ?4",
                    cairn_db::turso::params![payload.get("title").and_then(|v| v.as_str()), payload.get("stage").and_then(|v| v.as_str()), thread_row.id.as_str(), *number],
                ).await.map_err(|e| build_failure(index, item, e.to_string()))?;
                if changed == 0 {
                    return Err(build_failure(
                        index,
                        item,
                        format!("Thread branch {thread}/{number} not found"),
                    ));
                }
                format!("Patched cairn://p/{project}/{thread}/branches/{number}")
            }
        }
        (CairnResource::ThreadProcedures { project, name }, ChangeMode::Create) => {
            let payload = item
                .payload
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| {
                    build_failure(index, item, "mode=create requires an object payload")
                })?;
            let procedure_name = payload
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| build_failure(index, item, "payload.name is required"))?;
            let body = payload
                .get("body")
                .cloned()
                .ok_or_else(|| build_failure(index, item, "payload.body is required"))?;
            let description = payload
                .get("description")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let (db, project_id) = project_id(orch, project)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            let thread = thread_by_name(&db, &project_id, name)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            if dry_run {
                crate::threads::procedures::validate_name(procedure_name)
                    .map_err(|e| build_failure(index, item, e))?;
                crate::threads::procedures::validate_body(&body)
                    .map_err(|e| build_failure(index, item, e))?;
                format!("Would create procedure {procedure_name} in {project}/{name}")
            } else {
                crate::threads::procedures::create(&thread.id, procedure_name, description, body)
                    .await
                    .map_err(|e| build_failure(index, item, e))?;
                format!("Created procedure cairn://p/{project}/{name}/procedures/{procedure_name}")
            }
        }
        (
            CairnResource::ThreadProcedure {
                project,
                name,
                procedure,
            },
            mode @ (ChangeMode::Patch | ChangeMode::Delete),
        ) => {
            let (db, project_id) = project_id(orch, project)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            let thread = thread_by_name(&db, &project_id, name)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            match mode {
                ChangeMode::Patch => {
                    let payload = item
                        .payload
                        .as_ref()
                        .and_then(serde_json::Value::as_object)
                        .ok_or_else(|| {
                            build_failure(index, item, "mode=patch requires an object payload")
                        })?;
                    let body = payload.get("body").cloned();
                    let description = payload
                        .get("description")
                        .map(|value| {
                            if value.is_null() {
                                Ok(None)
                            } else {
                                value.as_str().map(|v| Some(v.to_string())).ok_or_else(|| {
                                    "payload.description must be a string or null".to_string()
                                })
                            }
                        })
                        .transpose()
                        .map_err(|e| build_failure(index, item, e))?;
                    if body.is_none() && description.is_none() {
                        return Err(build_failure(
                            index,
                            item,
                            "procedure patch requires body or description",
                        ));
                    }
                    if dry_run {
                        if let Some(body) = &body {
                            crate::threads::procedures::validate_body(body)
                                .map_err(|e| build_failure(index, item, e))?;
                        }
                        format!("Would patch procedure {procedure} in {project}/{name}")
                    } else {
                        crate::threads::procedures::update(
                            &thread.id,
                            procedure,
                            body,
                            description,
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e))?
                        .ok_or_else(|| {
                            build_failure(index, item, format!("Procedure `{procedure}` not found"))
                        })?;
                        format!(
                            "Patched procedure cairn://p/{project}/{name}/procedures/{procedure}"
                        )
                    }
                }
                ChangeMode::Delete => {
                    let referenced =
                        crate::orchestrator::automations::list_thread_automations(&db, &thread.id)
                            .await
                            .map_err(|e| build_failure(index, item, e))?
                            .into_iter()
                            .any(|automation| automation.procedure_name == *procedure);
                    if referenced {
                        return Err(build_failure(index, item, format!("Cannot delete procedure `{procedure}` while automations reference it; delete those automations first")));
                    }
                    if dry_run {
                        format!("Would delete procedure {procedure} from {project}/{name}")
                    } else {
                        if !crate::threads::procedures::delete(&thread.id, procedure)
                            .await
                            .map_err(|e| build_failure(index, item, e))?
                        {
                            return Err(build_failure(
                                index,
                                item,
                                format!("Procedure `{procedure}` not found"),
                            ));
                        }
                        format!("Deleted procedure {procedure} from {project}/{name}")
                    }
                }
                _ => unreachable!(),
            }
        }
        (CairnResource::ThreadAutomations { project, name }, ChangeMode::Create) => {
            let payload = item
                .payload
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| {
                    build_failure(index, item, "mode=create requires an object payload")
                })?;
            let procedure = payload
                .get("procedure")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| build_failure(index, item, "payload.procedure must be a string"))?;
            let every = payload
                .get("every")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    build_failure(index, item, "payload.every must be a duration string")
                })?;
            let every_ms = crate::duration::parse_duration_ms(every)
                .and_then(|v| i64::try_from(v).map_err(|_| "duration too large".into()))
                .map_err(|e| {
                    build_failure(index, item, format!("invalid automation every: {e}"))
                })?;
            let args = payload
                .get("args")
                .map(|value| {
                    value
                        .as_array()
                        .ok_or_else(|| "payload.args must be an array".to_string())?
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .map(str::to_string)
                                .ok_or_else(|| "payload.args values must be strings".to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()
                .map_err(|e| build_failure(index, item, e))?
                .unwrap_or_default();
            let (db, project_id) = project_id(orch, project)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            let thread = thread_by_name(&db, &project_id, name)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            if crate::threads::procedures::get(&thread.id, procedure)
                .await
                .map_err(|e| build_failure(index, item, e))?
                .is_none()
            {
                return Err(build_failure(
                    index,
                    item,
                    format!("Procedure `{procedure}` not found"),
                ));
            }
            if dry_run {
                format!("Would create automation for {procedure} in {project}/{name}")
            } else {
                let automation = crate::orchestrator::automations::create_thread_automation(
                    &db, &thread.id, procedure, args, every_ms, "agent",
                )
                .await
                .map_err(|e| build_failure(index, item, e))?;
                format!(
                    "Created automation cairn://p/{project}/{name}/automations/{}",
                    automation.id
                )
            }
        }
        (
            CairnResource::ThreadAutomation { project, name, id },
            mode @ (ChangeMode::Patch | ChangeMode::Delete),
        ) => {
            let (db, project_id) = project_id(orch, project)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            let thread = thread_by_name(&db, &project_id, name)
                .await
                .map_err(|e| build_failure(index, item, e))?;
            match mode {
                ChangeMode::Patch => {
                    let payload = item
                        .payload
                        .as_ref()
                        .and_then(serde_json::Value::as_object)
                        .ok_or_else(|| {
                            build_failure(index, item, "mode=patch requires an object payload")
                        })?;
                    let every_ms = payload
                        .get("every")
                        .map(|v| {
                            v.as_str()
                                .ok_or_else(|| "payload.every must be a string".to_string())
                                .and_then(crate::duration::parse_duration_ms)
                                .and_then(|v| {
                                    i64::try_from(v).map_err(|_| "duration too large".into())
                                })
                        })
                        .transpose()
                        .map_err(|e| build_failure(index, item, e))?;
                    let args = payload
                        .get("args")
                        .map(|value| {
                            value
                                .as_array()
                                .ok_or_else(|| "payload.args must be an array".to_string())?
                                .iter()
                                .map(|v| {
                                    v.as_str().map(str::to_string).ok_or_else(|| {
                                        "payload.args values must be strings".to_string()
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .transpose()
                        .map_err(|e| build_failure(index, item, e))?;
                    let state = payload.get("state").and_then(serde_json::Value::as_str);
                    if dry_run {
                        format!("Would patch automation {id} in {project}/{name}")
                    } else {
                        crate::orchestrator::automations::update_thread_automation(
                            &db, &thread.id, id, every_ms, args, state,
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e))?
                        .ok_or_else(|| {
                            build_failure(index, item, format!("Automation `{id}` not found"))
                        })?;
                        format!("Patched automation cairn://p/{project}/{name}/automations/{id}")
                    }
                }
                ChangeMode::Delete => {
                    if dry_run {
                        format!("Would delete automation {id} from {project}/{name}")
                    } else {
                        if !crate::orchestrator::automations::delete_thread_automation(
                            &db, &thread.id, id,
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e))?
                        {
                            return Err(build_failure(
                                index,
                                item,
                                format!("Automation `{id}` not found"),
                            ));
                        }
                        format!("Deleted automation {id} from {project}/{name}")
                    }
                }
                _ => unreachable!(),
            }
        }
        (CairnResource::ThreadDb { project, name }, ChangeMode::Patch) => {
            let uri = format!("cairn://p/{project}/{name}/db");
            let payload = item
                .payload
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| {
                    build_failure(index, item, "mode=patch requires an object payload")
                })?;
            let sql = payload
                .get("sql")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| build_failure(index, item, "payload.sql is required"))?;
            let statements = crate::threads::db_store::validate_write_sql(sql)
                .map_err(|error| build_failure(index, item, error))?;
            let params = statement_params(payload.get("params"), statements.len())
                .map_err(|error| build_failure(index, item, error))?;

            let (db, project_id) = project_id(orch, project)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let thread = thread_by_name(&db, &project_id, name)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let (context, context_db) =
                crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
                    .await
                    .map_err(|_| build_failure(index, item, thread_db_refusal(project, name)))?;
            let owner = owning_thread_id(&context_db, &context.job_id)
                .await
                .map_err(|_| build_failure(index, item, thread_db_refusal(project, name)))?;
            if owner.as_deref() != Some(thread.id.as_str()) {
                return Err(build_failure(index, item, thread_db_refusal(project, name)));
            }
            if dry_run {
                format!("Would apply {} statements to {uri}", statements.len())
            } else {
                let statement_count = statements.len();
                let changed = crate::threads::db_store::apply_write(&thread.id, statements, params)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!("Applied {statement_count} statements to {uri} ({changed} rows changed)")
            }
        }
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
                let _ = orch.services.emitter.emit(
                    "db-change",
                    crate::notify::thread_db_change(&thread, "insert"),
                );
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
                        crate::threads::crud::delete(
                            &db,
                            orch.services.emitter.as_ref(),
                            &thread.id,
                        )
                        .await
                        .map_err(|e| build_failure(index, item, e.to_string()))?;
                        let _ = orch.services.emitter.emit(
                            "db-change",
                            crate::notify::thread_db_change(&thread, "delete"),
                        );
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
                    if let Some(action) = payload.get("action") {
                        let action = action.as_str().ok_or_else(|| {
                            build_failure(index, item, "payload.action must be the string 'stop'")
                        })?;
                        if action != "stop" {
                            return Err(build_failure(
                                index,
                                item,
                                "payload.action must be 'stop'",
                            ));
                        }
                        if payload.len() != 1 {
                            return Err(build_failure(
                                index,
                                item,
                                "payload.action cannot be combined with thread metadata",
                            ));
                        }
                        let job_id = db
                            .read(|conn| {
                                let project = project.to_string();
                                let name = name.to_string();
                                Box::pin(async move {
                                    crate::threads::session_job_id_by_name_conn(
                                        conn, &project, &name,
                                    )
                                    .await
                                })
                            })
                            .await
                            .map_err(|error| build_failure(index, item, error.to_string()))?
                            .ok_or_else(|| {
                                build_failure(
                                    index,
                                    item,
                                    format!("thread {project}/{name} has no session job"),
                                )
                            })?;
                        return match crate::orchestrator::lifecycle::live_run_id_for_job(
                            orch, &job_id,
                        ) {
                            Some(run_id) if dry_run => Ok(Some(format!(
                                "Would stop {name}: interrupt run {run_id}'s active turn and park the session warm (resumable)"
                            ))),
                            Some(run_id) => {
                                crate::orchestrator::lifecycle::stop_session(orch, &run_id)
                                    .map_err(|error| build_failure(index, item, error))?;
                                Ok(Some(format!(
                                    "Stopped {name}: interrupted run {run_id}'s active turn and parked the session warm (resumable; cascades to child runs)"
                                )))
                            }
                            None => {
                                let job = crate::jobs::queries::get_job(&db, &job_id)
                                    .await
                                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                                if job.status.is_terminal() {
                                    Ok(Some(format!(
                                        "thread {name} has no active run to stop"
                                    )))
                                } else if dry_run {
                                    Ok(Some(format!(
                                        "Would stop {name}: cancel its pending prompt, drop the pending successor, stop child runs, and idle the job (no live run attached)"
                                    )))
                                } else {
                                    crate::orchestrator::lifecycle::stop_job(orch, &job_id)
                                        .map_err(|error| build_failure(index, item, error))?;
                                    let status = crate::jobs::queries::get_job(&db, &job_id)
                                        .await
                                        .map_err(|error| {
                                            build_failure(index, item, error.to_string())
                                        })?
                                        .status;
                                    Ok(Some(format!(
                                        "Stopped {name}: cancelled pending input, stopped child runs, and the job is now {status} (steerable for a follow-up)"
                                    )))
                                }
                            }
                        };
                    }
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
                        let outcome = crate::threads::crud::update_with_lifecycle(
                            orch,
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
                        let _ = orch.services.emitter.emit(
                            "db-change",
                            crate::notify::thread_update_db_change(
                                &outcome.previous,
                                &outcome.thread,
                            ),
                        );
                        format!("Patched thread cairn://p/{project}/{}", outcome.thread.name)
                    }
                }
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
