//! First-class thread resource mutations.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_db::models::CreateThread;
use cairn_uri::thread_name::validate_thread_name;
use cairn_uri::CairnResource;

use crate::resources::db_rows::{self, PayloadUse, TableShape};
use crate::threads::crud::by_name as thread_by_name;
use crate::threads::db_store::{
    apply_write, police, split_write_sql, RowAction, RowKey, RowTarget, WriteScope, WriteStatement,
};

fn thread_db_refusal(project: &str, name: &str) -> String {
    format!(
        "cairn://p/{project}/{name}/db is thread '{name}'s own store, writable only by that thread and the tasks it spawns, or by the operator through a page it owns or the app's database browser. You can read it with `?sql=`."
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
    crate::resources::db_rows::sql_param(value).map_err(|error| format!("payload.params: {error}"))
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

/// The write scope of a thread's database, once the writer is shown to own it:
/// the writing run belongs to that thread's session or to a task it spawned,
/// or the write is a page's, made through a page that thread owns. The one
/// ownership rule for SQL and structured writes alike.
///
/// A page write's owner comes from its typed origin, which only Rust sets; it
/// never looks for a run, so it cannot borrow one.
async fn owned_thread_scope(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    name: &str,
) -> Result<WriteScope, String> {
    let (db, project_id) = project_id(orch, project).await?;
    let thread = thread_by_name(&db, &project_id, name).await?;
    let acting = acting_thread_id(orch, request, project, &db, &project_id).await;
    if acting.as_deref() != Some(thread.id.as_str()) {
        return Err(thread_db_refusal(project, name));
    }
    Ok(WriteScope {
        thread_id: thread.id,
        project_id,
        project: cairn_uri::canonical_project(project),
        thread_name: thread.name,
    })
}

/// The thread a write acts in, from exactly one source: an operator's row
/// write acts for the one thread its typed origin names (a page's owner, or the
/// thread the database browser is showing; only Rust sets it, and it never
/// looks for a run), a project service's call acts for the thread its
/// delegation names (a service's own standing run acts for none), and any other
/// run acts in its own thread.
async fn acting_thread_id(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    db: &cairn_db::storage::LocalDb,
    project_id: &str,
) -> Option<String> {
    use crate::project_services::authority::ServiceRunRole;
    if let Some(origin) = request.row_write_origin() {
        return Some(origin.owner_thread_id.clone());
    }
    let run_id = request.run_id.as_deref()?;
    if let Some(service) = orch.project_services.runs.get(run_id) {
        let ServiceRunRole::Delegated { scope, .. } = &service.role else {
            return None;
        };
        if service.project_key != cairn_uri::canonical_project(project) {
            return None;
        }
        let name = scope.writable_thread()?;
        return thread_by_name(db, project_id, name)
            .await
            .ok()
            .map(|thread| thread.id);
    }
    run_thread_id(orch, request).await
}

/// The thread that owns the asking run's own job, resolved from the run's
/// context (never from anything the request body says).
pub(super) async fn run_thread_id(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<String> {
    let (context, context_db) =
        crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
            .await
            .ok()?;
    owning_thread_id(&context_db, &context.job_id)
        .await
        .ok()
        .flatten()
}

/// The thread a queue order is for, and who placed it.
pub(super) type QueueOrderer = (
    std::sync::Arc<cairn_db::storage::LocalDb>,
    cairn_db::models::Thread,
    String,
);

/// Who orders a thread's queue (cairn/5848): the thread itself (its sessions
/// and their tasks, or a page it owns) or the operator, whose requests carry no
/// run. Returns the project's database, the thread, and the name a hold moved
/// ahead records as having placed it. A reorder skips no cap and moves no
/// other thread's place, so this identification is proportionate.
pub(super) async fn queue_orderer(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    thread: &str,
) -> Result<QueueOrderer, String> {
    let (db, project_id, target) =
        crate::execution::gates::reorder::queue_thread(orch, project, thread).await?;
    let no_run = request
        .run_id
        .as_deref()
        .filter(|run| !run.is_empty())
        .is_none();
    if no_run && request.row_write_origin().is_none() {
        return Ok((db, target, "the operator".to_string()));
    }
    let acting = acting_thread_id(orch, request, project, &db, &project_id).await;
    if acting.as_deref() == Some(target.id.as_str()) {
        let name = target.name.clone();
        return Ok((db, target, name));
    }
    let other = match acting {
        Some(id) => crate::threads::crud::list(&db, &project_id)
            .await
            .ok()
            .and_then(|threads| threads.into_iter().find(|thread| thread.id == id))
            .map_or(id, |thread| thread.name),
        None => "no thread".to_string(),
    };
    Err(format!(
        "Only {thread} (its sessions and their tasks) or the operator orders {thread}'s queue; \
         this run acts for {other}. Nothing changed."
    ))
}

/// Why `place:"head"` and `after:` never go together.
pub(super) const HEAD_WITH_AFTER: &str = "place:\"head\" orders cap-held work; an after:-gated \
     execution has no place until its gates clear. File it without after:, or order it later \
     with cairn:~/queue {order:[N]}. Nothing changed.";

/// The upfront checks of `place:\"head\"` at filing, run before anything is
/// written: it orders cap-held work only, in the queue of the thread the
/// filed issue belongs to, and only that thread or the operator may ask.
/// Returns who placed it.
pub(super) async fn head_placed_by(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    parent_thread: Option<&str>,
    filing: &str,
    has_after: bool,
) -> Result<String, String> {
    if has_after {
        return Err(HEAD_WITH_AFTER.to_string());
    }
    let Some(thread) = parent_thread else {
        return Err(format!(
            "place:\"head\" files an execution at the head of its parent thread's own queue, and \
             {filing} has no parent thread. File it without place. Nothing changed."
        ));
    };
    let (_, _, placed_by) = queue_orderer(orch, request, project, thread).await?;
    Ok(placed_by)
}

/// The name of the thread issue `number` of `project` is a child of.
pub(super) async fn issue_parent_thread(
    orch: &Orchestrator,
    project: &str,
    number: i32,
) -> Result<Option<String>, String> {
    let db = orch.db.for_project(project).await;
    db.query_opt_text(
        "SELECT t.name FROM issues i
           JOIN projects p ON p.id = i.project_id
           JOIN threads t ON t.id = i.parent_thread_id
          WHERE p.key = ?1 AND i.number = ?2",
        (cairn_uri::canonical_project(project), number),
    )
    .await
    .map_err(|error| error.to_string())
}

/// The thread a write acts for, by id and name, if it acts for one.
pub(super) async fn acting_thread(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
) -> Option<(String, String)> {
    let (db, project_id) = project_id(orch, project).await.ok()?;
    let acting = acting_thread_id(orch, request, project, &db, &project_id).await?;
    // The asking thread may live in another project (a thread in A moving B's
    // queued issues), so its name is looked up wherever the thread is, not in
    // the target's list. The id is what a grant matches; the name is display.
    let mut name = None;
    for db in orch.db.all_dbs().await {
        if let Ok(Some(found)) = db
            .query_opt_text("SELECT name FROM threads WHERE id = ?1", (acting.clone(),))
            .await
        {
            name = Some(found);
            break;
        }
    }
    let name = name.unwrap_or_else(|| acting.clone());
    Some((acting, name))
}

/// The title of issue `number` of `project`, for a prompt's summary.
pub(super) async fn issue_title(orch: &Orchestrator, project: &str, number: i32) -> Option<String> {
    let db = orch.db.for_project(project).await;
    db.query_opt_text(
        "SELECT i.title FROM issues i JOIN projects p ON p.id = i.project_id
          WHERE p.key = ?1 AND i.number = ?2",
        (cairn_uri::canonical_project(project), number),
    )
    .await
    .ok()
    .flatten()
}

/// The name of the thread a write acts in, if it acts in one.
pub(super) async fn acting_thread_name(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
) -> Option<String> {
    let (db, project_id) = project_id(orch, project).await.ok()?;
    let acting = acting_thread_id(orch, request, project, &db, &project_id).await?;
    crate::threads::crud::list(&db, &project_id)
        .await
        .ok()?
        .into_iter()
        .find(|thread| thread.id == acting)
        .map(|thread| thread.name)
}

/// A structured row write's payload: a column map, absent meaning empty.
fn row_payload(item: &ChangeItem) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    match item.payload.as_ref() {
        None | Some(serde_json::Value::Null) => Ok(serde_json::Map::new()),
        Some(serde_json::Value::Object(map)) => Ok(map.clone()),
        Some(_) => Err("the payload is a column map: {column: value, ...}".to_string()),
    }
}

/// The query a structured write's target carries. The resource parser drops
/// it, so it is read off the raw target, the way the browser's
/// `return_content` is.
fn target_query(item: &ChangeItem) -> Result<Vec<cairn_common::query::QueryParam>, String> {
    item.target
        .split_once('?')
        .map(|(_, query)| cairn_common::query::parse_query_params(query))
        .transpose()
        .map(Option::unwrap_or_default)
}

/// A row write that takes no query refuses one rather than ignoring it: a
/// `where=` on an append or a row URI would otherwise read as a filter that
/// did nothing.
fn refuse_target_query(item: &ChangeItem, form: &str) -> Result<(), String> {
    if target_query(item)?.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{form} takes no query; where= scopes only a patch or delete of a table's collection URI"
        ))
    }
}

/// The live shape of the table a structured write names, from the owning
/// thread's store.
async fn write_target_shape(scope: &WriteScope, table: &str) -> Result<TableShape, String> {
    let root = scope.db_root();
    if table == cairn_uri::RESERVED_TABLE_SEGMENT {
        return Err(cairn_uri::reserved_table_segment_refusal(&root));
    }
    let db = crate::threads::db_store::open(&scope.thread_id).await?;
    db_rows::load_shape(&db, table)
        .await
        .map_err(crate::threads::db_store::sanitize_error)?
        .ok_or_else(|| db_rows::no_table(&root, table))
}

async fn project_id(
    orch: &Orchestrator,
    project: &str,
) -> Result<(std::sync::Arc<cairn_db::storage::LocalDb>, String), String> {
    let db = orch.db.for_project(project).await;
    let id = crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
    Ok((db, id))
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
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::ThreadQueue { project, thread }, ChangeMode::Patch) => {
            let order = item
                .payload
                .as_ref()
                .and_then(|payload| payload.get("order"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.order is this thread's children by issue number, first to start \
                         first: {order:[5841, 5829]}. Nothing changed.",
                    )
                })?;
            let (db, target, placed_by) = queue_orderer(orch, request, project, thread)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                format!(
                    "Would order {thread}'s queue: {}",
                    serde_json::Value::from(order.clone())
                )
            } else {
                crate::execution::gates::reorder::order_thread_queue(
                    orch, &db, &target, &order, &placed_by,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
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
            let statements =
                split_write_sql(sql).map_err(|error| build_failure(index, item, error))?;
            let params = statement_params(payload.get("params"), statements.len())
                .map_err(|error| build_failure(index, item, error))?;
            let statements: Vec<WriteStatement> = statements
                .into_iter()
                .zip(params)
                .map(|(sql, params)| WriteStatement {
                    sql,
                    params,
                    row: None,
                })
                .collect();
            police(&statements).map_err(|error| build_failure(index, item, error))?;

            let scope = owned_thread_scope(orch, request, project, name)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                format!("Would apply {} statements to {uri}", statements.len())
            } else {
                let statement_count = statements.len();
                let outcome = apply_write(&scope, statements, orch.services.emitter.as_ref())
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!(
                    "Applied {statement_count} statements to {uri} ({} rows changed)",
                    outcome.changed
                )
            }
        }
        (
            CairnResource::ThreadDbTable {
                project,
                name,
                table,
            },
            ChangeMode::Append,
        ) => {
            refuse_target_query(item, "An append")
                .map_err(|error| build_failure(index, item, error))?;
            let payload = row_payload(item).map_err(|error| build_failure(index, item, error))?;
            let scope = owned_thread_scope(orch, request, project, name)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let shape = write_target_shape(&scope, table)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let append = db_rows::parse_append(&shape, payload)
                .map_err(|error| build_failure(index, item, error))?;
            let key = if shape.primary_key().is_some() {
                RowKey::Returned
            } else {
                RowKey::Unaddressed
            };
            // A bulk append is one `apply_write`, so its rows commit or roll
            // back together.
            let statements: Vec<WriteStatement> = db_rows::append_sql(&shape, &append)
                .map_err(|error| build_failure(index, item, error))?
                .into_iter()
                .enumerate()
                .map(|(position, (sql, params))| WriteStatement {
                    sql,
                    params,
                    row: Some(RowTarget {
                        table: shape.table.clone(),
                        action: RowAction::Insert,
                        key: key.clone(),
                        position: append.bulk.then_some(position),
                    }),
                })
                .collect();
            police(&statements).map_err(|error| build_failure(index, item, error))?;
            let collection = cairn_uri::build_db_table_uri(&scope.db_root(), table);
            let (verb, would) = if append.upsert {
                ("Upserted", "upsert")
            } else {
                ("Inserted", "insert")
            };
            let count = statements.len();
            if dry_run {
                format!("Would {would} {count} row(s) into {collection}")
            } else {
                let outcome = apply_write(&scope, statements, orch.services.emitter.as_ref())
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                let unaddressed = format!(
                    "`{table}` has a composite (or no) primary key, so its rows have no URI"
                );
                if append.bulk {
                    *applied_data = Some(serde_json::json!({
                        "uris": outcome.rows,
                        "count": count,
                    }));
                    match key {
                        RowKey::Unaddressed => {
                            format!("{verb} {count} rows into {collection}; {unaddressed}")
                        }
                        _ => format!(
                            "{verb} {count} rows into {collection}; the change's data.uris names each row, in order"
                        ),
                    }
                } else {
                    match outcome.rows.into_iter().flatten().next() {
                        Some(row) => {
                            let summary = format!("{verb} {row}");
                            *applied_data = Some(serde_json::json!({ "uri": row }));
                            summary
                        }
                        None => format!("{verb} one row into {collection}; {unaddressed}"),
                    }
                }
            }
        }
        (
            CairnResource::ThreadDbTable {
                project,
                name,
                table,
            },
            mode @ (ChangeMode::Patch | ChangeMode::Delete),
        ) => {
            let mode_name = if mode == ChangeMode::Patch {
                "patch"
            } else {
                "delete"
            };
            let params = target_query(item).map_err(|error| build_failure(index, item, error))?;
            let payload = row_payload(item).map_err(|error| build_failure(index, item, error))?;
            let scope = owned_thread_scope(orch, request, project, name)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let collection = cairn_uri::build_db_table_uri(&scope.db_root(), table);
            if !params.iter().any(|param| param.key == "where") {
                return Err(build_failure(
                    index,
                    item,
                    db_rows::collection_write_needs_where(&collection, mode_name),
                ));
            }
            if mode == ChangeMode::Patch && payload.is_empty() {
                return Err(build_failure(
                    index,
                    item,
                    "patch needs the columns to change: payload:{column: value, ...}",
                ));
            }
            if mode == ChangeMode::Delete && !payload.is_empty() {
                return Err(build_failure(index, item, "delete takes no payload"));
            }
            let shape = write_target_shape(&scope, table)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let filters = db_rows::parse_write_filters(&params, &shape, &collection, mode_name)
                .map_err(|error| build_failure(index, item, error))?;
            let (sql, params, action) = if mode == ChangeMode::Patch {
                let columns = db_rows::payload_columns(&shape, &payload, PayloadUse::Update)
                    .map_err(|error| build_failure(index, item, error))?;
                let (sql, params) = db_rows::update_where_sql(&shape, &filters, &columns);
                (sql, params, RowAction::Update)
            } else {
                let (sql, params) = db_rows::delete_where_sql(&shape, &filters);
                (sql, params, RowAction::Delete)
            };
            let statement = WriteStatement {
                sql,
                params,
                row: Some(RowTarget {
                    table: shape.table.clone(),
                    action,
                    key: RowKey::Matched,
                    position: None,
                }),
            };
            police(std::slice::from_ref(&statement))
                .map_err(|error| build_failure(index, item, error))?;
            let scoped = format!("{collection}{}", db_rows::describe_filters(&filters));
            let verb = if action == RowAction::Update {
                "Updated"
            } else {
                "Deleted"
            };
            if dry_run {
                format!(
                    "Would have {} the rows of {scoped}",
                    verb.to_ascii_lowercase()
                )
            } else {
                let outcome = apply_write(&scope, vec![statement], orch.services.emitter.as_ref())
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                *applied_data = Some(serde_json::json!({ "count": outcome.changed }));
                format!("{verb} {} row(s) of {scoped}", outcome.changed)
            }
        }
        (
            CairnResource::ThreadDbRow {
                project,
                name,
                table,
                pk,
            },
            mode @ (ChangeMode::Patch | ChangeMode::Delete),
        ) => {
            refuse_target_query(item, "A row URI")
                .map_err(|error| build_failure(index, item, error))?;
            let payload = row_payload(item).map_err(|error| build_failure(index, item, error))?;
            if mode == ChangeMode::Patch && payload.is_empty() {
                return Err(build_failure(
                    index,
                    item,
                    "patch needs the columns to change: payload:{column: value, ...}",
                ));
            }
            if mode == ChangeMode::Delete && !payload.is_empty() {
                return Err(build_failure(index, item, "delete takes no payload"));
            }
            let scope = owned_thread_scope(orch, request, project, name)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let root = scope.db_root();
            let row_uri = cairn_uri::build_db_row_uri(&root, table, pk);
            let shape = write_target_shape(&scope, table)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let key = shape.primary_key().ok_or_else(|| {
                build_failure(index, item, db_rows::rows_have_no_uri(&root, table))
            })?;
            let (sql, params, action) = if mode == ChangeMode::Patch {
                let columns = db_rows::payload_columns(&shape, &payload, PayloadUse::Update)
                    .map_err(|error| build_failure(index, item, error))?;
                let (sql, params) = db_rows::update_sql(&shape, key, pk, &columns);
                (sql, params, RowAction::Update)
            } else {
                let (sql, params) = db_rows::delete_sql(&shape, key, pk);
                (sql, params, RowAction::Delete)
            };
            let statement = WriteStatement {
                sql,
                params,
                row: Some(RowTarget {
                    table: shape.table.clone(),
                    action,
                    key: RowKey::Known(pk.clone()),
                    position: None,
                }),
            };
            police(std::slice::from_ref(&statement))
                .map_err(|error| build_failure(index, item, error))?;
            let verb = if action == RowAction::Update {
                "Updated"
            } else {
                "Deleted"
            };
            if dry_run {
                format!("Would have {} {row_uri}", verb.to_ascii_lowercase())
            } else {
                apply_write(&scope, vec![statement], orch.services.emitter.as_ref())
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                let summary = format!("{verb} {row_uri}");
                *applied_data = Some(serde_json::json!({ "uri": row_uri }));
                summary
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
                    // The placement default is the thread's, keyed by its id, so
                    // it rides beside any metadata change and a rename leaves it
                    // where it was.
                    // Validated (and a name resolved) before anything is
                    // written, like every other key here: a refused patch
                    // applies none of its keys.
                    let placement_default = payload
                        .get("executor")
                        .map(|value| crate::fleet::placement_default::prepare_patch(orch, value))
                        .transpose()
                        .map_err(|e| build_failure(index, item, e))?;
                    let placement_owner =
                        crate::fleet::placement_default::DefaultOwner::Thread(thread.id.clone());
                    if payload.len() == 1 {
                        if let Some(prepared) = placement_default {
                            return crate::fleet::placement_default::commit_patch(
                                orch,
                                request,
                                &db,
                                &placement_owner,
                                prepared,
                                dry_run,
                            )
                            .await
                            .map(Some)
                            .map_err(|e| build_failure(index, item, e));
                        }
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
                    // The payload names only the model, so no provider narrows
                    // it: a name two providers offer is refused naming each,
                    // never settled by guessing from its spelling.
                    let model = match model {
                        Some(model) => Some(
                            crate::threads::crud::resolve_thread_model(
                                orch, &db, &thread.id, None, model,
                            )
                            .await
                            .map_err(|e| build_failure(index, item, e.to_string()))?,
                        ),
                        None => None,
                    };
                    let patched = if dry_run {
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
                                model,
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
                    };
                    match placement_default {
                        Some(prepared) => {
                            let line = crate::fleet::placement_default::commit_patch(
                                orch,
                                request,
                                &db,
                                &placement_owner,
                                prepared,
                                dry_run,
                            )
                            .await
                            .map_err(|e| build_failure(index, item, e))?;
                            format!("{patched}. {line}")
                        }
                        None => patched,
                    }
                }
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
