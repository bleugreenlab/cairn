//! The app's database browser (cairn/5914): the operator reading any
//! database's schema and rows, and writing rows of a thread's database. It has
//! no grammar of its own. Every read and write is one the row grammar at
//! `<root>/db/<table>[/<pk>]` already answers; this module is only the browser's
//! two doors into it.
//!
//! # Reads
//!
//! [`read_db`] is a run-less read of database URIs, dispatched through the read
//! verb's own door ([`crate::dispatch::dispatch_tool`]), so it answers exactly
//! what a run-less `read` answers and is scrubbed at the same final crossing a
//! model-visible read is. It admits only database URIs: a thread database's
//! overview, schema, tables and rows in the caller's own project, and the
//! application database's. It widens nothing. The runner's machine secret
//! already admits a run-less read of every one of those URIs at `/api/mcp`, and
//! the invoke route that carries this door takes no weaker credential.
//!
//! # Writes
//!
//! [`write_db_rows`] is the operator's row write through the browser: the same
//! authority class a page write has ([`crate::row_writes`]), with the browser as
//! the instrument. Its gate admits only the row grammar's writes (an `append` to
//! a table, a `patch` or `delete` of a row or of a `where=`-scoped set of rows)
//! in ONE thread's tables in the caller's project. The instrument is set here,
//! from the door, never from the input. `cairn://db` is read-only at every URI
//! (tenet #183) and is refused before anything is dispatched.

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::protocol::{RowWriteInstrument, RowWriteOrigin};
use cairn_uri::{canonical_project, parse_uri, CairnResource};
use serde_json::{json, Value};

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::pages::PageOperator;
use crate::row_writes::{self, GatedChange, InputError};

/// The most URIs one browser read carries.
const MAX_READ_PATHS: usize = 16;

const NOUN: &str = "the database browser";

/// The refusal for any write to the application database.
const APP_DB_READ_ONLY: &str =
    "cairn://db is the application database, read-only at every URI; the database browser writes rows of a thread's tables";

/// Why a browser call was not answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbBrowserError {
    BadRequest(String),
    Refused(String),
    Failed(String),
}

impl std::fmt::Display for DbBrowserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) | Self::Refused(message) | Self::Failed(message) => {
                f.write_str(message)
            }
        }
    }
}

/// The project id a URI's project segment names, or why it names none.
async fn project_id_of(orch: &Orchestrator, project: &str) -> Result<String, String> {
    let db = orch.db.for_project(project).await;
    crate::mcp::handlers::run_context::project_id_by_key(&db, project).await
}

/// A thread URI's project must be the caller's: the browser is a pane of one
/// project, and its runner answers for that project's threads.
async fn require_project(
    orch: &Orchestrator,
    project_id: &str,
    project: &str,
    target: &str,
) -> Result<(), String> {
    match project_id_of(orch, project).await {
        Ok(id) if id == project_id => Ok(()),
        _ => Err(format!(
            "{target}: the database browser reads and writes thread databases in its own project, not in {project}"
        )),
    }
}

/// `target` as a database URI the browser may read, or why it is not one.
async fn gate_read(orch: &Orchestrator, project_id: &str, target: &str) -> Result<(), String> {
    if !target.starts_with("cairn://") {
        return Err(format!(
            "{target}: the database browser reads database URIs by canonical address (cairn://p/PROJECT/THREAD/db..., or cairn://db...)"
        ));
    }
    let identity = target.split(['?', '#']).next().unwrap_or_default();
    let resource =
        parse_uri(identity).and_then(|parsed| cairn_uri::delegate_thread_descendant(parsed).ok());
    match resource {
        Some(
            CairnResource::ThreadDb { project, .. }
            | CairnResource::ThreadDbSchema { project, .. }
            | CairnResource::ThreadDbTable { project, .. }
            | CairnResource::ThreadDbRow { project, .. },
        ) => require_project(orch, project_id, &project, target).await,
        Some(CairnResource::Db | CairnResource::DbTable { .. } | CairnResource::DbRow { .. }) => {
            Ok(())
        }
        _ => Err(format!(
            "{target}: the database browser reads only database URIs (a thread's .../db, .../db/schema, .../db/<table>[/<pk>], or cairn://db[/<table>[/<pk>]])"
        )),
    }
}

/// Read database URIs as a run-less read, returning the `{result, reminders}`
/// callback response the `read_batch` tool returns, so the SDK client parses it
/// unchanged. Any URI that is not a database URI refuses the whole call.
pub async fn read_db(
    orch: &Orchestrator,
    project_id: &str,
    paths: &[String],
) -> Result<Value, DbBrowserError> {
    if paths.is_empty() || paths.len() > MAX_READ_PATHS {
        return Err(DbBrowserError::BadRequest(format!(
            "read_db takes 1 to {MAX_READ_PATHS} database URIs"
        )));
    }
    let targets: Vec<String> = paths.iter().map(|path| path.trim().to_string()).collect();
    let mut refusals = Vec::new();
    for target in &targets {
        if let Err(refusal) = gate_read(orch, project_id, target).await {
            refusals.push(refusal);
        }
    }
    if !refusals.is_empty() {
        return Err(DbBrowserError::Refused(refusals.join("\n")));
    }
    let request = McpCallbackRequest {
        tool: "read_batch".to_string(),
        payload: json!({ "paths": targets, "include_bodies": true }),
        ..Default::default()
    };
    let cursors = Mutex::new(HashMap::new());
    let output = crate::dispatch::dispatch_tool(orch, &request, &cursors)
        .await
        .into_inner();
    Ok(json!({ "result": output.content, "reminders": [] }))
}

/// One write's thread, as the first change named it.
struct WriteThread {
    project: String,
    name: String,
}

/// Every change as a canonical row write in ONE thread's tables, or every
/// refusal.
fn gate_write(changes: &[Value]) -> Result<(Vec<GatedChange>, WriteThread), Vec<String>> {
    let mut gated = Vec::with_capacity(changes.len());
    let mut refusals = Vec::new();
    let mut thread: Option<WriteThread> = None;
    for change in changes {
        let change = match row_writes::read_change(change, NOUN) {
            Ok(change) => change,
            Err(refusal) => {
                refusals.push(refusal);
                continue;
            }
        };
        let target = change.target.as_str();
        if !target.starts_with("cairn://") {
            refusals.push(format!(
                "{target}: the database browser writes rows by canonical address, cairn://p/PROJECT/THREAD/db/<table>[/<pk>]"
            ));
            continue;
        }
        let identity = target.split(['?', '#']).next().unwrap_or_default();
        if matches!(
            parse_uri(identity),
            Some(CairnResource::Db | CairnResource::DbTable { .. } | CairnResource::DbRow { .. })
        ) {
            refusals.push(format!("{target}: {APP_DB_READ_ONLY}"));
            continue;
        }
        let row_target = match row_writes::row_target(target, target) {
            Ok(Some(row_target)) => row_target,
            Ok(None) => {
                refusals.push(format!(
                    "{target}: the database browser writes only rows of a thread's tables (append to .../db/<table>, patch or delete .../db/<table>/<pk> or .../db/<table>?where=...)"
                ));
                continue;
            }
            Err(refusal) => {
                refusals.push(refusal);
                continue;
            }
        };
        match &thread {
            None => {
                thread = Some(WriteThread {
                    project: row_target.project.clone(),
                    name: row_target.thread.clone(),
                });
            }
            Some(first)
                if canonical_project(&first.project) != canonical_project(&row_target.project)
                    || first.name != row_target.thread =>
            {
                refusals.push(format!(
                    "{target}: one browser write touches one thread's rows; this batch began in {}",
                    cairn_uri::build_thread_uri(&first.project, &first.name)
                ));
                continue;
            }
            Some(_) => {}
        }
        let Some(mutation) = row_target.mutation(&change.mode) else {
            refusals.push(format!(
                "{target}: mode `{}` is refused; the database browser appends to .../db/<table>, and patches or deletes .../db/<table>/<pk> or .../db/<table>?where=...",
                change.mode
            ));
            continue;
        };
        gated.push(GatedChange::new(
            &change,
            target.to_string(),
            &row_target,
            mutation,
        ));
    }
    match thread {
        Some(thread) if refusals.is_empty() => Ok((gated, thread)),
        _ => Err(refusals),
    }
}

/// Write rows of one thread's database as `operator` through the database
/// browser, returning the `{result, reminders}` callback response the `write`
/// tool returns, so the SDK client parses its change report unchanged.
pub async fn write_db_rows(
    orch: &Orchestrator,
    project_id: &str,
    input: &Value,
    operator: &PageOperator,
) -> Result<Value, DbBrowserError> {
    let (changes, atomic) =
        row_writes::split_input(input, "write_db_rows", NOUN).map_err(|error| match error {
            InputError::BadRequest(message) => DbBrowserError::BadRequest(message),
            InputError::Refused(message) => DbBrowserError::Refused(message),
        })?;
    let (gated, thread) =
        gate_write(&changes).map_err(|refusals| DbBrowserError::Refused(refusals.join("\n")))?;
    let first = gated
        .first()
        .and_then(GatedChange::target)
        .unwrap_or_default()
        .to_string();
    require_project(orch, project_id, &thread.project, &first)
        .await
        .map_err(DbBrowserError::Refused)?;
    let db = orch.db.for_project(&thread.project).await;
    let owner_thread_id = crate::threads::crud::list(&db, project_id)
        .await
        .map_err(|error| DbBrowserError::Failed(error.to_string()))?
        .into_iter()
        .find(|candidate| candidate.name == thread.name)
        .map(|found| found.id)
        .ok_or_else(|| {
            DbBrowserError::Refused(format!(
                "{first}: no thread `{}` in {}",
                thread.name, thread.project
            ))
        })?;

    let origin = RowWriteOrigin {
        instrument: RowWriteInstrument::DbBrowser,
        owner_thread_id,
        owner: cairn_uri::build_thread_uri(&thread.project, &thread.name),
        project_id: project_id.to_string(),
        actor: operator.actor.clone(),
        appearance: operator.appearance.clone(),
    };
    let raw = row_writes::dispatch(orch, &origin, &gated, atomic).await;
    let report: Value = serde_json::from_str(&raw)
        .map_err(|_| DbBrowserError::Failed(format!("the write did not complete: {raw}")))?;
    if row_writes::is_blocking(&report) {
        return Err(DbBrowserError::Refused(
            "a write through the database browser cannot wait on an answer".to_string(),
        ));
    }
    row_writes::journal(orch, &origin, &report, &gated).await;
    Ok(json!({ "result": raw, "reminders": [] }))
}

#[cfg(test)]
mod tests;
