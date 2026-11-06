//! Bridged reads: what a page's frame may read, and with whose authority.
//!
//! # Authority
//!
//! A page is agent-authored code, so a bridged read never runs with the
//! viewer's authority. It runs as the page's OWNER would run it: a thread's
//! page as that thread, a draft on an issue node as that node's run. Two
//! things make that true:
//!
//! - Every deictic target is expanded against the owner before dispatch:
//!   `cairn:~/...` joins the owner's canonical home (the same join the read
//!   path uses for an agent), and `cairn:@/...` its project. Nothing in the
//!   batch is relative to the viewer.
//! - The batch is dispatched as a read with no run identity, and a read with
//!   no run identity is otherwise the operator's own. Every producer that
//!   widens on that is gated here instead: attachments are reachable only
//!   inside the owner's project, and the run-windowed posts corpus is refused.
//!   The structural fix, an explicit owner context the run-keyed producers
//!   consult, replaces these special cases (see the issue linked from
//!   `docs/pages.md`).
//!
//! # Rows only
//!
//! On top of the owner's authority, a page reads only database rows (tenet
//! #1312): the tables of thread databases in its own project, and the
//! application database's announced tables read-only, as one table's rows,
//! one row, or `?sql=` over either store ([`gate_targets`]). Every other
//! target is refused with the pattern that replaces it: a service reads the
//! source and writes rows, and the page shows the rows. The gate also caps
//! every delivery at [`PAGE_ROW_CAP`] rows and asks for JSON, and
//! [`target_scope`] names the tables whose change re-reads a target, which is
//! all the host's liveness consults. So no page, however it is written, holds
//! a subscription that re-reads on "any change" or delivers an unbounded body.
//!
//! The viewer is recorded on every logged call as the actor who had the page
//! open. The caller derives that record from verified credentials only.
//!
//! # The reader's grants
//!
//! A frame viewer is only recorded: a page never borrows a person's authority
//! (cairn/5450). A rendered read is different, because its output goes to the
//! agent that asked: a page must not show an agent what that agent could not
//! read itself. So a bridged read's effective authority is the INTERSECTION
//! of the owner's authority, the page's manifest, and the reader's grants
//! ([`PageReader`]). Intersection only narrows, so no read ever runs with more
//! than the owner's authority. Where the reader's grants cannot be represented,
//! every read is refused: the absence of a representable grant never selects
//! the owner's authority instead.
//!
//! A page reads only databases, so the intersection is a question about
//! databases: which ones may the reading agent read itself? Through its own
//! read path an agent run may read the application database (read-only) and
//! every thread database, which is everything the owner gate admits, so an
//! agent reader narrows nothing today. A reader whose grants cannot be
//! represented is still refused every read.
//!
//! # Origin
//!
//! A bridged read is dispatched with an explicit origin marker
//! ([`PAGE_READ_ORIGIN`]) on its request, so a page reading another page's
//! rendered view is refused rather than spawning a render from inside a render.
//! The marker travels with the request through every dispatch, including one
//! a spawned task performs, which a task-local would not survive.

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::query::{encode_query_params, split_target_query, QueryParam};
use cairn_common::read::{ReadBatchEnvelope, SegmentKind};
use cairn_common::uri::{canonical_project, parse_uri, CairnResource};
use serde_json::{json, Value};

use super::log::{record, PageCall, PageCallOutcome, PageCallVerb};
use super::store::{load_page, PageAddress, PageOwner, PageRow};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::resources::sql_policy::{referenced_tables, CatalogEntry};
use crate::storage::{LocalDb, RowExt};

/// Why a bridged read is being made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageReadPurpose {
    /// A one-shot `cairn.read` / `cairn.readBatch`.
    Read,
    /// A subscription's opening read.
    SubscribeOpen,
    /// A subscription's re-read on a change to its table. Not logged here: the
    /// opening call's row covers the target, and the host that ran the
    /// re-reads records them as counted `refresh` rows.
    SubscribeRefresh,
}

impl PageReadPurpose {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "read" => Self::Read,
            "subscribe-open" => Self::SubscribeOpen,
            "subscribe-refresh" => Self::SubscribeRefresh,
            _ => return None,
        })
    }

    fn verb(self) -> PageCallVerb {
        match self {
            Self::Read => PageCallVerb::Read,
            Self::SubscribeOpen | Self::SubscribeRefresh => PageCallVerb::Subscribe,
        }
    }
}

/// `page_read`'s arguments.
#[derive(Debug, Clone)]
pub struct PageReadRequest {
    pub project_id: String,
    /// The canonical page URI.
    pub page: String,
    pub version: i32,
    pub paths: Vec<String>,
    pub include_bodies: bool,
    pub purpose: PageReadPurpose,
    /// Whose grants the read is intersected with.
    pub reader: PageReader,
}

/// The `_cairn_origin` a bridged read's dispatch carries.
pub(crate) const PAGE_READ_ORIGIN: &str = "page";

/// Whether `request` is a page's own bridged read.
pub(crate) fn is_page_read(request: &McpCallbackRequest) -> bool {
    request.payload.get("_cairn_origin").and_then(Value::as_str) == Some(PAGE_READ_ORIGIN)
}

/// Who a bridged read is FOR, as distinct from whose authority it runs with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageReader {
    /// A person with the page open in a frame. Recorded on the log, never
    /// consulted: the cairn/5450 ruling that a page does not borrow a viewer's
    /// authority.
    FrameViewer,
    /// A run-less read: the operator's own, the same standing a run-less read
    /// has everywhere else on the read path. It narrows nothing, because the
    /// operator may already read everything a page's owner may.
    Operator,
    /// An agent run reading the page's rendered view.
    Agent(AgentReader),
    /// A reader whose grants cannot be represented. Every read is refused with
    /// this reason.
    Unrepresentable(String),
}

/// An agent run reading a page's rendered view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentReader {
    pub run_id: String,
    /// The run's canonical home.
    pub home: String,
}

impl PageReader {
    /// The reader of a resource read: the authenticated agent run behind
    /// `request`, the operator for a run-less read, or unrepresentable when a
    /// run id does not resolve to an agent.
    pub(crate) async fn for_request(orch: &Orchestrator, request: &McpCallbackRequest) -> Self {
        let Some(run_id) = request.run_id.as_deref() else {
            return Self::Operator;
        };
        match crate::mcp::handlers::run_context::authenticated_agent_for_run(&orch.db, run_id).await
        {
            Ok(agent) => Self::Agent(AgentReader {
                run_id: agent.run_id,
                home: agent.home,
            }),
            Err(error) => Self::Unrepresentable(format!(
                "the reading run `{run_id}` does not resolve to an agent ({error}), so its read grants cannot be intersected with the page's; the page's reads are refused rather than run with its owner's authority alone"
            )),
        }
    }

    /// The actor the call log records for this reader, or `None` to keep the
    /// caller's own record (a frame's verified viewer).
    pub(crate) fn actor(&self) -> Option<Value> {
        match self {
            Self::FrameViewer => None,
            Self::Operator | Self::Unrepresentable(_) => Some(json!({ "kind": "anonymous" })),
            Self::Agent(agent) => Some(json!({
                "kind": "agent",
                "node": agent.home,
                "run_id": agent.run_id,
            })),
        }
    }

    /// The targets this reader may receive, or every refusal. All-or-nothing,
    /// like the owner gate. An agent run may read every database the owner
    /// gate admits (see "The reader's grants" above), so only a reader whose
    /// grants cannot be represented narrows anything.
    pub(crate) fn narrow(&self, targets: &[PageTarget]) -> Result<(), Vec<String>> {
        match self {
            Self::FrameViewer | Self::Operator | Self::Agent(_) => Ok(()),
            Self::Unrepresentable(why) => Err(targets
                .iter()
                .map(|target| format!("{}: {why}", target.uri))
                .collect()),
        }
    }
}

/// `page_log_event`'s arguments.
#[derive(Debug, Clone)]
pub struct PageLogEvent {
    pub project_id: String,
    pub page: String,
    pub version: i32,
    pub verb: PageCallVerb,
    pub outcome: PageCallOutcome,
    pub targets: Vec<String>,
    pub detail: String,
}

/// Why a bridged call did not return a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageCallError {
    /// The request itself is malformed or names no such page.
    BadRequest(String),
    /// A target the page may not read. Logged.
    Refused(String),
    /// The read could not complete. Logged.
    Failed(String),
}

impl std::fmt::Display for PageCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) | Self::Refused(message) | Self::Failed(message) => {
                f.write_str(message)
            }
        }
    }
}

pub(super) struct ResolvedPage {
    pub(super) address: PageAddress,
    pub(super) project_id: String,
    pub(super) owner: PageOwner,
    pub(super) row: PageRow,
}

/// The page version a call names, refusing one from another project.
pub(super) async fn resolve(
    orch: &Orchestrator,
    project_id: &str,
    page: &str,
    version: i32,
) -> Result<ResolvedPage, PageCallError> {
    let address = PageAddress::parse(page).map_err(PageCallError::BadRequest)?;
    let db = orch.db.for_project(&address.project).await;
    let owning_project =
        crate::mcp::handlers::run_context::project_id_by_key(&db, &address.project)
            .await
            .map_err(PageCallError::BadRequest)?;
    if owning_project != project_id {
        return Err(PageCallError::BadRequest(format!(
            "{page} belongs to project `{}`, not to the project this call names",
            address.project
        )));
    }
    let (owner, row) = load_page(&db, &address, Some(version))
        .await
        .map_err(PageCallError::BadRequest)?
        .ok_or_else(|| {
            PageCallError::BadRequest(format!("{} has no version {version}", address.uri()))
        })?;
    Ok(ResolvedPage {
        address,
        project_id: owning_project,
        owner,
        row,
    })
}

/// The most rows one page read delivers. A `limit` above it is clamped and the
/// result says `hasMore`; a page that wants more reads the next window with
/// `offset`. Sized for what one screen shows and one structured clone into the
/// frame stays cheap at: a table of 200 rows is already past what a reader
/// scans, while 1000 issue rows with descriptions run to megabytes per delivery.
pub const PAGE_ROW_CAP: usize = 200;

/// The application tables a page may read. Each announces its writes as a
/// `db-change` naming the table, which is what re-reads a page's subscription;
/// a table that announced nothing would sit stale on the page, so it is
/// refused instead. A page's own pages are rows of `artifacts`.
pub const PAGE_APP_TABLES: &[&str] = &[
    "issues",
    "comments",
    "messages",
    "threads",
    "executions",
    "jobs",
    "tenets",
    "posts",
    "artifacts",
];

/// What a page reads instead of anything that is not rows, named in every
/// refusal of such a target.
const ROWS_ONLY: &str = "a page reads only database rows: its thread's tables (cairn:~/db/<table>, cairn:~/db/<table>/<pk>), the application database read-only (cairn://db/<table>), or ?sql= over either, through useRows, useRow and useQuery. Anything else (an https API, vcs history, transcripts, any other resource) reaches a page as rows a service writes: a service method, called on demand or on the service's schedule, reads the source with its own authority and writes rows to the thread's database, and the page shows those rows";

/// The database a page target reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PageStore {
    /// A thread's database in the page's own project.
    Thread { project: String, name: String },
    /// The application database, read-only.
    App,
}

/// What a page target reads from its store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PageReadShape {
    /// One table's rows (`.../db/<table>`).
    Rows { table: String },
    /// One row by primary key (`.../db/<table>/<pk>`).
    Row { table: String },
    /// A read-only `?sql=` statement over the store.
    Sql { sql: String },
}

/// One admitted page target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageTarget {
    /// The canonical target the read dispatches: deictics expanded against the
    /// owner, `limit` capped at [`PAGE_ROW_CAP`], `format=json`.
    pub(crate) uri: String,
    pub(crate) store: PageStore,
    pub(crate) shape: PageReadShape,
}

/// Each requested target as the owner would read it, or every refusal.
///
/// Refusal is all-or-nothing: a batch with one refused target reads nothing,
/// so a page never receives a partial answer it might mistake for a whole one.
pub(crate) fn gate_targets(
    address: &PageAddress,
    paths: &[String],
) -> Result<Vec<PageTarget>, Vec<String>> {
    let home = address.home_uri();
    let owner_project = canonical_project(&address.project);
    let mut allowed = Vec::with_capacity(paths.len());
    let mut refusals = Vec::new();
    for path in paths {
        let target = path.trim();
        match gate_target(address, &home, &owner_project, target) {
            Ok(target) => allowed.push(target),
            Err(why) => refusals.push(format!("{target}: {why}")),
        }
    }
    if refusals.is_empty() {
        Ok(allowed)
    } else {
        Err(refusals)
    }
}

fn gate_target(
    address: &PageAddress,
    home: &str,
    owner_project: &str,
    target: &str,
) -> Result<PageTarget, String> {
    let resolved = if crate::resources::mutations::is_home_relative_target(target) {
        if address.thread_name().is_none() && target.starts_with("cairn:~/db") {
            return Err(
                "a draft page has no thread database; its thread adopts it first, and then cairn:~/db is that thread's"
                    .to_string(),
            );
        }
        crate::resources::resolve_home_relative_against(home, target)
            .ok_or_else(|| format!("could not resolve against the page's owner {home}"))?
    } else {
        target.to_string()
    };
    let split = split_target_query(&resolved)?;
    let resource = match parse_uri(&split.identity) {
        Some(resource) => crate::resources::delegate_thread_descendant(resource)?,
        None => return Err(ROWS_ONLY.to_string()),
    };
    let sql = split
        .params
        .iter()
        .find(|param| param.key == "sql")
        .map(|param| param.value.clone());
    let catalog =
        || format!("a database's catalog and schema are for agents, not pages; {ROWS_ONLY}");
    let thread = |project: String, name: String| -> Result<PageStore, String> {
        if canonical_project(&project) == owner_project {
            Ok(PageStore::Thread {
                project: owner_project.to_string(),
                name,
            })
        } else {
            Err(format!(
                "a page reads thread databases in its own project ({owner_project}), not in {project}"
            ))
        }
    };
    let (store, shape) = match resource {
        CairnResource::ThreadDbTable {
            project,
            name,
            table,
        } => (thread(project, name)?, PageReadShape::Rows { table }),
        CairnResource::ThreadDbRow {
            project,
            name,
            table,
            ..
        } => (thread(project, name)?, PageReadShape::Row { table }),
        CairnResource::ThreadDb { project, name } => match sql {
            Some(sql) => (thread(project, name)?, PageReadShape::Sql { sql }),
            None => return Err(catalog()),
        },
        CairnResource::DbTable { table } => (PageStore::App, PageReadShape::Rows { table }),
        CairnResource::DbRow { table, .. } => (PageStore::App, PageReadShape::Row { table }),
        CairnResource::Db => match sql {
            Some(sql) => (PageStore::App, PageReadShape::Sql { sql }),
            None => return Err(catalog()),
        },
        CairnResource::ThreadDbSchema { .. } => return Err(catalog()),
        _ => return Err(ROWS_ONLY.to_string()),
    };
    if let (PageStore::App, PageReadShape::Rows { table } | PageReadShape::Row { table }) =
        (&store, &shape)
    {
        if let Some(refusal) = app_table_refusal(table) {
            return Err(refusal);
        }
    }
    let uri = with_page_params(&split.identity, split.params, &shape);
    Ok(PageTarget { uri, store, shape })
}

/// Why a page may not read the application table `table`, if it may not.
fn app_table_refusal(table: &str) -> Option<String> {
    (!PAGE_APP_TABLES
        .iter()
        .any(|admitted| admitted.eq_ignore_ascii_case(table)))
    .then(|| {
        format!(
            "`{table}` is not an application table a page reads: a page's read re-runs only when its own table announces a change, and the tables that announce one to pages are {}",
            PAGE_APP_TABLES.join(", ")
        )
    })
}

/// The query a page read dispatches with: the page's own keys, `limit` at most
/// [`PAGE_ROW_CAP`] (the cap when absent) on a collection or SQL read, and
/// `format=json`. `sql` goes last: its value is taken verbatim, so nothing
/// after it can be mistaken for part of the statement.
fn with_page_params(identity: &str, params: Vec<QueryParam>, shape: &PageReadShape) -> String {
    let mut kept = Vec::with_capacity(params.len() + 2);
    let mut sql = None;
    let mut limit = None;
    for param in params {
        match param.key.as_str() {
            "format" => {}
            "limit" => limit = Some(param),
            "sql" => sql = Some(param),
            _ => kept.push(param),
        }
    }
    if matches!(shape, PageReadShape::Row { .. }) {
        // A row read takes no limit; the engine refuses one by name.
        kept.extend(limit);
    } else {
        let value = match limit {
            None => PAGE_ROW_CAP.to_string(),
            Some(param) => match param.value.parse::<usize>() {
                Ok(requested) => requested.min(PAGE_ROW_CAP).to_string(),
                // Not a number: passed on, so the engine refuses it by name.
                Err(_) => param.value,
            },
        };
        kept.push(QueryParam {
            key: "limit".to_string(),
            value,
        });
    }
    kept.push(QueryParam {
        key: "format".to_string(),
        value: "json".to_string(),
    });
    kept.extend(sql);
    format!("{identity}?{}", encode_query_params(&kept))
}

/// The tables whose change re-reads `target`, in the shape the host matches a
/// `db-change` against: `{store: "thread", thread, tables}` or `{store: "app",
/// tables}`. A row or collection read follows its own table. A SQL read follows
/// the tables the statement reads, derived here from the store's catalog
/// ([`referenced_tables`]); `tables: null` (a thread database not created
/// yet, whose first tables the statement may name) follows every table in that
/// thread. A SQL read of the application database that reaches a table outside
/// [`PAGE_APP_TABLES`] is refused.
pub(crate) async fn target_scope(
    orch: &Orchestrator,
    target: &PageTarget,
) -> Result<Value, PageCallError> {
    let tables: Option<Vec<String>> = match &target.shape {
        PageReadShape::Rows { table } | PageReadShape::Row { table } => Some(vec![table.clone()]),
        PageReadShape::Sql { sql } => match &target.store {
            PageStore::App => Some(statement_tables(&orch.db.local, sql).await?),
            PageStore::Thread { project, name } => {
                match crate::resources::open_thread_store(orch, project, name)
                    .await
                    .map_err(PageCallError::Failed)?
                {
                    Some(db) => Some(statement_tables(&db, sql).await?),
                    None => None,
                }
            }
        },
    };
    Ok(match &target.store {
        PageStore::Thread { name, .. } => {
            json!({ "store": "thread", "thread": name, "tables": tables })
        }
        PageStore::App => {
            if let Some(refusal) = tables
                .iter()
                .flatten()
                .find_map(|table| app_table_refusal(table))
            {
                return Err(PageCallError::Refused(format!("{}: {refusal}", target.uri)));
            }
            json!({ "store": "app", "tables": tables })
        }
    })
}

/// The tables `sql` reads in `db`.
async fn statement_tables(db: &LocalDb, sql: &str) -> Result<Vec<String>, PageCallError> {
    let catalog = db
        .query_all(
            "SELECT name, type, sql FROM sqlite_schema WHERE type IN ('table', 'view')",
            (),
            |row| {
                Ok(CatalogEntry {
                    name: row.text(0)?,
                    view_sql: if row.text(1)? == "view" {
                        row.text(2).ok()
                    } else {
                        None
                    },
                })
            },
        )
        .await
        .map_err(|error| {
            PageCallError::Failed(format!(
                "the tables this SQL reads could not be named: {error}"
            ))
        })?;
    Ok(referenced_tables(sql, &catalog))
}

/// Run the batch as the owner would: deictics already expanded, no run id,
/// every target whole up to `byte_ceiling` and refused past it.
pub(crate) async fn dispatch_as_owner(
    orch: &Orchestrator,
    targets: &[String],
    include_bodies: bool,
    byte_ceiling: usize,
) -> String {
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        cwd: String::new(),
        run_id: None,
        tool: "read_batch".to_string(),
        payload: json!({
            "paths": targets,
            "include_bodies": include_bodies,
            "_cairn_origin": PAGE_READ_ORIGIN,
        }),
        tool_use_id: None,
        thread_id: None,
        origin: None,
    };
    let cursors = Mutex::new(HashMap::new());
    crate::mcp::handlers::read::read_batch(
        orch,
        &request,
        &cursors,
        crate::storage::render::ReadBudget::Whole { byte_ceiling },
    )
    .await
}

/// The read's envelope, or why there is none. A read that would have waited
/// on a permission answer comes back as a plain message rather than an
/// envelope; a page has no one to answer it, so that is an error here, never a
/// suspension.
pub(crate) fn envelope_or_error(raw: &str) -> Result<ReadBatchEnvelope, String> {
    serde_json::from_str(raw).map_err(|_| {
        format!("the read did not complete, and a page's read cannot wait on a permission answer: {raw}")
    })
}

fn segment_errors(envelope: &ReadBatchEnvelope) -> Vec<String> {
    envelope
        .segments
        .iter()
        .filter(|segment| segment.kind == SegmentKind::Error)
        .map(|segment| segment.uri.clone())
        .collect()
}

pub(super) async fn log_call(
    orch: &Orchestrator,
    page: &ResolvedPage,
    actor: &Value,
    verb: PageCallVerb,
    targets: &[String],
    outcome: PageCallOutcome,
    detail: &str,
) {
    let uri = page.address.uri();
    let call = PageCall {
        project_id: &page.project_id,
        page_artifact_id: &page.row.id,
        page_uri: &uri,
        page_version: page.row.version,
        actor,
        verb,
        targets,
        outcome,
        detail,
    };
    if let Err(error) = record(&orch.db.local, &call).await {
        log::warn!("page call for {uri} was not logged: {error}");
    }
}

/// A bridged read for a page's frame, returning the `{result, reminders}`
/// callback response the MCP `read_batch` tool returns, so the SDK client
/// parses it unchanged, plus `scopes`: each target's [`target_scope`], in
/// target order, which is what the host's liveness re-reads on.
pub async fn page_read(
    orch: &Orchestrator,
    request: &PageReadRequest,
    actor: &Value,
) -> Result<Value, PageCallError> {
    if request.paths.is_empty() {
        return Err(PageCallError::BadRequest(
            "page_read requires a non-empty `paths` array".to_string(),
        ));
    }
    let page = resolve(orch, &request.project_id, &request.page, request.version).await?;
    let verb = request.purpose.verb();
    let reader_actor = request.reader.actor();
    let actor = reader_actor.as_ref().unwrap_or(actor);
    let gated = gate_targets(&page.address, &request.paths)
        .and_then(|targets| request.reader.narrow(&targets).map(|()| targets));
    let mut scopes = Vec::new();
    let scoped = match gated {
        Ok(targets) => {
            let mut outcome = Ok(());
            for target in &targets {
                match target_scope(orch, target).await {
                    Ok(scope) => scopes.push(scope),
                    Err(error) => {
                        outcome = Err(error);
                        break;
                    }
                }
            }
            outcome.map(|()| targets)
        }
        Err(refusals) => Err(PageCallError::Refused(refusals.join("\n"))),
    };
    let targets = match scoped {
        Ok(targets) => targets,
        Err(error) => {
            let outcome = match error {
                PageCallError::Refused(_) => PageCallOutcome::Refused,
                _ => PageCallOutcome::Error,
            };
            log_call(
                orch,
                &page,
                actor,
                verb,
                &request.paths,
                outcome,
                &error.to_string(),
            )
            .await;
            return Err(error);
        }
    };
    let targets: Vec<String> = targets.into_iter().map(|target| target.uri).collect();

    let raw = dispatch_as_owner(
        orch,
        &targets,
        request.include_bodies,
        crate::storage::render::WHOLE_READ_BYTE_CEILING,
    )
    .await;
    let envelope = match envelope_or_error(&raw) {
        Ok(envelope) => envelope,
        Err(message) => {
            log_call(
                orch,
                &page,
                actor,
                verb,
                &targets,
                PageCallOutcome::Error,
                &message,
            )
            .await;
            return Err(PageCallError::Failed(message));
        }
    };

    if request.purpose != PageReadPurpose::SubscribeRefresh {
        let errors = segment_errors(&envelope);
        let (outcome, detail) = if errors.is_empty() {
            (
                PageCallOutcome::Ok,
                format!("{} target(s) read", targets.len()),
            )
        } else {
            (
                PageCallOutcome::Error,
                format!("{} target(s) failed: {}", errors.len(), errors.join(", ")),
            )
        };
        log_call(orch, &page, actor, verb, &targets, outcome, &detail).await;
    }
    Ok(json!({ "result": raw, "reminders": [], "scopes": scopes }))
}

/// Record something a page's frame or its host observed: a diagnostic, a
/// stopped navigation, a refused write, a subscription event. The page version
/// must exist.
pub async fn page_log_event(
    orch: &Orchestrator,
    event: &PageLogEvent,
    actor: &Value,
) -> Result<(), PageCallError> {
    let page = resolve(orch, &event.project_id, &event.page, event.version).await?;
    let uri = page.address.uri();
    record(
        &orch.db.local,
        &PageCall {
            project_id: &page.project_id,
            page_artifact_id: &page.row.id,
            page_uri: &uri,
            page_version: page.row.version,
            actor,
            verb: event.verb,
            targets: &event.targets,
            outcome: event.outcome,
            detail: &event.detail,
        },
    )
    .await
    .map_err(PageCallError::Failed)
}
