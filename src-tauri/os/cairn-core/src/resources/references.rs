//! The one resolver from a cited URI to its reference entry.
//!
//! [`cairn_common::references`] defines what an entry is and how a block reads;
//! this module decides which entry a URI gets, from bounded, targeted reads of
//! the store. Every reader of a references block comes through here: the agent
//! delivery paths append [`references_block`], and the transcript's invoke
//! command returns [`resolve_references`] as data, so a message reads the same
//! to the agent that received it and to the operator watching.
//!
//! Resolution is reference-first and fail-quiet. Each URI costs a few indexed
//! statements, never a full resource read, because a block is built while a
//! resume holds its launch lock. A URI that does not parse, names a kind with no
//! entry, or does not resolve contributes nothing rather than an error, and the
//! whole pass is bounded by [`RESOLVE_BUDGET`]: a store too slow to answer costs
//! the message its block, never its delivery.

use std::time::Duration;

use cairn_common::references::{
    file_reference, quotation_excerpt, reference_uris, render_references_block,
    without_references_block, ReferenceEntry, References, MAX_REFERENCE_ENTRIES,
};
use cairn_db::turso::params;
use cairn_uri::{canonical_project, parse_uri, CairnResource, LabelContext};
use futures_util::{stream, StreamExt};

use super::common::{
    find_node_job, find_task_job, get_named_artifact_for_job, read_resource, resolve_issue_id,
};
use super::transcript::EventRow;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// The most URIs one text resolves. A text naming more still says how many it
/// named; only the first this many are looked up.
const MAX_CANDIDATES: usize = 24;

/// How many URIs resolve at once.
const RESOLVE_CONCURRENCY: usize = 8;

/// The ceiling on one text's whole resolution pass.
const RESOLVE_BUDGET: Duration = Duration::from_secs(3);

/// The artifact a node's generic `/artifact` alias stands for in a reference:
/// the same choice a review push makes, since that alias names a node's PR.
const ALIAS_ARTIFACT: &str = "create-pr";

/// The references block for `text`, rendered for an agent, or `None` when
/// `text` names nothing that resolves.
pub(crate) async fn references_block(orch: &Orchestrator, text: &str) -> Option<String> {
    render_references_block(&resolve_references(orch, text).await, &|at| {
        crate::clock::stamp(at)
    })
}

/// Every entry `text`'s distinct URIs resolve to, in first-appearance order,
/// capped at [`MAX_REFERENCE_ENTRIES`] with the remainder counted.
pub async fn resolve_references(orch: &Orchestrator, text: &str) -> References {
    let uris = reference_uris(text);
    if uris.is_empty() {
        return References::default();
    }
    let named = uris.len();
    let considered = named.min(MAX_CANDIDATES);
    // Owned URIs, so each resolution future borrows nothing but the
    // orchestrator and stays `Send` for the resolver's callers.
    let pass = stream::iter(uris.into_iter().take(considered).enumerate())
        .map(|(index, uri)| async move { (index, resolve_reference(orch, &uri).await) })
        .buffer_unordered(RESOLVE_CONCURRENCY)
        .collect::<Vec<_>>();
    let mut resolved = match tokio::time::timeout(RESOLVE_BUDGET, pass).await {
        Ok(resolved) => resolved,
        Err(_) => {
            log::warn!("references: resolving {considered} URIs exceeded {RESOLVE_BUDGET:?}");
            return References::default();
        }
    };
    resolved.sort_by_key(|(index, _)| *index);
    let mut entries: Vec<ReferenceEntry> = resolved
        .into_iter()
        .filter_map(|(_, entry)| entry)
        .collect();
    let more = entries.len().saturating_sub(MAX_REFERENCE_ENTRIES) + (named - considered);
    entries.truncate(MAX_REFERENCE_ENTRIES);
    References { entries, more }
}

/// The entry one URI resolves to, or `None` when it names nothing a reader
/// could be shown.
pub async fn resolve_reference(orch: &Orchestrator, uri: &str) -> Option<ReferenceEntry> {
    if uri.starts_with("file:") {
        return file_reference(uri);
    }
    let resource = cairn_uri::delegate_thread_descendant(parse_uri(uri)?).ok()?;
    let cite = Cite {
        uri: uri.to_string(),
        resource: resource.kind().slug(),
        label: resource.label(&LabelContext::default()),
    };
    match resource {
        CairnResource::Issue { project, number } => {
            issue_preview(orch, &cite, &project, number).await
        }
        CairnResource::Thread {
            project,
            name,
            path,
        } if path.is_empty() => thread_preview(orch, &cite, &project, &name).await,
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            let node = NodeRef::new(&project, number, exec_seq, &node_id, None);
            node_preview(orch, &cite, &node).await
        }
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            let node = NodeRef::new(&project, number, exec_seq, &node_id, Some(&task_name));
            node_preview(orch, &cite, &node).await
        }
        CairnResource::NodeArtifact {
            project,
            number,
            exec_seq,
            node_id,
            name,
        } => {
            let node = NodeRef::new(&project, number, exec_seq, &node_id, None);
            artifact_preview(orch, &cite, &node, name.as_deref()).await
        }
        CairnResource::TaskArtifact {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            name,
        } => {
            let node = NodeRef::new(&project, number, exec_seq, &node_id, Some(&task_name));
            artifact_preview(orch, &cite, &node, name.as_deref()).await
        }
        CairnResource::NodeChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            turn_seq,
        } => {
            let node = NodeRef::new(&project, number, exec_seq, &node_id, None);
            turn_quotation(orch, &cite, &node, turn_seq).await
        }
        CairnResource::TaskChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            turn_seq,
        } => {
            let node = NodeRef::new(&project, number, exec_seq, &node_id, Some(&task_name));
            turn_quotation(orch, &cite, &node, turn_seq).await
        }
        CairnResource::IssueComment {
            project,
            number,
            comment_seq,
        } => issue_comment_quotation(orch, &cite, &project, number, comment_seq).await,
        CairnResource::Post { id } => post_preview(orch, &cite, id).await,
        CairnResource::PostComment {
            post_id,
            comment_id,
        } => post_comment_quotation(orch, &cite, post_id, comment_id).await,
        CairnResource::ProjectTenet { project, key } => {
            tenet_preview(orch, &cite, &project, &key).await
        }
        CairnResource::ProjectCheckObservation { project, handle } => {
            check_preview(orch, &cite, &project, &handle).await
        }
        CairnResource::Executor { name } => executor_preview(orch, &cite, &name),
        _ => None,
    }
}

/// Note a URI that did not resolve. It contributes no entry, which is the
/// contract, but the reason belongs in the log rather than nowhere.
fn unresolved(uri: &str, error: &str) {
    log::debug!("references: {uri} did not resolve: {error}");
    #[cfg(test)]
    eprintln!("references: {uri} did not resolve: {error}");
}

/// The URI being resolved and its resource kind's slug, which every entry
/// carries so each reader names the kind from the one registry: the transcript
/// through its URI chip, an agent through the kind's contract name.
///
/// `label` is the address's condensed label, the title of an entry whose
/// resource has no title of its own (a thread, an executor) or lacks one (an
/// untitled post, a node of an untitled issue).
struct Cite {
    uri: String,
    resource: String,
    label: String,
}

/// A node or task coordinate, as every node-family arm needs it. Owned, so a
/// read's statements can carry it without borrowing past the connection.
#[derive(Clone)]
struct NodeRef {
    project: String,
    number: i32,
    exec_seq: i32,
    node_id: String,
    task: Option<String>,
}

impl NodeRef {
    fn new(project: &str, number: i32, exec_seq: i32, node_id: &str, task: Option<&str>) -> Self {
        Self {
            project: project.to_string(),
            number,
            exec_seq,
            node_id: node_id.to_string(),
            task: task.map(str::to_string),
        }
    }

    /// The home this coordinate addresses: a thread's (number 0) or a node's,
    /// with the task segment when there is one.
    fn home(&self) -> String {
        let base = if self.number == 0 {
            format!("cairn://p/{}/{}", self.project, self.node_id)
        } else {
            cairn_uri::build_node_uri(&self.project, self.number, self.exec_seq, &self.node_id)
        };
        match &self.task {
            Some(task) => format!("{base}/task/{task}"),
            None => base,
        }
    }

    /// The job this coordinate names, resolved inside a read.
    async fn job_id(&self, conn: &crate::storage::TrackedConnection) -> Result<String, String> {
        Ok(match &self.task {
            Some(task) => {
                find_task_job(
                    conn,
                    &self.project,
                    self.number,
                    self.exec_seq,
                    &self.node_id,
                    task,
                )
                .await?
                .1
                .id
            }
            None => {
                find_node_job(
                    conn,
                    &self.project,
                    self.number,
                    self.exec_seq,
                    &self.node_id,
                )
                .await?
                .id
            }
        })
    }
}

/// The first line of `text` that is prose rather than a markdown heading, rule,
/// or blank, collapsed to one bounded line.
fn first_prose_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("---"))
        .map(cairn_attention::delivery::one_line_excerpt)
}

async fn issue_preview(
    orch: &Orchestrator,
    cite: &Cite,
    project: &str,
    number: i32,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(project).await;
    let key = project.to_string();
    let (title, status, description, updated_at) = read_resource(&db, move |conn| {
        Box::pin(async move {
            let (_, issue_id) = resolve_issue_id(conn, &key, number).await?;
            let mut rows = conn
                .query(
                    "SELECT title, status, description, updated_at FROM issues WHERE id = ?1",
                    params![issue_id.as_str()],
                )
                .await
                .map_err(|e| e.to_string())?;
            let row = rows
                .next()
                .await
                .map_err(|e| e.to_string())?
                .ok_or("issue vanished")?;
            Ok::<_, String>((
                row.text(0).map_err(|e| e.to_string())?,
                row.text(1).map_err(|e| e.to_string())?,
                row.opt_text(2).map_err(|e| e.to_string())?,
                row.opt_i64(3).map_err(|e| e.to_string())?,
            ))
        })
    })
    .await
    .map_err(|error| unresolved(&cite.uri, &error))
    .ok()?;
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title,
        status: Some(status),
        line: description.as_deref().and_then(first_prose_line),
        at: updated_at,
    })
}

async fn thread_preview(
    orch: &Orchestrator,
    cite: &Cite,
    project: &str,
    name: &str,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(project).await;
    let key = canonical_project(project);
    let thread = name.to_string();
    let (status, jurisdiction, updated_at) = read_resource(&db, move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT t.status, t.jurisdiction, t.updated_at
                     FROM threads t JOIN projects p ON p.id = t.project_id
                     WHERE p.key = ?1 AND t.name = ?2 LIMIT 1",
                    params![key.as_str(), thread.as_str()],
                )
                .await
                .map_err(|e| e.to_string())?;
            let row = rows
                .next()
                .await
                .map_err(|e| e.to_string())?
                .ok_or("no such thread")?;
            Ok::<_, String>((
                row.text(0).map_err(|e| e.to_string())?,
                row.opt_text(1).map_err(|e| e.to_string())?,
                row.opt_i64(2).map_err(|e| e.to_string())?,
            ))
        })
    })
    .await
    .map_err(|error| unresolved(&cite.uri, &error))
    .ok()?;
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title: cite.label.clone(),
        status: Some(status),
        line: jurisdiction.as_deref().and_then(first_prose_line),
        at: updated_at,
    })
}

async fn node_preview(orch: &Orchestrator, cite: &Cite, node: &NodeRef) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(&node.project).await;
    let key = node.clone();
    let (status, updated_at, issue_title, failure) = read_resource(&db, move |conn| {
        Box::pin(async move {
            let node = key;
            let job_id = node.job_id(conn).await?;
            let mut rows = conn
                .query(
                    "SELECT j.status, j.updated_at, i.title
                     FROM jobs j LEFT JOIN issues i ON i.id = j.issue_id
                     WHERE j.id = ?1",
                    params![job_id.as_str()],
                )
                .await
                .map_err(|e| e.to_string())?;
            let row = rows
                .next()
                .await
                .map_err(|e| e.to_string())?
                .ok_or("job vanished")?;
            let status = row.text(0).map_err(|e| e.to_string())?;
            let updated_at = row.opt_i64(1).map_err(|e| e.to_string())?;
            let issue_title = row.opt_text(2).map_err(|e| e.to_string())?;
            drop(rows);
            let failure = if status == "failed" {
                cairn_attention::delivery::job_failure_reason_conn(conn, &job_id)
                    .await
                    .map_err(|e| e.to_string())?
            } else {
                None
            };
            Ok::<_, String>((status, updated_at, issue_title, failure))
        })
    })
    .await
    .map_err(|error| unresolved(&cite.uri, &error))
    .ok()?;
    // The URI already says which node this is; the title says what it is for.
    let title = issue_title
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| cite.label.clone());
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title,
        status: Some(status),
        line: failure,
        at: updated_at,
    })
}

/// A node's work product named by its artifact's own `title` and `summary`;
/// the body is never inlined. `None` for an artifact that does not resolve,
/// does not parse, or has no title.
async fn artifact_preview(
    orch: &Orchestrator,
    cite: &Cite,
    node: &NodeRef,
    name: Option<&str>,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(&node.project).await;
    let name = name.unwrap_or(ALIAS_ARTIFACT).to_string();
    let artifact_name = name.clone();
    let key = node.clone();
    let data = read_resource(&db, move |conn| {
        Box::pin(async move {
            let node = key;
            let job_id = node.job_id(conn).await?;
            Ok::<_, String>(
                get_named_artifact_for_job(conn, &job_id, &artifact_name)
                    .await
                    .map(|artifact| artifact.data),
            )
        })
    })
    .await
    .map_err(|error| unresolved(&cite.uri, &error))
    .ok()
    .flatten()?;
    let value: serde_json::Value = serde_json::from_str(&data).ok()?;
    let field = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(cairn_attention::delivery::one_line_excerpt)
    };
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title: field("title")?,
        status: None,
        line: field("summary"),
        at: None,
    })
}

/// A chat turn quoted by the words that opened it.
///
/// The opening `user` event is quoted when there is one, attributed to whoever
/// actually wrote it: a direct message delivered as the turn's input names its
/// sender and an attention update names Cairn, so only a message typed into the
/// chat reads as the operator's. A turn with no `user` event (a launch, or an
/// automatic resume) quotes the agent's first reply instead, attributed to the
/// home that wrote it, and never presents Cairn's own text as anyone's.
async fn turn_quotation(
    orch: &Orchestrator,
    cite: &Cite,
    node: &NodeRef,
    turn_seq: i32,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(&node.project).await;
    let rows = match &node.task {
        Some(task) => {
            super::node::load_task_chat_turn(
                &db,
                &node.project,
                node.number,
                node.exec_seq,
                &node.node_id,
                task,
                turn_seq,
            )
            .await
        }
        None => {
            super::node::load_node_chat_turn(
                &db,
                &node.project,
                node.number,
                node.exec_seq,
                &node.node_id,
                turn_seq,
            )
            .await
        }
    }
    .map_err(|error| unresolved(&cite.uri, &error))
    .ok()?;
    let (author, at, text) = turn_opening(&rows, &node.home())?;
    let (text, elided) = quotation_excerpt(&text);
    Some(ReferenceEntry::Quotation {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        author,
        at: Some(at),
        text,
        elided,
    })
}

/// Who opened a turn, when, and with what words.
fn turn_opening(rows: &[EventRow], home: &str) -> Option<(String, i64, String)> {
    let content = |row: &EventRow| {
        serde_json::from_str::<serde_json::Value>(&row.data)
            .ok()?
            .get("content")?
            .as_str()
            .map(str::to_string)
    };
    for row in rows.iter().filter(|row| row.event_type == "user") {
        let Some(raw) = content(row) else { continue };
        let body = strip_turn_machinery(&raw);
        if body.is_empty() {
            continue;
        }
        let (author, words) = attribute_user_input(&body);
        let words = without_references_block(&words).trim().to_string();
        if !words.is_empty() {
            return Some((author, row.created_at, words));
        }
    }
    rows.iter()
        .filter(|row| row.event_type == "assistant")
        .find_map(|row| {
            let text = content(row)?;
            let text = without_references_block(text.trim()).trim().to_string();
            (!text.is_empty()).then(|| (home.to_string(), row.created_at, text))
        })
}

/// A turn's input without the machinery Cairn leads it with: the clock stamp
/// (`[Fri 09:12 PDT — resumed]`, or the first turn's dated prefix) and any
/// `<system-reminder>` block. A leading `[Direct message from …]` is content,
/// not a stamp, and is kept for [`attribute_user_input`].
fn strip_turn_machinery(content: &str) -> String {
    let mut body = content.to_string();
    while let (Some(start), Some(end)) = (
        body.find("<system-reminder>"),
        body.find("</system-reminder>"),
    ) {
        if end < start {
            break;
        }
        body.replace_range(start..end + "</system-reminder>".len(), "");
    }
    let mut rest = body.trim();
    while rest.starts_with('[') && !rest.starts_with("[Direct message from ") {
        match rest.find(']') {
            Some(end) => rest = rest[end + 1..].trim_start(),
            None => break,
        }
    }
    rest.trim().to_string()
}

/// Who wrote a turn's `user` input and the words they wrote, with a direct
/// message's reply hint removed.
fn attribute_user_input(body: &str) -> (String, String) {
    if let Some(rest) = body.strip_prefix("[Direct message from ") {
        if let Some((sender, words)) = rest.split_once(']') {
            let words = words
                .split("\nTo reply, use the message tool")
                .next()
                .unwrap_or_default();
            return (sender.to_string(), words.trim().to_string());
        }
    }
    if body.starts_with("Attention update (") {
        return ("Cairn (an attention update)".into(), body.to_string());
    }
    ("the operator".into(), body.to_string())
}

async fn issue_comment_quotation(
    orch: &Orchestrator,
    cite: &Cite,
    project: &str,
    number: i32,
    comment_seq: i32,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(project).await;
    let key = project.to_string();
    let (content, source, created_at) = read_resource(&db, move |conn| {
        Box::pin(async move {
            let (_, issue_id) = resolve_issue_id(conn, &key, number).await?;
            let mut rows = conn
                .query(
                    "SELECT content, source, created_at FROM comments WHERE issue_id = ?1 AND seq = ?2",
                    params![issue_id.as_str(), comment_seq as i64],
                )
                .await
                .map_err(|e| e.to_string())?;
            let row = rows
                .next()
                .await
                .map_err(|e| e.to_string())?
                .ok_or("no such comment")?;
            Ok::<_, String>((
                row.text(0).map_err(|e| e.to_string())?,
                row.text(1).map_err(|e| e.to_string())?,
                row.i64(2).map_err(|e| e.to_string())?,
            ))
        })
    })
    .await
    .map_err(|error| unresolved(&cite.uri, &error))
    .ok()?;
    let (text, elided) = quotation_excerpt(without_references_block(&content));
    Some(ReferenceEntry::Quotation {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        author: if source == "agent" {
            "an agent"
        } else {
            "the operator"
        }
        .into(),
        at: Some(created_at),
        text,
        elided,
    })
}

async fn post_preview(orch: &Orchestrator, cite: &Cite, id: i64) -> Option<ReferenceEntry> {
    let post = orch.db.local.get_post(id).await.ok()??;
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title: post
            .title
            .clone()
            .filter(|title| !title.trim().is_empty())
            .unwrap_or_else(|| cite.label.clone()),
        status: None,
        line: first_prose_line(&post.content),
        at: Some(post.created_at),
    })
}

async fn post_comment_quotation(
    orch: &Orchestrator,
    cite: &Cite,
    post_id: i64,
    comment_id: i64,
) -> Option<ReferenceEntry> {
    let db = &orch.db.local;
    let post = db.get_post(post_id).await.ok()??;
    let comment = super::posts::find_post_comment(db, post_id, comment_id)
        .await
        .ok()??;
    let context = super::posts::PostContext::resolve(db, None, std::slice::from_ref(&post)).await;
    let (text, elided) = quotation_excerpt(without_references_block(&comment.content));
    Some(ReferenceEntry::Quotation {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        author: context.author(&comment.author, &comment.appearance),
        at: Some(comment.created_at),
        text,
        elided,
    })
}

async fn tenet_preview(
    orch: &Orchestrator,
    cite: &Cite,
    project: &str,
    key: &str,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project)
        .await
        .ok()?;
    let number = crate::tenets::resolve_number(&db, &project_id, key)
        .await
        .ok()??;
    let tenet = crate::tenets::get(&db, &project_id, number).await.ok()??;
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title: tenet.row.statement.clone(),
        status: Some(tenet.row.status.as_str().to_string()),
        line: first_prose_line(&tenet.row.statement),
        at: Some(tenet.row.updated_at),
    })
}

async fn check_preview(
    orch: &Orchestrator,
    cite: &Cite,
    project: &str,
    handle: &str,
) -> Option<ReferenceEntry> {
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project)
        .await
        .ok()?;
    let observation =
        crate::execution::cache::get_check_result_observation_by_handle(db, &project_id, handle)
            .ok()??;
    let commit = observation
        .source_commit_sha
        .get(..12)
        .unwrap_or(&observation.source_commit_sha);
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title: observation.check_name.clone(),
        status: Some(observation.verdict.clone()),
        line: Some(format!("at commit {commit}")),
        at: Some(epoch_seconds_from_millis(observation.evaluated_at)),
    })
}

/// A preview's `at` is epoch seconds, as every other arm's source column is.
/// Check observations are the exception at the store: their `ran_at`, which an
/// alias copies as `evaluated_at`, is epoch milliseconds.
fn epoch_seconds_from_millis(millis: i64) -> i64 {
    millis.div_euclid(1_000)
}

fn executor_preview(orch: &Orchestrator, cite: &Cite, name: &str) -> Option<ReferenceEntry> {
    let executors = orch.fleet.inspect_executors(crate::fleet::unix_time_ms());
    let executor = executors.iter().find(|executor| executor.name == name)?;
    let capabilities = &executor.health.advertisement.capabilities;
    Some(ReferenceEntry::Preview {
        uri: cite.uri.clone(),
        resource: cite.resource.clone(),
        title: cite.label.clone(),
        status: Some("attached".into()),
        line: Some(format!(
            "{} / {}, {}",
            capabilities.os,
            capabilities.arch,
            if executor.colocated {
                "colocated"
            } else {
                "enrolled"
            }
        )),
        at: None,
    })
}

/// An orchestrator over one migrated test database, for tests of readers that
/// append a references block.
#[cfg(test)]
pub(crate) fn test_orchestrator(db: crate::storage::LocalDb) -> Orchestrator {
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap().keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    OrchestratorBuilder::new(db_state, services, config_dir).build()
}

#[cfg(test)]
#[path = "references_tests.rs"]
mod tests;
