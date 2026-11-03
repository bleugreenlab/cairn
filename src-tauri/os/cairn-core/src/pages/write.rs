//! Bridged writes: what a page's frame may write, and as whom.
//!
//! # Authority
//!
//! A page write is allowed only when three terms all hold, and each refuses on
//! its own; nothing falls back to a broader authority.
//!
//! 1. **Actor.** The caller is a verified operator. The transport derives the
//!    [`PageOperator`] from verified credentials only and refuses everyone
//!    else before this module is reached.
//! 2. **Manifest.** Every target is a row write to a table the stored page
//!    version's `manifest.writes` declares: `append` to `cairn:~/db/<table>`,
//!    `patch` or `delete` on `cairn:~/db/<table>/<pk>`, or `patch` or `delete`
//!    on `cairn:~/db/<table>?where=...`. The manifest is read from the stored
//!    row, never from the frame.
//! 3. **Owner.** The page is a thread's (a draft cannot write) and the target
//!    is that thread's own database. The write path's single ownership rule
//!    (`owned_thread_scope`) takes the owner from the request's typed origin.
//!
//! The batch is gated whole before anything is dispatched: one refused item
//! writes nothing, and every refusal is reported. The operator's own
//! authority is never consulted: the dispatched request carries the page
//! origin, which the write path confines to row writes and never lets cross
//! an approval boundary.
//!
//! # Record
//!
//! A row write records no author in the row (the table's schema is its
//! agent's), so provenance lives beside it, keyed by row URI: a `page_calls`
//! row naming the actor and the page@version, and an authorization journal
//! event per row carrying the actor, its verified appearance, and the page as
//! the instrument.

use cairn_common::authorization::AuthorityMutation;
use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use cairn_common::protocol::{DispatchOrigin, PageWriteOrigin};
use cairn_common::uri::{canonical_project, parse_uri, CairnResource};
use serde_json::{json, Value};

use super::data::{write_scope_table, write_scopes};
use super::log::{PageCallOutcome, PageCallVerb};
use super::read::{log_call, resolve, PageCallError, ResolvedPage};
use super::store::{load_page, PageAddress, PageOwner};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// `page_write`'s arguments. `input` is the SDK's write input as the frame
/// sent it: `{changes, atomic?}`.
#[derive(Debug, Clone)]
pub struct PageWriteRequest {
    pub project_id: String,
    /// The canonical page URI.
    pub page: String,
    pub version: i32,
    pub input: Value,
}

/// The verified operator a page write is made as.
#[derive(Debug, Clone)]
pub struct PageOperator {
    /// The principal the verified invoke appearance names.
    pub actor: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    /// The viewer record the call log stores, as reads record it.
    pub log_actor: Value,
}

/// One gated change, canonical and ready to dispatch.
struct GatedChange {
    item: Value,
    mutation: AuthorityMutation,
    /// The canonical row collection the change writes into.
    collection: String,
}

fn page_label(address: &PageAddress, version: i32) -> String {
    format!("{}@v{version}", address.name)
}

/// Every change as the owner's canonical row write, or every refusal.
fn gate_write_targets(
    address: &PageAddress,
    version: i32,
    scopes: &[String],
    changes: &[Value],
) -> Result<Vec<GatedChange>, Vec<String>> {
    let home = address.home_uri();
    let owner_project = canonical_project(&address.project);
    let thread = address.thread_name().unwrap_or_default();
    let declared: Vec<&str> = scopes
        .iter()
        .filter_map(|scope| write_scope_table(scope))
        .collect();
    let manifest_refusal = |target: &str| {
        format!(
            "{target}: refused by {}'s manifest.writes [{}]; a page writes only rows of the tables it declares",
            page_label(address, version),
            scopes.join(", ")
        )
    };

    let mut gated = Vec::with_capacity(changes.len());
    let mut refusals = Vec::new();
    for change in changes {
        let Some(object) = change.as_object() else {
            refusals.push("each change is an object {target, mode, payload}".to_string());
            continue;
        };
        if let Some(key) = object
            .keys()
            .find(|key| !matches!(key.as_str(), "target" | "mode" | "payload"))
        {
            refusals.push(format!(
                "a page's change carries target, mode and payload only, not `{key}`"
            ));
            continue;
        }
        let target = object
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let mode = object
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if target.starts_with("file:") {
            refusals.push(format!(
                "{target}: file: targets are refused; a page writes rows of its own thread's tables, never a checkout"
            ));
            continue;
        }
        let resolved = if crate::resources::mutations::is_home_relative_target(&target) {
            match crate::resources::resolve_home_relative_against(&home, &target) {
                Some(resolved) => resolved,
                None => {
                    refusals.push(format!(
                        "{target}: could not resolve against the page's owner {home}"
                    ));
                    continue;
                }
            }
        } else if target.starts_with("cairn://") {
            target.clone()
        } else {
            refusals.push(manifest_refusal(&target));
            continue;
        };
        if resolved.contains('#') {
            refusals.push(format!("{target}: a write target carries no fragment"));
            continue;
        }
        // A query is the `where=` that scopes a collection patch or delete,
        // and nothing else; the write path parses and enforces it.
        let scoped = match resolved.split_once('?') {
            None => false,
            Some((_, query)) => {
                let only_where = cairn_common::query::parse_query_params(query)
                    .is_ok_and(|params| params.iter().all(|param| param.key == "where"));
                if !only_where {
                    refusals.push(format!(
                        "{target}: a write target's only query is where=, scoping a patch or delete of a table's rows"
                    ));
                    continue;
                }
                true
            }
        };
        let resource = parse_uri(&resolved)
            .and_then(|parsed| crate::resources::delegate_thread_descendant(parsed).ok());
        let (project, name, table, is_row) = match &resource {
            Some(CairnResource::ThreadDbTable {
                project,
                name,
                table,
            }) => (project, name, table, false),
            Some(CairnResource::ThreadDbRow {
                project,
                name,
                table,
                ..
            }) => (project, name, table, true),
            _ => {
                refusals.push(manifest_refusal(&target));
                continue;
            }
        };
        if *project != owner_project || name != thread || !declared.contains(&table.as_str()) {
            refusals.push(manifest_refusal(&target));
            continue;
        }
        let mutation = match (is_row, scoped, mode.as_str()) {
            (false, false, "append") => AuthorityMutation::Create,
            (true, false, "patch") | (false, true, "patch") => AuthorityMutation::Update,
            (true, false, "delete") | (false, true, "delete") => AuthorityMutation::Delete,
            _ => {
                refusals.push(format!(
                    "{target}: mode `{mode}` is refused; a page appends to cairn:~/db/<table>, and patches or deletes cairn:~/db/<table>/<pk> or cairn:~/db/<table>?where=..."
                ));
                continue;
            }
        };
        let mut item = object.clone();
        item.insert("target".to_string(), Value::String(resolved));
        gated.push(GatedChange {
            item: Value::Object(item),
            mutation,
            collection: cairn_common::uri::build_db_table_uri(
                &format!("{}/db", cairn_common::uri::build_thread_uri(project, name)),
                table,
            ),
        });
    }
    if refusals.is_empty() {
        Ok(gated)
    } else {
        Err(refusals)
    }
}

/// The write input's changes and atomic flag, refusing every other key.
fn split_input(input: &Value) -> Result<(Vec<Value>, Option<bool>), PageCallError> {
    let object = input.as_object().ok_or_else(|| {
        PageCallError::BadRequest(
            "a page write's input is an object {changes, atomic?}".to_string(),
        )
    })?;
    let extra: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !matches!(*key, "changes" | "atomic"))
        .collect();
    if !extra.is_empty() {
        return Err(PageCallError::Refused(format!(
            "a page write carries only changes and atomic; {} refused",
            extra
                .iter()
                .map(|key| format!("`{key}` is"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let changes = object
        .get("changes")
        .and_then(Value::as_array)
        .filter(|changes| !changes.is_empty())
        .cloned()
        .ok_or_else(|| {
            PageCallError::BadRequest("page_write requires a non-empty `changes` array".to_string())
        })?;
    let atomic = match object.get("atomic") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(atomic)) => Some(*atomic),
        Some(_) => {
            return Err(PageCallError::BadRequest(
                "`atomic` is a boolean".to_string(),
            ));
        }
    };
    Ok((changes, atomic))
}

/// The URIs a change report names as written: each applied change's row URI
/// (every row's, for a bulk append), or its target when it names none.
fn written_uris(report: &Value) -> Vec<String> {
    report
        .get("applied")
        .and_then(Value::as_array)
        .map(|applied| {
            applied
                .iter()
                .flat_map(|change| {
                    let mut uris: Vec<String> = change
                        .pointer("/data/uris")
                        .and_then(Value::as_array)
                        .map(|uris| {
                            uris.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default();
                    if uris.is_empty() {
                        uris.extend(
                            change
                                .pointer("/data/uri")
                                .or_else(|| change.get("target"))
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        );
                    }
                    uris
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn refuse(
    orch: &Orchestrator,
    page: &ResolvedPage,
    operator: &PageOperator,
    targets: &[String],
    detail: String,
) -> PageCallError {
    log_call(
        orch,
        page,
        &actor_record(page, operator),
        PageCallVerb::Write,
        targets,
        PageCallOutcome::Refused,
        &detail,
    )
    .await;
    PageCallError::Refused(detail)
}

/// The call log's actor for a write: the viewer record, with the page version
/// it acted through.
fn actor_record(page: &ResolvedPage, operator: &PageOperator) -> Value {
    let mut record = operator.log_actor.clone();
    if let Some(object) = record.as_object_mut() {
        object.insert(
            "instrument".to_string(),
            json!({
                "page": page.address.uri(),
                "version": page.row.version,
                "owner": page.address.home_uri(),
            }),
        );
    }
    record
}

/// A bridged write for a page's frame, made as `operator` through the page,
/// returning exactly the `{result, reminders}` callback response the MCP
/// `write` tool returns, so the SDK client parses its change report unchanged.
pub async fn page_write(
    orch: &Orchestrator,
    request: &PageWriteRequest,
    operator: &PageOperator,
) -> Result<Value, PageCallError> {
    let page = resolve(orch, &request.project_id, &request.page, request.version).await?;
    let requested: Vec<String> = request
        .input
        .get("changes")
        .and_then(Value::as_array)
        .map(|changes| {
            changes
                .iter()
                .filter_map(|change| change.get("target").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let (changes, atomic) = match split_input(&request.input) {
        Ok(split) => split,
        Err(PageCallError::Refused(detail)) => {
            return Err(refuse(orch, &page, operator, &requested, detail).await);
        }
        Err(error) => return Err(error),
    };

    let owner_thread_id = match &page.owner {
        PageOwner::Thread { thread_id } => thread_id.clone(),
        PageOwner::Node { .. } => {
            let detail = format!(
                "{} is a draft on an issue node, and a draft cannot write; its thread adopts it first",
                page.address.uri()
            );
            return Err(refuse(orch, &page, operator, &requested, detail).await);
        }
    };

    // A frame showing a superseded version would otherwise write under scopes
    // the owner has since removed.
    let db = orch.db.for_project(&page.address.project).await;
    let newest = load_page(&db, &page.address, None)
        .await
        .map_err(PageCallError::Failed)?
        .map_or(page.row.version, |(_, row)| row.version);
    if newest != page.row.version {
        let detail = format!(
            "page {} was updated to v{newest}; this frame is showing v{} and reloads",
            page.address.uri(),
            page.row.version
        );
        return Err(refuse(orch, &page, operator, &requested, detail).await);
    }

    let scopes = write_scopes(&page.row.data);
    let gated = match gate_write_targets(&page.address, page.row.version, &scopes, &changes) {
        Ok(gated) => gated,
        Err(refusals) => {
            return Err(refuse(orch, &page, operator, &requested, refusals.join("\n")).await);
        }
    };

    let version = u32::try_from(page.row.version)
        .map_err(|_| PageCallError::Failed("a stored page version is positive".to_string()))?;
    let origin = PageWriteOrigin {
        page: page.address.uri(),
        version,
        owner_thread_id,
        owner: page.address.home_uri(),
        project_id: page.project_id.clone(),
        actor: operator.actor.clone(),
        appearance: operator.appearance.clone(),
    };
    let mut payload = json!({
        "changes": gated.iter().map(|change| change.item.clone()).collect::<Vec<_>>(),
    });
    if let Some(atomic) = atomic {
        payload["atomic"] = Value::Bool(atomic);
    }
    let dispatched = McpCallbackRequest {
        tool: "write".to_string(),
        payload,
        origin: Some(DispatchOrigin::PageWrite(Box::new(origin.clone()))),
        ..Default::default()
    };
    let raw = crate::mcp::handlers::write::handle_write(orch, &dispatched).await;
    let canonical: Vec<String> = gated
        .iter()
        .filter_map(|change| change.item.get("target").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    let actor = actor_record(&page, operator);

    let report: Value = match serde_json::from_str(&raw) {
        Ok(report) => report,
        Err(_) => {
            let detail = format!("the write did not complete: {raw}");
            log_call(
                orch,
                &page,
                &actor,
                PageCallVerb::Write,
                &canonical,
                PageCallOutcome::Error,
                &detail,
            )
            .await;
            return Err(PageCallError::Failed(detail));
        }
    };
    // Unreachable past the gate, and closed if it is ever reached: a page has
    // no one to answer a blocking append or an approval.
    if report
        .get("blocking")
        .is_some_and(|blocking| !blocking.is_null())
    {
        let detail = "a page write cannot wait on an answer".to_string();
        log_call(
            orch,
            &page,
            &actor,
            PageCallVerb::Write,
            &canonical,
            PageCallOutcome::Refused,
            &detail,
        )
        .await;
        return Err(PageCallError::Refused(detail));
    }

    let written = written_uris(&report);
    let failures: Vec<String> = report
        .get("failures")
        .and_then(Value::as_array)
        .map(|failures| {
            failures
                .iter()
                .map(|failure| {
                    format!(
                        "{}: {}",
                        failure
                            .get("target")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                        failure
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let (outcome, detail) = if failures.is_empty() {
        (
            PageCallOutcome::Ok,
            format!("{} row(s) written", written.len()),
        )
    } else {
        (
            PageCallOutcome::Error,
            format!(
                "{} written, {} failed:\n{}",
                written.len(),
                failures.len(),
                failures.join("\n")
            ),
        )
    };
    let logged = if written.is_empty() {
        &canonical
    } else {
        &written
    };
    log_call(
        orch,
        &page,
        &actor,
        PageCallVerb::Write,
        logged,
        outcome,
        &detail,
    )
    .await;

    let journaled = journal_entries(&report, &gated);
    if !journaled.is_empty() {
        let authority = crate::authorization::page_write_actor(orch, &origin);
        crate::authorization::journal_instrumented_write(
            &authority,
            &operator.appearance,
            &journaled,
        )
        .await;
    }
    Ok(json!({ "result": raw, "reminders": [] }))
}

/// One journal entry per applied change, matched to its gated change by index.
fn journal_entries(
    report: &Value,
    gated: &[GatedChange],
) -> Vec<crate::authorization::InstrumentedWrite> {
    report
        .get("applied")
        .and_then(Value::as_array)
        .map(|applied| {
            applied
                .iter()
                .filter_map(|change| {
                    let index = usize::try_from(change.get("index")?.as_u64()?).ok()?;
                    let gated = gated.get(index)?;
                    let summary = change
                        .pointer("/data/uri")
                        .or_else(|| change.get("target"))
                        .and_then(Value::as_str)?
                        .to_string();
                    Some(crate::authorization::InstrumentedWrite {
                        mutation: gated.mutation,
                        collection: gated.collection.clone(),
                        summary,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(super) fn gate_for_test(
    address: &PageAddress,
    version: i32,
    scopes: &[String],
    changes: &[Value],
) -> Result<Vec<String>, Vec<String>> {
    gate_write_targets(address, version, scopes, changes).map(|gated| {
        gated
            .into_iter()
            .filter_map(|change| {
                change
                    .item
                    .get("target")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect()
    })
}
