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

use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use cairn_common::protocol::{RowWriteInstrument, RowWriteOrigin};
use cairn_uri::canonical_project;
use serde_json::{json, Value};

use super::data::{write_scope_table, write_scopes};
use super::log::{PageCallOutcome, PageCallVerb};
use super::read::{log_call, resolve, PageCallError, ResolvedPage};
use super::store::{load_page, PageAddress, PageOwner};
use crate::orchestrator::Orchestrator;
use crate::row_writes::{self, GatedChange, InputError};

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

/// The verified operator a row write is made as: through a page, or through
/// the app's database browser.
#[derive(Debug, Clone)]
pub struct PageOperator {
    /// The principal the verified invoke appearance names.
    pub actor: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    /// The viewer record the call log stores, as reads record it.
    pub log_actor: Value,
}

const PAGE_NOUN: &str = "a page";

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
        let change = match row_writes::read_change(change, PAGE_NOUN) {
            Ok(change) => change,
            Err(refusal) => {
                refusals.push(refusal);
                continue;
            }
        };
        let target = change.target.as_str();
        if target.starts_with("file:") {
            refusals.push(format!(
                "{target}: file: targets are refused; a page writes rows of its own thread's tables, never a checkout"
            ));
            continue;
        }
        let resolved = if crate::resources::mutations::is_home_relative_target(target) {
            match crate::resources::resolve_home_relative_against(&home, target) {
                Some(resolved) => resolved,
                None => {
                    refusals.push(format!(
                        "{target}: could not resolve against the page's owner {home}"
                    ));
                    continue;
                }
            }
        } else if target.starts_with("cairn://") {
            target.to_string()
        } else {
            refusals.push(manifest_refusal(target));
            continue;
        };
        let row_target = match row_writes::row_target(&resolved, target) {
            Ok(Some(row_target)) => row_target,
            Ok(None) => {
                refusals.push(manifest_refusal(target));
                continue;
            }
            Err(refusal) => {
                refusals.push(refusal);
                continue;
            }
        };
        if canonical_project(&row_target.project) != owner_project
            || row_target.thread != thread
            || !declared.contains(&row_target.table.as_str())
        {
            refusals.push(manifest_refusal(target));
            continue;
        }
        let Some(mutation) = row_target.mutation(&change.mode) else {
            refusals.push(format!(
                "{target}: mode `{}` is refused; a page appends to cairn:~/db/<table>, and patches or deletes cairn:~/db/<table>/<pk> or cairn:~/db/<table>?where=...",
                change.mode
            ));
            continue;
        };
        gated.push(GatedChange::new(&change, resolved, &row_target, mutation));
    }
    if refusals.is_empty() {
        Ok(gated)
    } else {
        Err(refusals)
    }
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
    let (changes, atomic) = match row_writes::split_input(&request.input, "page_write", PAGE_NOUN) {
        Ok(split) => split,
        Err(InputError::Refused(detail)) => {
            return Err(refuse(orch, &page, operator, &requested, detail).await);
        }
        Err(InputError::BadRequest(detail)) => return Err(PageCallError::BadRequest(detail)),
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
    let origin = RowWriteOrigin {
        instrument: RowWriteInstrument::Page {
            page: page.address.uri(),
            version,
        },
        owner_thread_id,
        owner: page.address.home_uri(),
        project_id: page.project_id.clone(),
        actor: operator.actor.clone(),
        appearance: operator.appearance.clone(),
    };
    let raw = row_writes::dispatch(orch, &origin, &gated, atomic).await;
    let canonical: Vec<String> = gated
        .iter()
        .filter_map(GatedChange::target)
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
    if row_writes::is_blocking(&report) {
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

    let written = row_writes::written_uris(&report);
    let failures = row_writes::failures(&report);
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

    row_writes::journal(orch, &origin, &report, &gated).await;
    Ok(json!({ "result": raw, "reminders": [] }))
}
