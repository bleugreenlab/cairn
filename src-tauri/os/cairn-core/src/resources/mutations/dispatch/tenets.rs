//! Tenet mutations: a thread proposes, withdraws, and dismisses evidence that
//! misreads the work; nothing here answers.
//!
//! A resource write carries no verified operator credential, and a request
//! with no run identity is indistinguishable from an agent shell that dropped
//! its own, so the verdict form is refused to every caller of this surface.
//! The operator answers through the desktop's authenticated invoke path, which
//! calls [`crate::tenets::answer`] with the principal it verified.

use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::tenets::{Proposal, Proposer, TenetOrigin, TenetStatus};
use cairn_common::uri::{build_project_tenets_uri, CairnResource};

/// Payload keys a tenet patch may carry. Anything else is an attempt to edit
/// the tenet, which is refused as such rather than as an unknown key.
const PATCH_KEYS: &[&str] = &["withdraw", "dismiss", "verdict", "comment"];

/// The proposing run: its authenticated principal and its thread lineage, in
/// the database that owns `project`.
async fn proposer(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<(Proposer, std::sync::Arc<crate::storage::LocalDb>, String)> {
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            build_failure(
                index,
                item,
                format!("proposing or withdrawing a tenet, or dismissing its evidence, is a thread's act and needs its authenticated run: {error}"),
            )
        })?;
    let (principal, _) = agent
        .appearance(orch.services.clock.now())
        .map_err(|error| build_failure(index, item, error))?;
    let db = orch.db.for_project(project).await;
    let proposer = Proposer::for_agent(&db, &agent, principal)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    if proposer.project_id != project_id {
        return Err(build_failure(
            index,
            item,
            "a thread proposes, withdraws, and dismisses evidence on tenets in its own project only",
        ));
    }
    Ok((proposer, db, project_id))
}

fn proposal(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Proposal> {
    let statement = payload
        .get("statement")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                "payload.statement must be a string: one sentence of how the project does things, for the operator to answer yes or no",
            )
        })?;
    let rationale = match payload.get("rationale") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.rationale must be a string: why, the alternatives ruled out, and the canonical URIs where it was argued",
                    )
                })?
                .to_string(),
        ),
    };
    let supersedes = match payload.get("supersedes") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|number| *number > 0).ok_or_else(|| {
            build_failure(index, item, "payload.supersedes must be a tenet number")
        })?),
    };
    Ok(Proposal {
        statement: statement.to_string(),
        rationale,
        supersedes,
        origin: TenetOrigin::Standing,
    })
}

/// `payload.dismiss` as a dismissal. Whether its reason says anything is the
/// domain's refusal, so every caller hears the same one.
fn dismissal(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
) -> ResourceMutationResult<crate::tenets::evidence::Dismissal> {
    const SHAPE: &str = "payload.dismiss must be {source, reason}: the evidence row's source URI as the tenet's Evidence section lists it, and why the tenet stands";
    let map = value
        .as_object()
        .ok_or_else(|| build_failure(index, item, SHAPE))?;
    if map.keys().any(|key| key != "source" && key != "reason") {
        return Err(build_failure(index, item, SHAPE));
    }
    let text = |key: &str| {
        map.get(key)
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .ok_or_else(|| build_failure(index, item, SHAPE))
    };
    Ok(crate::tenets::evidence::Dismissal {
        source_uri: text("source")?,
        reason: text("reason")?,
    })
}

async fn tenet_number(
    db: &crate::storage::LocalDb,
    project_id: &str,
    key: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<i64> {
    crate::tenets::resolve_number(db, project_id, key)
        .await
        .map_err(|error| build_failure(index, item, error))?
        .ok_or_else(|| build_failure(index, item, format!("no tenet `{key}` in this project")))
}

/// The proposing thread's `dismiss` of one evidence row.
#[allow(clippy::too_many_arguments)]
async fn dismiss(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    project: &str,
    key: &str,
    value: &serde_json::Value,
) -> ResourceMutationResult<String> {
    let dismissal = dismissal(index, item, value)?;
    if dry_run {
        return Ok(format!(
            "Would dismiss the evidence from {} on tenet {}/{key}",
            dismissal.source_uri.trim(),
            build_project_tenets_uri(project)
        ));
    }
    let (proposer, db, project_id) = proposer(orch, request, project, index, item).await?;
    let number = tenet_number(&db, &project_id, key, index, item).await?;
    let dismissed = crate::tenets::evidence::dismiss(
        &db,
        orch.services.emitter.as_ref(),
        orch.services.clock.now(),
        &project_id,
        number,
        &proposer.thread_id,
        dismissal,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;
    let effect = match (dismissed.sign.as_str(), dismissed.already_counted) {
        ("contradicts", false) => "it no longer counts toward a contradiction wake",
        ("contradicts", true) => {
            "the wake that already counted it stands; it counts toward no later one"
        }
        _ => "it carried no wake weight; the dismissal records that the judge misread",
    };
    Ok(format!(
        "Dismissed the {} evidence from {} on tenet {}: it stays on the record with your reason, and {effect}.",
        dismissed.sign,
        dismissed.source_uri,
        crate::tenets::tenet_uri(project, dismissed.number)
    ))
}

fn verdict_refusal(project: &str, key: &str) -> String {
    format!(
        "{} (tenet {}/{key})",
        crate::tenets::AGENT_VERDICT_REFUSAL,
        build_project_tenets_uri(project)
    )
}

/// The refusal for a proposal in the retired `{question, options}` shape.
/// Runs before the contract gate so the caller hears that a tenet is now a
/// statement, not only that `question` and `options` are unknown keys.
pub(super) fn retired_shape_refusal(resource: &CairnResource, item: &ChangeItem) -> Option<String> {
    if !matches!(resource, CairnResource::ProjectTenets { .. }) || item.mode != ChangeMode::Append {
        return None;
    }
    let payload = item.payload.as_ref()?.as_object()?;
    (payload.contains_key("question") || payload.contains_key("options"))
        .then(|| crate::tenets::retired_proposal_shape_refusal("payload."))
}

/// Why a write to tenet `key` is an edit rather than a withdrawal or an
/// answer, and what to do instead. Runs before the contract gate so a caller
/// trying to change a tenet hears that tenets are immutable, not that it used
/// an unknown key.
pub(super) async fn edit_refusal(
    orch: &Orchestrator,
    resource: &CairnResource,
    item: &ChangeItem,
) -> Option<String> {
    let CairnResource::ProjectTenet { project, key } = resource else {
        return None;
    };
    let edits = match item.mode {
        ChangeMode::Patch => item
            .payload
            .as_ref()
            .and_then(|payload| payload.as_object())
            .is_some_and(|map| map.keys().any(|key| !PATCH_KEYS.contains(&key.as_str()))),
        ChangeMode::Append => false,
        _ => true,
    };
    if !edits {
        return None;
    }
    let db = orch.db.for_project(project).await;
    let answered = match crate::mcp::handlers::run_context::project_id_by_key(&db, project).await {
        Ok(project_id) => match crate::tenets::resolve_number(&db, &project_id, key).await {
            Ok(Some(number)) => crate::tenets::get(&db, &project_id, number)
                .await
                .ok()
                .flatten()
                .filter(|view| view.row.status.has_verdict())
                .map(|view| view.row.number),
            _ => None,
        },
        Err(_) => None,
    };
    Some(match answered {
        Some(number) => crate::tenets::successor_refusal(number),
        None => crate::tenets::IMMUTABLE_REFUSAL.to_string(),
    })
}

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
        (CairnResource::ProjectTenets { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let proposal = proposal(index, item, payload)?;
            if dry_run {
                format!("Would propose tenet: {}", proposal.statement.trim())
            } else {
                let (proposer, db, _) = proposer(orch, request, project, index, item).await?;
                let tenet = crate::tenets::propose(
                    &db,
                    orch.services.emitter.as_ref(),
                    orch.services.clock.now(),
                    &proposer,
                    proposal,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let uri = crate::tenets::tenet_uri(project, tenet.number);
                *applied_data = Some(serde_json::json!({ "tenet": uri }));
                format!(
                    "Proposed tenet {uri}: it awaits the operator's verdict. A pending proposal blocks only what it gates."
                )
            }
        }
        (CairnResource::ProjectTenet { project, key }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if payload.get("verdict").is_some() {
                return Err(build_failure(index, item, verdict_refusal(project, key)));
            }
            if let Some(value) = payload.get("dismiss") {
                if payload.get("withdraw").is_some() {
                    return Err(build_failure(
                        index,
                        item,
                        "withdraw and dismiss are separate acts; send each in its own patch",
                    ));
                }
                let summary =
                    dismiss(orch, request, index, item, dry_run, project, key, value).await?;
                return Ok(Some(summary));
            }
            if payload.get("withdraw").and_then(|value| value.as_bool()) != Some(true) {
                return Err(build_failure(
                    index,
                    item,
                    "payload.withdraw must be true, or payload.dismiss {source, reason} names an evidence row that misreads the work; a tenet is otherwise immutable",
                ));
            }
            if dry_run {
                format!(
                    "Would withdraw tenet {}/{key}",
                    build_project_tenets_uri(project)
                )
            } else {
                let (proposer, db, project_id) =
                    proposer(orch, request, project, index, item).await?;
                let number = tenet_number(&db, &project_id, key, index, item).await?;
                let tenet = crate::tenets::withdraw(
                    &db,
                    orch.services.emitter.as_ref(),
                    orch.services.clock.now(),
                    &project_id,
                    number,
                    &proposer.thread_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                debug_assert_eq!(tenet.status, TenetStatus::Withdrawn);
                format!(
                    "Withdrew tenet {}. To ask again, propose a new tenet.",
                    crate::tenets::tenet_uri(project, tenet.number)
                )
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
