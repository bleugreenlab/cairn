//! Tenet mutations: a thread proposes and withdraws; nothing here answers.
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
const PATCH_KEYS: &[&str] = &["withdraw", "verdict", "comment"];

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
                format!("proposing or withdrawing a tenet is a thread's act and needs its authenticated run: {error}"),
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
            "a thread proposes and withdraws tenets in its own project only",
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
            if payload.get("withdraw").and_then(|value| value.as_bool()) != Some(true) {
                return Err(build_failure(
                    index,
                    item,
                    "payload.withdraw must be true; a tenet is otherwise immutable",
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
                let number = crate::tenets::resolve_number(&db, &project_id, key)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
                    .ok_or_else(|| {
                        build_failure(index, item, format!("no tenet `{key}` in this project"))
                    })?;
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
