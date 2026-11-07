//! The two actions on one durable attachment: materialize, and remove.
//!
//! Reading an attachment gives an agent its bytes, which is what a read is for.
//! Some tools want a filesystem path instead, and the honest way to give them one
//! is to copy the immutable object into whichever cell the calling run actually
//! holds — local or remote, the run's own — and hand back that cell-local path,
//! stated as ephemeral. Materialization follows placement. It never asks a run to
//! pin itself to the machine the upload happened on, which is the whole defect
//! this family exists to close.
//!
//! Removal deletes the stored bytes and leaves a tombstone on every address
//! (`attachments::remove_attachment`). It is the operator's to allow: an agent's
//! delete names its authority request ahead of any side effect
//! ([`attachment_removal_authority`], called from `authorization::prepare`), the
//! write handler raises the operator's card when no grant covers it, and the
//! dispatch arm re-derives the same request and authorizes it, consuming a once
//! grant, just before the removal. The tombstone cites the journal row.

use super::super::{
    build_failure, payload_non_empty_str, target_resource_for_request, ResourceMutationResult,
};
use crate::authorization::normalize;
use crate::authorization::prepare::Prepared;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_common::authorization::AuthorityDecision;
use cairn_common::uri::{AttachmentRef, AttachmentScopeRef, CairnResource};

/// One stored object, by either spelling of its address.
fn stored_object(resource: &CairnResource) -> Option<(&str, &AttachmentRef)> {
    match resource {
        CairnResource::ProjectAttachment { project, reference }
        | CairnResource::ProjectImage { project, reference } => Some((project.as_str(), reference)),
        _ => None,
    }
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let Some((project, reference)) = stored_object(resource) else {
        return Ok(None);
    };
    let summary = match item.mode {
        ChangeMode::Patch if matches!(resource, CairnResource::ProjectAttachment { .. }) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let action = payload_non_empty_str(payload, "action", &[])
                .ok_or_else(|| build_failure(index, item, "payload.action is required"))?;
            if action != "materialize" {
                return Err(build_failure(
                    index,
                    item,
                    format!("payload.action must be 'materialize', got '{action}'"),
                ));
            }
            materialize(orch, request, index, item, dry_run, project, reference).await?
        }
        ChangeMode::Delete => {
            remove(orch, request, index, item, dry_run, project, reference).await?
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

async fn materialize(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    project: &str,
    reference: &AttachmentRef,
) -> ResourceMutationResult<String> {
    let AttachmentRef::Scoped { ordinal, .. } = reference else {
        return Err(build_failure(
            index,
            item,
            "a content-hash permalink cannot be materialized; use the scoped \
             attachments/{n} address, which names the record that carries the file name",
        ));
    };
    // The run is what binds this write to a cell: materialization lands where the
    // caller executes, and a caller with no run has no cell to land in.
    let (run, db) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    if !run.project_key.eq_ignore_ascii_case(project) {
        return Err(build_failure(
            index,
            item,
            "Attachment URI does not belong to the authenticated run project",
        ));
    }
    let fetched = crate::attachments::fetch_attachment(&db, &run.project_id, project, reference)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?;

    if dry_run {
        return Ok(format!(
            "Would materialize {} ({} bytes) into this run's execution cell",
            fetched.file_name,
            fetched.bytes.len()
        ));
    }

    let fleet_config = crate::config::settings::load_fleet(&orch.config_dir);
    let wait_horizon_unix_ms = crate::fleet::default_wait_horizon_unix_ms(&fleet_config);
    let acquisition = crate::fleet::residency::job_residency_acquisition(
        orch,
        &db,
        &run.job_id,
        wait_horizon_unix_ms,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;
    // Acquisition is idempotent, so this joins the cell the run's batches,
    // REPLs, and terminals already share rather than creating a second one.
    let fence = crate::fleet::residency::acquire(orch, acquisition)
        .await
        .map_err(|refusal| build_failure(index, item, refusal.diagnostic))?;

    let size = fetched.bytes.len();
    let path = crate::fleet::residency::materialize_attachment(
        orch,
        &fence,
        cairn_common::executor_protocol::AttachmentMaterializationRequest {
            ordinal: *ordinal,
            file_name: fetched.file_name.clone(),
            contents: fetched.bytes,
        },
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;

    Ok(format!(
        "Materialized {} ({size} bytes) at {path}. That path is cell-scoped and is \
         reclaimed with the cell; the durable address is {}.",
        fetched.file_name,
        cairn_common::uri::build_project_attachment_uri(project, reference)
    ))
}

/// A delete on a stored object, as asked: the record, the reason, and whether
/// other references to the same bytes go too.
struct RemovalAsk {
    scope: AttachmentScopeRef,
    ordinal: i32,
    attachment_uri: String,
    reason: String,
    include_shared: bool,
}

fn removal_ask(
    project: &str,
    reference: &AttachmentRef,
    item: &ChangeItem,
) -> Result<RemovalAsk, String> {
    let AttachmentRef::Scoped { scope, ordinal } = reference else {
        return Err(
            "a content-hash permalink names bytes, not a record; remove the stored object \
             through its scoped images/{n} or attachments/{n} address"
                .to_string(),
        );
    };
    let payload = item.payload.as_ref();
    let reason = payload
        .and_then(|payload| payload_non_empty_str(payload, "reason", &[]))
        .map(|reason| reason.trim().to_string())
        .filter(|reason| !reason.is_empty())
        .ok_or_else(|| {
            "removing a stored object needs payload.reason: it is recorded on the tombstone \
             every address shows afterwards, and the operator reads it before approving"
                .to_string()
        })?;
    let include_shared = payload
        .and_then(|payload| payload.get("includeShared"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Ok(RemovalAsk {
        scope: scope.clone(),
        ordinal: *ordinal,
        attachment_uri: cairn_common::uri::build_project_attachment_uri(project, reference),
        reason,
        include_shared,
    })
}

async fn owning_db(
    orch: &Orchestrator,
    project: &str,
) -> Result<(std::sync::Arc<LocalDb>, String), String> {
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
    Ok((db, project_id))
}

/// The authority a delete on a stored object crosses, for
/// `authorization::prepare`: `None` unless the change is one.
///
/// Everything no approval could change is settled here first, so the operator
/// is never asked to approve a removal that would then be refused: a missing
/// reason, a permalink, an unregistered address, shared bytes not included. An
/// address already removed crosses nothing, and the dispatch arm reports it.
pub(crate) async fn attachment_removal_authority(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
) -> Option<Prepared> {
    if item.mode != ChangeMode::Delete {
        return None;
    }
    let resource = target_resource_for_request(orch, request, item)
        .await
        .ok()?;
    let (project, reference) = stored_object(&resource)?;
    let ask = match removal_ask(project, reference, item) {
        Ok(ask) => ask,
        Err(error) => return Some(Prepared::Invalid(error)),
    };
    let (db, project_id) = match owning_db(orch, project).await {
        Ok(resolved) => resolved,
        Err(error) => return Some(Prepared::Invalid(error)),
    };
    match crate::attachments::removal_preflight(
        &db,
        &orch.db.local,
        &project_id,
        project,
        &ask.scope,
        ask.ordinal,
        ask.include_shared,
    )
    .await
    {
        Ok(preflight) if preflight.row.removal.is_some() => None,
        Ok(_) => Some(Prepared::Authority(normalize::stored_content_removal(
            &ask.attachment_uri,
            &ask.reason,
            ask.include_shared,
        ))),
        Err(error) => Some(Prepared::Refused(format!("Refused: {error}"))),
    }
}

async fn remove(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    project: &str,
    reference: &AttachmentRef,
) -> ResourceMutationResult<String> {
    let ask =
        removal_ask(project, reference, item).map_err(|error| build_failure(index, item, error))?;
    let (db, project_id) = owning_db(orch, project)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let preflight = crate::attachments::removal_preflight(
        &db,
        &orch.db.local,
        &project_id,
        project,
        &ask.scope,
        ask.ordinal,
        ask.include_shared,
    )
    .await
    .map_err(|error| build_failure(index, item, error.to_string()))?;
    if preflight.row.removal.is_some() {
        return Ok(format!(
            "{} was already removed; nothing changed.",
            preflight.uri
        ));
    }
    if dry_run {
        let shared = if preflight.others.is_empty() {
            String::new()
        } else {
            format!(", with {}", preflight.others.join(", "))
        };
        return Ok(format!(
            "Would remove {}{shared} and delete its stored bytes, with the operator's approval",
            preflight.uri
        ));
    }

    let authority =
        normalize::stored_content_removal(&ask.attachment_uri, &ask.reason, ask.include_shared);
    let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
        return Err(build_failure(
            index,
            item,
            format!(
                "Denied: only an authenticated run can be authorized to {}. The operator \
                 removes stored content from the app.",
                authority.summary
            ),
        ));
    };
    let (decision, decision_id) = crate::authorization::authorize_recorded(&actor, &authority)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    match decision {
        AuthorityDecision::AllowedByGrant { .. } => {}
        // Policy classifies every stored object as a boundary, so a direct
        // decision means classification and this door disagree. Refused rather
        // than trusted: a removal is not undoable.
        AuthorityDecision::Direct => {
            return Err(build_failure(
                index,
                item,
                "Refused: stored-content removal was not classified as an operator-approval \
                 boundary, so it cannot proceed",
            ))
        }
        AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) => {
            return Err(build_failure(
                index,
                item,
                crate::authorization::refusal_message(&authority, reason),
            ))
        }
    }
    let remover = actor
        .principal
        .node_uri
        .clone()
        .or_else(|| {
            actor
                .principal
                .run_id
                .clone()
                .map(|run| format!("run {run}"))
        })
        .unwrap_or_else(|| "an agent run".to_string());
    let outcome = crate::attachments::remove_attachment(
        &db,
        &orch.db.local,
        &project_id,
        project,
        &ask.scope,
        ask.ordinal,
        &crate::attachments::RemovalRequest {
            removed_by: format!("{remover}, with the operator's approval"),
            reason: ask.reason,
            decision_id,
            include_shared: ask.include_shared,
        },
    )
    .await
    .map_err(|error| build_failure(index, item, error.to_string()))?;
    Ok(outcome.summary())
}
