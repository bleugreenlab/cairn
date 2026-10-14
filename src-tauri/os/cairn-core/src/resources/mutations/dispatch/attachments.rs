//! The two actions on one durable attachment: materialize, and retire.
//!
//! Reading an attachment gives an agent its bytes, which is what a read is for.
//! Some tools want a filesystem path instead, and the honest way to give them one
//! is to copy the immutable object into whichever cell the calling run actually
//! holds — local or remote, the run's own — and hand back that cell-local path,
//! stated as ephemeral. Materialization follows placement. It never asks a run to
//! pin itself to the machine the upload happened on, which is the whole defect
//! this family exists to close.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{AttachmentRef, CairnResource};

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let CairnResource::ProjectAttachment { project, reference } = resource else {
        return Ok(None);
    };
    let summary = match item.mode {
        ChangeMode::Patch => {
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
        ChangeMode::Delete => retire(orch, index, item, dry_run, project, reference).await?,
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

async fn retire(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    project: &str,
    reference: &AttachmentRef,
) -> ResourceMutationResult<String> {
    let AttachmentRef::Scoped { scope, ordinal } = reference else {
        return Err(build_failure(
            index,
            item,
            "a content-hash permalink has no reference row to retire",
        ));
    };
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let uri = cairn_common::uri::build_project_attachment_uri(project, reference);
    if dry_run {
        crate::attachments::resolve_attachment(&db, &project_id, project, scope, *ordinal)
            .await
            .map_err(|error| build_failure(index, item, error.to_string()))?;
        return Ok(format!("Would retire {uri}"));
    }
    crate::attachments::retire_attachment(&db, &project_id, project, scope, *ordinal)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?;
    Ok(format!(
        "Retired {uri}. The address no longer resolves. The object itself is untouched: \
         reclaiming stored bytes needs a reference inventory spanning every database and \
         broker that shares them, which is cairn/4648's work and not this one's."
    ))
}
