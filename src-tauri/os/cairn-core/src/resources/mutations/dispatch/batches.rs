//! `patch {action:"cancel"}` on a node's or task's `batches/<request_id>`:
//! cancel that one run batch ([`crate::mcp::handlers::run::cancel`]).
//!
//! The address is the batch's own: the request id the rebase hold, the
//! executor's placement page, and the batch's handle all show. A cancel
//! addressed under the wrong node is refused naming the right one, so a
//! principal never cancels a batch it did not mean.
//!
//! The operator may cancel any batch. A run may cancel a batch of its own
//! job, or of a live job on the same branch (a node and its tasks hold that
//! branch together at the base-advance boundary). Anyone else is refused,
//! naming who can.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::Writer;
use crate::mcp::handlers::run::{cancel, detached};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (project, number, exec_seq, node_id, task_name, request_id) = match (resource, item.mode) {
        (
            CairnResource::NodeBatch {
                project,
                number,
                exec_seq,
                node_id,
                request_id,
            },
            ChangeMode::Patch,
        ) => (project, *number, *exec_seq, node_id, None, request_id),
        (
            CairnResource::TaskBatch {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                request_id,
            },
            ChangeMode::Patch,
        ) => (
            project,
            *number,
            *exec_seq,
            node_id,
            Some(task_name.as_str()),
            request_id,
        ),
        _ => return Ok(None),
    };
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
    match payload_non_empty_str(payload, "action", &[]) {
        Some("cancel") => {}
        other => {
            return Err(build_failure(
                index,
                item,
                format!(
                    "payload.action must be 'cancel', got {}",
                    other.map_or_else(|| "nothing".to_string(), |action| format!("'{action}'"))
                ),
            ))
        }
    }
    let db = orch.db.for_project(project).await;
    let fail = |error: String| build_failure(index, item, error);
    let addressed = crate::resources::resolve_node_or_task_job_id_for_read(
        &db, project, number, exec_seq, node_id, task_name,
    )
    .await
    .map_err(fail)?;
    let owner = batch_owner(&db, request_id).await.map_err(fail)?;
    let Some(owner) = owner else {
        return Err(fail(not_in_flight(&db, request_id).await));
    };
    if owner != addressed {
        let right = crate::jobs::queries::home_uri_for_job(&db, &owner)
            .await
            .ok()
            .flatten()
            .map(|home| format!(" Address it at `{home}/batches/{request_id}`."))
            .unwrap_or_default();
        return Err(fail(format!(
            "Refused: run batch `{request_id}` belongs to a different job than this address names.{right} Nothing was cancelled."
        )));
    }
    let writer = super::writer(orch, request).await.map_err(fail)?;
    authorize(&db, &writer, &owner, request_id)
        .await
        .map_err(fail)?;
    if dry_run {
        return Ok(Some(format!(
            "Would cancel run batch `{request_id}` on behalf of {}",
            writer.principal()
        )));
    }
    let report = cancel::cancel_batch(
        orch,
        request_id,
        writer.principal(),
        cancel::CANCEL_SETTLE_WAIT,
    )
    .await;
    if !report.reached() {
        return Err(fail(report.sentence(request_id)));
    }
    Ok(Some(report.sentence(request_id)))
}

/// The job a batch runs for: its live control's, its detached row's, or its
/// open run-batch wait's. `None` when no record of it is in flight.
pub(super) async fn batch_owner(db: &LocalDb, request_id: &str) -> Result<Option<String>, String> {
    if let Some(owner) = detached::live_owner(request_id) {
        return Ok(Some(owner.job_id));
    }
    if let Some(batch) = detached::load(db, request_id).await? {
        return Ok(matches!(
            batch.state,
            detached::DetachedState::Parked | detached::DetachedState::Running
        )
        .then_some(batch.job_id));
    }
    db.query_opt_text(
        "SELECT job_id FROM agent_waits
          WHERE state IN ('pending', 'resolving')
            AND json_extract(condition_json, '$.kind') = 'run_batch'
            AND json_extract(condition_json, '$.request_id') = ?1
          LIMIT 1",
        (request_id.to_string(),),
    )
    .await
    .map_err(|error| format!("read run batch `{request_id}`: {error}"))
}

/// Why a batch with no in-flight record cannot be cancelled.
async fn not_in_flight(db: &LocalDb, request_id: &str) -> String {
    match detached::load(db, request_id).await {
        Ok(Some(batch)) => format!(
            "Nothing was cancelled: run batch `{request_id}` already ended ({}).",
            batch
                .verdict
                .as_deref()
                .or(batch.lost_evidence.as_deref())
                .unwrap_or("settled")
        ),
        _ => format!(
            "Nothing was cancelled: no run batch `{request_id}` is in flight. A batch whose call already answered has ended; one an earlier runner placed and nothing records settles from its machine's own report."
        ),
    }
}

/// The operator, the batch's own job, or a live job on the same branch.
async fn authorize(
    db: &LocalDb,
    writer: &Writer,
    owner: &str,
    request_id: &str,
) -> Result<(), String> {
    let Writer::Run { job_id, .. } = writer else {
        return Ok(());
    };
    if job_id == owner
        || crate::orchestrator::base_advance::shares_live_branch(db, job_id, owner).await?
    {
        return Ok(());
    }
    let holder = crate::jobs::queries::home_uri_for_job(db, owner)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| format!("job `{owner}`"));
    Err(format!(
        "Refused: run batch `{request_id}` belongs to {holder}. Only that node, a node sharing its branch, or the operator may cancel it. Nothing was cancelled."
    ))
}
